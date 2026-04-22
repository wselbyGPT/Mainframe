from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from common.auth_provider import IdentityProviderAdapter, LocalIdentityProvider
from common.db import (
    JobTransitionError,
    create_refresh_session,
    cancel_job,
    cleanup_old_jobs,
    create_job,
    ensure_identity,
    get_job,
    get_job_events,
    get_job_events_since,
    get_refresh_session_by_jti,
    get_user_by_username,
    get_spool_sections,
    init_db,
    is_token_revoked,
    list_jobs,
    requeue_job,
    revoke_refresh_session,
    revoke_token,
    rotate_refresh_session,
    retry_job,
    search_spool_sections,
    upsert_user,
)
from common.config import settings
from common.observability import get_logger, parse_iso8601, setup_logging
from common.slo import compute_slo_report, get_objective_report
from common.spool_parser import summarize_sections
from common.template_schemas import (
    TemplateSchemaError,
    UnknownTemplateError,
    get_template_catalog,
    get_template_schema,
    normalize_and_validate_template_params,
)

app = FastAPI(title='TK4 Portal')
setup_logging()
logger = get_logger(__name__)
_STATIC_DIR = Path(__file__).resolve().parent / 'static'
app.mount('/static', StaticFiles(directory=str(_STATIC_DIR)), name='static')

_SSE_POLL_SECONDS = 1.0
_SSE_KEEPALIVE_SECONDS = 15.0
_SSE_MAX_BATCH_SIZE = 100
_SSE_BACKLOG_LIMIT = 200
_TERMINAL_STATES = {'completed', 'failed', 'canceled'}
_CANONICAL_STAGE_ORDER = ['queued', 'connecting', 'logon', 'submit', 'poll', 'capture', 'done', 'failed']
_cleanup_task: asyncio.Task[Any] | None = None
_IDENTITY_PROVIDER: IdentityProviderAdapter = LocalIdentityProvider(settings.auth_default_users)


class CreateJobRequest(BaseModel):
    template_id: str = Field(default='hello-world')
    params: dict[str, Any] = Field(default_factory=dict)


class LoginRequest(BaseModel):
    username: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


@app.on_event('startup')
def startup() -> None:
    init_db()
    _seed_auth_storage()
    _start_cleanup_task()
    logger.info('app.startup')


@app.on_event('shutdown')
async def shutdown() -> None:
    global _cleanup_task
    if _cleanup_task:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
        _cleanup_task = None
    logger.info('app.shutdown')


@app.get('/api/healthz')
def healthz() -> dict[str, str]:
    return {'status': 'ok'}


@app.get('/api/ops/dashboard')
def ops_dashboard() -> dict[str, Any]:
    jobs_payload = list_jobs()
    terminal = [job for job in jobs_payload if job.get('state') in _TERMINAL_STATES]
    failing = [job for job in terminal if job.get('state') in {'failed', 'canceled'}]
    total = len(jobs_payload)
    stage_stats = _aggregate_stage_metrics(jobs_payload)
    return {
        'health': {
            'status': 'degraded' if failing else 'ok',
            'total_jobs': total,
            'terminal_jobs': len(terminal),
            'failed_jobs': len(failing),
            'failure_rate': round((len(failing) / total), 4) if total else 0.0,
        },
        'stage_metrics': stage_stats,
        'generated_at': _iso_now(),
    }




@app.get('/api/ops/slo')
def ops_slo() -> dict[str, Any]:
    jobs_payload = list_jobs()
    events_by_job_id = {job['id']: get_job_events(job['id']) for job in jobs_payload}
    return compute_slo_report(jobs=jobs_payload, events_by_job_id=events_by_job_id)


@app.get('/api/ops/slo/{objective_id}')
def ops_slo_objective(objective_id: str) -> dict[str, Any]:
    jobs_payload = list_jobs()
    events_by_job_id = {job['id']: get_job_events(job['id']) for job in jobs_payload}
    objective = get_objective_report(objective_id, jobs=jobs_payload, events_by_job_id=events_by_job_id)
    if objective is None:
        raise HTTPException(status_code=404, detail={'code': 'slo_objective_not_found', 'message': 'SLO objective not found'})
    return objective

@app.get('/api/ops/metrics')
def ops_metrics() -> PlainTextResponse:
    jobs_payload = list_jobs()
    terminal = [job for job in jobs_payload if job.get('state') in _TERMINAL_STATES]
    failing = [job for job in terminal if job.get('state') in {'failed', 'canceled'}]

    spool_type_totals: dict[str, dict[str, int]] = {}
    spool_rc_nonzero_total = 0
    spool_abend_total = 0
    for job in jobs_payload:
        sections = get_spool_sections(job['id'])
        if not sections:
            continue
        summary = summarize_sections(sections)
        spool_rc_nonzero_total += int(summary.get('nonzero_rc_sections', 0))
        spool_abend_total += int(summary.get('abend_sections', 0))
        for section_type, item in (summary.get('section_types') or {}).items():
            current = spool_type_totals.setdefault(str(section_type), {'sections': 0, 'lines': 0})
            current['sections'] += int(item.get('sections', 0))
            current['lines'] += int(item.get('lines', 0))

    slo_report = compute_slo_report(
        jobs=jobs_payload,
        events_by_job_id={job['id']: get_job_events(job['id']) for job in jobs_payload},
    )
    body = _render_metrics(
        jobs_total=len(jobs_payload),
        jobs_terminal_total=len(terminal),
        jobs_failed_total=len(failing),
        spool_type_totals=spool_type_totals,
        spool_rc_nonzero_total=spool_rc_nonzero_total,
        spool_abend_total=spool_abend_total,
        slo_report=slo_report,
    )
    return PlainTextResponse(content=body, media_type='text/plain; version=0.0.4')


@app.get('/')
def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / 'index.html')


@app.get('/api/templates')
def templates_catalog(
    include_pack_metadata: bool = Query(default=False),
    grouped: bool = Query(default=False),
) -> dict[str, Any]:
    payload: dict[str, Any] = {'templates': get_template_catalog(include_pack_metadata=include_pack_metadata)}
    if grouped:
        payload['packs'] = get_template_catalog(include_pack_metadata=include_pack_metadata, grouped=True)
    return payload


@app.get('/api/templates/{template_id}')
def template_details(template_id: str, include_pack_metadata: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return get_template_schema(template_id, include_pack_metadata=include_pack_metadata)
    except UnknownTemplateError as exc:
        raise HTTPException(status_code=404, detail=exc.to_dict()) from exc


@app.post('/api/login')
def login_route(request: LoginRequest) -> dict[str, str]:
    principal = _IDENTITY_PROVIDER.authenticate(request.username, request.password)
    if not principal:
        raise HTTPException(status_code=401, detail={'code': 'invalid_credentials', 'message': 'Invalid credentials'})
    user = get_user_by_username(principal.subject)
    if not user:
        raise HTTPException(status_code=500, detail={'code': 'auth_user_missing', 'message': 'Auth user not initialized'})
    issued_at = _utcnow_dt()
    access_token = _mint_access_token(user, issued_at=issued_at)
    refresh_token = _mint_refresh_token(user, issued_at=issued_at)
    return {
        'access_token': access_token,
        'refresh_token': refresh_token,
        'token_type': 'Bearer',
        'username': principal.subject,
        'role': principal.role,
    }


@app.post('/api/refresh')
def refresh_route(request: RefreshRequest) -> dict[str, str]:
    payload = _decode_and_validate_jwt(request.refresh_token, expected_token_type='refresh')
    session = get_refresh_session_by_jti(str(payload['jti']))
    if not session or session.get('revoked_at'):
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Refresh session revoked'})

    expires_at = _parse_iso_datetime(str(session['expires_at']))
    if expires_at <= _utcnow_dt():
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Refresh session expired'})

    user = get_user_by_username(str(payload['sub']))
    if not user:
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Refresh user not found'})

    now = _utcnow_dt()
    next_refresh_jti = str(uuid.uuid4())
    revoke_token(
        jti=str(payload['jti']),
        token_type='refresh',
        user_id=str(user['id']),
        session_id=str(session['id']),
        expires_at=str(payload['exp_iso']),
        reason='refresh_rotation',
    )
    rotate_refresh_session(session_id=str(session['id']), next_refresh_jti=next_refresh_jti, last_seen=now.isoformat())

    access_token = _mint_access_token(user, issued_at=now, session_id=str(session['id']))
    refresh_token = _mint_refresh_token(
        user,
        issued_at=now,
        session_id=str(session['id']),
        refresh_jti=next_refresh_jti,
        persist_session=False,
    )
    return {
        'access_token': access_token,
        'refresh_token': refresh_token,
        'token_type': 'Bearer',
        'username': str(user['username']),
        'role': str(user['role']),
    }


@app.post('/api/logout')
def logout_route(request: RefreshRequest, authorization: str | None = Header(default=None)) -> dict[str, str]:
    principal = _require_auth_principal(authorization)
    access_payload = _decode_and_validate_jwt(principal['token'], expected_token_type='access')
    refresh_payload = _decode_and_validate_jwt(request.refresh_token, expected_token_type='refresh')
    if access_payload['sub'] != refresh_payload['sub']:
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Token subject mismatch'})

    session = get_refresh_session_by_jti(str(refresh_payload['jti']))
    if session:
        revoke_refresh_session(str(session['id']), revoked_at=_utcnow_dt().isoformat())
    revoke_token(
        jti=str(access_payload['jti']),
        token_type='access',
        user_id=str(principal['user_id']),
        session_id=str(session['id']) if session else None,
        expires_at=str(access_payload['exp_iso']),
        reason='logout',
    )
    revoke_token(
        jti=str(refresh_payload['jti']),
        token_type='refresh',
        user_id=str(principal['user_id']),
        session_id=str(session['id']) if session else None,
        expires_at=str(refresh_payload['exp_iso']),
        reason='logout',
    )
    return {'status': 'revoked'}


@app.get('/api/jobs')
def jobs() -> list[dict[str, Any]]:
    return [_build_job_payload(job) for job in list_jobs()]


@app.post('/api/jobs')
def create_job_route(request: CreateJobRequest, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    principal = _require_auth_principal(authorization)
    try:
        normalized_params = normalize_and_validate_template_params(request.template_id, request.params)
    except TemplateSchemaError as exc:
        raise HTTPException(status_code=422, detail=exc.to_dict()) from exc
    return _build_job_payload(create_job(request.template_id, principal['username'], normalized_params))


@app.get('/api/jobs/{job_id}')
def get_job_route(job_id: str) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    return _build_job_payload(job)


@app.get('/api/jobs/{job_id}/spool')
def get_spool_route(
    job_id: str,
    query: str | None = Query(default=None),
    section_type: str | None = Query(default=None),
) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    sections = search_spool_sections(job_id, query=query, section_type=section_type)
    return {'job_id': job_id, 'query': query, 'section_type': section_type, 'sections': sections}


@app.get('/api/jobs/{job_id}/spool/{section_type}')
def get_spool_section_route(job_id: str, section_type: str, query: str | None = Query(default=None)) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    sections = search_spool_sections(job_id, query=query, section_type=section_type)
    if not sections:
        raise HTTPException(status_code=404, detail='Spool section not found')
    return {
        'job_id': job_id,
        'section_type': section_type,
        'query': query,
        'content': '\n\n'.join(s['content_text'] for s in sections),
    }


@app.get('/api/jobs/{job_id}/spool/text')
def get_spool_as_text_route(
    job_id: str,
    section_type: str | None = Query(default=None),
    query: str | None = Query(default=None),
) -> PlainTextResponse:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    sections = search_spool_sections(job_id, query=query, section_type=section_type)
    if not sections:
        raise HTTPException(status_code=404, detail='Spool content not found')
    text = '\n\n'.join(
        f"===== {item['section_type'].upper()} #{item['ordinal']} =====\n{item['content_text']}" for item in sections
    )
    filename_suffix = section_type.strip().lower() if section_type else 'all'
    headers = {'Content-Disposition': f'attachment; filename="{job_id}-{filename_suffix}.txt"'}
    return PlainTextResponse(content=text, headers=headers)


def _build_job_payload(job: dict[str, Any]) -> dict[str, Any]:
    payload = dict(job)
    events = get_job_events(job['id'])
    normalized_params = json.loads(payload.get('input_params_json') or '{}')
    stage_timeline = _build_stage_timeline(payload, events)
    payload['events'] = events
    payload['normalized_params'] = normalized_params
    payload['timestamps'] = {
        'created_at': payload.get('created_at'),
        'started_at': payload.get('started_at'),
        'finished_at': payload.get('finished_at'),
        'updated_at': payload.get('updated_at'),
    }
    payload['stage_model'] = {
        'current': _canonical_stage(payload.get('stage'), payload.get('state')),
        'ordered_stages': list(_CANONICAL_STAGE_ORDER),
        'timeline': stage_timeline,
        'durations_ms': _build_stage_durations(stage_timeline, payload),
    }
    payload['artifact_links'] = _build_artifact_links(payload)
    payload['event_summary'] = {
        'count': len(events),
        'last_event': events[-1]['event_type'] if events else None,
    }
    payload['attempt_info'] = {
        'attempt': payload.get('attempt', 1),
        'parent_job_id': payload.get('parent_job_id'),
        'retry_of_job_id': payload.get('retry_of_job_id'),
    }
    return payload


def _canonical_stage(stage: str | None, state: str | None) -> str:
    stage_value = (stage or '').strip().lower()
    state_value = (state or '').strip().lower()
    if stage_value in {'', 'none'}:
        if state_value in {'queued'}:
            return 'queued'
        if state_value in {'starting', 'submitted', 'running'}:
            return 'connecting'
        if state_value in {'completed'}:
            return 'done'
        if state_value in {'failed', 'canceled'}:
            return 'failed'
        return 'queued'

    mapping = {
        'queued': 'queued',
        'starting': 'connecting',
        'logging_in': 'logon',
        'cleanup_dataset': 'submit',
        'allocate_dataset': 'submit',
        'writing_jcl': 'submit',
        'submitting': 'submit',
        'waiting_for_completion': 'poll',
        'reading_spool': 'capture',
        'done': 'done',
        'unexpected': 'failed',
        'canceled': 'failed',
    }
    return mapping.get(stage_value, 'failed')


def _build_stage_timeline(job: dict[str, Any], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stage_first_seen: dict[str, str] = {'queued': job.get('created_at') or ''}
    for item in events:
        payload = item.get('payload') or {}
        event_state = payload.get('state')
        event_stage = payload.get('stage')
        stage_name = _canonical_stage(event_stage, event_state)
        if stage_name not in stage_first_seen:
            stage_first_seen[stage_name] = item.get('ts') or ''

    current = _canonical_stage(job.get('stage'), job.get('state'))
    if current in {'failed'} and stage_first_seen.get('failed') in {None, ''}:
        stage_first_seen['failed'] = job.get('finished_at') or job.get('updated_at') or ''
    if current in {'done'} and stage_first_seen.get('done') in {None, ''}:
        stage_first_seen['done'] = job.get('finished_at') or job.get('updated_at') or ''

    out: list[dict[str, Any]] = []
    for stage_name in _CANONICAL_STAGE_ORDER:
        if stage_name in stage_first_seen and stage_first_seen[stage_name]:
            out.append({'stage': stage_name, 'first_seen_at': stage_first_seen[stage_name]})
    return out


def _iso_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _build_stage_durations(timeline: list[dict[str, Any]], job: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for index, item in enumerate(timeline):
        start_at = item.get('first_seen_at')
        if index + 1 < len(timeline):
            end_at = timeline[index + 1].get('first_seen_at')
        else:
            end_at = job.get('finished_at') or job.get('updated_at')
        duration = _duration_ms(start_at, end_at)
        out.append({'stage': item.get('stage'), 'duration_ms': duration, 'start_at': start_at, 'end_at': end_at})
    return out


def _duration_ms(start_at: str | None, end_at: str | None) -> int | None:
    start_dt = parse_iso8601(start_at)
    end_dt = parse_iso8601(end_at)
    if not start_dt or not end_dt:
        return None
    value = int((end_dt - start_dt).total_seconds() * 1000)
    return value if value >= 0 else None


def _aggregate_stage_metrics(jobs_payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregate: dict[str, list[int]] = {}
    for job in jobs_payload:
        payload = _build_job_payload(job)
        for item in payload.get('stage_model', {}).get('durations_ms', []):
            stage = item.get('stage')
            duration = item.get('duration_ms')
            if not stage or duration is None:
                continue
            aggregate.setdefault(stage, []).append(int(duration))

    out: list[dict[str, Any]] = []
    for stage in _CANONICAL_STAGE_ORDER:
        durations = aggregate.get(stage, [])
        if not durations:
            continue
        durations_sorted = sorted(durations)
        p95_index = max(0, int(len(durations_sorted) * 0.95) - 1)
        out.append(
            {
                'stage': stage,
                'samples': len(durations_sorted),
                'avg_ms': int(sum(durations_sorted) / len(durations_sorted)),
                'p95_ms': durations_sorted[p95_index],
                'max_ms': durations_sorted[-1],
            }
        )
    return out


def _build_artifact_links(job: dict[str, Any]) -> dict[str, Any]:
    job_id = job['id']
    return {
        'spool': f'/api/jobs/{job_id}/spool',
        'spool_text': f'/api/jobs/{job_id}/spool/text',
        'spool_sections': {
            'jes': f'/api/jobs/{job_id}/spool/jes',
            'jcl': f'/api/jobs/{job_id}/spool/jcl',
            'sysout': f'/api/jobs/{job_id}/spool/sysout',
        },
    }


def _render_metrics(
    jobs_total: int,
    jobs_terminal_total: int,
    jobs_failed_total: int,
    spool_type_totals: dict[str, dict[str, int]],
    spool_rc_nonzero_total: int,
    spool_abend_total: int,
    slo_report: dict[str, Any],
) -> str:
    lines = [
        '# HELP tk4_jobs_total Total jobs persisted in the portal database.',
        '# TYPE tk4_jobs_total gauge',
        f'tk4_jobs_total {jobs_total}',
        '# HELP tk4_jobs_terminal_total Total jobs currently in terminal states.',
        '# TYPE tk4_jobs_terminal_total gauge',
        f'tk4_jobs_terminal_total {jobs_terminal_total}',
        '# HELP tk4_jobs_failed_total Total terminal jobs in failed or canceled state.',
        '# TYPE tk4_jobs_failed_total gauge',
        f'tk4_jobs_failed_total {jobs_failed_total}',
        '# HELP tk4_spool_nonzero_rc_sections_total Spool sections containing RC values other than 0000.',
        '# TYPE tk4_spool_nonzero_rc_sections_total counter',
        f'tk4_spool_nonzero_rc_sections_total {spool_rc_nonzero_total}',
        '# HELP tk4_spool_abend_sections_total Spool sections containing ABEND signatures.',
        '# TYPE tk4_spool_abend_sections_total counter',
        f'tk4_spool_abend_sections_total {spool_abend_total}',
        '# HELP tk4_spool_sections_total Spool sections partitioned by section type.',
        '# TYPE tk4_spool_sections_total gauge',
    ]
    for section_type in sorted(spool_type_totals):
        metrics = spool_type_totals[section_type]
        lines.append(f'tk4_spool_sections_total{{section_type="{section_type}"}} {metrics["sections"]}')
    lines.extend(
        [
            '# HELP tk4_spool_lines_total Total lines captured in spool sections by section type.',
            '# TYPE tk4_spool_lines_total gauge',
        ]
    )
    for section_type in sorted(spool_type_totals):
        metrics = spool_type_totals[section_type]
        lines.append(f'tk4_spool_lines_total{{section_type="{section_type}"}} {metrics["lines"]}')

    lines.extend(
        [
            '# HELP tk4_slo_objective_status SLO objective status encoded as met=2, at-risk=1, breached=0.',
            '# TYPE tk4_slo_objective_status gauge',
            '# HELP tk4_slo_error_budget_remaining_ratio Remaining error budget ratio for the primary window.',
            '# TYPE tk4_slo_error_budget_remaining_ratio gauge',
            '# HELP tk4_slo_burn_rate Burn-rate in short and long windows.',
            '# TYPE tk4_slo_burn_rate gauge',
            '# HELP tk4_slo_sli_ratio Rolling SLI ratio per objective and window.',
            '# TYPE tk4_slo_sli_ratio gauge',
            '# HELP tk4_slo_samples_total Rolling sample count per objective and window.',
            '# TYPE tk4_slo_samples_total counter',
        ]
    )
    status_map = {'met': 2, 'at-risk': 1, 'breached': 0}
    for objective in slo_report.get('objectives', []):
        objective_id = objective.get('objective_id', 'unknown')
        status = objective.get('status', 'breached')
        lines.append(f'tk4_slo_objective_status{{objective_id="{objective_id}",status="{status}"}} {status_map.get(status, 0)}')
        remaining_ratio = objective.get('error_budget', {}).get('remaining_ratio')
        if remaining_ratio is not None:
            lines.append(f'tk4_slo_error_budget_remaining_ratio{{objective_id="{objective_id}"}} {remaining_ratio}')
        short_burn = objective.get('burn_rate', {}).get('short')
        if short_burn is not None:
            lines.append(f'tk4_slo_burn_rate{{objective_id="{objective_id}",window="short"}} {short_burn}')
        long_burn = objective.get('burn_rate', {}).get('long')
        if long_burn is not None:
            lines.append(f'tk4_slo_burn_rate{{objective_id="{objective_id}",window="long"}} {long_burn}')
        for window_name, window_metrics in (objective.get('windows') or {}).items():
            sli = window_metrics.get('sli')
            if sli is not None:
                lines.append(
                    f'tk4_slo_sli_ratio{{objective_id="{objective_id}",window="{window_name}"}} {sli}'
                )
            lines.append(
                f'tk4_slo_samples_total{{objective_id="{objective_id}",window="{window_name}"}} {int(window_metrics.get("total", 0))}'
            )
    return '\n'.join(lines) + '\n'


def _seed_auth_storage() -> None:
    for username, data in settings.auth_default_users.items():
        password_hash = hashlib.sha256(str(data['password']).encode('utf-8')).hexdigest()
        role = str(data['role'])
        upsert_user(username, password_hash=password_hash, role=role)
        user = get_user_by_username(username)
        if user:
            ensure_identity(user_id=str(user['id']), provider='local', subject=username)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def _b64url_decode(value: str) -> bytes:
    padding = '=' * (-len(value) % 4)
    return base64.urlsafe_b64decode(f'{value}{padding}')


def _sign_jwt(claims: dict[str, Any]) -> str:
    header = {'alg': 'HS256', 'typ': 'JWT'}
    header_b64 = _b64url_encode(json.dumps(header, separators=(',', ':')).encode('utf-8'))
    payload_b64 = _b64url_encode(json.dumps(claims, separators=(',', ':')).encode('utf-8'))
    signing_input = f'{header_b64}.{payload_b64}'.encode('ascii')
    signature = hmac.new(settings.auth_secret_key.encode('utf-8'), signing_input, hashlib.sha256).digest()
    return f'{header_b64}.{payload_b64}.{_b64url_encode(signature)}'


def _decode_and_validate_jwt(token: str, expected_token_type: str) -> dict[str, Any]:
    try:
        header_b64, payload_b64, signature_b64 = token.split('.')
    except ValueError as exc:
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Malformed token'}) from exc
    signing_input = f'{header_b64}.{payload_b64}'.encode('ascii')
    expected_sig = hmac.new(settings.auth_secret_key.encode('utf-8'), signing_input, hashlib.sha256).digest()
    actual_sig = _b64url_decode(signature_b64)
    if not hmac.compare_digest(expected_sig, actual_sig):
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Invalid token signature'})
    claims = json.loads(_b64url_decode(payload_b64))
    now_ts = int(_utcnow_dt().timestamp())
    if claims.get('iss') != settings.auth_issuer or claims.get('aud') != settings.auth_audience:
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Invalid token audience'})
    if claims.get('typ') != expected_token_type:
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Invalid token type'})
    if int(claims.get('exp', 0)) <= now_ts:
        raise HTTPException(status_code=401, detail={'code': 'auth_expired', 'message': 'Token expired'})
    if is_token_revoked(str(claims.get('jti'))):
        raise HTTPException(status_code=401, detail={'code': 'auth_revoked', 'message': 'Token revoked'})
    claims['exp_iso'] = datetime.fromtimestamp(int(claims['exp']), tz=timezone.utc).isoformat()
    return claims


def _mint_access_token(user: dict[str, Any], issued_at: datetime, session_id: str | None = None) -> str:
    iat = int(issued_at.timestamp())
    exp = int((issued_at + timedelta(seconds=settings.auth_access_token_ttl_seconds)).timestamp())
    claims = {
        'iss': settings.auth_issuer,
        'aud': settings.auth_audience,
        'sub': str(user['username']),
        'uid': str(user['id']),
        'role': str(user['role']),
        'typ': 'access',
        'iat': iat,
        'exp': exp,
        'jti': str(uuid.uuid4()),
        'sid': session_id,
    }
    return _sign_jwt(claims)


def _mint_refresh_token(
    user: dict[str, Any],
    issued_at: datetime,
    session_id: str | None = None,
    refresh_jti: str | None = None,
    persist_session: bool = True,
) -> str:
    iat = int(issued_at.timestamp())
    exp_dt = issued_at + timedelta(seconds=settings.auth_refresh_token_ttl_seconds)
    jti = refresh_jti or str(uuid.uuid4())
    if persist_session:
        session = create_refresh_session(
            user_id=str(user['id']),
            refresh_jti=jti,
            issued_at=issued_at.isoformat(),
            expires_at=exp_dt.isoformat(),
        )
        session_id = str(session['id'])
    claims = {
        'iss': settings.auth_issuer,
        'aud': settings.auth_audience,
        'sub': str(user['username']),
        'uid': str(user['id']),
        'role': str(user['role']),
        'typ': 'refresh',
        'iat': iat,
        'exp': int(exp_dt.timestamp()),
        'jti': jti,
        'sid': session_id,
    }
    return _sign_jwt(claims)


def _utcnow_dt() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _require_auth_principal(authorization: str | None) -> dict[str, str]:
    if not authorization:
        raise HTTPException(status_code=401, detail={'code': 'auth_required', 'message': 'Authorization required'})
    scheme, _, token = authorization.partition(' ')
    if scheme.lower() != 'bearer' or not token:
        raise HTTPException(status_code=401, detail={'code': 'auth_invalid', 'message': 'Invalid authorization header'})
    claims = _decode_and_validate_jwt(token, expected_token_type='access')
    return {
        'username': str(claims['sub']),
        'role': str(claims.get('role', 'submitter')),
        'user_id': str(claims.get('uid', '')),
        'token': token,
    }


def _require_job_ownership(job_id: str, principal: dict[str, str]) -> None:
    if principal['role'] == 'admin':
        return
    job = get_job(job_id)
    if not job:
        return
    if job.get('submitted_by') != principal['username']:
        raise HTTPException(
            status_code=403,
            detail={
                'code': 'forbidden',
                'message': f"User '{principal['username']}' cannot manage job owned by '{job.get('submitted_by')}'",
            },
        )


def _apply_transition(job_id: str, transition_fn: Any, authorization: str | None) -> dict[str, Any]:
    principal = _require_auth_principal(authorization)
    _require_job_ownership(job_id, principal)
    try:
        job = transition_fn(job_id)
    except JobTransitionError as exc:
        raise HTTPException(status_code=409, detail=exc.to_dict()) from exc
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    return _build_job_payload(job)


@app.post('/api/jobs/{job_id}/cancel')
def cancel_job_route(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    return _apply_transition(job_id, cancel_job, authorization)


@app.post('/api/jobs/{job_id}/retry')
def retry_job_route(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    return _apply_transition(job_id, retry_job, authorization)


@app.post('/api/jobs/{job_id}/requeue')
def requeue_job_route(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    return _apply_transition(job_id, requeue_job, authorization)


def _to_sse_frame(event_id: int, event_type: str, payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, separators=(',', ':'))
    return f'id: {event_id}\nevent: {event_type}\ndata: {payload_json}\n\n'


@app.get('/api/jobs/{job_id}/events')
@app.get('/api/jobs/{job_id}/events/stream')
async def stream_job_events_route(
    job_id: str,
    request: Request,
    last_event_id: str | None = Header(default=None, alias='Last-Event-ID'),
    linger_seconds: float = Query(default=1.0, ge=0.0, le=30.0),
) -> StreamingResponse:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})

    try:
        cursor = int(last_event_id) if last_event_id else 0
    except ValueError as exc:
        raise HTTPException(status_code=400, detail='Invalid Last-Event-ID header') from exc

    async def _event_stream() -> Any:
        last_ping_at = asyncio.get_event_loop().time()
        terminal_seen_at: float | None = None
        latest_job_state = job.get('state', '')
        terminal_seen = latest_job_state in _TERMINAL_STATES

        while True:
            if await request.is_disconnected():
                return

            events = get_job_events_since(job_id, cursor, limit=_SSE_MAX_BATCH_SIZE)
            if cursor == 0 and len(events) > _SSE_BACKLOG_LIMIT:
                events = events[-_SSE_BACKLOG_LIMIT:]

            if events:
                for item in events:
                    cursor = item['id']
                    latest_job_state = item['payload'].get('state', latest_job_state)
                    if latest_job_state in _TERMINAL_STATES:
                        terminal_seen = True
                        terminal_seen_at = asyncio.get_event_loop().time()
                    yield _to_sse_frame(item['id'], item['event_type'], item['payload'])
                continue

            if not terminal_seen:
                refreshed = get_job(job_id)
                latest_job_state = (refreshed or {}).get('state', latest_job_state)
                if latest_job_state in _TERMINAL_STATES:
                    terminal_seen = True
                    terminal_seen_at = asyncio.get_event_loop().time()
            elif terminal_seen_at is not None and asyncio.get_event_loop().time() - terminal_seen_at >= linger_seconds:
                return

            now = asyncio.get_event_loop().time()
            if now - last_ping_at >= _SSE_KEEPALIVE_SECONDS:
                last_ping_at = now
                yield ': ping\n\n'

            await asyncio.sleep(_SSE_POLL_SECONDS)

    headers = {
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
    }
    return StreamingResponse(_event_stream(), media_type='text/event-stream', headers=headers)


def _start_cleanup_task() -> None:
    global _cleanup_task
    if settings.cleanup_interval_seconds <= 0:
        return
    loop = asyncio.get_event_loop()
    _cleanup_task = loop.create_task(_cleanup_loop())


async def _cleanup_loop() -> None:
    while True:
        await asyncio.sleep(settings.cleanup_interval_seconds)
        cleanup_old_jobs(settings.spool_retention_days, limit=settings.cleanup_batch_size)
