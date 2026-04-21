from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from common.db import (
    JobTransitionError,
    cancel_job,
    create_job,
    get_job,
    get_job_events,
    get_job_events_since,
    get_spool_sections,
    init_db,
    list_jobs,
    requeue_job,
    retry_job,
)
from common.template_schemas import (
    TemplateSchemaError,
    UnknownTemplateError,
    get_template_catalog,
    get_template_schema,
    normalize_and_validate_template_params,
)
from common.templates import get_template_provenance
from common.config import settings

app = FastAPI(title='TK4 Portal')

_SSE_POLL_SECONDS = 1.0
_SSE_KEEPALIVE_SECONDS = 15.0
_SSE_MAX_BATCH_SIZE = 100
_SSE_BACKLOG_LIMIT = 200
_TERMINAL_STATES = {'completed', 'failed', 'canceled'}
_CANONICAL_STAGE_ORDER = ['queued', 'connecting', 'logon', 'submit', 'poll', 'capture', 'done', 'failed']


class CreateJobRequest(BaseModel):
    template_id: str = Field(default='hello-world')
    submitted_by: str = Field(default='anonymous')
    params: dict[str, Any] = Field(default_factory=dict)


@app.on_event('startup')
def startup() -> None:
    init_db()


@app.get('/api/healthz')
def healthz() -> dict[str, str]:
    return {'status': 'ok'}


@app.get('/api/templates')
def templates_catalog() -> dict[str, Any]:
    return {'templates': get_template_catalog()}


@app.get('/api/templates/{template_id}')
def template_details(template_id: str) -> dict[str, Any]:
    try:
        return get_template_schema(template_id)
    except UnknownTemplateError as exc:
        raise HTTPException(status_code=404, detail=exc.to_dict()) from exc


@app.get('/api/jobs')
def jobs() -> list[dict[str, Any]]:
    return [_build_job_payload(job) for job in list_jobs()]


@app.post('/api/jobs')
def create_job_route(request: CreateJobRequest) -> dict[str, Any]:
    try:
        normalized_params = normalize_and_validate_template_params(request.template_id, request.params)
        template_provenance = get_template_provenance(request.template_id)
    except TemplateSchemaError as exc:
        raise HTTPException(status_code=422, detail=exc.to_dict()) from exc
    return _build_job_payload(
        create_job(
            request.template_id,
            request.submitted_by,
            normalized_params,
            template_version=template_provenance['template_version'],
            template_hash=template_provenance['template_hash'],
            target_host=settings.tk4_host,
            target_port=settings.tk4_port,
        )
    )


@app.get('/api/jobs/{job_id}')
def get_job_route(job_id: str) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    return _build_job_payload(job)


@app.get('/api/jobs/{job_id}/spool')
def get_spool_route(job_id: str) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    return {'job_id': job_id, 'sections': get_spool_sections(job_id)}


@app.get('/api/jobs/{job_id}/spool/{section_type}')
def get_spool_section_route(job_id: str, section_type: str) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    sections = [s for s in get_spool_sections(job_id) if s['section_type'] == section_type]
    if not sections:
        raise HTTPException(status_code=404, detail='Spool section not found')
    return {'job_id': job_id, 'section_type': section_type, 'content': '\n\n'.join(s['content_text'] for s in sections)}


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


def _build_artifact_links(job: dict[str, Any]) -> dict[str, Any]:
    job_id = job['id']
    return {
        'spool': f'/api/jobs/{job_id}/spool',
        'spool_sections': {
            'jes': f'/api/jobs/{job_id}/spool/jes',
            'jcl': f'/api/jobs/{job_id}/spool/jcl',
            'sysout': f'/api/jobs/{job_id}/spool/sysout',
        },
    }


def _apply_transition(job_id: str, transition_fn: Any) -> dict[str, Any]:
    try:
        job = transition_fn(job_id)
    except JobTransitionError as exc:
        raise HTTPException(status_code=409, detail=exc.to_dict()) from exc
    if not job:
        raise HTTPException(status_code=404, detail={'code': 'job_not_found', 'message': 'Job not found'})
    return _build_job_payload(job)


@app.post('/api/jobs/{job_id}/cancel')
def cancel_job_route(job_id: str) -> dict[str, Any]:
    return _apply_transition(job_id, cancel_job)


@app.post('/api/jobs/{job_id}/retry')
def retry_job_route(job_id: str) -> dict[str, Any]:
    return _apply_transition(job_id, retry_job)


@app.post('/api/jobs/{job_id}/requeue')
def requeue_job_route(job_id: str) -> dict[str, Any]:
    return _apply_transition(job_id, requeue_job)


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
