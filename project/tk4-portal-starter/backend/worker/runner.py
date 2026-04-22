from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Callable

from common.config import settings
from common.db import add_event, get_job, replace_spool_sections, update_job
from common.observability import get_logger, setup_logging, stage_duration_ms
from common.spool_parser import split_spool
from common.templates import render_template
from worker.s3270_client import S3270Client, S3270Error
from worker.profiles import get_profile
from worker.profiles.base import WorkerProfile
from worker.screen_recognizers import (
    extract_abend,
    extract_job_id,
    extract_return_code,
    has_dataset_error,
    has_login_error,
    in_input_mode,
    is_ready,
    looks_like_tso_screen,
    looks_done,
    looks_jcl_error,
    normalize,
    wants_applid,
    wants_password,
    wants_userid,
)


@dataclass
class AutomationFailure(RuntimeError):
    stage: str
    detail: str
    screen: str = ''
    code: str = 'automation_error'
    retryable: bool = False
    remediation: tuple[str, ...] = ()

    def __str__(self) -> str:
        base = f'[{self.stage}] {self.detail}'
        if self.screen:
            return base + '\n\nScreen:\n' + self.screen
        return base


@dataclass
class JobAborted(RuntimeError):
    stage: str
    reason: str


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int
    delay_seconds: float


_STAGE_RETRY_POLICIES: dict[str, RetryPolicy] = {
    'logging_in': RetryPolicy(attempts=2, delay_seconds=1.0),
    'cleanup_dataset': RetryPolicy(attempts=2, delay_seconds=0.5),
    'allocate_dataset': RetryPolicy(attempts=2, delay_seconds=0.75),
    'writing_jcl': RetryPolicy(attempts=2, delay_seconds=0.75),
    'submitting': RetryPolicy(attempts=3, delay_seconds=0.75),
    'waiting_for_completion': RetryPolicy(attempts=2, delay_seconds=1.0),
    'reading_spool': RetryPolicy(attempts=2, delay_seconds=0.75),
}
setup_logging()
logger = get_logger(__name__)


def _payload_with_profile(payload: dict, profile: WorkerProfile) -> dict:
    return {**payload, 'profile': profile.name}


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _guard_active_attempt(job_id: str, attempt: int, stage: str) -> dict:
    fresh = get_job(job_id)
    if not fresh:
        raise JobAborted(stage=stage, reason='job_not_found')
    if int(fresh.get('attempt') or 1) != int(attempt):
        raise JobAborted(stage=stage, reason='attempt_superseded')
    if fresh.get('state') == 'canceled':
        raise JobAborted(stage=stage, reason='job_canceled')
    return fresh


def run_job(job: dict) -> None:
    job_id = job['id']
    attempt = int(job.get('attempt') or 1)
    params = json.loads(job['input_params_json'])
    job_name = params['job_name']
    profile = get_profile(settings.worker_adapter_profile)
    _guard_active_attempt(job_id, attempt, 'starting')
    update_job(job_id, job_name=job_name)
    add_event(
        job_id,
        'job.state',
        _payload_with_profile({'state': 'starting', 'attempt': attempt}, profile),
        attempt=attempt,
    )
    logger.info('job.start', job_id=job_id, attempt=attempt, context={'job_name': job_name})

    try:
        if settings.dry_run:
            _run_dry(job_id, attempt, job_name, params, profile)
            return

        _guard_active_attempt(job_id, attempt, 'writing_jcl')
        jcl = render_template(job['template_id'], params)
        result = _run_real(job_id, attempt, job_name, jcl, profile)
        _guard_active_attempt(job_id, attempt, 'reading_spool')
        sections = split_spool(result['raw_spool'])
        replace_spool_sections(job_id, sections, attempt=attempt)
        update_job(
            job_id,
            state='completed' if result['result'] in {'success', 'warning'} else 'failed',
            result=result['result'],
            mainframe_job_id=result.get('mainframe_job_id'),
            return_code=result.get('return_code'),
            abend_code=result.get('abend_code'),
            stage='done',
            finished_at=_utcnow(),
        )
        add_event(job_id, 'job.result', _payload_with_profile({**result, 'attempt': attempt}, profile), attempt=attempt)
        logger.info('job.result', job_id=job_id, attempt=attempt, context={'result': result['result']})
    except JobAborted as exc:
        if exc.reason == 'job_canceled':
            update_job(
                job_id,
                state='canceled',
                result='canceled',
                stage='canceled',
                finished_at=_utcnow(),
            )
            add_event(
                job_id,
                'job.canceled',
                _payload_with_profile({'stage': exc.stage, 'attempt': attempt}, profile),
                attempt=attempt,
            )
            logger.warning('job.canceled', job_id=job_id, stage=exc.stage, attempt=attempt)
        else:
            add_event(
                job_id,
                'job.superseded',
                _payload_with_profile({'stage': exc.stage, 'attempt': attempt, 'reason': exc.reason}, profile),
                attempt=attempt,
            )
            logger.warning('job.superseded', job_id=job_id, stage=exc.stage, attempt=attempt, context={'reason': exc.reason})
    except AutomationFailure as exc:
        _guard_active_attempt(job_id, attempt, exc.stage)
        if exc.screen:
            replace_spool_sections(job_id, split_spool(exc.screen), attempt=attempt)
        failure_payload = _build_failure_payload(exc, profile)
        update_job(
            job_id,
            state='failed',
            result='error',
            error_text=json.dumps(failure_payload, separators=(',', ':')),
            stage=exc.stage,
            finished_at=_utcnow(),
        )
        add_event(
            job_id,
            'job.error',
            {**failure_payload, 'attempt': attempt},
            attempt=attempt,
        )
        logger.error('job.error', job_id=job_id, stage=exc.stage, attempt=attempt, context=failure_payload)
    except Exception as exc:
        _guard_active_attempt(job_id, attempt, 'unexpected')
        update_job(
            job_id,
            state='failed',
            result='error',
            error_text=str(exc),
            stage='unexpected',
            finished_at=_utcnow(),
        )
        add_event(
            job_id,
            'job.error',
            _payload_with_profile({'stage': 'unexpected', 'detail': str(exc), 'attempt': attempt}, profile),
            attempt=attempt,
        )
        logger.error('job.error', job_id=job_id, stage='unexpected', attempt=attempt, context={'detail': str(exc)})


def _run_dry(job_id: str, attempt: int, job_name: str, params: dict, profile: WorkerProfile) -> None:
    _guard_active_attempt(job_id, attempt, 'submitted')
    add_event(
        job_id,
        'job.state',
        _payload_with_profile({'state': 'submitted', 'attempt': attempt}, profile),
        attempt=attempt,
    )
    time.sleep(1)
    _guard_active_attempt(job_id, attempt, 'waiting_for_completion')
    raw_spool = f"""IEF236I ALLOC. FOR {job_name} STEP1
IEF142I {job_name} STEP1 - STEP WAS EXECUTED - COND CODE 0000
//{job_name:<8} JOB ,'WEB JOB',CLASS=A,MSGCLASS=H,MSGLEVEL=(1,1)
//STEP1    EXEC PGM=IEBGENER
//SYSUT1   DD *
{params['message']}
/*
//SYSUT2   DD SYSOUT=H
//SYSPRINT DD SYSOUT=H
//SYSIN    DD DUMMY
HELLO FROM DRY RUN
"""
    _guard_active_attempt(job_id, attempt, 'reading_spool')
    replace_spool_sections(job_id, split_spool(raw_spool), attempt=attempt)
    update_job(
        job_id,
        state='completed',
        result='success',
        mainframe_job_id='JOB00001',
        return_code='0000',
        stage='done',
        finished_at=_utcnow(),
    )
    add_event(
        job_id,
        'job.result',
        _payload_with_profile({'result': 'success', 'mainframe_job_id': 'JOB00001', 'return_code': '0000', 'attempt': attempt}, profile),
        attempt=attempt,
    )


def _capture(client: S3270Client, settle: float = 0.25) -> str:
    time.sleep(settle)
    return normalize(client.ascii())


def _wait_for_ready(
    client: S3270Client,
    stage: str,
    profile: WorkerProfile,
    attempts: int | None = None,
    delay: float | None = None,
) -> str:
    attempts = attempts if attempts is not None else profile.ready_attempts
    delay = delay if delay is not None else profile.ready_delay_seconds
    screen = _capture(client)
    for _ in range(attempts):
        if is_ready(screen, profile):
            return screen
        time.sleep(delay)
        screen = _capture(client)
    raise AutomationFailure(
        stage=stage,
        detail='Did not reach READY prompt',
        screen=screen,
        code='ready_prompt_timeout',
        retryable=True,
        remediation=(
            'Verify the TK4 host is reachable and not overloaded.',
            'Confirm the session is attached to a TSO region before issuing commands.',
        ),
    )


def _issue_tso_command(
    client: S3270Client,
    command: str,
    stage: str,
    profile: WorkerProfile,
    attempts: int = 20,
    delay: float = 0.5,
) -> str:
    _wait_for_ready(client, stage, profile)
    client.string(command)
    client.enter()
    screen = _capture(client)
    for _ in range(attempts):
        if is_ready(screen, profile):
            return screen
        time.sleep(delay)
        screen = _capture(client)
    raise AutomationFailure(
        stage=stage,
        detail=f'Command did not return to READY: {command}',
        screen=screen,
        code='command_no_ready',
        retryable=True,
        remediation=(
            'Check whether the command started an interactive panel requiring additional input.',
            'Review the captured screen text for IKJ* diagnostics and adjust command syntax.',
        ),
    )


def _logon(client: S3270Client, profile: WorkerProfile) -> str:
    client.connect(settings.tk4_host, settings.tk4_port)
    try:
        client.wait_output(timeout=settings.tso_timeout_seconds)
    except S3270Error:
        pass
    screen = _capture(client)

    if profile.should_acknowledge_applid(screen, wants_applid(screen, profile)):
        client.enter()
        screen = _capture(client)

    for _ in range(profile.logon_attempts):
        if is_ready(screen, profile):
            return screen
        if wants_userid(screen, profile) and wants_password(screen, profile):
            client.erase_input()
            client.string(settings.tso_user)
            client.tab()
            client.string(settings.tso_pass)
            client.enter()
        elif wants_userid(screen, profile):
            client.erase_input()
            client.string(settings.tso_user)
            client.enter()
        elif wants_password(screen, profile):
            client.erase_input()
            client.string(settings.tso_pass)
            client.enter()
        else:
            client.enter()
        time.sleep(0.75)
        screen = _capture(client)
    if has_login_error(screen, profile):
        raise AutomationFailure(
            stage='logging_in',
            detail='TSO logon rejected credentials or authorization',
            screen=screen,
            code='credentials_rejected',
            remediation=(
                'Verify TSO_USER and TSO_PASS credentials.',
                'Confirm the account is not revoked and has TSO access.',
            ),
        )
    raise AutomationFailure(
        stage='logging_in',
        detail='Unable to recognize or complete TSO logon flow',
        screen=screen,
        code='unrecognized_logon_screen',
        retryable=True,
        remediation=(
            *(profile.remediation_hints.get('unrecognized_logon_screen', ())),
            'Compare the current TK4 startup banner to recognizer patterns and add image-specific prompts.',
            'Ensure the connected LPAR routes to TSO and not CICS/IMS sign-on.',
        ),
    )


def _submit_jcl_via_edit(client: S3270Client, job_name: str, jcl: str, profile: WorkerProfile) -> str:
    dataset = f"{settings.tso_prefix}.TK4P.{job_name}.CNTL"
    _issue_tso_command(client, f"DELETE '{dataset}'", 'cleanup_dataset', profile, attempts=6)
    alloc_cmd = f"ALLOC DA('{dataset}') NEW TRACKS SPACE(1,1) RECFM(F B) LRECL(80) BLKSIZE(3120)"
    _issue_tso_command(client, alloc_cmd, 'allocate_dataset', profile, attempts=10)

    client.string(f"EDIT '{dataset}' NEW")
    client.enter()
    time.sleep(0.75)
    screen = _capture(client)
    if profile.should_force_input_command(in_input_mode(screen, profile)):
        client.string('INPUT')
        client.enter()
        time.sleep(0.5)
        screen = _capture(client)
        if not in_input_mode(screen, profile):
            raise AutomationFailure(
                stage='writing_jcl',
                detail='Could not enter EDIT INPUT mode',
                screen=screen,
                code='edit_input_mode_missing',
                retryable=True,
                remediation=(
                    'Verify EDIT is available and dataset allocation succeeded.',
                    'If ISPF panels appear, update flow to navigate to line-mode EDIT.',
                ),
            )

    for line in [line[:80] for line in jcl.splitlines()]:
        if line:
            client.string(line)
        client.enter()
        time.sleep(0.08)
    client.enter()
    time.sleep(0.5)
    _capture(client)

    client.string('SAVE')
    client.enter()
    time.sleep(0.3)
    _capture(client)
    client.string('END')
    client.enter()
    _wait_for_ready(client, 'edit_exit', profile, attempts=8)

    client.string(f"SUBMIT '{dataset}'")
    client.enter()
    time.sleep(0.8)
    combined = _capture(client)
    for _ in range(5):
        if extract_job_id(combined, profile):
            return combined
        time.sleep(0.5)
        combined += '\n' + _capture(client)
    raise AutomationFailure(
        stage='submitting',
        detail='Did not capture JOB id after SUBMIT',
        screen=combined,
        code='submit_job_id_missing',
        retryable=True,
        remediation=(
            'Inspect submit response for JCL syntax errors or JES routing messages.',
            'Increase post-SUBMIT capture attempts if host latency is high.',
        ),
    )


def _poll_job(client: S3270Client, job_name: str, mainframe_job_id: str, profile: WorkerProfile) -> str:
    last = ''
    for _ in range(settings.job_poll_attempts):
        status_text = _issue_tso_command(
            client, f'STATUS {job_name}({mainframe_job_id})', 'waiting_for_completion', profile, attempts=10
        )
        last = status_text
        if looks_done(status_text, profile) or looks_jcl_error(status_text, profile):
            return status_text
        time.sleep(settings.job_poll_seconds)
    return last


def _read_output(client: S3270Client, job_name: str, mainframe_job_id: str, profile: WorkerProfile) -> str:
    _wait_for_ready(client, 'output_start', profile)
    client.string(f'OUTPUT {job_name}({mainframe_job_id}) PRINT(*) PAUSE')
    client.enter()
    chunks = []
    last = ''
    for _ in range(20):
        time.sleep(0.8)
        screen = _capture(client)
        if screen and screen != last:
            chunks.append(screen)
            last = screen
        next_cmd = profile.spool_continue_command(is_ready(screen, profile))
        if next_cmd is None:
            break
        client.string(next_cmd)
        client.enter()
    return '\n\n----- SCREEN -----\n\n'.join(chunks)


def _run_real(job_id: str, attempt: int, job_name: str, jcl: str, profile: WorkerProfile) -> dict[str, str | None]:
    _guard_active_attempt(job_id, attempt, 'logging_in')
    add_event(
        job_id,
        'job.state',
        _payload_with_profile({'state': 'logging_in', 'attempt': attempt}, profile),
        attempt=attempt,
    )
    with S3270Client() as client:
        login_screen = _run_with_stage_retry(job_id, attempt, 'logging_in', profile, lambda: _logon(client, profile))
        _guard_active_attempt(job_id, attempt, 'logging_in')
        add_event(
            job_id,
            'job.screen',
            _payload_with_profile({'stage': 'logging_in', 'preview': login_screen[:600], 'attempt': attempt}, profile),
            attempt=attempt,
        )
        _guard_active_attempt(job_id, attempt, 'writing_jcl')
        add_event(
            job_id,
            'job.state',
            _payload_with_profile({'state': 'writing_jcl', 'attempt': attempt}, profile),
            attempt=attempt,
        )
        submit_text = _run_with_stage_retry(
            job_id,
            attempt,
            'writing_jcl',
            profile,
            lambda: _submit_jcl_via_edit(client, job_name, jcl, profile),
        )
        mainframe_job_id = extract_job_id(submit_text, profile)
        add_event(
            job_id,
            'job.mainframe_id',
            _payload_with_profile({'value': mainframe_job_id, 'attempt': attempt}, profile),
            attempt=attempt,
        )
        _guard_active_attempt(job_id, attempt, 'waiting_for_completion')
        add_event(
            job_id,
            'job.state',
            _payload_with_profile({'state': 'waiting_for_completion', 'attempt': attempt}, profile),
            attempt=attempt,
        )
        status_text = _run_with_stage_retry(
            job_id,
            attempt,
            'waiting_for_completion',
            profile,
            lambda: _poll_job(client, job_name, mainframe_job_id, profile),
        )
        _guard_active_attempt(job_id, attempt, 'reading_spool')
        add_event(
            job_id,
            'job.state',
            _payload_with_profile({'state': 'reading_spool', 'attempt': attempt}, profile),
            attempt=attempt,
        )
        spool_text = _run_with_stage_retry(
            job_id,
            attempt,
            'reading_spool',
            profile,
            lambda: _read_output(client, job_name, mainframe_job_id, profile),
        )
        raw_spool = status_text + '\n\n' + submit_text + '\n\n' + spool_text
        result = 'success'
        if looks_jcl_error(raw_spool, profile):
            result = 'jcl_error'
        rc = extract_return_code(raw_spool, profile)
        abend = extract_abend(raw_spool, profile)
        if abend:
            result = 'abend'
        elif rc and rc != '0000':
            result = 'warning'
        return {
            'result': result,
            'mainframe_job_id': mainframe_job_id,
            'return_code': rc,
            'abend_code': abend,
            'raw_spool': raw_spool,
        }


def _run_with_stage_retry(
    job_id: str, attempt: int, stage: str, profile: WorkerProfile, fn: Callable[[], str]
) -> str:
    policy = _STAGE_RETRY_POLICIES.get(stage, RetryPolicy(attempts=1, delay_seconds=0.0))
    last_failure: AutomationFailure | None = None
    for current_attempt in range(1, policy.attempts + 1):
        _guard_active_attempt(job_id, attempt, stage)
        add_event(
            job_id,
            'job.retry',
            _payload_with_profile(
                {'stage': stage, 'stage_attempt': current_attempt, 'stage_attempts': policy.attempts, 'attempt': attempt},
                profile,
            ),
            attempt=attempt,
        )
        started_at = _utcnow()
        logger.info(
            'stage.start',
            job_id=job_id,
            stage=stage,
            attempt=attempt,
            context={'stage_attempt': current_attempt, 'stage_attempts': policy.attempts},
        )
        try:
            result = fn()
            ended_at = _utcnow()
            duration_ms = stage_duration_ms(started_at, ended_at)
            add_event(
                job_id,
                'job.stage_duration',
                _payload_with_profile(
                    {
                        'stage': stage,
                        'stage_attempt': current_attempt,
                        'duration_ms': duration_ms,
                        'started_at': started_at,
                        'finished_at': ended_at,
                        'attempt': attempt,
                    },
                    profile,
                ),
                attempt=attempt,
            ),
            logger.info(
                'stage.done',
                job_id=job_id,
                stage=stage,
                attempt=attempt,
                context={'stage_attempt': current_attempt, 'duration_ms': duration_ms},
            )
            return result
        except AutomationFailure as exc:
            classified = _classify_failure(exc, profile)
            last_failure = classified
            ended_at = _utcnow()
            duration_ms = stage_duration_ms(started_at, ended_at)
            add_event(
                job_id,
                'job.retry.failed_attempt',
                _payload_with_profile(
                    {
                        'stage': stage,
                        'stage_attempt': current_attempt,
                        'code': classified.code,
                        'retryable': classified.retryable,
                        'detail': classified.detail,
                        'duration_ms': duration_ms,
                        'attempt': attempt,
                    },
                    profile,
                ),
                attempt=attempt,
            )
            logger.warning(
                'stage.failed',
                job_id=job_id,
                stage=stage,
                attempt=attempt,
                context={'stage_attempt': current_attempt, 'duration_ms': duration_ms, 'code': classified.code},
            )
            if not classified.retryable or current_attempt >= policy.attempts:
                raise classified
            time.sleep(policy.delay_seconds)
    if last_failure:
        raise last_failure
    raise AutomationFailure(stage=stage, detail='Stage failed without details', code='unknown_stage_failure')


def _classify_failure(exc: AutomationFailure, profile: WorkerProfile) -> AutomationFailure:
    screen = exc.screen or ''
    stage = exc.stage
    if stage == 'logging_in':
        if has_login_error(screen, profile):
            return AutomationFailure(
                stage=stage,
                detail='TSO rejected credentials during logon',
                screen=screen,
                code='credentials_rejected',
                retryable=False,
                remediation=(
                    'Reset TSO_USER/TSO_PASS to valid credentials.',
                    'Verify the user is not locked and has TSO logon permission.',
                ),
            )
        if not looks_like_tso_screen(screen, profile):
            return AutomationFailure(
                stage=stage,
                detail='Connected session is not showing an expected TSO context',
                screen=screen,
                code='unexpected_host_screen',
                retryable=True,
                remediation=(
                    'Verify TK4_HOST/TK4_PORT points to the intended TK4 tn3270 endpoint.',
                    'Adjust recognizers for this image if custom banners or menus are used.',
                ),
            )
    if stage in {'cleanup_dataset', 'allocate_dataset', 'writing_jcl', 'submitting'} and has_dataset_error(screen, profile):
        return AutomationFailure(
            stage=stage,
            detail='Dataset operation failed during JCL staging',
            screen=screen,
            code='dataset_stage_failed',
            retryable=False,
            remediation=(
                'Confirm TSO_PREFIX exists and user has create/delete authority for target HLQ.',
                'Check catalog and volume availability for allocation.',
            ),
        )
    return exc


def _build_failure_payload(exc: AutomationFailure, profile: WorkerProfile | None = None) -> dict:
    active_profile = profile or get_profile(settings.worker_adapter_profile)
    classified = _classify_failure(exc, active_profile)
    return {
        'stage': classified.stage,
        'code': classified.code,
        'category': _failure_category(classified.stage, classified.code),
        'detail': classified.detail,
        'retryable': classified.retryable,
        'remediation': list(classified.remediation),
        'screen': classified.screen,
        'profile': active_profile.name,
    }


def _failure_category(stage: str, code: str) -> str:
    if code in {'credentials_rejected'}:
        return 'auth'
    if stage in {'cleanup_dataset', 'allocate_dataset', 'writing_jcl', 'submitting'}:
        return 'submission'
    if stage in {'waiting_for_completion'}:
        return 'execution'
    if stage in {'reading_spool'}:
        return 'capture'
    return 'connectivity'
