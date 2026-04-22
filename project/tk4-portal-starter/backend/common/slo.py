from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from common.observability import parse_iso8601

_TERMINAL_FAILURE_STATES = {'failed', 'canceled'}
_FAILURE_RESULTS = {'error', 'failed', 'jcl_error', 'abend'}
_CANONICAL_STAGE_ORDER = ['queued', 'connecting', 'logon', 'submit', 'poll', 'capture', 'done', 'failed']
_STAGE_TO_CANONICAL = {
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


@dataclass(frozen=True)
class WindowConfig:
    name: str
    duration: timedelta


@dataclass(frozen=True)
class SLOObjective:
    objective_id: str
    display_name: str
    indicator: str
    target: float
    primary_window: str = '24h'
    latency_stage: str | None = None
    latency_threshold_ms: int | None = None


WINDOWS: tuple[WindowConfig, ...] = (
    WindowConfig(name='5m', duration=timedelta(minutes=5)),
    WindowConfig(name='1h', duration=timedelta(hours=1)),
    WindowConfig(name='24h', duration=timedelta(hours=24)),
)
WINDOW_BY_NAME = {item.name: item for item in WINDOWS}

DEFAULT_OBJECTIVES: tuple[SLOObjective, ...] = (
    SLOObjective('availability', 'Availability', 'availability', 0.995),
    SLOObjective('success_rate', 'Success Rate', 'success_rate', 0.99),
    SLOObjective('latency_poll', 'Poll Stage Latency', 'latency', 0.95, latency_stage='poll', latency_threshold_ms=60_000),
)


def compute_slo_report(
    jobs: list[dict[str, Any]],
    events_by_job_id: dict[str, list[dict[str, Any]]],
    objectives: list[SLOObjective] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    effective_objectives = objectives or list(DEFAULT_OBJECTIVES)
    now_dt = _normalize_now(now)

    objective_payload = [
        _evaluate_objective(objective, jobs=jobs, events_by_job_id=events_by_job_id, now=now_dt)
        for objective in effective_objectives
    ]
    return {
        'generated_at': now_dt.isoformat(),
        'windows': [
            {'name': window.name, 'seconds': int(window.duration.total_seconds())}
            for window in WINDOWS
        ],
        'objectives': objective_payload,
    }


def get_objective_report(
    objective_id: str,
    jobs: list[dict[str, Any]],
    events_by_job_id: dict[str, list[dict[str, Any]]],
    objectives: list[SLOObjective] | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    report = compute_slo_report(jobs=jobs, events_by_job_id=events_by_job_id, objectives=objectives, now=now)
    for item in report['objectives']:
        if item['objective_id'] == objective_id:
            return item
    return None


def _evaluate_objective(
    objective: SLOObjective,
    jobs: list[dict[str, Any]],
    events_by_job_id: dict[str, list[dict[str, Any]]],
    now: datetime,
) -> dict[str, Any]:
    samples = _build_samples(objective, jobs=jobs, events_by_job_id=events_by_job_id)
    windows_payload: dict[str, Any] = {}
    for window in WINDOWS:
        window_start = now - window.duration
        in_window = [sample for sample in samples if sample['ts'] >= window_start and sample['ts'] <= now]
        total = len(in_window)
        good = sum(1 for sample in in_window if sample['good'])
        bad = total - good
        sli = good / total if total else None
        windows_payload[window.name] = {
            'total': total,
            'good': good,
            'bad': bad,
            'sli': round(sli, 6) if sli is not None else None,
        }

    primary = windows_payload[objective.primary_window]
    short_window = windows_payload['5m']
    long_window = windows_payload['1h']
    short_burn = _burn_rate(short_window['good'], short_window['total'], objective.target)
    long_burn = _burn_rate(long_window['good'], long_window['total'], objective.target)
    status = _status_for_objective(primary_sli=primary['sli'], target=objective.target, short_burn=short_burn, long_burn=long_burn)

    error_budget = _error_budget(primary['good'], primary['total'], objective.target)
    return {
        'objective_id': objective.objective_id,
        'display_name': objective.display_name,
        'indicator': objective.indicator,
        'target': objective.target,
        'status': status,
        'windows': windows_payload,
        'error_budget': error_budget,
        'burn_rate': {'short': short_burn, 'long': long_burn},
        'config': {
            'primary_window': objective.primary_window,
            'latency_stage': objective.latency_stage,
            'latency_threshold_ms': objective.latency_threshold_ms,
        },
    }


def _build_samples(
    objective: SLOObjective,
    jobs: list[dict[str, Any]],
    events_by_job_id: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    if objective.indicator in {'availability', 'success_rate'}:
        return _terminal_outcome_samples(objective.indicator, jobs)
    if objective.indicator == 'latency':
        return _latency_samples(jobs, events_by_job_id, objective.latency_stage, int(objective.latency_threshold_ms or 0))
    return []


def _terminal_outcome_samples(indicator: str, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for job in jobs:
        state = str(job.get('state') or '').lower()
        if state not in {'completed', 'failed', 'canceled'}:
            continue
        ts = parse_iso8601(job.get('finished_at') or job.get('updated_at'))
        if not ts:
            continue
        result = str(job.get('result') or '').lower()
        if indicator == 'availability':
            good = state not in _TERMINAL_FAILURE_STATES
        else:
            good = state == 'completed' and result not in _FAILURE_RESULTS
        out.append({'ts': ts, 'good': good})
    return out


def _latency_samples(
    jobs: list[dict[str, Any]],
    events_by_job_id: dict[str, list[dict[str, Any]]],
    stage_name: str | None,
    threshold_ms: int,
) -> list[dict[str, Any]]:
    if not stage_name or threshold_ms <= 0:
        return []
    out: list[dict[str, Any]] = []
    for job in jobs:
        timeline = _build_stage_timeline(job, events_by_job_id.get(str(job.get('id')), []))
        durations = _build_stage_durations(timeline, job)
        for item in durations:
            if item['stage'] != stage_name:
                continue
            if item['duration_ms'] is None or not item.get('end_at'):
                continue
            end_ts = parse_iso8601(item.get('end_at'))
            if not end_ts:
                continue
            out.append({'ts': end_ts, 'good': int(item['duration_ms']) <= threshold_ms})
    return out


def _canonical_stage(stage: str | None, state: str | None) -> str:
    stage_value = (stage or '').strip().lower()
    state_value = (state or '').strip().lower()
    if stage_value in {'', 'none'}:
        if state_value == 'queued':
            return 'queued'
        if state_value in {'starting', 'submitted', 'running'}:
            return 'connecting'
        if state_value == 'completed':
            return 'done'
        if state_value in {'failed', 'canceled'}:
            return 'failed'
        return 'queued'
    return _STAGE_TO_CANONICAL.get(stage_value, 'failed')


def _build_stage_timeline(job: dict[str, Any], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stage_first_seen: dict[str, str] = {'queued': str(job.get('created_at') or '')}
    for event in events:
        payload = event.get('payload') or {}
        stage_name = _canonical_stage(payload.get('stage'), payload.get('state'))
        if stage_name not in stage_first_seen:
            stage_first_seen[stage_name] = str(event.get('ts') or '')

    current = _canonical_stage(job.get('stage'), job.get('state'))
    if current == 'failed' and not stage_first_seen.get('failed'):
        stage_first_seen['failed'] = str(job.get('finished_at') or job.get('updated_at') or '')
    if current == 'done' and not stage_first_seen.get('done'):
        stage_first_seen['done'] = str(job.get('finished_at') or job.get('updated_at') or '')

    return [
        {'stage': stage_name, 'first_seen_at': stage_first_seen[stage_name]}
        for stage_name in _CANONICAL_STAGE_ORDER
        if stage_name in stage_first_seen and stage_first_seen[stage_name]
    ]


def _build_stage_durations(timeline: list[dict[str, Any]], job: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for index, item in enumerate(timeline):
        start_at = item.get('first_seen_at')
        if index + 1 < len(timeline):
            end_at = timeline[index + 1].get('first_seen_at')
        else:
            end_at = job.get('finished_at') or job.get('updated_at')
        start_dt = parse_iso8601(start_at)
        end_dt = parse_iso8601(end_at)
        duration_ms = None
        if start_dt and end_dt:
            raw_ms = int((end_dt - start_dt).total_seconds() * 1000)
            if raw_ms >= 0:
                duration_ms = raw_ms
        out.append({'stage': item.get('stage'), 'duration_ms': duration_ms, 'end_at': end_at})
    return out


def _burn_rate(good: int, total: int, target: float) -> float | None:
    if total == 0:
        return None
    bad_fraction = 1 - (good / total)
    budget_fraction = 1 - target
    if budget_fraction <= 0:
        return None
    return round(bad_fraction / budget_fraction, 6)


def _error_budget(good: int, total: int, target: float) -> dict[str, float | int]:
    budget_fraction = max(0.0, 1 - target)
    if total == 0:
        return {'allowed_bad_events': 0, 'observed_bad_events': 0, 'remaining_ratio': 1.0}
    observed_bad = total - good
    allowed_bad = total * budget_fraction
    remaining_ratio = 1.0 if allowed_bad == 0 else max(0.0, min(1.0, (allowed_bad - observed_bad) / allowed_bad))
    return {
        'allowed_bad_events': round(allowed_bad, 3),
        'observed_bad_events': observed_bad,
        'remaining_ratio': round(remaining_ratio, 6),
    }


def _status_for_objective(
    primary_sli: float | None,
    target: float,
    short_burn: float | None,
    long_burn: float | None,
) -> str:
    if primary_sli is None:
        return 'met'
    if primary_sli < target:
        return 'breached'
    if (long_burn is not None and long_burn >= 1.0) or (short_burn is not None and short_burn >= 2.0):
        return 'at-risk'
    return 'met'


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)
