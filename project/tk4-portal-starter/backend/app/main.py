from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from common.db import (
    create_job,
    get_job,
    get_job_events,
    get_job_events_since,
    get_spool_sections,
    init_db,
    list_jobs,
)
from common.template_schemas import (
    TemplateSchemaError,
    UnknownTemplateError,
    get_template_catalog,
    get_template_schema,
    normalize_and_validate_template_params,
)

app = FastAPI(title='TK4 Portal')

_SSE_POLL_SECONDS = 1.0
_SSE_KEEPALIVE_SECONDS = 15.0
_SSE_MAX_BATCH_SIZE = 100
_SSE_BACKLOG_LIMIT = 200
_TERMINAL_STATES = {'completed', 'failed'}


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
    return list_jobs()


@app.post('/api/jobs')
def create_job_route(request: CreateJobRequest) -> dict[str, Any]:
    try:
        normalized_params = normalize_and_validate_template_params(request.template_id, request.params)
    except TemplateSchemaError as exc:
        raise HTTPException(status_code=422, detail=exc.to_dict()) from exc
    return create_job(request.template_id, request.submitted_by, normalized_params)


@app.get('/api/jobs/{job_id}')
def get_job_route(job_id: str) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='Job not found')
    job['events'] = get_job_events(job_id)
    return job


@app.get('/api/jobs/{job_id}/spool')
def get_spool_route(job_id: str) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='Job not found')
    return {'job_id': job_id, 'sections': get_spool_sections(job_id)}


@app.get('/api/jobs/{job_id}/spool/{section_type}')
def get_spool_section_route(job_id: str, section_type: str) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='Job not found')
    sections = [s for s in get_spool_sections(job_id) if s['section_type'] == section_type]
    if not sections:
        raise HTTPException(status_code=404, detail='Spool section not found')
    return {'job_id': job_id, 'section_type': section_type, 'content': '\n\n'.join(s['content_text'] for s in sections)}


def _to_sse_frame(event_id: int, event_type: str, payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, separators=(',', ':'))
    return f'id: {event_id}\nevent: {event_type}\ndata: {payload_json}\n\n'


@app.get('/api/jobs/{job_id}/events/stream')
async def stream_job_events_route(
    job_id: str,
    request: Request,
    last_event_id: str | None = Header(default=None, alias='Last-Event-ID'),
    linger_seconds: float = Query(default=1.0, ge=0.0, le=30.0),
) -> StreamingResponse:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='Job not found')

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
