from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from common.db import create_job, get_job, get_job_events, get_spool_sections, init_db, list_jobs

app = FastAPI(title='TK4 Portal')


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


@app.get('/api/jobs')
def jobs() -> list[dict[str, Any]]:
    return list_jobs()


@app.post('/api/jobs')
def create_job_route(request: CreateJobRequest) -> dict[str, Any]:
    return create_job(request.template_id, request.submitted_by, request.params)


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
