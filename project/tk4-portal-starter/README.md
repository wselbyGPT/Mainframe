# TK4 Portal Starter

This starter bundle exposes a small FastAPI API plus a polling worker that can either:

1. run in `DRY_RUN=1` mode and simulate TK4 jobs, or
2. run in `DRY_RUN=0` mode and attempt a real private `s3270` session to TK4/Hercules.

## What is implemented

- `POST /api/jobs` to queue a template-driven JCL job
- worker polling loop
- SQLite job store
- `s3270` subprocess wrapper
- common TK4/TSO screen recognition helpers
- best-effort TSO logon flow
- best-effort JCL submission via TSO line-mode EDIT
- best-effort `STATUS` polling and `OUTPUT` capture
- spool normalization into JES / JCL / SYSOUT sections

## Important limitations

This bundle was not live-tested against your specific TK4 image. The real worker path is designed to fail with a very explicit stage name and captured screen text if the host presents a different logon screen or command flow than expected.

The code assumes a private tn3270 listener, usually `127.0.0.1:3270`, and a line-mode TSO workflow.

## Environment

Copy `.env.example` to `.env` and set at least:

- `DRY_RUN=0`
- `TK4_HOST=host.docker.internal` or the private IP of the host running Hercules
- `TK4_PORT=3270`
- `TSO_USER=...`
- `TSO_PASS=...`
- `TSO_PREFIX=...` (usually same as userid)

## Run

```bash
mkdir -p var/uploads var/artifacts var/logs
cp .env.example .env

docker compose up --build
```

## Quick test

```bash
curl http://localhost:8080/api/healthz

curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "hello-world",
    "submitted_by": "william",
    "params": {
      "job_name": "HELLO1",
      "message": "HELLO FROM THE WEB PORTAL"
    }
  }'
```

Then inspect:

```bash
curl http://localhost:8080/api/jobs
curl http://localhost:8080/api/jobs/<job-id>
curl http://localhost:8080/api/jobs/<job-id>/spool/raw
```
