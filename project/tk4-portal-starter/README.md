# TK4 Portal Starter

This starter bundle exposes a small FastAPI API plus a polling worker that can either:

1. run in `DRY_RUN=1` mode and simulate TK4 jobs, or
2. run in `DRY_RUN=0` mode and attempt a real private `s3270` session to TK4/Hercules.

## What is implemented

- `GET /api/templates` to discover available JCL templates and parameter specs
- `POST /api/jobs` to queue a template-driven JCL job
- `GET /api/jobs/{job_id}` to return rich job detail, stage timeline, normalized params, and artifact links
- `GET /api/jobs/{job_id}/events` for SSE job event streaming (`/events/stream` remains supported)
- lifecycle endpoints:
  - `POST /api/jobs/{job_id}/cancel`
  - `POST /api/jobs/{job_id}/retry`
  - `POST /api/jobs/{job_id}/requeue`
- worker polling loop
- SQLite job store
- `s3270` subprocess wrapper
- common TK4/TSO screen recognition helpers
- best-effort TSO logon flow
- best-effort JCL submission via TSO line-mode EDIT
- best-effort `STATUS` polling and `OUTPUT` capture
- spool normalization into JES / JCL / SYSOUT sections

### Canonical stage model

The API now exposes a canonical stage model for observability:

`queued -> connecting -> logon -> submit -> poll -> capture -> done/failed`

`GET /api/jobs/{job_id}` returns:

- `stage_model.current`
- `stage_model.timeline` (first-seen timestamp per stage)
- `normalized_params`
- `artifact_links` (`/spool` and per-section links)
- execution provenance fields:
  - `template_version`, `template_hash`
  - `rendered_jcl`
  - `worker_version`, `worker_build`
  - `target_host`, `target_port`

## Template catalog

Use `GET /api/templates` to discover template IDs, descriptions, and required/optional parameters.

Current templates:

- `hello-world` (defaults: `job_name=HELLO1`, `message=HELLO FROM WEB PORTAL`)
- `idcams-listcat` (requires `level`, default `job_name=LISTCAT`)
- `iebgener-copy` (requires `input_dataset`, `output_dataset`, default `job_name=IEBGEN`)
- `sort-basic` (requires `input_dataset`, `output_dataset`, default `job_name=SORTJOB`, default `sort_fields=1,10,CH,A`)

### Validation behavior

`POST /api/jobs` validates the `template_id` and `params` before queuing the job.

- Unknown template IDs return HTTP `422`.
- Missing required params return HTTP `422` with a message naming the missing field.
- Invalid params (for example `job_name`) return HTTP `422` with a message naming the invalid field.

`job_name` normalization and validation rules:

- normalized to uppercase
- must be at most 8 characters
- must start with `A-Z`
- remaining characters may be `A-Z`, `0-9`, `#`, `$`, `@`


## Template contracts and schemas

Use:

- `GET /api/templates` for all templates and their parameter contracts
- `GET /api/templates/{template_id}` for one template

Each parameter includes:

- `type`
- `required`
- `default` (when optional)
- `help`
- `examples`
- `format` (when applicable, such as `jcl_job_name`)

Normalization is applied before enqueue and persisted to `jobs.input_params_json`:

- all string params are trimmed
- `job_name` is uppercased and truncated to 8 chars, then validated against `^[A-Z][A-Z0-9#$@]{0,7}$`
- optional params receive schema defaults

### Validation error format (`422`)

`POST /api/jobs` returns structured details:

```json
{
  "detail": {
    "code": "template_params_invalid",
    "message": "Template parameters failed validation",
    "errors": [
      {
        "path": "params.level",
        "reason": "missing_required_field",
        "expected": {"type": "string"}
      }
    ]
  }
}
```

Unknown template IDs return:

```json
{
  "detail": {
    "code": "unknown_template_id",
    "message": "Unknown template_id 'does-not-exist'",
    "errors": [
      {
        "path": "template_id",
        "reason": "unsupported_value",
        "expected": {
          "one_of": ["hello-world", "idcams-listcat", "iebgener-copy", "sort-basic"]
        },
        "actual": "does-not-exist"
      }
    ]
  }
}
```

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

## Quick API examples

```bash
curl http://localhost:8080/api/healthz
curl http://localhost:8080/api/templates
```

### `hello-world`

Valid:

```bash
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

Invalid (bad `job_name` format):

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "hello-world",
    "submitted_by": "william",
    "params": {
      "job_name": "1BAD",
      "message": "HELLO"
    }
  }'
```

### `idcams-listcat`

Valid:

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "idcams-listcat",
    "submitted_by": "william",
    "params": {
      "job_name": "LISTCAT",
      "level": "SYS1"
    }
  }'
```

Invalid (missing required `level`):

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "idcams-listcat",
    "submitted_by": "william",
    "params": {
      "job_name": "LISTCAT"
    }
  }'
```

### `iebgener-copy`

Valid:

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "iebgener-copy",
    "submitted_by": "william",
    "params": {
      "job_name": "COPYJOB",
      "input_dataset": "SYS1.PROCLIB",
      "output_dataset": "IBMUSER.PROCLIB"
    }
  }'
```

Invalid (missing `output_dataset`):

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "iebgener-copy",
    "submitted_by": "william",
    "params": {
      "job_name": "COPYJOB",
      "input_dataset": "SYS1.PROCLIB"
    }
  }'
```

### `sort-basic`

Valid:

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "sort-basic",
    "submitted_by": "william",
    "params": {
      "job_name": "SORTJOB",
      "input_dataset": "IBMUSER.INPUT",
      "output_dataset": "IBMUSER.OUTPUT",
      "sort_fields": "1,8,CH,A"
    }
  }'
```

Invalid (wrong type for `sort_fields`):

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "sort-basic",
    "submitted_by": "william",
    "params": {
      "job_name": "SORTJOB",
      "input_dataset": "IBMUSER.INPUT",
      "output_dataset": "IBMUSER.OUTPUT",
      "sort_fields": 123
    }
  }'
```

### Validation error example

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "template_id": "idcams-listcat",
    "submitted_by": "william",
    "params": {
      "job_name": "LISTCAT"
    }
  }'
```

Expected response status: `422`

Then inspect:

```bash
curl http://localhost:8080/api/jobs
curl http://localhost:8080/api/jobs/<job-id>
curl http://localhost:8080/api/jobs/<job-id>/spool/raw
```

## Server-Sent Events (SSE) job event stream

You can consume incremental job events with:

- `GET /api/jobs/{job_id}/events/stream`
- response type: `text/event-stream`
- SSE frame shape:
  - `id: <monotonic event id>`
  - `event: <event type>`
  - `data: <JSON payload>`

### Stream semantics

- The stream sends historical backlog first (bounded window), then tails new events.
- Keepalive comments (`: ping`) are emitted periodically for idle connections.
- The stream remains open until a terminal job state is observed (`completed` or `failed`) and a final flush/linger period elapses.
- Unknown jobs return `404` without opening a stream.

### Curl example

```bash

## Job lifecycle and transition contracts

The API enforces explicit state transitions and returns deterministic errors:

- `404` (`code=job_not_found`) when `{job_id}` does not exist.
- `409` (`code=invalid_transition`) when the transition is not allowed from the current state.

### Transition rules

- **cancel** (`POST /api/jobs/{job_id}/cancel`)
  - allowed from: `queued`, `starting`, `submitted`, `running`, `logging_in`, `writing_jcl`, `waiting_for_completion`, `reading_spool`
  - result: `state=canceled`, `result=canceled`
- **retry** (`POST /api/jobs/{job_id}/retry`)
  - allowed from failed terminal runs (`state=failed` or failure result such as `error`, `jcl_error`, `abend`)
  - result: same `job_id`, new `attempt`, state reset to `queued`
- **requeue** (`POST /api/jobs/{job_id}/requeue`)
  - allowed from: `failed`, `canceled`, and non-terminal stuck states
  - rejected from: `completed` (unless clone semantics are introduced later)
  - result: same `job_id`, new `attempt`, state reset to `queued`

### Attempt / provenance model

- The system uses a **same job ID, incrementing attempt** model.
- `jobs.attempt` increments on `retry`/`requeue`.
- Provenance fields are preserved across attempts:
  - `template_id`, `submitted_by`, `input_params_json`
- Execution fields are reset for each new attempt:
  - `started_at`, `finished_at`, `mainframe_job_id`, `return_code`, `abend_code`, `error_text`, `stage`
- `spool_sections` and `job_events` are stored with `attempt`, so prior attempts remain auditable.

### State diagram (high-level)

```text
queued -> starting -> logging_in -> writing_jcl -> waiting_for_completion -> reading_spool -> completed|failed
   |                          |                                                |
   +--------------------------+------------------------------ cancel ----------> canceled

failed --retry--> queued (attempt+1)
failed|canceled|stuck_non_terminal --requeue--> queued (attempt+1)
completed --requeue--> 409 invalid_transition
```

### Lifecycle response payload (example)

```json
{
  "id": "a66ef4f7-1d50-47a3-b0e8-2a1f56af2df7",
  "state": "queued",
  "attempt": 2,
  "attempt_info": {
    "attempt": 2,
    "parent_job_id": "a66ef4f7-1d50-47a3-b0e8-2a1f56af2df7",
    "retry_of_job_id": "a66ef4f7-1d50-47a3-b0e8-2a1f56af2df7"
  },
  "event_summary": {
    "count": 1,
    "last_event": "job.retried"
  },
  "events": [
    {
      "event_type": "job.retried",
      "payload": {
        "state": "queued",
        "attempt": 2,
        "previous_attempt": 1
      }
    }
  ]
}
```
curl -N http://localhost:8080/api/jobs/<job-id>/events/stream
```

### Browser example

```html
<script>
  const source = new EventSource("http://localhost:8080/api/jobs/<job-id>/events/stream");
  source.onmessage = (evt) => console.log("message", evt.lastEventId, evt.data);
  source.addEventListener("job.completed", (evt) => {
    console.log("completed", evt.lastEventId, JSON.parse(evt.data));
    source.close();
  });
</script>
```

### Reconnection with `Last-Event-ID`

When reconnecting manually (for non-browser clients), send the last seen SSE event id:

```bash
curl -N \
  -H "Last-Event-ID: 42" \
  http://localhost:8080/api/jobs/<job-id>/events/stream
```

The server resumes with events where `id > 42`, preventing duplicate delivery during reconnect.
