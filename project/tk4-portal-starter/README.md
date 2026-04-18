# TK4 Portal Starter

This starter bundle exposes a small FastAPI API plus a polling worker that can either:

1. run in `DRY_RUN=1` mode and simulate TK4 jobs, or
2. run in `DRY_RUN=0` mode and attempt a real private `s3270` session to TK4/Hercules.

## What is implemented

- `GET /api/templates` to discover available JCL templates and parameter specs
- `POST /api/jobs` to queue a template-driven JCL job
- worker polling loop
- SQLite job store
- `s3270` subprocess wrapper
- common TK4/TSO screen recognition helpers
- best-effort TSO logon flow
- best-effort JCL submission via TSO line-mode EDIT
- best-effort `STATUS` polling and `OUTPUT` capture
- spool normalization into JES / JCL / SYSOUT sections

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

### `idcams-listcat`

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

### `iebgener-copy`

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

### `sort-basic`

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
