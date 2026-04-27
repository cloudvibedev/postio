# postio

Postio is a configurable HTTP ingestion gateway built with Axum and OpenTelemetry. In v0 it routes `POST` requests to AWS SNS, SQS, and S3 sinks.

- `/health` as a liveness endpoint that returns `200 OK` with a JSON status payload.
- `/echo` to reflect the incoming request across all HTTP verbs with tracing spans per method when OTEL is enabled.
- Configured ingestion routes from `POSTIO_CONFIG`.

## Template Bootstrap

After creating a new repository from this template, run:

- `./scripts/init-template.sh`

The script uses the current repository directory name as the new Cargo package/bin name, updates the main hardcoded references (`Cargo.toml`, Rust imports, README, `.env.example`, and `Dockerfile.artifact`), then runs `cargo build` and `OTEL_ENABLED=false cargo test`.

If you want to override the detected name, pass it explicitly:

- `./scripts/init-template.sh my-new-api`

## Getting Started

### Prerequisites

- Rust toolchain (stable).

### Quick start

1. Optionally start local observability services:
   - `docker compose up -d jaeger`
2. Optionally set:
   - `APP_HOST` and `APP_PORT` (defaults: `127.0.0.1:8080`).
   - `APP_CORS_ALLOW_ORIGINS` (comma-separated or `*`) and `APP_BODY_LIMIT_BYTES`.
   - `POSTIO_CONFIG` to point at a YAML/JSON route config (default: `config/example.yaml`).
   - `OTEL_ENABLED=false` to disable OpenTelemetry export and HTTP tracing middleware while keeping structured logs.
3. Run:
   - `cargo run`

Swagger UI is available at `/docs` with the generated OpenAPI contract.

### Ingestion config

v0 supports only `POST` routes and these sinks:

- `sns`
- `sqs`
- `s3`

Example:

```yaml
routes:
  - id: topic-input
    path: /events/{topic}
    sink:
      type: sns
      topic: "{{ params.topic }}"

  - id: queue-input
    path: /queue
    sink:
      type: sqs
      queueUrl: https://sqs.us-east-1.amazonaws.com/123456789012/my-queue

  - id: file-input
    path: /file/{bucket}/{filename}
    sink:
      type: s3
      bucket: "{{ params.bucket }}"
      key: "{{ params.filename }}"
```

Templates can read `params`, `query`, `headers`, `body`, and `context`.

### Artifact image

`Dockerfile.artifact` expects a prebuilt binary in `artifacts/<bin-name>/<arch>/` and accepts `BIN_NAME` as a build argument. Example:

`docker build -f Dockerfile.artifact --build-arg TARGETARCH=amd64 --build-arg BIN_NAME=postio .`

### Testing

- Unit tests: `cargo test`
- Integration tests: `cargo test --test integration`

## Architecture

This template is organized around four main layers:

- `routes/`: transport and protocol adapters for HTTP
- `services/`: business rules and use-case orchestration
- `repositories/`: external integration adapters when needed
- `dto/`: request/response contracts, validation, and data transformation structs

Preferred flow:

- `route -> dto -> service -> repository -> service -> dto -> route`

Guidelines:

- keep HTTP details inside `routes/`
- keep business decisions inside `services/`
- keep queues and external API clients inside `repositories/`
- keep payload contracts and transformation structs inside `dto/`
- let `AppState` carry shared application dependencies instead of exposing raw clients when possible

## Project Layout

```
src/
  config.rs         # environment loading
  dto/              # request/response contracts and shared transport payloads
  routes/           # HTTP transport handlers plus wiring
  services/         # business rules and use-case orchestration
```

Adjust the repositories and services to fit your application, then expand the router with new modules as needed.
