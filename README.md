# postio

Starter project for building APIs with Axum and OpenTelemetry. It exposes:

- `/health` as a liveness endpoint that returns `200 OK` with a JSON status payload.
- `/echo` to reflect the incoming request across all HTTP verbs with tracing spans per method when OTEL is enabled.

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
   - `OTEL_ENABLED=false` to disable OpenTelemetry export and HTTP tracing middleware while keeping structured logs.
3. Run:
   - `cargo run`

Swagger UI is available at `/docs` with the generated OpenAPI contract.

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
