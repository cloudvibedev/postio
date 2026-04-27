# postio

Starter project for building APIs with Axum, PostgreSQL, and OpenTelemetry. It exposes:

- `/health` as a liveness endpoint that returns `200 OK` with a JSON status payload.
- `/echo` to reflect the incoming request across all HTTP verbs with tracing spans per method when OTEL is enabled.

## Template Bootstrap

After creating a new repository from this template, run:

- `./scripts/init-template.sh`

The script uses the current repository directory name as the new Cargo package/bin name, updates the main hardcoded references (`Cargo.toml`, Rust imports, README, `.env.example`, and `Dockerfile.artifact`), then runs `cargo build` and `OTEL_ENABLED=false cargo test`.

When Docker is provided through a context such as Colima, the script exports that context socket as `DOCKER_HOST` so testcontainers can start the integration test services.

If you want to override the detected name, pass it explicitly:

- `./scripts/init-template.sh my-new-api`

## Getting Started

### Prerequisites

- Rust toolchain (stable).
- PostgreSQL access. You can use the included Docker Compose if you want a local DB.

### Quick start

1. Start a database (optional example using Compose):
   - `docker compose up -d postgres jaeger`
   - If you are upgrading from an older Postgres image and see a volume layout error, recreate the Postgres volume once:
   - `docker compose down -v`
   - `docker compose up -d postgres jaeger`
2. Set `DATABASE_URL` (example for the Compose service):
   - `export DATABASE_URL=postgres://postgres:postgres@localhost:5453/postgres`
3. Optionally set:
   - `APP_HOST` and `APP_PORT` (defaults: `127.0.0.1:8080`).
   - `APP_CORS_ALLOW_ORIGINS` (comma-separated or `*`) and `APP_BODY_LIMIT_BYTES`.
   - `OTEL_ENABLED=false` to disable OpenTelemetry export and HTTP tracing middleware while keeping structured logs.
4. Run:
   - `cargo run`

Migrations are managed by SQLx and executed on startup from `migrations/`.
Swagger UI is available at `/docs` with the generated OpenAPI contract.

### Artifact image

`Dockerfile.artifact` expects a prebuilt binary in `artifacts/<bin-name>/<arch>/` and accepts `BIN_NAME` as a build argument. Example:

`docker build -f Dockerfile.artifact --build-arg TARGETARCH=amd64 --build-arg BIN_NAME=postio .`

### SQLx note

The SQLx query macros use the database schema at compile time. Make sure `DATABASE_URL` is set when building. If you prefer offline builds, run `cargo sqlx prepare` and set `SQLX_OFFLINE=true`.

### Testing

- Unit tests: `cargo test`
- Integration tests: `cargo test --test integration`
  - Requires Docker; tests spin up `pgvector/pgvector:pg18` and, unless `OTEL_ENABLED=false`, a Jaeger collector via testcontainers.

## Architecture

This template is organized around four main layers:

- `routes/`: transport and protocol adapters for HTTP
- `services/`: business rules and use-case orchestration
- `repositories/`: persistence and external integration adapters
- `dto/`: request/response contracts, validation, and data transformation structs

Preferred flow:

- `route -> dto -> service -> repository -> service -> dto -> route`

Guidelines:

- keep HTTP details inside `routes/`
- keep business decisions inside `services/`
- keep SQLx, queues, and external API clients inside `repositories/`
- keep payload contracts and transformation structs inside `dto/`
- let `AppState` carry concrete repositories instead of exposing raw driver clients when possible

## Project Layout

```
src/
  config.rs         # environment loading
  db.rs             # connection pool + migrations
  dto/              # request/response contracts and shared transport payloads
  repositories/     # DB, queue, cache, and external integration adapters
  routes/           # HTTP transport handlers plus wiring
  services/         # business rules and use-case orchestration
```

Adjust the repositories and services to fit your application, then expand the router with new modules as needed.
