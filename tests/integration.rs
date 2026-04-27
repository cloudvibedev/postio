use std::net::{IpAddr, Ipv4Addr};

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    response::Response,
    Router,
};
use http_body_util::BodyExt;
use postio::{
    config::{otel_enabled_from_env, AppConfig, CorsConfig, DEFAULT_BODY_LIMIT_BYTES},
    libs::telemetry,
    routes::create_router,
    state::AppState,
};
use serde_json::Value;
use tokio::sync::OnceCell;
use tower::ServiceExt;
use tracing::info_span;

async fn setup_router() -> Router {
    init_telemetry().await;
    let state = AppState::new();
    let config = AppConfig {
        host: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        cors: CorsConfig::Permissive,
        body_limit_bytes: DEFAULT_BODY_LIMIT_BYTES,
        otel_enabled: otel_enabled_from_env(),
    };

    create_router(state, &config)
}

static TELEMETRY_GUARD: OnceCell<telemetry::TelemetryGuard> = OnceCell::const_new();
static ENV_LOADED: OnceCell<()> = OnceCell::const_new();

async fn response_json(response: Response) -> Value {
    let body = response
        .into_body()
        .collect()
        .await
        .expect("failed to read response body")
        .to_bytes();
    serde_json::from_slice(&body).expect("failed to parse json response")
}

async fn init_telemetry() {
    load_env().await;
    if !otel_enabled_from_env() {
        TELEMETRY_GUARD
            .get_or_init(|| async {
                telemetry::init_tracing(false).expect("failed to init tracing")
            })
            .await;
        return;
    }

    let endpoint = otel_endpoint().await;
    if let Some(endpoint) = endpoint {
        set_env_if_missing("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
        set_env_if_missing("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint);
    }
    set_env_if_missing("OTEL_EXPORTER_OTLP_TIMEOUT", "2000");
    set_env_if_missing("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "2000");
    set_env_if_missing("OTEL_TRACES_SAMPLER", "always_on");
    set_env_if_missing("OTEL_USE_SIMPLE_EXPORTER", "true");
    set_env_if_missing("OTEL_BSP_SCHEDULE_DELAY", "200");
    set_env_if_missing(
        "OTEL_SERVICE_NAME",
        concat!(env!("CARGO_PKG_NAME"), "-tests"),
    );

    TELEMETRY_GUARD
        .get_or_init(|| async { telemetry::init_tracing(true).expect("failed to init tracing") })
        .await;
}

async fn otel_endpoint() -> Option<String> {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()
}

async fn flush_telemetry() {
    if let Some(guard) = TELEMETRY_GUARD.get() {
        guard.force_flush().expect("failed to flush telemetry");
    }
}

async fn load_env() {
    ENV_LOADED
        .get_or_init(|| async {
            dotenvy::dotenv().ok();
        })
        .await;
}

fn set_env_if_missing(key: &str, value: &str) {
    if std::env::var(key).is_err() {
        std::env::set_var(key, value);
    }
}

async fn response_bytes(response: Response) -> bytes::Bytes {
    response
        .into_body()
        .collect()
        .await
        .expect("failed to read response body")
        .to_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_returns_status() {
    let _span = info_span!("integration_test", test = "health").entered();
    let router = setup_router().await;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "ok");
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    flush_telemetry().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_routes_reflect_request() {
    let _span = info_span!("integration_test", test = "echo").entered();
    let router = setup_router().await;
    let methods = [
        Method::GET,
        Method::POST,
        Method::PUT,
        Method::PATCH,
        Method::DELETE,
        Method::OPTIONS,
    ];

    for method in methods {
        let request = Request::builder()
            .method(method.clone())
            .uri("/echo")
            .header("x-test", "value")
            .body(Body::from("payload"))
            .expect("build request");

        let response = router
            .clone()
            .oneshot(request)
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::OK);
        if method == Method::OPTIONS {
            let body = response_bytes(response).await;
            if !body.is_empty() {
                let json_body: Value =
                    serde_json::from_slice(&body).expect("failed to parse json response");
                assert_eq!(json_body["method"], method.as_str());
                assert_eq!(json_body["path"], "/echo");
                assert_eq!(json_body["body"], "payload");
                assert_eq!(json_body["headers"]["x-test"][0], "value");
            }
            continue;
        }

        let body = response_json(response).await;
        assert_eq!(body["method"], method.as_str());
        assert_eq!(body["path"], "/echo");
        assert_eq!(body["body"], "payload");
        assert_eq!(body["headers"]["x-test"][0], "value");
    }

    let head_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::HEAD)
                .uri("/echo")
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("request failed");

    assert_eq!(head_response.status(), StatusCode::OK);
    flush_telemetry().await;
}
