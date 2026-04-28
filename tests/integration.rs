use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};

use aws_sdk_sqs::{
    config::{Credentials, Region},
    Client as SqsClient,
};
use axum::{
    body::Body,
    extract::State as AxumState,
    http::{Method, Request, StatusCode},
    response::Response,
    routing::post,
    Json, Router,
};
use http_body_util::BodyExt;
use postio::{
    bridge::{
        config::{BridgeConfig, RouteConfig, SinkConfig},
        dispatcher::{DispatchRequest, DispatchResponse, SinkDispatcher},
    },
    config::{otel_enabled_from_env, AppConfig, CorsConfig, DEFAULT_BODY_LIMIT_BYTES},
    libs::telemetry,
    pipeline::{
        config::{PipelineConfig, SourceConfig, TargetConfig},
        resources::PipelineResources,
        runtime::PipelineRuntime,
    },
    routes::create_router,
    state::AppState,
};
use serde_json::Value;
use tokio::sync::OnceCell;
use tower::ServiceExt;
use tracing::info_span;

async fn setup_router() -> Router {
    setup_router_with_bridge_config(
        BridgeConfig {
            routes: Vec::new(),
            pipeline: None,
        },
        Arc::new(NoopDispatcher),
    )
    .await
}

async fn setup_router_with_bridge_config(
    bridge_config: BridgeConfig,
    dispatcher: Arc<dyn SinkDispatcher>,
) -> Router {
    init_telemetry().await;
    let state = match bridge_config
        .pipeline
        .clone()
        .filter(|pipeline| pipeline.enabled)
    {
        Some(pipeline) => AppState::with_pipeline(
            dispatcher,
            PipelineRuntime::spawn(
                pipeline,
                PipelineResources::new(test_sqs_client(), reqwest::Client::new()),
            ),
        ),
        None => AppState::new(dispatcher),
    };
    let config = AppConfig {
        host: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        cors: CorsConfig::Permissive,
        body_limit_bytes: DEFAULT_BODY_LIMIT_BYTES,
        otel_enabled: otel_enabled_from_env(),
        bridge_config_path: "config/example.yaml".to_string(),
    };

    create_router(state, &config, &bridge_config)
}

fn test_sqs_client() -> SqsClient {
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::for_tests())
        .endpoint_url("http://127.0.0.1:4566")
        .build();
    SqsClient::from_conf(config)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_ingest_route_dispatches_to_sink() {
    let dispatcher = Arc::new(RecordingDispatcher::default());
    let bridge_config = BridgeConfig {
        routes: vec![RouteConfig {
            id: "events".to_string(),
            method: "POST".to_string(),
            path: "/events/{topic}".to_string(),
            sink: SinkConfig::Sns {
                topic: Some("{{ params.topic }}".to_string()),
                topic_arn: None,
                subject: None,
                message: None,
                attributes: None,
            },
        }],
        pipeline: None,
    };
    let router = setup_router_with_bridge_config(bridge_config, dispatcher.clone()).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/events/orders?source=test")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"hello":"world"}"#))
                .expect("build request"),
        )
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = response_json(response).await;
    assert_eq!(body["routeId"], "events");
    assert_eq!(body["sink"], "test");
    let calls = dispatcher.calls.lock().expect("calls");
    assert_eq!(calls[0].template_context.params["topic"], "orders");
    assert_eq!(calls[0].template_context.query["source"], "test");
    assert_eq!(calls[0].body["hello"], "world");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multipart_ingest_route_extracts_form_fields_and_file() {
    let dispatcher = Arc::new(RecordingDispatcher::default());
    let bridge_config = BridgeConfig {
        routes: vec![RouteConfig {
            id: "upload".to_string(),
            method: "POST".to_string(),
            path: "/upload/{bucket}".to_string(),
            sink: SinkConfig::S3 {
                bucket: "{{ params.bucket }}".to_string(),
                key: "{{ form.tenant }}/{{ file.filename }}".to_string(),
                content_type: None,
                object: None,
                metadata: None,
            },
        }],
        pipeline: None,
    };
    let router = setup_router_with_bridge_config(bridge_config, dispatcher.clone()).await;
    let boundary = "postio-test-boundary";
    let body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"tenant\"\r\n\
         \r\n\
         acme\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"file\"; filename=\"hello.txt\"\r\n\
         Content-Type: text/plain\r\n\
         \r\n\
         hello from multipart\r\n\
         --{boundary}--\r\n"
    );

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/upload/archive")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .expect("build request"),
        )
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let calls = dispatcher.calls.lock().expect("calls");
    let call = calls.first().expect("dispatch call");
    assert_eq!(call.template_context.params["bucket"], "archive");
    assert_eq!(call.template_context.form["tenant"], "acme");
    assert_eq!(call.template_context.file["filename"], "hello.txt");
    assert_eq!(call.template_context.file["contentType"], "text/plain");
    assert_eq!(call.body["form"]["tenant"], "acme");
    assert_eq!(call.body["file"]["filename"], "hello.txt");
    let file = call.file.as_ref().expect("uploaded file");
    assert_eq!(file.field_name.as_deref(), Some("file"));
    assert_eq!(file.file_name.as_deref(), Some("hello.txt"));
    assert_eq!(file.content_type.as_deref(), Some("text/plain"));
    assert_eq!(file.bytes.as_ref(), b"hello from multipart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_pipeline_sends_payload_to_http_target() {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let target_app = Router::new()
        .route("/target", post(capture_pipeline_target))
        .with_state(captured.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind target server");
    let target_addr = listener.local_addr().expect("target addr");
    tokio::spawn(async move {
        axum::serve(listener, target_app)
            .await
            .expect("target server failed");
    });

    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "http-to-http".to_string(),
            enabled: true,
            source: SourceConfig::Http {
                method: "POST".to_string(),
                path: "/pipe/{tenant}".to_string(),
            },
            target: TargetConfig::Http {
                method: "POST".to_string(),
                url: format!("http://{target_addr}/target"),
                headers: None,
                timeout_ms: Some(1000),
            },
        }),
    };
    let router = setup_router_with_bridge_config(bridge_config, Arc::new(NoopDispatcher)).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/pipe/acme?source=test")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"hello":"pipeline"}"#))
                .expect("build request"),
        )
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["pipelineId"], "http-to-http");
    assert_eq!(body["status"], "accepted");
    assert_eq!(body["targetType"], "http");
    assert_eq!(body["targetStatusCode"], 200);
    assert_eq!(body["body"], r#"{"received":true}"#);
    let captured = captured.lock().await;
    assert_eq!(captured.as_slice(), [r#"{"hello":"pipeline"}"#]);
}

async fn capture_pipeline_target(
    AxumState(captured): AxumState<Arc<tokio::sync::Mutex<Vec<String>>>>,
    body: String,
) -> Json<Value> {
    captured.lock().await.push(body);
    Json(serde_json::json!({ "received": true }))
}

struct NoopDispatcher;

#[async_trait::async_trait]
impl SinkDispatcher for NoopDispatcher {
    async fn dispatch(&self, request: DispatchRequest) -> anyhow::Result<DispatchResponse> {
        Ok(response_for(request, "test"))
    }
}

#[derive(Default)]
struct RecordingDispatcher {
    calls: Mutex<Vec<DispatchRequest>>,
}

#[async_trait::async_trait]
impl SinkDispatcher for RecordingDispatcher {
    async fn dispatch(&self, request: DispatchRequest) -> anyhow::Result<DispatchResponse> {
        self.calls.lock().expect("calls").push(request.clone());
        Ok(response_for(request, "test"))
    }
}

fn response_for(request: DispatchRequest, sink: &str) -> DispatchResponse {
    DispatchResponse {
        route_id: request.route.id,
        sink: sink.to_string(),
        status: "accepted".to_string(),
        message_id: Some("message-1".to_string()),
        bucket: None,
        key: None,
        etag: None,
    }
}
