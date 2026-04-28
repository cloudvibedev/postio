use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_sqs::{
    config::{Credentials, Region},
    Client as SqsClient,
};
use axum::{
    body::{Body, Bytes},
    extract::State as AxumState,
    http::{HeaderMap, Method, Request, StatusCode, Uri},
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
        config::{
            HttpSourceConfig, HttpTargetConfig, PipelineConfig, SourceConfig, SqsSourceConfig,
            SqsTargetConfig, TargetConfig, TemplateTransformConfig, TransformConfig,
            TransformTemplateOutput,
        },
        resources::PipelineResources,
        runtime::PipelineRuntime,
    },
    routes::create_router,
    state::AppState,
};
use serde_json::{json, Value};
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
    setup_router_with_sqs_client(bridge_config, dispatcher, test_sqs_client()).await
}

async fn setup_router_with_sqs_client(
    bridge_config: BridgeConfig,
    dispatcher: Arc<dyn SinkDispatcher>,
    sqs: SqsClient,
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
                PipelineResources::new(sqs, reqwest::Client::new()),
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

fn test_sqs_client_for(endpoint_url: String) -> SqsClient {
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::for_tests())
        .endpoint_url(endpoint_url)
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

    let status = response.status();
    let body = response_json(response).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
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
    let target_addr = spawn_http_target(captured.clone()).await;

    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "http-to-http".to_string(),
            enabled: true,
            source: SourceConfig::Http(HttpSourceConfig {
                method: "POST".to_string(),
                path: "/pipe/{tenant}".to_string(),
            }),
            transform: None,
            target: TargetConfig::Http(HttpTargetConfig {
                method: "POST".to_string(),
                url: format!("http://{target_addr}/target"),
                headers: None,
                timeout_ms: Some(1000),
            }),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_pipeline_sends_payload_to_sqs_target() {
    let sqs = MockSqs::spawn(vec![]).await;
    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "http-to-sqs".to_string(),
            enabled: true,
            source: SourceConfig::Http(HttpSourceConfig {
                method: "POST".to_string(),
                path: "/pipe".to_string(),
            }),
            transform: None,
            target: TargetConfig::Sqs(SqsTargetConfig {
                queue: None,
                queue_url: Some(sqs.queue_url("output")),
                delay_seconds: Some(3),
            }),
        }),
    };
    let router = setup_router_with_sqs_client(
        bridge_config,
        Arc::new(NoopDispatcher),
        test_sqs_client_for(sqs.endpoint_url()),
    )
    .await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/pipe")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"hello":"sqs"}"#))
                .expect("build request"),
        )
        .await
        .expect("request failed");

    let status = response.status();
    let body = response_json(response).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["pipelineId"], "http-to-sqs");
    assert_eq!(body["status"], "accepted");
    assert_eq!(body["targetType"], "sqs");
    assert_eq!(body["targetStatusCode"], 202);
    assert_eq!(body["messageId"], "mock-sent-1");
    let sent = sqs.sent_messages().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].queue_url, sqs.queue_url("output"));
    assert_eq!(sent[0].body, r#"{"hello":"sqs"}"#);
    assert_eq!(sent[0].delay_seconds, Some(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_pipeline_template_transform_sends_payload_to_sqs_target() {
    let sqs = MockSqs::spawn(vec![]).await;
    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "http-to-sqs-template".to_string(),
            enabled: true,
            source: SourceConfig::Http(HttpSourceConfig {
                method: "POST".to_string(),
                path: "/pipe/{tenant}".to_string(),
            }),
            transform: Some(TransformConfig::Template(TemplateTransformConfig {
                output: TransformTemplateOutput {
                    attributes: Some(BTreeMap::from([
                        ("event".to_string(), "{{ body.event }}".into()),
                        ("tenant".to_string(), "{{ params.tenant }}".into()),
                        ("priority".to_string(), json!("{{ body.priority }}")),
                    ])),
                    body: Some(json!({
                        "event": "{{ body.event }}",
                        "tenant": "{{ params.tenant }}",
                        "source": "{{ query.source }}",
                        "requestId": "{{ context.requestId }}",
                        "original": "{{ body }}"
                    })),
                    ..TransformTemplateOutput::default()
                },
            })),
            target: TargetConfig::Sqs(SqsTargetConfig {
                queue: None,
                queue_url: Some(sqs.queue_url("output")),
                delay_seconds: None,
            }),
        }),
    };
    let router = setup_router_with_sqs_client(
        bridge_config,
        Arc::new(NoopDispatcher),
        test_sqs_client_for(sqs.endpoint_url()),
    )
    .await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/pipe/acme?source=test")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"event":"order.created","priority":3,"total":42}"#,
                ))
                .expect("build request"),
        )
        .await
        .expect("request failed");

    let status = response.status();
    let body = response_json(response).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["status"], "accepted");
    let sent = sqs.sent_messages().await;
    assert_eq!(sent.len(), 1);
    let transformed: Value = serde_json::from_str(&sent[0].body).expect("sqs body should be json");
    assert_eq!(transformed["event"], "order.created");
    assert_eq!(transformed["tenant"], "acme");
    assert_eq!(transformed["source"], "test");
    assert_eq!(transformed["original"]["total"], 42);
    assert_eq!(sent[0].attributes["event"], "order.created");
    assert_eq!(sent[0].attributes["tenant"], "acme");
    assert_eq!(sent[0].attributes["priority"], "3");
    assert!(transformed["requestId"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_pipeline_template_transform_sends_payload_and_headers_to_http_target() {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<CapturedHttpRequest>::new()));
    let target_addr = spawn_http_target_with_headers(captured.clone()).await;

    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "http-to-http-template".to_string(),
            enabled: true,
            source: SourceConfig::Http(HttpSourceConfig {
                method: "POST".to_string(),
                path: "/pipe".to_string(),
            }),
            transform: Some(TransformConfig::Template(TemplateTransformConfig {
                output: TransformTemplateOutput {
                    headers: Some(BTreeMap::from([(
                        "x-postio-event".to_string(),
                        "{{ body.event }}".to_string(),
                    )])),
                    query: Some(BTreeMap::from([
                        ("event".to_string(), "{{ body.event }}".into()),
                        ("priority".to_string(), json!("{{ body.priority }}")),
                        ("source".to_string(), "{{ headers.x-source }}".into()),
                    ])),
                    body: Some(json!({
                        "event": "{{ body.event }}",
                        "source": "{{ headers.x-source }}"
                    })),
                    ..TransformTemplateOutput::default()
                },
            })),
            target: TargetConfig::Http(HttpTargetConfig {
                method: "POST".to_string(),
                url: format!("http://{target_addr}/target"),
                headers: None,
                timeout_ms: Some(1000),
            }),
        }),
    };
    let router = setup_router_with_bridge_config(bridge_config, Arc::new(NoopDispatcher)).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/pipe")
                .header("content-type", "application/json")
                .header("x-source", "integration")
                .body(Body::from(r#"{"event":"invoice.paid","priority":3}"#))
                .expect("build request"),
        )
        .await
        .expect("request failed");

    assert_eq!(response.status(), StatusCode::OK);
    let captured = captured.lock().await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].header.as_deref(), Some("invoice.paid"));
    assert_eq!(
        captured[0].query.as_deref(),
        Some("event=invoice.paid&priority=3&source=integration")
    );
    assert_eq!(
        captured[0].body,
        r#"{"event":"invoice.paid","source":"integration"}"#
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_pipeline_reports_failed_http_target() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind unused target port");
    let target_addr = listener.local_addr().expect("target addr");
    drop(listener);

    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "http-to-failed-http".to_string(),
            enabled: true,
            source: SourceConfig::Http(HttpSourceConfig {
                method: "POST".to_string(),
                path: "/pipe".to_string(),
            }),
            transform: None,
            target: TargetConfig::Http(HttpTargetConfig {
                method: "POST".to_string(),
                url: format!("http://{target_addr}/target"),
                headers: None,
                timeout_ms: Some(200),
            }),
        }),
    };
    let router = setup_router_with_bridge_config(bridge_config, Arc::new(NoopDispatcher)).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/pipe")
                .body(Body::from(r#"{"hello":"failure"}"#))
                .expect("build request"),
        )
        .await
        .expect("request failed");

    let status = response.status();
    let body = response_json(response).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert_eq!(body["pipelineId"], "http-to-failed-http");
    assert_eq!(body["status"], "failed");
    assert!(
        body["error"]
            .as_str()
            .expect("error message")
            .contains("failed to send http target"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_pipeline_sends_payload_to_http_target_and_deletes_source_message() {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let target_addr = spawn_http_target(captured.clone()).await;
    let sqs = MockSqs::spawn(vec![MockSqsMessage {
        message_id: "source-message-1".to_string(),
        receipt_handle: "receipt-1".to_string(),
        body: r#"{"from":"sqs"}"#.to_string(),
    }])
    .await;
    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "sqs-to-http".to_string(),
            enabled: true,
            source: SourceConfig::Sqs(SqsSourceConfig {
                queue: None,
                queue_url: Some(sqs.queue_url("input")),
                batch_size: 1,
                wait_time_seconds: 0,
                visibility_timeout_seconds: Some(5),
            }),
            transform: None,
            target: TargetConfig::Http(HttpTargetConfig {
                method: "POST".to_string(),
                url: format!("http://{target_addr}/target"),
                headers: None,
                timeout_ms: Some(1000),
            }),
        }),
    };

    let _router = setup_router_with_sqs_client(
        bridge_config,
        Arc::new(NoopDispatcher),
        test_sqs_client_for(sqs.endpoint_url()),
    )
    .await;

    wait_until(Duration::from_secs(3), || {
        let captured = captured.clone();
        async move { !captured.lock().await.is_empty() }
    })
    .await;
    assert_eq!(captured.lock().await.as_slice(), [r#"{"from":"sqs"}"#]);
    wait_until(Duration::from_secs(3), || {
        let sqs = sqs.clone();
        async move {
            sqs.deleted_receipts()
                .await
                .contains(&"receipt-1".to_string())
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_pipeline_template_transform_sends_payload_and_headers_to_http_target() {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<CapturedHttpRequest>::new()));
    let target_addr = spawn_http_target_with_headers(captured.clone()).await;
    let sqs = MockSqs::spawn(vec![MockSqsMessage {
        message_id: "source-message-1".to_string(),
        receipt_handle: "receipt-1".to_string(),
        body: r#"{"event":"order.created","order":{"id":"ord-1","total":99.9}}"#.to_string(),
    }])
    .await;
    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "sqs-to-http-template".to_string(),
            enabled: true,
            source: SourceConfig::Sqs(SqsSourceConfig {
                queue: None,
                queue_url: Some(sqs.queue_url("input")),
                batch_size: 1,
                wait_time_seconds: 0,
                visibility_timeout_seconds: Some(5),
            }),
            transform: Some(TransformConfig::Template(TemplateTransformConfig {
                output: TransformTemplateOutput {
                    headers: Some(BTreeMap::from([(
                        "x-postio-event".to_string(),
                        "{{ body.event }}".to_string(),
                    )])),
                    body: Some(json!({
                        "event": "{{ body.event }}",
                        "orderId": "{{ body.order.id }}",
                        "total": "{{ body.order.total }}",
                        "sourceType": "{{ context.sourceType }}",
                        "requestId": "{{ context.requestId }}",
                        "original": "{{ body }}"
                    })),
                    ..TransformTemplateOutput::default()
                },
            })),
            target: TargetConfig::Http(HttpTargetConfig {
                method: "POST".to_string(),
                url: format!("http://{target_addr}/target"),
                headers: None,
                timeout_ms: Some(1000),
            }),
        }),
    };

    let _router = setup_router_with_sqs_client(
        bridge_config,
        Arc::new(NoopDispatcher),
        test_sqs_client_for(sqs.endpoint_url()),
    )
    .await;

    wait_until(Duration::from_secs(3), || {
        let captured = captured.clone();
        async move { !captured.lock().await.is_empty() }
    })
    .await;

    let captured = captured.lock().await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].header.as_deref(), Some("order.created"));
    let transformed: Value =
        serde_json::from_str(&captured[0].body).expect("http target body should be json");
    assert_eq!(transformed["event"], "order.created");
    assert_eq!(transformed["orderId"], "ord-1");
    assert_eq!(transformed["total"], 99.9);
    assert_eq!(transformed["sourceType"], "sqs");
    assert_eq!(transformed["original"]["order"]["id"], "ord-1");
    assert!(transformed["requestId"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
    drop(captured);

    wait_until(Duration::from_secs(3), || {
        let sqs = sqs.clone();
        async move {
            sqs.deleted_receipts()
                .await
                .contains(&"receipt-1".to_string())
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_pipeline_sends_payload_to_sqs_target_and_deletes_source_message() {
    let sqs = MockSqs::spawn(vec![MockSqsMessage {
        message_id: "source-message-1".to_string(),
        receipt_handle: "receipt-1".to_string(),
        body: r#"{"relay":"sqs"}"#.to_string(),
    }])
    .await;
    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "sqs-to-sqs".to_string(),
            enabled: true,
            source: SourceConfig::Sqs(SqsSourceConfig {
                queue: None,
                queue_url: Some(sqs.queue_url("input")),
                batch_size: 1,
                wait_time_seconds: 0,
                visibility_timeout_seconds: None,
            }),
            transform: None,
            target: TargetConfig::Sqs(SqsTargetConfig {
                queue: None,
                queue_url: Some(sqs.queue_url("output")),
                delay_seconds: None,
            }),
        }),
    };

    let _router = setup_router_with_sqs_client(
        bridge_config,
        Arc::new(NoopDispatcher),
        test_sqs_client_for(sqs.endpoint_url()),
    )
    .await;

    wait_until(Duration::from_secs(3), || {
        let sqs = sqs.clone();
        async move { !sqs.sent_messages().await.is_empty() }
    })
    .await;
    let sent = sqs.sent_messages().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].queue_url, sqs.queue_url("output"));
    assert_eq!(sent[0].body, r#"{"relay":"sqs"}"#);
    wait_until(Duration::from_secs(3), || {
        let sqs = sqs.clone();
        async move {
            sqs.deleted_receipts()
                .await
                .contains(&"receipt-1".to_string())
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqs_pipeline_template_transform_sends_attributes_to_sqs_target() {
    let sqs = MockSqs::spawn(vec![MockSqsMessage {
        message_id: "source-message-1".to_string(),
        receipt_handle: "receipt-1".to_string(),
        body: r#"{"event":"order.relayed","tenant":"acme","priority":2}"#.to_string(),
    }])
    .await;
    let bridge_config = BridgeConfig {
        routes: Vec::new(),
        pipeline: Some(PipelineConfig {
            id: "sqs-to-sqs-template".to_string(),
            enabled: true,
            source: SourceConfig::Sqs(SqsSourceConfig {
                queue: None,
                queue_url: Some(sqs.queue_url("input")),
                batch_size: 1,
                wait_time_seconds: 0,
                visibility_timeout_seconds: None,
            }),
            transform: Some(TransformConfig::Template(TemplateTransformConfig {
                output: TransformTemplateOutput {
                    attributes: Some(BTreeMap::from([
                        ("event".to_string(), "{{ body.event }}".into()),
                        ("tenant".to_string(), "{{ body.tenant }}".into()),
                        ("priority".to_string(), json!("{{ body.priority }}")),
                    ])),
                    body: Some(json!({
                        "event": "{{ body.event }}",
                        "tenant": "{{ body.tenant }}",
                        "sourceType": "{{ context.sourceType }}",
                        "original": "{{ body }}"
                    })),
                    ..TransformTemplateOutput::default()
                },
            })),
            target: TargetConfig::Sqs(SqsTargetConfig {
                queue: None,
                queue_url: Some(sqs.queue_url("output")),
                delay_seconds: None,
            }),
        }),
    };

    let _router = setup_router_with_sqs_client(
        bridge_config,
        Arc::new(NoopDispatcher),
        test_sqs_client_for(sqs.endpoint_url()),
    )
    .await;

    wait_until(Duration::from_secs(3), || {
        let sqs = sqs.clone();
        async move { !sqs.sent_messages().await.is_empty() }
    })
    .await;
    let sent = sqs.sent_messages().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].queue_url, sqs.queue_url("output"));
    let transformed: Value = serde_json::from_str(&sent[0].body).expect("sqs body should be json");
    assert_eq!(transformed["event"], "order.relayed");
    assert_eq!(transformed["tenant"], "acme");
    assert_eq!(transformed["sourceType"], "sqs");
    assert_eq!(sent[0].attributes["event"], "order.relayed");
    assert_eq!(sent[0].attributes["tenant"], "acme");
    assert_eq!(sent[0].attributes["priority"], "2");
    wait_until(Duration::from_secs(3), || {
        let sqs = sqs.clone();
        async move {
            sqs.deleted_receipts()
                .await
                .contains(&"receipt-1".to_string())
        }
    })
    .await;
}

async fn spawn_http_target(captured: Arc<tokio::sync::Mutex<Vec<String>>>) -> SocketAddr {
    let target_app = Router::new()
        .route("/target", post(capture_pipeline_target))
        .with_state(captured);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind target server");
    let target_addr = listener.local_addr().expect("target addr");
    tokio::spawn(async move {
        axum::serve(listener, target_app)
            .await
            .expect("target server failed");
    });
    target_addr
}

async fn capture_pipeline_target(
    AxumState(captured): AxumState<Arc<tokio::sync::Mutex<Vec<String>>>>,
    body: String,
) -> Json<Value> {
    captured.lock().await.push(body);
    Json(serde_json::json!({ "received": true }))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CapturedHttpRequest {
    body: String,
    header: Option<String>,
    query: Option<String>,
}

async fn spawn_http_target_with_headers(
    captured: Arc<tokio::sync::Mutex<Vec<CapturedHttpRequest>>>,
) -> SocketAddr {
    let target_app = Router::new()
        .route("/target", post(capture_pipeline_target_with_headers))
        .with_state(captured);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind target server");
    let target_addr = listener.local_addr().expect("target addr");
    tokio::spawn(async move {
        axum::serve(listener, target_app)
            .await
            .expect("target server failed");
    });
    target_addr
}

async fn capture_pipeline_target_with_headers(
    AxumState(captured): AxumState<Arc<tokio::sync::Mutex<Vec<CapturedHttpRequest>>>>,
    uri: Uri,
    headers: HeaderMap,
    body: String,
) -> Json<Value> {
    captured.lock().await.push(CapturedHttpRequest {
        body,
        header: headers
            .get("x-postio-event")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string),
        query: uri.query().map(ToString::to_string),
    });
    Json(serde_json::json!({ "received": true }))
}

#[derive(Clone)]
struct MockSqs {
    addr: SocketAddr,
    state: Arc<MockSqsState>,
}

#[derive(Default)]
struct MockSqsState {
    messages: tokio::sync::Mutex<VecDeque<MockSqsMessage>>,
    sent: tokio::sync::Mutex<Vec<MockSqsSend>>,
    deleted: tokio::sync::Mutex<Vec<String>>,
}

#[derive(Clone)]
struct MockSqsMessage {
    message_id: String,
    receipt_handle: String,
    body: String,
}

#[derive(Clone, Debug)]
struct MockSqsSend {
    queue_url: String,
    body: String,
    delay_seconds: Option<i64>,
    attributes: BTreeMap<String, String>,
}

impl MockSqs {
    async fn spawn(messages: Vec<MockSqsMessage>) -> Self {
        let state = Arc::new(MockSqsState {
            messages: tokio::sync::Mutex::new(messages.into()),
            sent: tokio::sync::Mutex::new(Vec::new()),
            deleted: tokio::sync::Mutex::new(Vec::new()),
        });
        let app = Router::new()
            .route("/", post(handle_mock_sqs))
            .fallback(post(handle_mock_sqs))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock sqs");
        let addr = listener.local_addr().expect("mock sqs addr");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock sqs server failed");
        });
        Self { addr, state }
    }

    fn endpoint_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn queue_url(&self, queue: &str) -> String {
        format!("http://{}/000000000000/{queue}", self.addr)
    }

    async fn sent_messages(&self) -> Vec<MockSqsSend> {
        self.state.sent.lock().await.clone()
    }

    async fn deleted_receipts(&self) -> Vec<String> {
        self.state.deleted.lock().await.clone()
    }
}

async fn handle_mock_sqs(
    AxumState(state): AxumState<Arc<MockSqsState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Json<Value> {
    let body: Value = serde_json::from_slice(&body).expect("mock sqs request must be json");
    let target = headers
        .get("x-amz-target")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    match target {
        "AmazonSQS.SendMessage" => {
            let mut sent = state.sent.lock().await;
            let message_id = format!("mock-sent-{}", sent.len() + 1);
            sent.push(MockSqsSend {
                queue_url: body["QueueUrl"].as_str().unwrap_or_default().to_string(),
                body: body["MessageBody"].as_str().unwrap_or_default().to_string(),
                delay_seconds: body["DelaySeconds"].as_i64(),
                attributes: sqs_message_attributes_from_request(&body),
            });
            Json(serde_json::json!({
                "MD5OfMessageBody": "mock-md5",
                "MessageId": message_id
            }))
        }
        "AmazonSQS.ReceiveMessage" => {
            let message = state.messages.lock().await.pop_front();
            let messages = message
                .map(|message| {
                    vec![serde_json::json!({
                        "MessageId": message.message_id,
                        "ReceiptHandle": message.receipt_handle,
                        "Body": message.body
                    })]
                })
                .unwrap_or_default();
            Json(serde_json::json!({ "Messages": messages }))
        }
        "AmazonSQS.DeleteMessage" => {
            if let Some(receipt_handle) = body["ReceiptHandle"].as_str() {
                state.deleted.lock().await.push(receipt_handle.to_string());
            }
            Json(serde_json::json!({}))
        }
        "AmazonSQS.GetQueueUrl" => {
            let queue_name = body["QueueName"].as_str().unwrap_or("queue");
            Json(serde_json::json!({
                "QueueUrl": format!("http://mock-sqs/000000000000/{queue_name}")
            }))
        }
        _ => Json(serde_json::json!({})),
    }
}

fn sqs_message_attributes_from_request(body: &Value) -> BTreeMap<String, String> {
    body["MessageAttributes"]
        .as_object()
        .into_iter()
        .flat_map(|attributes| attributes.iter())
        .filter_map(|(key, value)| {
            value["StringValue"]
                .as_str()
                .map(|value| (key.clone(), value.to_string()))
        })
        .collect()
}

async fn wait_until<F, Fut>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(timeout, async {
        loop {
            if condition().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("condition timed out");
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
