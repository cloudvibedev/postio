use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use opentelemetry::{global, propagation::Extractor};
use tracing::{info, info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    bridge::config::BridgeConfig,
    error::ApiError,
    pipeline::{config::SourceConfig, model::CompletionResponse},
    state::AppState,
};

pub fn router(config: &BridgeConfig) -> Router<AppState> {
    let Some(pipeline) = config.pipeline.as_ref().filter(|pipeline| pipeline.enabled) else {
        return Router::new();
    };
    let SourceConfig::Http(source) = &pipeline.source else {
        return Router::new();
    };
    if !source.method.eq_ignore_ascii_case("POST") {
        return Router::new();
    }

    let pipeline_id = Arc::new(pipeline.id.clone());
    Router::new().route(
        &source.path,
        post(move |state, params, query, uri, headers, body| {
            handle_pipeline_http(
                state,
                params,
                query,
                uri,
                headers,
                body,
                Arc::clone(&pipeline_id),
            )
        }),
    )
}

async fn handle_pipeline_http(
    State(state): State<AppState>,
    Path(params): Path<BTreeMap<String, String>>,
    Query(query): Query<BTreeMap<String, String>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
    pipeline_id: Arc<String>,
) -> Result<impl IntoResponse, ApiError> {
    let runtime = state
        .pipeline()
        .ok_or_else(|| ApiError::Unexpected(anyhow::anyhow!("pipeline runtime is not enabled")))?;
    let request_body_bytes = body.len();
    let span = info_span!(
        "postio.pipeline.http.request",
        pipeline.id = %pipeline_id,
        http.method = "POST",
        http.target = %uri,
        request.body_bytes = request_body_bytes,
        response.status_code = tracing::field::Empty,
        pipeline.status = tracing::field::Empty,
    );
    let _ = span.set_parent(extract_trace_context(&headers));

    async move {
        info!("pipeline http request received");
        let response = runtime
            .submit_http(params, query, headers, body, uri.path().to_string())
            .instrument(info_span!(
                "postio.pipeline.http.submit",
                pipeline.id = %pipeline_id
            ))
            .await?;
        let status = http_status_for_response(&response);
        let body = response
            .http_body
            .clone()
            .unwrap_or_else(|| serde_json::to_value(&response).expect("completion serializes"));
        tracing::Span::current().record("response.status_code", status.as_u16());
        tracing::Span::current().record("pipeline.status", response.status.as_str());
        Ok((status, Json(body)))
    }
    .instrument(span)
    .await
}

fn http_status_for_response(response: &CompletionResponse) -> StatusCode {
    if let Some(status) = response
        .http_status_code
        .and_then(|status| StatusCode::from_u16(status).ok())
    {
        return status;
    }

    if response.status == "rejected" {
        return StatusCode::UNPROCESSABLE_ENTITY;
    }

    if response.status == "failed" {
        return StatusCode::BAD_GATEWAY;
    }

    match response.target_type.as_deref() {
        Some("http") => response
            .target_status_code
            .and_then(|status| StatusCode::from_u16(status).ok())
            .unwrap_or(StatusCode::OK),
        _ => StatusCode::ACCEPTED,
    }
}

fn extract_trace_context(headers: &HeaderMap) -> opentelemetry::Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)))
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}
