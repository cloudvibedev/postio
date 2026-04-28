use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use tracing::{info, info_span, Instrument};

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
    let SourceConfig::Http { method, path } = &pipeline.source else {
        return Router::new();
    };
    if !method.eq_ignore_ascii_case("POST") {
        return Router::new();
    }

    let pipeline_id = Arc::new(pipeline.id.clone());
    Router::new().route(
        path,
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
        tracing::Span::current().record("response.status_code", status.as_u16());
        tracing::Span::current().record("pipeline.status", response.status.as_str());
        Ok((status, Json(response)))
    }
    .instrument(span)
    .await
}

fn http_status_for_response(response: &CompletionResponse) -> StatusCode {
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
