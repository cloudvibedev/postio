use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use serde_json::Value;
use tracing::{info, info_span, Instrument};
use uuid::Uuid;

use crate::{
    bridge::{
        config::{BridgeConfig, RouteConfig},
        dispatcher::DispatchRequest,
        template::TemplateContext,
    },
    error::ApiError,
    state::AppState,
};

pub fn router(config: &BridgeConfig) -> Router<AppState> {
    let mut router = Router::new();
    for route in config
        .routes
        .iter()
        .filter(|route| route.method.eq_ignore_ascii_case("POST"))
    {
        let route_config = Arc::new(route.clone());
        router = router.route(
            &route.path,
            post(move |state, params, query, uri, headers, body| {
                handle_ingest(
                    state,
                    params,
                    query,
                    uri,
                    headers,
                    body,
                    Arc::clone(&route_config),
                )
            }),
        );
    }
    router
}

async fn handle_ingest(
    State(state): State<AppState>,
    Path(params): Path<BTreeMap<String, String>>,
    Query(query): Query<BTreeMap<String, String>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
    route: Arc<RouteConfig>,
) -> Result<impl IntoResponse, ApiError> {
    let request_id = Uuid::new_v4().to_string();
    let route_id = route.id.clone();
    let sink_type = route.sink.type_name();
    let request_path = uri.path().to_string();
    let request_body_bytes = body.len();
    let ingest_span = info_span!(
        "postio.ingest.request",
        request_id = %request_id,
        route_id = %route_id,
        sink_type = %sink_type,
        http.method = "POST",
        http.route = %route.path,
        http.target = %uri,
        request.body_bytes = request_body_bytes,
        response.status_code = tracing::field::Empty,
        sink.status = tracing::field::Empty,
    );

    async move {
        info!("ingest request received");
        let body = {
            let _span = info_span!("postio.ingest.parse_body").entered();
            parse_body(&body)?
        };
        let body_kind = match &body {
            Value::Null => "empty",
            Value::Object(_) => "json_object",
            Value::Array(_) => "json_array",
            Value::String(_) => "text",
            Value::Bool(_) | Value::Number(_) => "json_scalar",
        };
        info!(body.kind = body_kind, "request body parsed");

        let template_context = {
            let _span = info_span!(
                "postio.ingest.build_context",
                path.params_count = params.len(),
                query.params_count = query.len(),
                headers.count = headers.len()
            )
            .entered();
            TemplateContext {
                params,
                query,
                headers: headers_to_map(&headers),
                body: body.clone(),
                context: BTreeMap::from([
                    ("requestId".to_string(), request_id.clone()),
                    ("timestamp".to_string(), chrono::Utc::now().to_rfc3339()),
                    ("method".to_string(), "POST".to_string()),
                    ("path".to_string(), request_path),
                    ("routeId".to_string(), route_id.clone()),
                ]),
            }
        };

        let response = state
            .dispatcher()
            .dispatch(DispatchRequest {
                route: (*route).clone(),
                template_context,
                body,
            })
            .instrument(info_span!(
                "postio.ingest.dispatch",
                route_id = %route_id,
                sink_type = %sink_type
            ))
            .await?;
        let status = if response.status == "created" {
            StatusCode::CREATED
        } else {
            StatusCode::ACCEPTED
        };
        tracing::Span::current().record("response.status_code", status.as_u16());
        tracing::Span::current().record("sink.status", response.status.as_str());
        info!(
            http.status_code = status.as_u16(),
            sink.status = response.status.as_str(),
            sink.message_id = response.message_id.as_deref(),
            sink.bucket = response.bucket.as_deref(),
            sink.key = response.key.as_deref(),
            "ingest request completed"
        );

        Ok((status, Json(response)))
    }
    .instrument(ingest_span)
    .await
}

fn parse_body(body: &[u8]) -> Result<Value, ApiError> {
    if body.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(body)
        .or_else(|_| String::from_utf8(body.to_vec()).map(Value::String))
        .map_err(|_| ApiError::BadRequest("request body must be valid utf-8".to_string()))
}

fn headers_to_map(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(key, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (key.as_str().to_string(), value.to_string()))
        })
        .collect()
}
