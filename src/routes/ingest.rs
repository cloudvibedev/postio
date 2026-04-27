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
    let body = parse_body(&body)?;
    let template_context = TemplateContext {
        params,
        query,
        headers: headers_to_map(&headers),
        body: body.clone(),
        context: BTreeMap::from([
            ("requestId".to_string(), Uuid::new_v4().to_string()),
            ("timestamp".to_string(), chrono::Utc::now().to_rfc3339()),
            ("method".to_string(), "POST".to_string()),
            ("path".to_string(), uri.path().to_string()),
            ("routeId".to_string(), route.id.clone()),
        ]),
    };

    let response = state
        .dispatcher()
        .dispatch(DispatchRequest {
            route: (*route).clone(),
            template_context,
            body,
        })
        .await?;
    let status = if response.status == "created" {
        StatusCode::CREATED
    } else {
        StatusCode::ACCEPTED
    };

    Ok((status, Json(response)))
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
