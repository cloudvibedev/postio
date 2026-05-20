use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use futures_util::stream;
use serde_json::Value;
use tracing::{info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::{
    bridge::{
        config::{BridgeConfig, RouteConfig},
        dispatcher::{DispatchRequest, UploadedFile},
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
                    Arc::clone(&route_config),
                    body,
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
    route: Arc<RouteConfig>,
    body: Bytes,
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
        http.request.method = "POST",
        http.route = %route.path,
        http.target = %uri,
        request.body_bytes = request_body_bytes,
        response.status_code = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        sink.status = tracing::field::Empty,
    );

    async move {
        info!("ingest request received");
        let parsed_body = parse_body(&headers, body)
            .instrument(info_span!("postio.ingest.parse_body"))
            .await?;
        let body_kind = match &parsed_body.body {
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
                form: parsed_body.form.clone(),
                file: file_metadata_json(parsed_body.file.as_ref()),
                body: parsed_body.body.clone(),
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
                body: parsed_body.body,
                file: parsed_body.file,
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
        tracing::Span::current().record("http.response.status_code", status.as_u16());
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

struct ParsedBody {
    body: Value,
    form: BTreeMap<String, String>,
    file: Option<UploadedFile>,
}

async fn parse_body(headers: &HeaderMap, body: Bytes) -> Result<ParsedBody, ApiError> {
    if is_multipart(headers) {
        return parse_multipart_body(headers, body).await;
    }

    if body.is_empty() {
        return Ok(ParsedBody {
            body: Value::Null,
            form: BTreeMap::new(),
            file: None,
        });
    }
    let body = serde_json::from_slice(&body)
        .or_else(|_| String::from_utf8(body.to_vec()).map(Value::String))
        .map_err(|_| ApiError::BadRequest("request body must be valid utf-8".to_string()))?;

    Ok(ParsedBody {
        body,
        form: BTreeMap::new(),
        file: None,
    })
}

async fn parse_multipart_body(headers: &HeaderMap, body: Bytes) -> Result<ParsedBody, ApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            ApiError::BadRequest("multipart request requires content-type".to_string())
        })?;
    let boundary = multipart_boundary(content_type).ok_or_else(|| {
        ApiError::BadRequest("multipart request requires boundary in content-type".to_string())
    })?;
    let stream = stream::once(async move { Ok::<Bytes, std::io::Error>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut form = BTreeMap::new();
    let mut file = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::BadRequest(format!("invalid multipart body: {error}")))?
    {
        let field_name = field.name().map(ToString::to_string);
        let file_name = field.file_name().map(ToString::to_string);
        let content_type = field.content_type().map(ToString::to_string);
        let bytes = field
            .bytes()
            .await
            .map_err(|error| ApiError::BadRequest(format!("invalid multipart field: {error}")))?;

        if file_name.is_some() {
            if file.is_some() {
                warn!("multipart request contains more than one file; using the first file");
                continue;
            }
            file = Some(UploadedFile {
                field_name,
                file_name,
                content_type,
                bytes,
            });
            continue;
        }

        if let Some(field_name) = field_name {
            let value = String::from_utf8(bytes.to_vec()).map_err(|_| {
                ApiError::BadRequest(format!("multipart field {field_name} must be valid utf-8"))
            })?;
            form.insert(field_name, value);
        }
    }

    let file_metadata = file_metadata_json(file.as_ref());
    let body = serde_json::json!({
        "form": form,
        "file": file_metadata,
    });

    Ok(ParsedBody { body, form, file })
}

fn is_multipart(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type
                    .trim()
                    .eq_ignore_ascii_case("multipart/form-data")
            })
        })
        .unwrap_or(false)
}

fn multipart_boundary(content_type: &str) -> Option<String> {
    content_type.split(';').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        key.trim().eq_ignore_ascii_case("boundary").then(|| {
            value
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string()
        })
    })
}

fn file_metadata_json(file: Option<&UploadedFile>) -> Value {
    match file {
        Some(file) => serde_json::json!({
            "fieldName": file.field_name.as_deref(),
            "filename": file.file_name.as_deref(),
            "contentType": file.content_type.as_deref(),
            "size": file.bytes.len(),
        }),
        None => Value::Null,
    }
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
