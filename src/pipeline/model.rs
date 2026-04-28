use std::collections::BTreeMap;

use bytes::Bytes;
use opentelemetry::Context as OtelContext;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::oneshot;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

#[derive(Debug)]
pub struct PipelineMessage {
    pub id: Uuid,
    pub pipeline_id: String,
    pub source: SourceContext,
    pub payload: Payload,
    pub metadata: MessageMetadata,
    pub target: TargetRequestOverrides,
    pub trace: TraceContext,
    pub attempt: u32,
    pub reply: Option<oneshot::Sender<CompletionResponse>>,
}

#[derive(Clone, Default)]
pub struct TraceContext {
    parent: OtelContext,
}

impl std::fmt::Debug for TraceContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TraceContext")
            .finish_non_exhaustive()
    }
}

impl TraceContext {
    pub fn current() -> Self {
        Self {
            parent: Span::current().context(),
        }
    }

    pub fn from_span(span: &Span) -> Self {
        Self {
            parent: span.context(),
        }
    }

    pub fn child_span(&self, span: Span) -> Span {
        let _ = span.set_parent(self.parent.clone());
        span
    }
}

#[derive(Debug, Clone)]
pub enum Payload {
    Raw(Bytes),
    Json(Value),
    Text(String),
    Empty,
}

#[derive(Debug, Clone)]
pub enum SourceContext {
    Http {
        method: String,
        path: String,
    },
    Sqs {
        queue_url: String,
        receipt_handle: String,
        message_id: Option<String>,
    },
}

impl SourceContext {
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceContext::Http { .. } => "http",
            SourceContext::Sqs { .. } => "sqs",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MessageMetadata {
    pub params: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TargetRequestOverrides {
    pub method: Option<String>,
    pub url: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub attributes: BTreeMap<String, String>,
    pub delay_seconds: Option<i32>,
}

#[derive(Debug)]
pub struct PipelineResult {
    pub message: PipelineMessage,
    pub target: Result<TargetResponse, String>,
}

#[derive(Debug, Clone)]
pub struct TargetResponse {
    pub target_type: &'static str,
    pub status_code: u16,
    pub body: Option<String>,
    pub message_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionResponse {
    pub pipeline_id: String,
    pub request_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Payload {
    pub fn decode(raw: Bytes) -> Self {
        if raw.is_empty() {
            return Payload::Empty;
        }
        match serde_json::from_slice::<Value>(&raw) {
            Ok(value) => Payload::Json(value),
            Err(_) => match String::from_utf8(raw.to_vec()) {
                Ok(value) => Payload::Text(value),
                Err(_) => Payload::Raw(raw),
            },
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        match self {
            Payload::Raw(bytes) => bytes.clone(),
            Payload::Json(value) => Bytes::from(value.to_string()),
            Payload::Text(value) => Bytes::from(value.clone()),
            Payload::Empty => Bytes::new(),
        }
    }

    pub fn to_string_body(&self) -> String {
        match self {
            Payload::Raw(bytes) => String::from_utf8_lossy(bytes).to_string(),
            Payload::Json(value) => value.to_string(),
            Payload::Text(value) => value.clone(),
            Payload::Empty => String::new(),
        }
    }

    pub fn to_template_value(&self) -> Value {
        match self {
            Payload::Raw(bytes) => String::from_utf8_lossy(bytes).into_owned().into(),
            Payload::Json(value) => value.clone(),
            Payload::Text(value) => Value::String(value.clone()),
            Payload::Empty => Value::Null,
        }
    }

    pub fn from_template_value(value: Value) -> Self {
        match value {
            Value::String(value) => Payload::Text(value),
            value => Payload::Json(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing::info_span;

    use super::TraceContext;

    #[test]
    fn trace_context_can_create_child_span_when_otel_layer_is_absent() {
        let span = TraceContext::default().child_span(info_span!("postio.test.child"));

        assert!(!span.is_none());
    }
}
