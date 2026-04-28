use std::collections::BTreeMap;

use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug)]
pub struct PipelineMessage {
    pub id: Uuid,
    pub pipeline_id: String,
    pub source: SourceContext,
    pub payload: Payload,
    pub metadata: MessageMetadata,
    pub attempt: u32,
    pub reply: Option<oneshot::Sender<CompletionResponse>>,
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

#[derive(Debug, Clone, Default)]
pub struct MessageMetadata {
    pub params: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub content_type: Option<String>,
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
}
