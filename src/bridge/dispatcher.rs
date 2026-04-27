use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

use crate::bridge::{config::RouteConfig, template::TemplateContext};

#[derive(Debug, Clone)]
pub struct DispatchRequest {
    pub route: RouteConfig,
    pub template_context: TemplateContext,
    pub body: Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DispatchResponse {
    pub route_id: String,
    pub sink: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
}

#[async_trait]
pub trait SinkDispatcher: Send + Sync {
    async fn dispatch(&self, request: DispatchRequest) -> anyhow::Result<DispatchResponse>;
}
