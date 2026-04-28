use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::{default_batch_size, default_method, default_wait_time_seconds};

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SourceConfig {
    Http(HttpSourceConfig),
    Sqs(SqsSourceConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpSourceConfig {
    #[serde(default = "default_method")]
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub completion: Option<HttpSourceCompletionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsSourceConfig {
    pub queue: Option<String>,
    pub queue_url: Option<String>,
    #[serde(default = "default_batch_size")]
    pub batch_size: i32,
    #[serde(default = "default_wait_time_seconds")]
    pub wait_time_seconds: i32,
    pub visibility_timeout_seconds: Option<i32>,
    #[serde(default)]
    pub completion: Option<SqsSourceCompletionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpSourceCompletionConfig {
    #[serde(default)]
    pub on_success: Option<HttpCompletionRule>,
    #[serde(default)]
    pub on_failure: Option<HttpCompletionRule>,
    #[serde(default)]
    pub on_validation_failure: Option<HttpCompletionRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpCompletionRule {
    #[serde(default)]
    pub response: Option<HttpCompletionResponseConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpCompletionResponseConfig {
    pub status: Option<u16>,
    #[serde(default)]
    pub body: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsSourceCompletionConfig {
    #[serde(default)]
    pub on_success: Option<SqsCompletionRule>,
    #[serde(default)]
    pub on_failure: Option<SqsCompletionRule>,
    #[serde(default)]
    pub on_validation_failure: Option<SqsCompletionRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsCompletionRule {
    pub action: Option<SqsCompletionAction>,
    #[serde(default)]
    pub dead_letter: Option<SqsDeadLetterConfig>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SqsCompletionAction {
    Ack,
    Retry,
    Drop,
    DeadLetter,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsDeadLetterConfig {
    pub queue: Option<String>,
    pub queue_url: Option<String>,
    pub delay_seconds: Option<i32>,
    #[serde(default)]
    pub attributes: Option<BTreeMap<String, String>>,
}

impl SourceConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceConfig::Http(_) => "http",
            SourceConfig::Sqs(_) => "sqs",
        }
    }
}
