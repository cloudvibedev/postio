use std::collections::BTreeMap;

use serde::Deserialize;

use super::default_method;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TargetConfig {
    Http(HttpTargetConfig),
    Sqs(SqsTargetConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpTargetConfig {
    #[serde(default = "default_method")]
    pub method: String,
    pub url: String,
    pub headers: Option<BTreeMap<String, String>>,
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub retry: Option<TargetRetryConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsTargetConfig {
    pub queue: Option<String>,
    pub queue_url: Option<String>,
    pub delay_seconds: Option<i32>,
    #[serde(default)]
    pub retry: Option<TargetRetryConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetRetryConfig {
    pub max_attempts: u32,
    pub backoff: RetryBackoffConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum RetryBackoffConfig {
    #[serde(rename_all = "camelCase")]
    Fixed { delay_ms: u64 },
    #[serde(rename_all = "camelCase")]
    Exponential { initial_ms: u64, max_ms: u64 },
}

impl TargetConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            TargetConfig::Http(_) => "http",
            TargetConfig::Sqs(_) => "sqs",
        }
    }
}
