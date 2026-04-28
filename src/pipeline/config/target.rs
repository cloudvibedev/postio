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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsTargetConfig {
    pub queue: Option<String>,
    pub queue_url: Option<String>,
    pub delay_seconds: Option<i32>,
}

impl TargetConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            TargetConfig::Http(_) => "http",
            TargetConfig::Sqs(_) => "sqs",
        }
    }
}
