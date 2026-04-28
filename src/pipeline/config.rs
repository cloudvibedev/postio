use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineConfig {
    pub id: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub source: SourceConfig,
    pub target: TargetConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SourceConfig {
    #[serde(rename_all = "camelCase")]
    Http {
        #[serde(default = "default_method")]
        method: String,
        path: String,
    },
    #[serde(rename_all = "camelCase")]
    Sqs {
        queue: Option<String>,
        queue_url: Option<String>,
        #[serde(default = "default_batch_size")]
        batch_size: i32,
        #[serde(default = "default_wait_time_seconds")]
        wait_time_seconds: i32,
        visibility_timeout_seconds: Option<i32>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TargetConfig {
    #[serde(rename_all = "camelCase")]
    Http {
        #[serde(default = "default_method")]
        method: String,
        url: String,
        headers: Option<BTreeMap<String, String>>,
        timeout_ms: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    Sqs {
        queue: Option<String>,
        queue_url: Option<String>,
        delay_seconds: Option<i32>,
    },
}

impl SourceConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceConfig::Http { .. } => "http",
            SourceConfig::Sqs { .. } => "sqs",
        }
    }
}

impl TargetConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            TargetConfig::Http { .. } => "http",
            TargetConfig::Sqs { .. } => "sqs",
        }
    }
}

fn default_enabled() -> bool {
    true
}

fn default_method() -> String {
    "POST".to_string()
}

fn default_batch_size() -> i32 {
    1
}

fn default_wait_time_seconds() -> i32 {
    10
}
