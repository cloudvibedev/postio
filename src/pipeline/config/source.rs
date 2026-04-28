use serde::Deserialize;

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
}

impl SourceConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceConfig::Http(_) => "http",
            SourceConfig::Sqs(_) => "sqs",
        }
    }
}
