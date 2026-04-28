use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineConfig {
    pub id: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub source: SourceConfig,
    #[serde(default)]
    pub transform: Option<TransformConfig>,
    pub target: TargetConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "engine", rename_all = "camelCase")]
pub enum TransformConfig {
    Template(TemplateTransformConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateTransformConfig {
    pub output: TransformTemplateOutput,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransformTemplateOutput {
    pub method: Option<String>,
    pub url: Option<String>,
    pub headers: Option<BTreeMap<String, String>>,
    pub query: Option<BTreeMap<String, Value>>,
    pub body: Option<Value>,
    pub delay_seconds: Option<i32>,
    pub attributes: Option<BTreeMap<String, Value>>,
}

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

impl SourceConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            SourceConfig::Http(_) => "http",
            SourceConfig::Sqs(_) => "sqs",
        }
    }
}

impl TargetConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            TargetConfig::Http(_) => "http",
            TargetConfig::Sqs(_) => "sqs",
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
