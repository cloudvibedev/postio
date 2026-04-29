use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "engine", rename_all = "camelCase")]
pub enum TransformConfig {
    Template(TemplateTransformConfig),
    Rhai(RhaiTransformConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateTransformConfig {
    pub output: TransformTemplateOutput,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RhaiTransformConfig {
    pub script: String,
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
