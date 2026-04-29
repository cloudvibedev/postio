use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::default_method;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "engine", rename_all = "camelCase")]
pub enum ValidateConfig {
    #[serde(rename = "jsonschema")]
    JsonSchema(JsonSchemaValidateConfig),
    Http(HttpValidateConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonSchemaValidateConfig {
    pub schema: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpValidateConfig {
    #[serde(default = "default_method")]
    pub method: String,
    pub url: String,
    pub headers: Option<BTreeMap<String, String>>,
    pub timeout_ms: Option<u64>,
}
