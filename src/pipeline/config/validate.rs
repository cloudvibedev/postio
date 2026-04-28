use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "engine", rename_all = "camelCase")]
pub enum ValidateConfig {
    #[serde(rename = "jsonschema")]
    JsonSchema(JsonSchemaValidateConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonSchemaValidateConfig {
    pub schema: Value,
}
