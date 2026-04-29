use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use rhai::{Array, Dynamic, Engine, Map, Scope, AST, FLOAT, INT};
use serde_json::{Number, Value};

use crate::pipeline::{
    config::{RhaiTransformConfig, TemplateTransformConfig, TransformConfig},
    model::{Payload, PipelineMessage},
};

pub enum PipelineTransformer {
    Noop,
    Template(TemplateTransformConfig),
    Rhai(RhaiTransformer),
}

pub struct RhaiTransformer {
    engine: Engine,
    ast: AST,
}

#[derive(Debug, Clone)]
pub struct TransformOutput {
    pub method: Option<String>,
    pub url: Option<String>,
    pub headers: Option<BTreeMap<String, String>>,
    pub query: Option<BTreeMap<String, String>>,
    pub body: Option<Value>,
    pub delay_seconds: Option<i32>,
    pub attributes: Option<BTreeMap<String, String>>,
}

impl PipelineTransformer {
    pub fn compile(config: Option<TransformConfig>) -> Result<Self> {
        match config {
            Some(TransformConfig::Template(config)) => Ok(Self::Template(config)),
            Some(TransformConfig::Rhai(config)) => {
                Ok(Self::Rhai(RhaiTransformer::compile(config)?))
            }
            None => Ok(Self::Noop),
        }
    }

    pub fn engine_name(&self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Template(_) => "template",
            Self::Rhai(_) => "rhai",
        }
    }

    pub fn rhai(&self) -> Option<&RhaiTransformer> {
        match self {
            Self::Rhai(transformer) => Some(transformer),
            _ => None,
        }
    }
}

impl RhaiTransformer {
    fn compile(config: RhaiTransformConfig) -> Result<Self> {
        if config.script.trim().is_empty() {
            return Err(anyhow!("rhai transform script cannot be empty"));
        }

        let mut engine = Engine::new();
        engine.set_max_operations(100_000);
        let ast = engine
            .compile(&config.script)
            .context("failed to compile rhai transform script")?;

        Ok(Self { engine, ast })
    }

    pub fn transform(&self, message: &PipelineMessage) -> Result<TransformOutput> {
        let mut scope = Scope::new();
        scope.push_constant("input", input_dynamic(message)?);
        let output = self
            .engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &self.ast)
            .context("failed to execute rhai transform script")?;
        let output = dynamic_to_json(output);
        transform_output_from_value(output)
    }
}

fn input_dynamic(message: &PipelineMessage) -> Result<Dynamic> {
    json_to_dynamic(Value::Object(
        [
            ("body".to_string(), message.payload.to_template_value()),
            (
                "headers".to_string(),
                string_map_to_value(&message.metadata.headers),
            ),
            (
                "params".to_string(),
                string_map_to_value(&message.metadata.params),
            ),
            (
                "query".to_string(),
                string_map_to_value(&message.metadata.query),
            ),
            (
                "context".to_string(),
                string_map_to_value(&BTreeMap::from([
                    ("requestId".to_string(), message.id.to_string()),
                    ("pipelineId".to_string(), message.pipeline_id.clone()),
                    ("attempt".to_string(), message.attempt.to_string()),
                    (
                        "sourceType".to_string(),
                        message.source.type_name().to_string(),
                    ),
                ])),
            ),
        ]
        .into_iter()
        .collect(),
    ))
}

fn string_map_to_value(map: &BTreeMap<String, String>) -> Value {
    Value::Object(
        map.iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect(),
    )
}

fn transform_output_from_value(value: Value) -> Result<TransformOutput> {
    let Value::Object(mut output) = value else {
        return Err(anyhow!("rhai transform must return an object"));
    };

    Ok(TransformOutput {
        method: optional_string(output.remove("method"), "method")?,
        url: optional_string(output.remove("url"), "url")?,
        headers: optional_string_map(output.remove("headers"), "headers")?,
        query: optional_string_map(output.remove("query"), "query")?,
        body: output.remove("body"),
        delay_seconds: optional_i32(output.remove("delaySeconds"), "delaySeconds")?,
        attributes: optional_string_map(output.remove("attributes"), "attributes")?,
    })
}

fn optional_string(value: Option<Value>, field: &str) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::String(value) => Ok(Some(value)),
        Value::Null => Ok(None),
        value => Err(anyhow!(
            "rhai transform field {field} must be a string, got {value}"
        )),
    }
}

fn optional_i32(value: Option<Value>, field: &str) -> Result<Option<i32>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Number(value) => value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| anyhow!("rhai transform field {field} must fit i32")),
        Value::Null => Ok(None),
        value => Err(anyhow!(
            "rhai transform field {field} must be an integer, got {value}"
        )),
    }
}

fn optional_string_map(
    value: Option<Value>,
    field: &str,
) -> Result<Option<BTreeMap<String, String>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Object(values) => Ok(Some(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, json_value_to_string(value))))
                .collect::<Result<_>>()?,
        )),
        Value::Null => Ok(None),
        value => Err(anyhow!(
            "rhai transform field {field} must be an object, got {value}"
        )),
    }
}

fn json_value_to_string(value: Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value,
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        value => value.to_string(),
    }
}

fn json_to_dynamic(value: Value) -> Result<Dynamic> {
    Ok(match value {
        Value::Null => Dynamic::UNIT,
        Value::Bool(value) => Dynamic::from_bool(value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Dynamic::from_int(value as INT)
            } else if let Some(value) = value.as_u64().and_then(|value| INT::try_from(value).ok()) {
                Dynamic::from_int(value)
            } else if let Some(value) = value.as_f64() {
                Dynamic::from_float(value as FLOAT)
            } else {
                return Err(anyhow!("unsupported json number"));
            }
        }
        Value::String(value) => Dynamic::from(value),
        Value::Array(values) => Dynamic::from_array(
            values
                .into_iter()
                .map(json_to_dynamic)
                .collect::<Result<Array>>()?,
        ),
        Value::Object(values) => {
            let mut map = Map::new();
            for (key, value) in values {
                map.insert(key.into(), json_to_dynamic(value)?);
            }
            Dynamic::from_map(map)
        }
    })
}

fn dynamic_to_json(value: Dynamic) -> Value {
    if value.is_unit() {
        Value::Null
    } else if let Some(value) = value.clone().try_cast::<bool>() {
        Value::Bool(value)
    } else if let Some(value) = value.clone().try_cast::<INT>() {
        Value::Number(Number::from(value))
    } else if let Some(value) = value.clone().try_cast::<FLOAT>() {
        Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    } else if let Some(value) = value.clone().try_cast::<String>() {
        Value::String(value)
    } else if let Some(value) = value.clone().try_cast::<Array>() {
        Value::Array(value.into_iter().map(dynamic_to_json).collect())
    } else if let Some(value) = value.clone().try_cast::<Map>() {
        Value::Object(
            value
                .into_iter()
                .map(|(key, value)| (key.to_string(), dynamic_to_json(value)))
                .collect(),
        )
    } else {
        Value::String(value.to_string())
    }
}

impl TransformOutput {
    pub fn apply_to(self, message: &mut PipelineMessage) {
        if let Some(body) = self.body {
            message.payload = Payload::from_template_value(body);
        }
        if let Some(method) = self.method {
            message.target.method = Some(method);
        }
        if let Some(url) = self.url {
            message.target.url = Some(url);
        }
        if let Some(headers) = self.headers {
            message.target.headers = headers;
        }
        if let Some(query) = self.query {
            message.target.query = query;
        }
        if let Some(attributes) = self.attributes {
            message.target.attributes = attributes;
        }
        if let Some(delay_seconds) = self.delay_seconds {
            message.target.delay_seconds = Some(delay_seconds);
        }
    }
}
