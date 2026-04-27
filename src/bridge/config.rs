use std::{collections::BTreeMap, fs, path::Path};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeConfig {
    pub routes: Vec<RouteConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteConfig {
    pub id: String,
    #[serde(default = "default_method")]
    pub method: String,
    pub path: String,
    pub sink: SinkConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SinkConfig {
    #[serde(rename_all = "camelCase")]
    Sns {
        topic: Option<String>,
        topic_arn: Option<String>,
        subject: Option<String>,
        message: Option<Value>,
        attributes: Option<BTreeMap<String, Value>>,
    },
    #[serde(rename_all = "camelCase")]
    Sqs {
        queue: Option<String>,
        queue_url: Option<String>,
        message: Option<Value>,
        attributes: Option<BTreeMap<String, Value>>,
    },
    #[serde(rename_all = "camelCase")]
    S3 {
        bucket: String,
        key: String,
        content_type: Option<String>,
        object: Option<Value>,
        metadata: Option<BTreeMap<String, Value>>,
    },
}

pub fn load_bridge_config(path: impl AsRef<Path>) -> Result<BridgeConfig> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read bridge config {}", path.display()))?;
    let config = match path.extension().and_then(|value| value.to_str()) {
        Some("json") => serde_json::from_str(&raw).context("failed to parse JSON config")?,
        _ => serde_yaml::from_str(&raw).context("failed to parse YAML config")?,
    };
    validate_config(config)
}

fn validate_config(config: BridgeConfig) -> Result<BridgeConfig> {
    for route in &config.routes {
        if route.id.trim().is_empty() {
            return Err(anyhow!("route id cannot be empty"));
        }
        if route.path.trim().is_empty() || !route.path.starts_with('/') {
            return Err(anyhow!("route {} path must start with /", route.id));
        }
        match &route.sink {
            SinkConfig::Sns {
                topic, topic_arn, ..
            } if topic.is_none() && topic_arn.is_none() => {
                return Err(anyhow!(
                    "route {} sns sink requires topic or topicArn",
                    route.id
                ));
            }
            SinkConfig::Sqs {
                queue, queue_url, ..
            } if queue.is_none() && queue_url.is_none() => {
                return Err(anyhow!(
                    "route {} sqs sink requires queue or queueUrl",
                    route.id
                ));
            }
            _ => {}
        }
    }
    Ok(config)
}

fn default_method() -> String {
    "POST".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_v0_routes() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
routes:
  - id: topic
    path: /events/{topic}
    sink:
      type: sns
      topic: "{{ params.topic }}"
  - id: file
    path: /file/{bucket}/{filename}
    sink:
      type: s3
      bucket: "{{ params.bucket }}"
      key: "{{ params.filename }}"
"#,
        )
        .expect("config parses");

        assert_eq!(config.routes.len(), 2);
    }
}
