use std::{collections::BTreeMap, fs, path::Path};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::pipeline::config::{PipelineConfig, SourceConfig, TargetConfig};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeConfig {
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    pub pipeline: Option<PipelineConfig>,
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

impl SinkConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            SinkConfig::Sns { .. } => "sns",
            SinkConfig::Sqs { .. } => "sqs",
            SinkConfig::S3 { .. } => "s3",
        }
    }
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
    if let Some(pipeline) = &config.pipeline {
        validate_pipeline(pipeline)?;
        validate_pipeline_route_conflicts(&config.routes, pipeline)?;
    }
    Ok(config)
}

fn default_method() -> String {
    "POST".to_string()
}

fn validate_pipeline(pipeline: &PipelineConfig) -> Result<()> {
    if pipeline.id.trim().is_empty() {
        return Err(anyhow!("pipeline id cannot be empty"));
    }
    if !pipeline.enabled {
        return Ok(());
    }

    match &pipeline.source {
        SourceConfig::Http { method, path } => {
            if !method.eq_ignore_ascii_case("POST") {
                return Err(anyhow!(
                    "pipeline {} http source supports only POST for now",
                    pipeline.id
                ));
            }
            if path.trim().is_empty() || !path.starts_with('/') {
                return Err(anyhow!(
                    "pipeline {} http source path must start with /",
                    pipeline.id
                ));
            }
        }
        SourceConfig::Sqs {
            queue, queue_url, ..
        } if queue.is_none() && queue_url.is_none() => {
            return Err(anyhow!(
                "pipeline {} sqs source requires queue or queueUrl",
                pipeline.id
            ));
        }
        _ => {}
    }

    match &pipeline.target {
        TargetConfig::Http { method, url, .. } => {
            if method.trim().is_empty() {
                return Err(anyhow!(
                    "pipeline {} http target method cannot be empty",
                    pipeline.id
                ));
            }
            if url.trim().is_empty() {
                return Err(anyhow!(
                    "pipeline {} http target url cannot be empty",
                    pipeline.id
                ));
            }
        }
        TargetConfig::Sqs {
            queue, queue_url, ..
        } if queue.is_none() && queue_url.is_none() => {
            return Err(anyhow!(
                "pipeline {} sqs target requires queue or queueUrl",
                pipeline.id
            ));
        }
        _ => {}
    }

    Ok(())
}

fn validate_pipeline_route_conflicts(
    routes: &[RouteConfig],
    pipeline: &PipelineConfig,
) -> Result<()> {
    let SourceConfig::Http { method, path } = &pipeline.source else {
        return Ok(());
    };
    if !pipeline.enabled || !method.eq_ignore_ascii_case("POST") {
        return Ok(());
    }

    if let Some(route) = routes
        .iter()
        .find(|route| route.method.eq_ignore_ascii_case("POST") && route.path == *path)
    {
        return Err(anyhow!(
            "pipeline {} http source path conflicts with route {}",
            pipeline.id,
            route.id
        ));
    }

    Ok(())
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
        assert!(config.pipeline.is_none());
    }

    #[test]
    fn parses_single_pipeline() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
pipeline:
  id: http-to-sqs
  source:
    type: http
    method: POST
    path: /orders
  target:
    type: sqs
    queue: orders-output
"#,
        )
        .expect("config parses");

        let config = validate_config(config).expect("config is valid");
        assert_eq!(config.routes.len(), 0);
        let pipeline = config.pipeline.expect("pipeline");
        assert_eq!(pipeline.id, "http-to-sqs");
        assert_eq!(pipeline.source.type_name(), "http");
        assert_eq!(pipeline.target.type_name(), "sqs");
    }

    #[test]
    fn rejects_pipeline_http_path_conflict() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
routes:
  - id: legacy
    path: /orders
    sink:
      type: sqs
      queue: legacy-orders
pipeline:
  id: http-to-sqs
  source:
    type: http
    path: /orders
  target:
    type: sqs
    queue: orders-output
"#,
        )
        .expect("config parses");

        let error = validate_config(config).expect_err("conflict is rejected");
        assert!(
            error.to_string().contains("conflicts with route legacy"),
            "{error}"
        );
    }

    #[test]
    fn reports_sink_type_names() {
        let sink = SinkConfig::S3 {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            content_type: None,
            object: None,
            metadata: None,
        };

        assert_eq!(sink.type_name(), "s3");
    }
}
