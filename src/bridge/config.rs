use std::{collections::BTreeMap, fs, path::Path};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::pipeline::{
    config::{
        PipelineConfig, RetryBackoffConfig, SourceConfig, SqsCompletionAction, TargetConfig,
        TargetRetryConfig,
    },
    validation::PipelineValidator,
};

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
        SourceConfig::Http(config) => {
            if !config.method.eq_ignore_ascii_case("POST") {
                return Err(anyhow!(
                    "pipeline {} http source supports only POST for now",
                    pipeline.id
                ));
            }
            if config.path.trim().is_empty() || !config.path.starts_with('/') {
                return Err(anyhow!(
                    "pipeline {} http source path must start with /",
                    pipeline.id
                ));
            }
            if let Some(completion) = &config.completion {
                validate_http_completion_rule(
                    pipeline,
                    completion.on_success.as_ref(),
                    "onSuccess",
                )?;
                validate_http_completion_rule(
                    pipeline,
                    completion.on_failure.as_ref(),
                    "onFailure",
                )?;
                validate_http_completion_rule(
                    pipeline,
                    completion.on_validation_failure.as_ref(),
                    "onValidationFailure",
                )?;
            }
        }
        SourceConfig::Sqs(config) if config.queue.is_none() && config.queue_url.is_none() => {
            return Err(anyhow!(
                "pipeline {} sqs source requires queue or queueUrl",
                pipeline.id
            ));
        }
        SourceConfig::Sqs(config) => {
            if let Some(completion) = &config.completion {
                validate_sqs_completion_rule(
                    pipeline,
                    completion.on_success.as_ref(),
                    "onSuccess",
                )?;
                validate_sqs_completion_rule(
                    pipeline,
                    completion.on_failure.as_ref(),
                    "onFailure",
                )?;
                validate_sqs_completion_rule(
                    pipeline,
                    completion.on_validation_failure.as_ref(),
                    "onValidationFailure",
                )?;
            }
        }
    }

    match &pipeline.target {
        TargetConfig::Http(config) => {
            if config.method.trim().is_empty() {
                return Err(anyhow!(
                    "pipeline {} http target method cannot be empty",
                    pipeline.id
                ));
            }
            if config.url.trim().is_empty() {
                return Err(anyhow!(
                    "pipeline {} http target url cannot be empty",
                    pipeline.id
                ));
            }
            validate_target_retry(pipeline, config.retry.as_ref())?;
        }
        TargetConfig::Sqs(config) if config.queue.is_none() && config.queue_url.is_none() => {
            return Err(anyhow!(
                "pipeline {} sqs target requires queue or queueUrl",
                pipeline.id
            ));
        }
        TargetConfig::Sqs(config) => {
            validate_target_retry(pipeline, config.retry.as_ref())?;
        }
    }

    PipelineValidator::compile(pipeline.validate.clone())
        .with_context(|| format!("pipeline {} validate config is invalid", pipeline.id))?;

    Ok(())
}

fn validate_sqs_completion_rule(
    pipeline: &PipelineConfig,
    rule: Option<&crate::pipeline::config::SqsCompletionRule>,
    name: &str,
) -> Result<()> {
    let Some(rule) = rule else {
        return Ok(());
    };
    if !matches!(rule.action, Some(SqsCompletionAction::DeadLetter)) {
        return Ok(());
    }
    let Some(dead_letter) = &rule.dead_letter else {
        return Err(anyhow!(
            "pipeline {} sqs source completion {} deadLetter action requires deadLetter config",
            pipeline.id,
            name
        ));
    };
    if dead_letter.queue.is_none() && dead_letter.queue_url.is_none() {
        return Err(anyhow!(
            "pipeline {} sqs source completion {} deadLetter requires queue or queueUrl",
            pipeline.id,
            name
        ));
    }
    Ok(())
}

fn validate_http_completion_rule(
    pipeline: &PipelineConfig,
    rule: Option<&crate::pipeline::config::HttpCompletionRule>,
    name: &str,
) -> Result<()> {
    let Some(status) = rule
        .and_then(|rule| rule.response.as_ref())
        .and_then(|response| response.status)
    else {
        return Ok(());
    };
    if !(100..=599).contains(&status) {
        return Err(anyhow!(
            "pipeline {} http source completion {} response status must be between 100 and 599",
            pipeline.id,
            name
        ));
    }
    Ok(())
}

fn validate_target_retry(
    pipeline: &PipelineConfig,
    retry: Option<&TargetRetryConfig>,
) -> Result<()> {
    let Some(retry) = retry else {
        return Ok(());
    };
    if retry.max_attempts == 0 {
        return Err(anyhow!(
            "pipeline {} target retry maxAttempts must be greater than zero",
            pipeline.id
        ));
    }
    match &retry.backoff {
        RetryBackoffConfig::Fixed { delay_ms } if *delay_ms == 0 => {
            return Err(anyhow!(
                "pipeline {} target retry fixed delayMs must be greater than zero",
                pipeline.id
            ));
        }
        RetryBackoffConfig::Exponential { initial_ms, max_ms } => {
            if *initial_ms == 0 {
                return Err(anyhow!(
                    "pipeline {} target retry exponential initialMs must be greater than zero",
                    pipeline.id
                ));
            }
            if max_ms < initial_ms {
                return Err(anyhow!(
                    "pipeline {} target retry exponential maxMs must be greater than or equal to initialMs",
                    pipeline.id
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_pipeline_route_conflicts(
    routes: &[RouteConfig],
    pipeline: &PipelineConfig,
) -> Result<()> {
    let SourceConfig::Http(source) = &pipeline.source else {
        return Ok(());
    };
    if !pipeline.enabled || !source.method.eq_ignore_ascii_case("POST") {
        return Ok(());
    }

    if let Some(route) = routes
        .iter()
        .find(|route| route.method.eq_ignore_ascii_case("POST") && route.path == source.path)
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
  validate:
    engine: jsonschema
    schema:
      type: object
      required:
        - id
      properties:
        id:
          type: string
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

        let SourceConfig::Http(source) = pipeline.source else {
            panic!("expected http source");
        };
        assert_eq!(source.method, "POST");
        assert_eq!(source.path, "/orders");
        assert!(pipeline.validate.is_some());

        let TargetConfig::Sqs(target) = pipeline.target else {
            panic!("expected sqs target");
        };
        assert_eq!(target.queue.as_deref(), Some("orders-output"));
    }

    #[test]
    fn parses_pipeline_template_transform() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
pipeline:
  id: http-to-sqs
  source:
    type: http
    path: /orders/{tenant}
  transform:
    engine: template
    output:
      headers:
        x-event-type: "{{ body.type }}"
      body:
        tenant: "{{ params.tenant }}"
        payload: "{{ body }}"
      delaySeconds: 2
  target:
    type: sqs
    queue: orders-output
"#,
        )
        .expect("config parses");

        let config = validate_config(config).expect("config is valid");
        let pipeline = config.pipeline.expect("pipeline");
        let transform = pipeline.transform.expect("transform");
        let crate::pipeline::config::TransformConfig::Template(config) = transform;
        assert_eq!(
            config.output.headers.expect("headers")["x-event-type"],
            "{{ body.type }}"
        );
        assert_eq!(config.output.delay_seconds, Some(2));
        assert!(config.output.body.expect("body").is_object());
    }

    #[test]
    fn parses_pipeline_source_completion() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
pipeline:
  id: sqs-to-http
  source:
    type: sqs
    queue: orders-input
    completion:
      onFailure:
        action: deadLetter
        deadLetter:
          queue: orders-dlq
  target:
    type: http
    url: https://api.example.com/orders
"#,
        )
        .expect("config parses");

        let config = validate_config(config).expect("config is valid");
        let pipeline = config.pipeline.expect("pipeline");
        let SourceConfig::Sqs(source) = pipeline.source else {
            panic!("expected sqs source");
        };
        let completion = source.completion.expect("completion");
        let on_failure = completion.on_failure.expect("on failure");
        assert!(matches!(
            on_failure.action,
            Some(crate::pipeline::config::SqsCompletionAction::DeadLetter)
        ));
        assert_eq!(
            on_failure
                .dead_letter
                .expect("dead letter")
                .queue
                .as_deref(),
            Some("orders-dlq")
        );
    }

    #[test]
    fn rejects_dead_letter_completion_without_destination() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
pipeline:
  id: sqs-to-http
  source:
    type: sqs
    queue: orders-input
    completion:
      onFailure:
        action: deadLetter
        deadLetter: {}
  target:
    type: http
    url: https://api.example.com/orders
"#,
        )
        .expect("config parses");

        let error = validate_config(config).expect_err("invalid dead letter is rejected");
        assert!(
            error
                .to_string()
                .contains("deadLetter requires queue or queueUrl"),
            "{error}"
        );
    }

    #[test]
    fn parses_pipeline_target_retry() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
pipeline:
  id: http-to-sqs
  source:
    type: http
    path: /orders
  target:
    type: sqs
    queue: orders-output
    retry:
      maxAttempts: 3
      backoff:
        type: exponential
        initialMs: 200
        maxMs: 5000
"#,
        )
        .expect("config parses");

        let config = validate_config(config).expect("config is valid");
        let pipeline = config.pipeline.expect("pipeline");
        let TargetConfig::Sqs(target) = pipeline.target else {
            panic!("expected sqs target");
        };
        let retry = target.retry.expect("retry");
        assert_eq!(retry.max_attempts, 3);
        assert!(matches!(
            retry.backoff,
            crate::pipeline::config::RetryBackoffConfig::Exponential {
                initial_ms: 200,
                max_ms: 5000
            }
        ));
    }

    #[test]
    fn rejects_invalid_target_retry() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
pipeline:
  id: http-to-http
  source:
    type: http
    path: /orders
  target:
    type: http
    url: https://api.example.com/orders
    retry:
      maxAttempts: 0
      backoff:
        type: fixed
        delayMs: 100
"#,
        )
        .expect("config parses");

        let error = validate_config(config).expect_err("invalid retry is rejected");
        assert!(
            error
                .to_string()
                .contains("retry maxAttempts must be greater than zero"),
            "{error}"
        );
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
    fn rejects_invalid_pipeline_validate_schema() {
        let config: BridgeConfig = serde_yaml::from_str(
            r#"
pipeline:
  id: invalid-validator
  source:
    type: http
    path: /orders
  validate:
    engine: jsonschema
    schema:
      type: definitely-not-valid
  target:
    type: sqs
    queue: orders-output
"#,
        )
        .expect("config parses");

        let error = validate_config(config).expect_err("invalid jsonschema is rejected");
        assert!(
            error.to_string().contains("validate config is invalid"),
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
