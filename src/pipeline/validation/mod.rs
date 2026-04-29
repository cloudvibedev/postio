use std::sync::Arc;

use std::time::Duration;

use anyhow::{anyhow, Context, Result};

use crate::pipeline::{
    config::{HttpValidateConfig, ValidateConfig},
    model::{Payload, ValidationErrorDetail, ValidationFailure},
    resources::PipelineResources,
};

#[derive(Clone)]
pub enum PipelineValidator {
    Noop,
    JsonSchema(Arc<jsonschema::Validator>),
    Http(HttpValidateConfig),
}

impl PipelineValidator {
    pub fn compile(config: Option<ValidateConfig>) -> Result<Self> {
        match config {
            Some(ValidateConfig::JsonSchema(config)) => {
                let validator = jsonschema::validator_for(&config.schema)
                    .context("failed to compile jsonschema validator")?;
                Ok(PipelineValidator::JsonSchema(Arc::new(validator)))
            }
            Some(ValidateConfig::Http(config)) => {
                validate_http_config(&config).context("invalid http validator config")?;
                Ok(PipelineValidator::Http(config))
            }
            None => Ok(PipelineValidator::Noop),
        }
    }

    pub fn engine_name(&self) -> &'static str {
        match self {
            PipelineValidator::Noop => "noop",
            PipelineValidator::JsonSchema(_) => "jsonschema",
            PipelineValidator::Http(_) => "http",
        }
    }

    pub async fn validate(
        &self,
        payload: &Payload,
        resources: &PipelineResources,
    ) -> Result<(), ValidationFailure> {
        match self {
            PipelineValidator::Noop => Ok(()),
            PipelineValidator::JsonSchema(validator) => validate_jsonschema(validator, payload),
            PipelineValidator::Http(config) => validate_http(config, payload, resources).await,
        }
    }
}

fn validate_jsonschema(
    validator: &jsonschema::Validator,
    payload: &Payload,
) -> Result<(), ValidationFailure> {
    let instance = payload.to_template_value();
    let details: Vec<_> = validator
        .iter_errors(&instance)
        .map(|error| ValidationErrorDetail {
            path: error.instance_path().to_string(),
            message: error.to_string(),
        })
        .collect();

    if details.is_empty() {
        Ok(())
    } else {
        Err(ValidationFailure {
            error: "validation failed".to_string(),
            details,
        })
    }
}

async fn validate_http(
    config: &HttpValidateConfig,
    payload: &Payload,
    resources: &PipelineResources,
) -> Result<(), ValidationFailure> {
    let method = reqwest::Method::from_bytes(config.method.as_bytes()).map_err(|error| {
        validation_failure(format!(
            "invalid http validator method {}: {error}",
            config.method
        ))
    })?;
    let mut request = resources
        .http
        .request(method, &config.url)
        .body(payload.to_string_body());

    if let Some(timeout_ms) = config.timeout_ms {
        request = request.timeout(Duration::from_millis(timeout_ms));
    }
    if let Some(headers) = &config.headers {
        for (key, value) in headers {
            request = request.header(key, value);
        }
    }

    let response = request
        .send()
        .await
        .map_err(|error| validation_failure(format!("failed to call http validator: {error}")))?;
    if response.status() == reqwest::StatusCode::OK {
        return Ok(());
    }

    Err(validation_failure(format!(
        "http validator returned status {}",
        response.status()
    )))
}

fn validate_http_config(config: &HttpValidateConfig) -> Result<()> {
    if config.method.trim().is_empty() {
        return Err(anyhow!("http validator method cannot be empty"));
    }
    reqwest::Method::from_bytes(config.method.as_bytes())
        .with_context(|| format!("invalid http validator method {}", config.method))?;
    if config.url.trim().is_empty() {
        return Err(anyhow!("http validator url cannot be empty"));
    }
    Ok(())
}

fn validation_failure(message: String) -> ValidationFailure {
    ValidationFailure {
        error: "validation failed".to_string(),
        details: vec![ValidationErrorDetail {
            path: "/".to_string(),
            message,
        }],
    }
}
