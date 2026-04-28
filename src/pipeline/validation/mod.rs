use std::sync::Arc;

use anyhow::{Context, Result};

use crate::pipeline::{
    config::ValidateConfig,
    model::{Payload, ValidationErrorDetail, ValidationFailure},
};

#[derive(Clone)]
pub enum PipelineValidator {
    Noop,
    JsonSchema(Arc<jsonschema::Validator>),
}

impl PipelineValidator {
    pub fn compile(config: Option<ValidateConfig>) -> Result<Self> {
        match config {
            Some(ValidateConfig::JsonSchema(config)) => {
                let validator = jsonschema::validator_for(&config.schema)
                    .context("failed to compile jsonschema validator")?;
                Ok(PipelineValidator::JsonSchema(Arc::new(validator)))
            }
            None => Ok(PipelineValidator::Noop),
        }
    }

    pub fn engine_name(&self) -> &'static str {
        match self {
            PipelineValidator::Noop => "noop",
            PipelineValidator::JsonSchema(_) => "jsonschema",
        }
    }

    pub fn validate(&self, payload: &Payload) -> Result<(), ValidationFailure> {
        match self {
            PipelineValidator::Noop => Ok(()),
            PipelineValidator::JsonSchema(validator) => {
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
        }
    }
}
