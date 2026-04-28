use serde::Deserialize;

mod source;
mod target;
mod transform;
mod validate;

pub use source::{
    HttpCompletionResponseConfig, HttpCompletionRule, HttpSourceCompletionConfig, HttpSourceConfig,
    SourceConfig, SqsCompletionAction, SqsCompletionRule, SqsDeadLetterConfig,
    SqsSourceCompletionConfig, SqsSourceConfig,
};
pub use target::{HttpTargetConfig, SqsTargetConfig, TargetConfig};
pub use transform::{TemplateTransformConfig, TransformConfig, TransformTemplateOutput};
pub use validate::{JsonSchemaValidateConfig, ValidateConfig};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineConfig {
    pub id: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub source: SourceConfig,
    #[serde(default)]
    pub validate: Option<ValidateConfig>,
    #[serde(default)]
    pub transform: Option<TransformConfig>,
    pub target: TargetConfig,
}

fn default_enabled() -> bool {
    true
}

pub(super) fn default_method() -> String {
    "POST".to_string()
}

pub(super) fn default_batch_size() -> i32 {
    1
}

pub(super) fn default_wait_time_seconds() -> i32 {
    10
}
