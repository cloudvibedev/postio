use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use aws_sdk_sqs::types::MessageAttributeValue as SqsAttribute;
use axum::http::HeaderMap;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info_span, warn, Instrument};
use uuid::Uuid;

use crate::{
    bridge::template::{render_to_string, render_value, TemplateContext},
    pipeline::{
        config::{
            HttpCompletionRule, PipelineConfig, RetryBackoffConfig, SourceConfig,
            SqsCompletionAction, SqsCompletionRule, SqsDeadLetterConfig, TargetConfig,
            TargetRetryConfig,
        },
        model::{
            CompletionResponse, MessageMetadata, Payload, PipelineFailure, PipelineMessage,
            PipelineResult, SourceContext, TargetRequestOverrides, TargetResponse, TraceContext,
        },
        resources::PipelineResources,
        transform::PipelineTransformer,
        validation::PipelineValidator,
    },
};

const DEFAULT_CHANNEL_BUFFER: usize = 100;

#[derive(Clone)]
pub struct PipelineRuntime {
    config: Arc<PipelineConfig>,
    resources: PipelineResources,
    input_tx: mpsc::Sender<PipelineMessage>,
}

impl PipelineRuntime {
    pub fn spawn(config: PipelineConfig, resources: PipelineResources) -> Self {
        let config = Arc::new(config);
        let (input_tx, input_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER);
        let (validate_tx, validate_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER);
        let (transform_tx, transform_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER);
        let (target_tx, target_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER);
        let (completion_tx, completion_rx) = mpsc::channel(DEFAULT_CHANNEL_BUFFER);

        let validator = PipelineValidator::compile(config.validate.clone())
            .expect("pipeline validator should be valid before runtime startup");
        let transformer = PipelineTransformer::compile(config.transform.clone())
            .expect("pipeline transformer should be valid before runtime startup");

        tokio::spawn(run_decode_worker(input_rx, validate_tx));
        tokio::spawn(run_validate_worker(
            validate_rx,
            transform_tx,
            completion_tx.clone(),
            validator,
        ));
        tokio::spawn(run_transform_worker(
            transform_rx,
            target_tx,
            completion_tx.clone(),
            transformer,
        ));
        tokio::spawn(run_target_worker(
            target_rx,
            completion_tx,
            Arc::clone(&config),
            resources.clone(),
        ));
        tokio::spawn(run_completion_worker(
            completion_rx,
            resources.clone(),
            Arc::clone(&config),
        ));

        let runtime = Self {
            config,
            resources,
            input_tx,
        };
        runtime.spawn_source_if_needed();
        runtime
    }

    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    pub async fn submit_http(
        &self,
        params: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
        headers: HeaderMap,
        body: Bytes,
        path: String,
    ) -> Result<CompletionResponse> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let id = Uuid::new_v4();
        let message = PipelineMessage {
            id,
            pipeline_id: self.config.id.clone(),
            source: SourceContext::Http {
                method: "POST".to_string(),
                path,
            },
            payload: Payload::Raw(body),
            metadata: MessageMetadata {
                params,
                query,
                content_type: header_value(&headers, "content-type"),
                headers: headers_to_map(&headers),
            },
            target: TargetRequestOverrides::default(),
            trace: TraceContext::current(),
            attempt: 1,
            reply: Some(reply_tx),
        };

        self.input_tx
            .send(message)
            .await
            .map_err(|_| anyhow!("pipeline {} is not accepting messages", self.config.id))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("pipeline {} reply channel closed", self.config.id))
    }

    fn spawn_source_if_needed(&self) {
        let SourceConfig::Sqs(source) = self.config.source.clone() else {
            return;
        };

        let input_tx = self.input_tx.clone();
        let resources = self.resources.clone();
        let pipeline_id = self.config.id.clone();
        tokio::spawn(async move {
            run_sqs_source(
                pipeline_id,
                resources,
                input_tx,
                source.queue,
                source.queue_url,
                source.batch_size,
                source.wait_time_seconds,
                source.visibility_timeout_seconds,
            )
            .await;
        });
    }
}

async fn run_decode_worker(
    mut rx: mpsc::Receiver<PipelineMessage>,
    tx: mpsc::Sender<PipelineMessage>,
) {
    while let Some(mut message) = rx.recv().await {
        let span = message.trace.child_span(info_span!(
            "postio.pipeline.decode",
            pipeline.id = %message.pipeline_id,
            request.id = %message.id
        ));
        async {
            if let Payload::Raw(bytes) = &message.payload {
                message.payload = Payload::decode(bytes.clone());
            }
            if tx.send(message).await.is_err() {
                warn!("pipeline validate channel closed");
            }
        }
        .instrument(span)
        .await;
    }
}

async fn run_validate_worker(
    mut rx: mpsc::Receiver<PipelineMessage>,
    tx: mpsc::Sender<PipelineMessage>,
    completion_tx: mpsc::Sender<PipelineResult>,
    validator: PipelineValidator,
) {
    while let Some(message) = rx.recv().await {
        let engine = validator.engine_name();
        let span = message.trace.child_span(info_span!(
            "postio.pipeline.validate",
            pipeline.id = %message.pipeline_id,
            request.id = %message.id,
            engine = engine,
            result.status = tracing::field::Empty,
            validation.error_count = tracing::field::Empty
        ));
        async {
            match validator.validate(&message.payload) {
                Ok(()) => {
                    tracing::Span::current().record("result.status", "accepted");
                    tracing::Span::current().record("validation.error_count", 0);
                    if tx.send(message).await.is_err() {
                        warn!("pipeline transform channel closed");
                    }
                }
                Err(failure) => {
                    tracing::Span::current().record("result.status", "rejected");
                    tracing::Span::current()
                        .record("validation.error_count", failure.details.len());
                    warn!(
                        pipeline.id = %message.pipeline_id,
                        request.id = %message.id,
                        validation.error_count = failure.details.len(),
                        "payload validation rejected"
                    );
                    if completion_tx
                        .send(PipelineResult {
                            message,
                            target: Err(PipelineFailure::Validation(failure)),
                        })
                        .await
                        .is_err()
                    {
                        warn!("pipeline completion channel closed");
                    }
                }
            }
        }
        .instrument(span)
        .await;
    }
}

async fn run_transform_worker(
    mut rx: mpsc::Receiver<PipelineMessage>,
    tx: mpsc::Sender<PipelineMessage>,
    completion_tx: mpsc::Sender<PipelineResult>,
    transformer: PipelineTransformer,
) {
    let engine = transformer.engine_name();
    while let Some(mut message) = rx.recv().await {
        let span = message.trace.child_span(info_span!(
            "postio.pipeline.transform.request",
            pipeline.id = %message.pipeline_id,
            request.id = %message.id,
            engine = engine,
            result.status = tracing::field::Empty,
            error.kind = tracing::field::Empty
        ));
        async {
            match apply_transform(&transformer, &mut message) {
                Ok(()) => {
                    tracing::Span::current().record("result.status", "accepted");
                    if tx.send(message).await.is_err() {
                        warn!("pipeline target channel closed");
                    }
                }
                Err(error) => {
                    tracing::Span::current().record("result.status", "failed");
                    tracing::Span::current().record("error.kind", "transform_failed");
                    error!(
                        %error,
                        pipeline.id = %message.pipeline_id,
                        request.id = %message.id,
                        "pipeline transform failed"
                    );
                    if completion_tx
                        .send(PipelineResult {
                            message,
                            target: Err(PipelineFailure::Transform(error.to_string())),
                        })
                        .await
                        .is_err()
                    {
                        warn!("pipeline completion channel closed");
                    }
                }
            }
        }
        .instrument(span)
        .await;
    }
}

fn apply_transform(transform: &PipelineTransformer, message: &mut PipelineMessage) -> Result<()> {
    match transform {
        PipelineTransformer::Noop => {}
        PipelineTransformer::Template(config) => {
            let ctx = template_context(message);
            if let Some(body) = &config.output.body {
                message.payload = Payload::from_template_value(render_value(body, &ctx));
            }
            if let Some(method) = &config.output.method {
                message.target.method = Some(render_to_string(method, &ctx));
            }
            if let Some(url) = &config.output.url {
                message.target.url = Some(render_to_string(url, &ctx));
            }
            if let Some(headers) = &config.output.headers {
                message.target.headers = headers
                    .iter()
                    .map(|(key, value)| (key.clone(), render_to_string(value, &ctx)))
                    .collect();
            }
            if let Some(query) = &config.output.query {
                message.target.query = query
                    .iter()
                    .map(|(key, value)| (key.clone(), render_template_string(value, &ctx)))
                    .collect();
            }
            if let Some(attributes) = &config.output.attributes {
                message.target.attributes = attributes
                    .iter()
                    .map(|(key, value)| (key.clone(), render_template_string(value, &ctx)))
                    .collect();
            }
            if let Some(delay_seconds) = config.output.delay_seconds {
                message.target.delay_seconds = Some(delay_seconds);
            }
        }
        PipelineTransformer::Rhai(transformer) => {
            transformer.transform(message)?.apply_to(message);
        }
    }
    Ok(())
}

fn render_template_string(value: &serde_json::Value, ctx: &TemplateContext) -> String {
    match render_value(value, ctx) {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(value) => value,
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        value => value.to_string(),
    }
}

fn template_context(message: &PipelineMessage) -> TemplateContext {
    TemplateContext {
        params: message.metadata.params.clone(),
        query: message.metadata.query.clone(),
        headers: message.metadata.headers.clone(),
        form: BTreeMap::new(),
        file: serde_json::Value::Null,
        body: message.payload.to_template_value(),
        context: BTreeMap::from([
            ("requestId".to_string(), message.id.to_string()),
            ("pipelineId".to_string(), message.pipeline_id.clone()),
            ("attempt".to_string(), message.attempt.to_string()),
            (
                "sourceType".to_string(),
                message.source.type_name().to_string(),
            ),
        ]),
    }
}

fn completion_template_context(
    message: &PipelineMessage,
    response: &CompletionResponse,
) -> TemplateContext {
    let mut ctx = template_context(message);
    ctx.context
        .insert("status".to_string(), response.status.clone());
    if let Some(target_type) = &response.target_type {
        ctx.context
            .insert("targetType".to_string(), target_type.clone());
    }
    if let Some(target_status_code) = response.target_status_code {
        ctx.context.insert(
            "targetStatusCode".to_string(),
            target_status_code.to_string(),
        );
    }
    if let Some(message_id) = &response.message_id {
        ctx.context
            .insert("messageId".to_string(), message_id.clone());
    }
    if let Some(error) = &response.error {
        ctx.context.insert("error".to_string(), error.clone());
    }
    ctx
}

async fn run_target_worker(
    mut rx: mpsc::Receiver<PipelineMessage>,
    tx: mpsc::Sender<PipelineResult>,
    config: Arc<PipelineConfig>,
    resources: PipelineResources,
) {
    while let Some(message) = rx.recv().await {
        let target_type = config.target.type_name();
        let span = message.trace.child_span(info_span!(
            "postio.pipeline.target.send",
            pipeline.id = %message.pipeline_id,
            request.id = %message.id,
            target.type = target_type,
            target.status = tracing::field::Empty,
            result.status = tracing::field::Empty,
            error.kind = tracing::field::Empty
        ));
        let target = async {
            match send_to_target_with_retry(&config.target, &resources, &message).await {
                Ok(response) => {
                    tracing::Span::current().record("target.status", response.status_code);
                    tracing::Span::current().record("result.status", "accepted");
                    Ok(response)
                }
                Err(error) => {
                    tracing::Span::current().record("result.status", "failed");
                    tracing::Span::current().record("error.kind", "target_send_failed");
                    error!(
                        %error,
                        pipeline.id = %message.pipeline_id,
                        request.id = %message.id,
                        target.type = target_type,
                        "pipeline target failed"
                    );
                    Err(error)
                }
            }
        }
        .instrument(span)
        .await
        .map_err(|error| PipelineFailure::Target(error.to_string()));
        if tx.send(PipelineResult { message, target }).await.is_err() {
            warn!("pipeline completion channel closed");
        }
    }
}

async fn run_completion_worker(
    mut rx: mpsc::Receiver<PipelineResult>,
    resources: PipelineResources,
    config: Arc<PipelineConfig>,
) {
    while let Some(result) = rx.recv().await {
        let span = result.message.trace.child_span(info_span!(
            "postio.pipeline.complete",
            pipeline.id = %result.message.pipeline_id,
            request.id = %result.message.id,
            source.type = result.message.source.type_name(),
            completion.action = tracing::field::Empty,
            result.status = tracing::field::Empty,
            error.kind = tracing::field::Empty
        ));
        async {
            let (kind, mut response) = match result.target {
                Ok(target) => (
                    CompletionKind::Success,
                    CompletionResponse {
                        pipeline_id: result.message.pipeline_id.clone(),
                        request_id: result.message.id.to_string(),
                        status: "accepted".to_string(),
                        target_type: Some(target.target_type.to_string()),
                        target_status_code: Some(target.status_code),
                        body: target.body,
                        message_id: target.message_id,
                        error: None,
                        details: None,
                        http_status_code: None,
                        http_body: None,
                    },
                ),
                Err(PipelineFailure::Target(error)) => (
                    CompletionKind::TargetFailure,
                    CompletionResponse {
                        pipeline_id: result.message.pipeline_id.clone(),
                        request_id: result.message.id.to_string(),
                        status: "failed".to_string(),
                        target_type: None,
                        target_status_code: None,
                        body: None,
                        message_id: None,
                        error: Some(error),
                        details: None,
                        http_status_code: None,
                        http_body: None,
                    },
                ),
                Err(PipelineFailure::Transform(error)) => (
                    CompletionKind::TargetFailure,
                    CompletionResponse {
                        pipeline_id: result.message.pipeline_id.clone(),
                        request_id: result.message.id.to_string(),
                        status: "failed".to_string(),
                        target_type: None,
                        target_status_code: None,
                        body: None,
                        message_id: None,
                        error: Some(error),
                        details: None,
                        http_status_code: None,
                        http_body: None,
                    },
                ),
                Err(PipelineFailure::Validation(failure)) => (
                    CompletionKind::ValidationFailure,
                    CompletionResponse {
                        pipeline_id: result.message.pipeline_id.clone(),
                        request_id: result.message.id.to_string(),
                        status: "rejected".to_string(),
                        target_type: None,
                        target_status_code: None,
                        body: None,
                        message_id: None,
                        error: Some(failure.error),
                        details: Some(failure.details),
                        http_status_code: None,
                        http_body: None,
                    },
                ),
            };

            complete_source(
                &config.source,
                &resources,
                &result.message,
                kind,
                &mut response,
            )
            .await;
            tracing::Span::current().record("result.status", response.status.as_str());

            if let Some(reply) = result.message.reply {
                let _ = reply.send(response);
            }
        }
        .instrument(span)
        .await;
    }
}

#[derive(Debug, Clone, Copy)]
enum CompletionKind {
    Success,
    TargetFailure,
    ValidationFailure,
}

async fn complete_source(
    source_config: &SourceConfig,
    resources: &PipelineResources,
    message: &PipelineMessage,
    kind: CompletionKind,
    response: &mut CompletionResponse,
) {
    match (&message.source, source_config) {
        (SourceContext::Http { .. }, SourceConfig::Http(config)) => {
            let rule = config
                .completion
                .as_ref()
                .and_then(|completion| match kind {
                    CompletionKind::Success => completion.on_success.as_ref(),
                    CompletionKind::TargetFailure => completion.on_failure.as_ref(),
                    CompletionKind::ValidationFailure => completion.on_validation_failure.as_ref(),
                });
            apply_http_completion(rule, message, response);
        }
        (
            SourceContext::Sqs {
                queue_url,
                receipt_handle,
                ..
            },
            SourceConfig::Sqs(config),
        ) => {
            let rule = config
                .completion
                .as_ref()
                .and_then(|completion| match kind {
                    CompletionKind::Success => completion.on_success.as_ref(),
                    CompletionKind::TargetFailure => completion.on_failure.as_ref(),
                    CompletionKind::ValidationFailure => completion.on_validation_failure.as_ref(),
                });
            let default_action = match kind {
                CompletionKind::Success => SqsCompletionAction::Ack,
                CompletionKind::TargetFailure | CompletionKind::ValidationFailure => {
                    SqsCompletionAction::Retry
                }
            };
            complete_sqs_source(
                resources,
                message,
                queue_url,
                receipt_handle,
                rule,
                default_action,
            )
            .await;
        }
        _ => {}
    }
}

fn apply_http_completion(
    rule: Option<&HttpCompletionRule>,
    message: &PipelineMessage,
    response: &mut CompletionResponse,
) {
    let Some(response_config) = rule.and_then(|rule| rule.response.as_ref()) else {
        return;
    };

    if let Some(status) = response_config.status {
        response.http_status_code = Some(status);
    }
    if let Some(body) = &response_config.body {
        let ctx = completion_template_context(message, response);
        response.http_body = Some(render_value(body, &ctx));
    }
}

async fn complete_sqs_source(
    resources: &PipelineResources,
    message: &PipelineMessage,
    queue_url: &str,
    receipt_handle: &str,
    rule: Option<&SqsCompletionRule>,
    default_action: SqsCompletionAction,
) {
    let action = rule.and_then(|rule| rule.action).unwrap_or(default_action);
    tracing::Span::current().record("completion.action", sqs_completion_action_name(action));

    match action {
        SqsCompletionAction::Ack | SqsCompletionAction::Drop => {
            delete_sqs_source_message(resources, queue_url, receipt_handle).await;
        }
        SqsCompletionAction::Retry => {}
        SqsCompletionAction::DeadLetter => {
            let Some(dead_letter) = rule.and_then(|rule| rule.dead_letter.as_ref()) else {
                tracing::Span::current().record("error.kind", "dead_letter_not_configured");
                error!(
                    pipeline.id = %message.pipeline_id,
                    request.id = %message.id,
                    "sqs completion requested deadLetter without deadLetter config"
                );
                return;
            };
            match send_sqs_dead_letter(resources, message, dead_letter).await {
                Ok(()) => {
                    delete_sqs_source_message(resources, queue_url, receipt_handle).await;
                }
                Err(error) => {
                    tracing::Span::current().record("error.kind", "dead_letter_send_failed");
                    error!(
                        %error,
                        pipeline.id = %message.pipeline_id,
                        request.id = %message.id,
                        "failed to send sqs dead letter message"
                    );
                }
            }
        }
    }
}

fn sqs_completion_action_name(action: SqsCompletionAction) -> &'static str {
    match action {
        SqsCompletionAction::Ack => "ack",
        SqsCompletionAction::Retry => "retry",
        SqsCompletionAction::Drop => "drop",
        SqsCompletionAction::DeadLetter => "deadLetter",
    }
}

async fn delete_sqs_source_message(
    resources: &PipelineResources,
    queue_url: &str,
    receipt_handle: &str,
) {
    if let Err(error) = resources
        .sqs
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .send()
        .await
    {
        tracing::Span::current().record("error.kind", "sqs_delete_failed");
        error!(%error, "failed to delete processed sqs message");
    }
}

async fn send_sqs_dead_letter(
    resources: &PipelineResources,
    message: &PipelineMessage,
    dead_letter: &SqsDeadLetterConfig,
) -> Result<()> {
    let queue_url = resources
        .resolve_queue_url(
            dead_letter.queue.as_deref(),
            dead_letter.queue_url.as_deref(),
        )
        .await?;
    resources
        .sqs
        .send_message()
        .queue_url(queue_url)
        .message_body(message.payload.to_string_body())
        .set_delay_seconds(dead_letter.delay_seconds)
        .set_message_attributes(sqs_message_attributes(
            &dead_letter.attributes.clone().unwrap_or_default(),
        ))
        .send()
        .await
        .context("failed to send sqs dead letter message")?;
    Ok(())
}

async fn send_to_target(
    target: &TargetConfig,
    resources: &PipelineResources,
    message: &PipelineMessage,
) -> Result<TargetResponse> {
    match target {
        TargetConfig::Http(config) => {
            let method = message.target.method.as_ref().unwrap_or(&config.method);
            let url = message.target.url.as_ref().unwrap_or(&config.url);
            let method = reqwest::Method::from_bytes(method.as_bytes())
                .with_context(|| format!("invalid http target method {method}"))?;
            let mut request = resources
                .http
                .request(method, url)
                .body(message.payload.to_bytes());
            if let Some(timeout_ms) = config.timeout_ms {
                request = request.timeout(Duration::from_millis(timeout_ms));
            }
            if let Some(headers) = &config.headers {
                for (key, value) in headers {
                    request = request.header(key, value);
                }
            }
            for (key, value) in &message.target.headers {
                request = request.header(key, value);
            }
            if !message.target.query.is_empty() {
                request = request.query(&message.target.query);
            }
            if let Some(content_type) = &message.metadata.content_type {
                let has_content_type = config
                    .headers
                    .as_ref()
                    .is_some_and(|headers| contains_header(headers, "content-type"))
                    || contains_header(&message.target.headers, "content-type");
                if !has_content_type {
                    request = request.header("content-type", content_type);
                }
            }
            let response = request.send().await.context("failed to send http target")?;
            let status_code = response.status().as_u16();
            let body = response
                .text()
                .await
                .context("failed to read http target response")?;
            if status_code >= 500 {
                return Err(anyhow!(
                    "http target returned server error {status_code}: {body}"
                ));
            }
            Ok(TargetResponse {
                target_type: "http",
                status_code,
                body: Some(body),
                message_id: None,
            })
        }
        TargetConfig::Sqs(config) => {
            let delay_seconds = message.target.delay_seconds.or(config.delay_seconds);
            let queue_url = resources
                .resolve_queue_url(config.queue.as_deref(), config.queue_url.as_deref())
                .await?;
            let response = resources
                .sqs
                .send_message()
                .queue_url(queue_url)
                .message_body(message.payload.to_string_body())
                .set_message_attributes(sqs_message_attributes(&message.target.attributes))
                .set_delay_seconds(delay_seconds)
                .send()
                .await
                .context("failed to send sqs message")?;
            Ok(TargetResponse {
                target_type: "sqs",
                status_code: 202,
                body: None,
                message_id: response.message_id,
            })
        }
    }
}

async fn send_to_target_with_retry(
    target: &TargetConfig,
    resources: &PipelineResources,
    message: &PipelineMessage,
) -> Result<TargetResponse> {
    let retry = target_retry_config(target);
    let max_attempts = retry.map(|retry| retry.max_attempts).unwrap_or(1).max(1);
    let mut last_error = None;

    for attempt in 1..=max_attempts {
        let span = message.trace.child_span(info_span!(
            "postio.pipeline.target.attempt",
            pipeline.id = %message.pipeline_id,
            request.id = %message.id,
            target.type = target.type_name(),
            retry.attempt = attempt,
            retry.max_attempts = max_attempts,
            result.status = tracing::field::Empty,
            error.kind = tracing::field::Empty
        ));

        let result = async {
            match send_to_target(target, resources, message).await {
                Ok(response) => {
                    tracing::Span::current().record("result.status", "accepted");
                    Ok(response)
                }
                Err(error) => {
                    tracing::Span::current().record("result.status", "failed");
                    tracing::Span::current().record("error.kind", "target_send_failed");
                    Err(error)
                }
            }
        }
        .instrument(span)
        .await;

        match result {
            Ok(response) => return Ok(response),
            Err(error) if attempt < max_attempts => {
                warn!(
                    %error,
                    pipeline.id = %message.pipeline_id,
                    request.id = %message.id,
                    target.type = target.type_name(),
                    retry.attempt = attempt,
                    retry.max_attempts = max_attempts,
                    "target attempt failed; retrying"
                );
                last_error = Some(error);
                if let Some(retry) = retry {
                    tokio::time::sleep(retry_backoff_delay(retry, attempt)).await;
                }
            }
            Err(error) => {
                last_error = Some(error);
                break;
            }
        }
    }

    let error = last_error.unwrap_or_else(|| anyhow!("target failed without attempts"));
    Err(anyhow!(
        "target failed after {max_attempts} attempt(s): {error}"
    ))
}

fn target_retry_config(target: &TargetConfig) -> Option<&TargetRetryConfig> {
    match target {
        TargetConfig::Http(config) => config.retry.as_ref(),
        TargetConfig::Sqs(config) => config.retry.as_ref(),
    }
}

fn retry_backoff_delay(retry: &TargetRetryConfig, failed_attempt: u32) -> Duration {
    match &retry.backoff {
        RetryBackoffConfig::Fixed { delay_ms } => Duration::from_millis(*delay_ms),
        RetryBackoffConfig::Exponential { initial_ms, max_ms } => {
            let exponent = failed_attempt.saturating_sub(1).min(63);
            let factor = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
            Duration::from_millis(initial_ms.saturating_mul(factor).min(*max_ms))
        }
    }
}

fn sqs_message_attributes(
    attributes: &BTreeMap<String, String>,
) -> Option<HashMap<String, SqsAttribute>> {
    if attributes.is_empty() {
        return None;
    }

    Some(
        attributes
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    SqsAttribute::builder()
                        .data_type("String")
                        .string_value(value)
                        .build()
                        .expect("valid sqs string attribute"),
                )
            })
            .collect(),
    )
}

#[allow(clippy::too_many_arguments)]
async fn run_sqs_source(
    pipeline_id: String,
    resources: PipelineResources,
    input_tx: mpsc::Sender<PipelineMessage>,
    queue: Option<String>,
    queue_url: Option<String>,
    batch_size: i32,
    wait_time_seconds: i32,
    visibility_timeout_seconds: Option<i32>,
) {
    let queue_url = match resources
        .resolve_queue_url(queue.as_deref(), queue_url.as_deref())
        .await
    {
        Ok(value) => value,
        Err(error) => {
            error!(%error, pipeline.id = %pipeline_id, "failed to start sqs source");
            return;
        }
    };

    loop {
        let mut request = resources
            .sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(batch_size)
            .wait_time_seconds(wait_time_seconds);
        if let Some(visibility_timeout_seconds) = visibility_timeout_seconds {
            request = request.visibility_timeout(visibility_timeout_seconds);
        }

        match request.send().await {
            Ok(response) => {
                for message in response.messages() {
                    let receipt_handle = match message.receipt_handle() {
                        Some(value) => value.to_string(),
                        None => continue,
                    };
                    let id = Uuid::new_v4();
                    let receive_span = info_span!(
                        "postio.pipeline.receive",
                        pipeline.id = %pipeline_id,
                        request.id = %id,
                        source.type = "sqs",
                        sqs.message_id = message.message_id().unwrap_or_default()
                    );
                    let pipeline_message = PipelineMessage {
                        id,
                        pipeline_id: pipeline_id.clone(),
                        source: SourceContext::Sqs {
                            queue_url: queue_url.clone(),
                            receipt_handle,
                            message_id: message.message_id().map(ToString::to_string),
                        },
                        payload: Payload::Raw(Bytes::from(
                            message.body().unwrap_or_default().to_string(),
                        )),
                        metadata: MessageMetadata::default(),
                        target: TargetRequestOverrides::default(),
                        trace: TraceContext::from_span(&receive_span),
                        attempt: 1,
                        reply: None,
                    };
                    let sent = async {
                        if input_tx.send(pipeline_message).await.is_err() {
                            warn!(pipeline.id = %pipeline_id, "pipeline input channel closed");
                            return false;
                        }
                        true
                    }
                    .instrument(receive_span)
                    .await;
                    if !sent {
                        return;
                    }
                }
            }
            Err(error) => {
                warn!(%error, pipeline.id = %pipeline_id, "failed to poll sqs source");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

fn headers_to_map(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(key, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (key.as_str().to_string(), value.to_string()))
        })
        .collect()
}

fn header_value(headers: &HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn contains_header(headers: &BTreeMap<String, String>, key: &str) -> bool {
    headers
        .keys()
        .any(|header| header.eq_ignore_ascii_case(key))
}
