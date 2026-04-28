use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use axum::http::HeaderMap;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info_span, warn, Instrument};
use uuid::Uuid;

use crate::pipeline::{
    config::{PipelineConfig, SourceConfig, TargetConfig},
    model::{
        CompletionResponse, MessageMetadata, Payload, PipelineMessage, PipelineResult,
        SourceContext, TargetResponse,
    },
    resources::PipelineResources,
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

        tokio::spawn(run_decode_worker(input_rx, validate_tx));
        tokio::spawn(run_validate_noop_worker(validate_rx, transform_tx));
        tokio::spawn(run_transform_noop_worker(transform_rx, target_tx));
        tokio::spawn(run_target_worker(
            target_rx,
            completion_tx,
            Arc::clone(&config),
            resources.clone(),
        ));
        tokio::spawn(run_completion_worker(completion_rx, resources.clone()));

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
        let SourceConfig::Sqs {
            queue,
            queue_url,
            batch_size,
            wait_time_seconds,
            visibility_timeout_seconds,
        } = self.config.source.clone()
        else {
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
                queue,
                queue_url,
                batch_size,
                wait_time_seconds,
                visibility_timeout_seconds,
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
        async {
            if let Payload::Raw(bytes) = &message.payload {
                message.payload = Payload::decode(bytes.clone());
            }
            if tx.send(message).await.is_err() {
                warn!("pipeline validate channel closed");
            }
        }
        .instrument(info_span!("postio.pipeline.decode"))
        .await;
    }
}

async fn run_validate_noop_worker(
    mut rx: mpsc::Receiver<PipelineMessage>,
    tx: mpsc::Sender<PipelineMessage>,
) {
    while let Some(message) = rx.recv().await {
        async {
            if tx.send(message).await.is_err() {
                warn!("pipeline transform channel closed");
            }
        }
        .instrument(info_span!("postio.pipeline.validate", engine = "noop"))
        .await;
    }
}

async fn run_transform_noop_worker(
    mut rx: mpsc::Receiver<PipelineMessage>,
    tx: mpsc::Sender<PipelineMessage>,
) {
    while let Some(message) = rx.recv().await {
        async {
            if tx.send(message).await.is_err() {
                warn!("pipeline target channel closed");
            }
        }
        .instrument(info_span!(
            "postio.pipeline.transform.request",
            engine = "noop"
        ))
        .await;
    }
}

async fn run_target_worker(
    mut rx: mpsc::Receiver<PipelineMessage>,
    tx: mpsc::Sender<PipelineResult>,
    config: Arc<PipelineConfig>,
    resources: PipelineResources,
) {
    while let Some(message) = rx.recv().await {
        let target_type = config.target.type_name();
        let target = async { send_to_target(&config.target, &resources, &message).await }
            .instrument(info_span!(
                "postio.pipeline.target.send",
                pipeline.id = %message.pipeline_id,
                target.type = target_type
            ))
            .await
            .map_err(|error| error.to_string());
        if tx.send(PipelineResult { message, target }).await.is_err() {
            warn!("pipeline completion channel closed");
        }
    }
}

async fn run_completion_worker(
    mut rx: mpsc::Receiver<PipelineResult>,
    resources: PipelineResources,
) {
    while let Some(result) = rx.recv().await {
        async {
            let response = match result.target {
                Ok(target) => {
                    if let SourceContext::Sqs {
                        queue_url,
                        receipt_handle,
                        ..
                    } = &result.message.source
                    {
                        if let Err(error) = resources
                            .sqs
                            .delete_message()
                            .queue_url(queue_url)
                            .receipt_handle(receipt_handle)
                            .send()
                            .await
                        {
                            error!(%error, "failed to delete processed sqs message");
                        }
                    }
                    CompletionResponse {
                        pipeline_id: result.message.pipeline_id.clone(),
                        request_id: result.message.id.to_string(),
                        status: "accepted".to_string(),
                        target_type: Some(target.target_type.to_string()),
                        target_status_code: Some(target.status_code),
                        body: target.body,
                        message_id: target.message_id,
                        error: None,
                    }
                }
                Err(error) => CompletionResponse {
                    pipeline_id: result.message.pipeline_id.clone(),
                    request_id: result.message.id.to_string(),
                    status: "failed".to_string(),
                    target_type: None,
                    target_status_code: None,
                    body: None,
                    message_id: None,
                    error: Some(error),
                },
            };

            if let Some(reply) = result.message.reply {
                let _ = reply.send(response);
            }
        }
        .instrument(info_span!(
            "postio.pipeline.complete",
            pipeline.id = %result.message.pipeline_id
        ))
        .await;
    }
}

async fn send_to_target(
    target: &TargetConfig,
    resources: &PipelineResources,
    message: &PipelineMessage,
) -> Result<TargetResponse> {
    match target {
        TargetConfig::Http {
            method,
            url,
            headers,
            timeout_ms,
        } => {
            let method = reqwest::Method::from_bytes(method.as_bytes())
                .with_context(|| format!("invalid http target method {method}"))?;
            let mut request = resources
                .http
                .request(method, url)
                .body(message.payload.to_bytes());
            if let Some(timeout_ms) = timeout_ms {
                request = request.timeout(Duration::from_millis(*timeout_ms));
            }
            if let Some(headers) = headers {
                for (key, value) in headers {
                    request = request.header(key, value);
                }
            }
            if let Some(content_type) = &message.metadata.content_type {
                request = request.header("content-type", content_type);
            }
            let response = request.send().await.context("failed to send http target")?;
            let status_code = response.status().as_u16();
            let body = response
                .text()
                .await
                .context("failed to read http target response")?;
            Ok(TargetResponse {
                target_type: "http",
                status_code,
                body: Some(body),
                message_id: None,
            })
        }
        TargetConfig::Sqs {
            queue,
            queue_url,
            delay_seconds,
        } => {
            let queue_url = resources
                .resolve_queue_url(queue.as_deref(), queue_url.as_deref())
                .await?;
            let response = resources
                .sqs
                .send_message()
                .queue_url(queue_url)
                .message_body(message.payload.to_string_body())
                .set_delay_seconds(*delay_seconds)
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
                        attempt: 1,
                        reply: None,
                    };
                    if input_tx.send(pipeline_message).await.is_err() {
                        warn!(pipeline.id = %pipeline_id, "pipeline input channel closed");
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
