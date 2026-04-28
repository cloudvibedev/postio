use std::{collections::BTreeMap, sync::Arc};

use anyhow::{anyhow, Context, Result};
use aws_sdk_sqs::Client as SqsClient;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct PipelineResources {
    pub sqs: SqsClient,
    pub http: reqwest::Client,
    queue_cache: Arc<RwLock<BTreeMap<String, String>>>,
}

impl PipelineResources {
    pub fn new(sqs: SqsClient, http: reqwest::Client) -> Self {
        Self {
            sqs,
            http,
            queue_cache: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn resolve_queue_url(
        &self,
        queue: Option<&str>,
        queue_url: Option<&str>,
    ) -> Result<String> {
        if let Some(queue_url) = queue_url {
            return Ok(queue_url.to_string());
        }

        let queue = queue.context("sqs queue is required when queueUrl is absent")?;
        if queue.starts_with("http://") || queue.starts_with("https://") {
            return Ok(queue.to_string());
        }
        if let Some(queue_url) = self.queue_cache.read().await.get(queue) {
            return Ok(queue_url.clone());
        }

        let response = self
            .sqs
            .get_queue_url()
            .queue_name(queue)
            .send()
            .await
            .with_context(|| format!("failed to resolve sqs queue url for {queue}"))?;
        let queue_url = response
            .queue_url
            .ok_or_else(|| anyhow!("sqs queue url not returned for {queue}"))?;
        self.queue_cache
            .write()
            .await
            .insert(queue.to_string(), queue_url.clone());
        Ok(queue_url)
    }
}
