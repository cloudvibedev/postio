use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::{primitives::ByteStream, Client as S3Client};
use aws_sdk_sns::{types::MessageAttributeValue as SnsAttribute, Client as SnsClient};
use aws_sdk_sqs::{types::MessageAttributeValue as SqsAttribute, Client as SqsClient};
use serde_json::Value;
use tracing::{info, info_span, Instrument};

use crate::bridge::{
    config::SinkConfig,
    dispatcher::{DispatchRequest, DispatchResponse, SinkDispatcher},
    template::{render_to_string, render_value},
};

pub struct AwsDispatcher {
    sns: SnsClient,
    sqs: SqsClient,
    s3: S3Client,
    topic_cache: Mutex<BTreeMap<String, String>>,
    queue_cache: Mutex<BTreeMap<String, String>>,
}

impl AwsDispatcher {
    pub fn new(sns: SnsClient, sqs: SqsClient, s3: S3Client) -> Self {
        Self {
            sns,
            sqs,
            s3,
            topic_cache: Mutex::new(BTreeMap::new()),
            queue_cache: Mutex::new(BTreeMap::new()),
        }
    }
}

#[async_trait]
impl SinkDispatcher for AwsDispatcher {
    async fn dispatch(&self, request: DispatchRequest) -> Result<DispatchResponse> {
        let route_id = request.route.id.clone();
        let sink_type = request.route.sink.type_name();
        match request.route.sink.clone() {
            SinkConfig::Sns {
                topic,
                topic_arn,
                subject,
                message,
                attributes,
            } => {
                self.publish_sns(request, topic, topic_arn, subject, message, attributes)
                    .instrument(info_span!(
                        "postio.aws.sns",
                        route_id = %route_id,
                        sink_type = %sink_type
                    ))
                    .await
            }
            SinkConfig::Sqs {
                queue,
                queue_url,
                message,
                attributes,
            } => {
                self.send_sqs(request, queue, queue_url, message, attributes)
                    .instrument(info_span!(
                        "postio.aws.sqs",
                        route_id = %route_id,
                        sink_type = %sink_type
                    ))
                    .await
            }
            SinkConfig::S3 {
                bucket,
                key,
                content_type,
                object,
                metadata,
            } => {
                self.put_s3(request, bucket, key, content_type, object, metadata)
                    .instrument(info_span!(
                        "postio.aws.s3",
                        route_id = %route_id,
                        sink_type = %sink_type
                    ))
                    .await
            }
        }
    }
}

impl AwsDispatcher {
    async fn publish_sns(
        &self,
        request: DispatchRequest,
        topic: Option<String>,
        topic_arn: Option<String>,
        subject: Option<String>,
        message: Option<Value>,
        attributes: Option<BTreeMap<String, Value>>,
    ) -> Result<DispatchResponse> {
        let topic_arn = match topic_arn {
            Some(value) => render_to_string(&value, &request.template_context),
            None => {
                let topic = topic.context("sns topic is required when topicArn is absent")?;
                self.resolve_topic_arn(&render_to_string(&topic, &request.template_context))
                    .await?
            }
        };
        let message = message
            .map(|value| render_value(&value, &request.template_context))
            .unwrap_or(request.body);
        let response = self
            .sns
            .publish()
            .topic_arn(topic_arn)
            .set_subject(subject.map(|value| render_to_string(&value, &request.template_context)))
            .set_message_attributes(render_sns_attributes(attributes, &request.template_context))
            .message(stringify(message))
            .send()
            .instrument(info_span!("postio.aws.sns.publish"))
            .await
            .context("failed to publish sns message")?;
        info!(
            route_id = request.route.id.as_str(),
            aws.sns.message_id = response.message_id.as_deref(),
            "published sns message"
        );

        Ok(DispatchResponse {
            route_id: request.route.id,
            sink: "sns".to_string(),
            status: "accepted".to_string(),
            message_id: response.message_id,
            bucket: None,
            key: None,
            etag: None,
        })
    }

    async fn send_sqs(
        &self,
        request: DispatchRequest,
        queue: Option<String>,
        queue_url: Option<String>,
        message: Option<Value>,
        attributes: Option<BTreeMap<String, Value>>,
    ) -> Result<DispatchResponse> {
        let queue_url = match queue_url {
            Some(value) => render_to_string(&value, &request.template_context),
            None => {
                let queue = queue.context("sqs queue is required when queueUrl is absent")?;
                self.resolve_queue_url(&render_to_string(&queue, &request.template_context))
                    .await?
            }
        };
        let message = message
            .map(|value| render_value(&value, &request.template_context))
            .unwrap_or(request.body);
        let response = self
            .sqs
            .send_message()
            .queue_url(queue_url)
            .set_message_attributes(render_sqs_attributes(attributes, &request.template_context))
            .message_body(stringify(message))
            .send()
            .instrument(info_span!("postio.aws.sqs.send_message"))
            .await
            .context("failed to send sqs message")?;
        info!(
            route_id = request.route.id.as_str(),
            aws.sqs.message_id = response.message_id.as_deref(),
            "sent sqs message"
        );

        Ok(DispatchResponse {
            route_id: request.route.id,
            sink: "sqs".to_string(),
            status: "accepted".to_string(),
            message_id: response.message_id,
            bucket: None,
            key: None,
            etag: None,
        })
    }

    async fn put_s3(
        &self,
        request: DispatchRequest,
        bucket: String,
        key: String,
        content_type: Option<String>,
        object: Option<Value>,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<DispatchResponse> {
        let bucket = render_to_string(&bucket, &request.template_context);
        let key = render_to_string(&key, &request.template_context);
        let object = object
            .map(|value| render_value(&value, &request.template_context))
            .unwrap_or(request.body);
        let body = stringify(object);
        let response = self
            .s3
            .put_object()
            .bucket(bucket.clone())
            .key(key.clone())
            .body(ByteStream::from(body.into_bytes()))
            .set_content_type(content_type)
            .set_metadata(render_metadata(metadata, &request.template_context))
            .send()
            .instrument(info_span!(
                "postio.aws.s3.put_object",
                s3.bucket = %bucket,
                s3.key = %key
            ))
            .await
            .context("failed to put s3 object")?;
        info!(
            route_id = request.route.id.as_str(),
            s3.bucket = bucket.as_str(),
            s3.key = key.as_str(),
            s3.etag = response.e_tag.as_deref(),
            "put s3 object"
        );

        Ok(DispatchResponse {
            route_id: request.route.id,
            sink: "s3".to_string(),
            status: "created".to_string(),
            message_id: None,
            bucket: Some(bucket),
            key: Some(key),
            etag: response.e_tag,
        })
    }

    async fn resolve_topic_arn(&self, topic: &str) -> Result<String> {
        if topic.starts_with("arn:aws:sns:") {
            return Ok(topic.to_string());
        }
        if let Some(value) = self.topic_cache.lock().expect("topic cache").get(topic) {
            return Ok(value.clone());
        }

        let mut next_token = None;
        loop {
            let response = self
                .sns
                .list_topics()
                .set_next_token(next_token)
                .send()
                .instrument(info_span!("postio.aws.sns.list_topics", sns.topic = topic))
                .await
                .context("failed to list sns topics")?;
            if let Some(topic_arn) = response
                .topics()
                .iter()
                .filter_map(|item| item.topic_arn())
                .find(|arn| arn.ends_with(&format!(":{topic}")))
            {
                self.topic_cache
                    .lock()
                    .expect("topic cache")
                    .insert(topic.to_string(), topic_arn.to_string());
                return Ok(topic_arn.to_string());
            }
            next_token = response.next_token().map(ToString::to_string);
            if next_token.is_none() {
                return Err(anyhow!("sns topic not found: {topic}"));
            }
        }
    }

    async fn resolve_queue_url(&self, queue: &str) -> Result<String> {
        if queue.starts_with("https://") || queue.starts_with("http://") {
            return Ok(queue.to_string());
        }
        if let Some(value) = self.queue_cache.lock().expect("queue cache").get(queue) {
            return Ok(value.clone());
        }

        let response = self
            .sqs
            .get_queue_url()
            .queue_name(queue)
            .send()
            .instrument(info_span!(
                "postio.aws.sqs.get_queue_url",
                sqs.queue = queue
            ))
            .await
            .context("failed to resolve sqs queue url")?;
        let queue_url = response
            .queue_url
            .ok_or_else(|| anyhow!("sqs queue url not returned for {queue}"))?;
        self.queue_cache
            .lock()
            .expect("queue cache")
            .insert(queue.to_string(), queue_url.clone());
        Ok(queue_url)
    }
}

fn render_sns_attributes(
    attributes: Option<BTreeMap<String, Value>>,
    ctx: &crate::bridge::template::TemplateContext,
) -> Option<HashMap<String, SnsAttribute>> {
    attributes.map(|attributes| {
        attributes
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    SnsAttribute::builder()
                        .data_type("String")
                        .string_value(stringify(render_value(&value, ctx)))
                        .build()
                        .expect("valid sns string attribute"),
                )
            })
            .collect()
    })
}

fn render_sqs_attributes(
    attributes: Option<BTreeMap<String, Value>>,
    ctx: &crate::bridge::template::TemplateContext,
) -> Option<HashMap<String, SqsAttribute>> {
    attributes.map(|attributes| {
        attributes
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    SqsAttribute::builder()
                        .data_type("String")
                        .string_value(stringify(render_value(&value, ctx)))
                        .build()
                        .expect("valid sqs string attribute"),
                )
            })
            .collect()
    })
}

fn render_metadata(
    metadata: Option<BTreeMap<String, Value>>,
    ctx: &crate::bridge::template::TemplateContext,
) -> Option<HashMap<String, String>> {
    metadata.map(|metadata| {
        metadata
            .into_iter()
            .map(|(key, value)| (key, stringify(render_value(&value, ctx))))
            .collect()
    })
}

fn stringify(value: Value) -> String {
    match value {
        Value::String(value) => value,
        value => value.to_string(),
    }
}
