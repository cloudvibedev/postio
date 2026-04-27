use std::sync::Arc;

use crate::bridge::dispatcher::SinkDispatcher;

#[derive(Clone)]
pub struct AppState {
    dispatcher: Arc<dyn SinkDispatcher>,
}

impl AppState {
    pub fn new(dispatcher: Arc<dyn SinkDispatcher>) -> Self {
        Self { dispatcher }
    }

    pub fn dispatcher(&self) -> Arc<dyn SinkDispatcher> {
        Arc::clone(&self.dispatcher)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;

    use super::AppState;
    use crate::bridge::dispatcher::{DispatchRequest, DispatchResponse, SinkDispatcher};

    #[test]
    fn state_can_be_constructed() {
        let _state = AppState::new(Arc::new(NoopDispatcher));
    }

    struct NoopDispatcher;

    #[async_trait]
    impl SinkDispatcher for NoopDispatcher {
        async fn dispatch(&self, request: DispatchRequest) -> Result<DispatchResponse> {
            Ok(DispatchResponse {
                route_id: request.route.id,
                sink: "test".to_string(),
                status: "accepted".to_string(),
                message_id: None,
                bucket: None,
                key: None,
                etag: None,
            })
        }
    }
}
