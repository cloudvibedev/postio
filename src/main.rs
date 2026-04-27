use anyhow::Result;
use postio::{
    bridge::{aws::AwsDispatcher, config::load_bridge_config},
    config::AppConfig,
    libs::telemetry,
    routes::create_router,
    state::AppState,
};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let config = AppConfig::from_env()?;
    let bridge_config = load_bridge_config(&config.bridge_config_path)?;
    let _telemetry = telemetry::init_tracing(config.otel_enabled)?;
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let state = AppState::new(Arc::new(AwsDispatcher::new(
        aws_sdk_sns::Client::new(&aws_config),
        aws_sdk_sqs::Client::new(&aws_config),
        aws_sdk_s3::Client::new(&aws_config),
    )));

    let router = create_router(state, &config, &bridge_config);

    let addr = config.listen_addr()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("listening on {}", addr);

    axum::serve(listener, router).await?;
    Ok(())
}
