use anyhow::Result;
use postio::{config::AppConfig, libs::telemetry, routes::create_router, state::AppState};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let config = AppConfig::from_env()?;
    let _telemetry = telemetry::init_tracing(config.otel_enabled)?;
    let state = AppState::new();

    let router = create_router(state, &config);

    let addr = config.listen_addr()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("listening on {}", addr);

    axum::serve(listener, router).await?;
    Ok(())
}
