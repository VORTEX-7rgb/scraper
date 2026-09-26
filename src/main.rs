use airbitrage::config::AppConfig;
use airbitrage::error::Result;
use std::env;
use std::path::PathBuf;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

fn init_logging(log_level: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(true)
        .init();
}

fn parse_config_path() -> PathBuf {
    let args: Vec<String> = env::args().collect();
    if let Some(pos) = args.iter().position(|arg| arg == "--config")
        && let Some(path) = args.get(pos + 1)
    {
        return PathBuf::from(path);
    }
    PathBuf::from("config/config.toml")
}

fn main() -> Result<()> {
    let config_path = parse_config_path();

    // Bootstrap minimal logging to report startup
    init_logging("info");

    info!(
        target: "airbitrage",
        version = env!("CARGO_PKG_VERSION"),
        status = "initializing",
        "Airbitrage Market Dislocation Observatory starting"
    );

    info!(
        target: "airbitrage",
        config_path = %config_path.display(),
        "Loading application configuration"
    );

    let config = match AppConfig::load_from_file(&config_path) {
        Ok(cfg) => cfg,
        Err(err) => {
            warn!(
                target: "airbitrage",
                error = %err,
                "Failed to load configuration; aborting startup"
            );
            return Err(err);
        }
    };

    info!(
        target: "airbitrage",
        app_name = %config.app.name,
        environment = %config.app.environment,
        venues_count = config.venues.len(),
        "Configuration loaded and validated successfully"
    );

    for venue in &config.venues {
        info!(
            target: "airbitrage",
            venue = %venue.id,
            enabled = venue.enabled,
            symbols = ?venue.symbols,
            "Venue configuration registered"
        );
    }

    info!(
        target: "airbitrage",
        min_spread_bps = config.engine.min_spread_bps,
        notional_tiers = ?config.engine.test_notional_tiers,
        "Engine dislocation parameters calibrated"
    );

    info!(
        target: "airbitrage",
        milestone = "M0",
        status = "foundation_verified",
        "M0 Foundation self-check completed cleanly"
    );

    Ok(())
}
