use airbitrage::config::AppConfig;
use airbitrage::error::Result;
use airbitrage::market::OrderBook;
use airbitrage::types::{MarketEvent, MarketType, VenueId};
use airbitrage::venues::binance::BinanceClient;
use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
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

async fn run_binance_smoke_test(symbol: &str) -> Result<()> {
    info!(target: "airbitrage::smoke", symbol, "Starting live Binance public market data smoke test");

    let (event_tx, mut event_rx) = mpsc::channel::<MarketEvent>(1024);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let client = BinanceClient::new(symbol);
    let client_spot = client.clone();
    let client_futures = client.clone();

    let tx1 = event_tx.clone();
    let rx1 = shutdown_rx.clone();
    let tx2 = event_tx.clone();
    let rx2 = shutdown_rx.clone();

    tokio::spawn(async move {
        client_spot.run_spot_stream(tx1, rx1).await;
    });
    tokio::spawn(async move {
        client_futures.run_futures_stream(tx2, rx2).await;
    });

    let mut spot_book = OrderBook::new(VenueId::Binance, MarketType::Spot, symbol);
    let mut futures_book = OrderBook::new(VenueId::Binance, MarketType::LinearPerpetual, symbol);
    let mut spot_updates = 0;
    let mut futures_updates = 0;
    let mut latest_funding_rate = None;

    let timeout = Duration::from_secs(8);
    let start = Instant::now();

    while start.elapsed() < timeout {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                match event {
                    MarketEvent::OrderBookSnapshot { market_type, bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id, .. } => {
                        match market_type {
                            MarketType::Spot => {
                                spot_book.set_snapshot(bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id)?;
                                spot_updates += 1;
                            }
                            MarketType::LinearPerpetual => {
                                futures_book.set_snapshot(bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id)?;
                                futures_updates += 1;
                            }
                        }
                    }
                    MarketEvent::FundingRateUpdate { rate, .. } => {
                        latest_funding_rate = Some(rate);
                    }
                    _ => {}
                }

                if spot_updates >= 5 && futures_updates >= 5 {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }

    let _ = shutdown_tx.send(true);

    println!("\n================================================================================");
    println!("                     BINANCE PUBLIC MARKET DATA SMOKE TEST");
    println!("================================================================================");

    let spot_bid = spot_book
        .best_bid()
        .map(|l| l.price.to_string())
        .unwrap_or_else(|| "N/A".into());
    let spot_ask = spot_book
        .best_ask()
        .map(|l| l.price.to_string())
        .unwrap_or_else(|| "N/A".into());
    let spot_spread = spot_book
        .spread()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "N/A".into());
    println!("BINANCE SPOT {}", symbol.to_uppercase());
    println!("best_bid={}", spot_bid);
    println!("best_ask={}", spot_ask);
    println!("spread={}", spot_spread);
    println!("exchange_ts=N/A (partial depth)");
    println!("updates={}", spot_updates);

    println!("\n--------------------------------------------------------------------------------");

    let fut_bid = futures_book
        .best_bid()
        .map(|l| l.price.to_string())
        .unwrap_or_else(|| "N/A".into());
    let fut_ask = futures_book
        .best_ask()
        .map(|l| l.price.to_string())
        .unwrap_or_else(|| "N/A".into());
    let fut_spread = futures_book
        .spread()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "N/A".into());
    let funding = latest_funding_rate
        .map(|r| r.to_string())
        .unwrap_or_else(|| "N/A".into());
    println!("BINANCE USD-M FUTURES {}", symbol.to_uppercase());
    println!("best_bid={}", fut_bid);
    println!("best_ask={}", fut_ask);
    println!("spread={}", fut_spread);
    println!("exchange_ts={}", futures_book.exchange_ts_ms);
    println!("funding_rate={}", funding);
    println!("updates={}", futures_updates);

    println!("================================================================================\n");

    if spot_updates == 0 || futures_updates == 0 {
        return Err(airbitrage::error::EngineError::Transport(
            "Smoke test failed to receive updates from both Spot and Futures streams".into(),
        ));
    }

    info!(target: "airbitrage::smoke", "Live Binance smoke test completed successfully");
    Ok(())
}

async fn run_binance_soak_test(symbol: &str, duration_secs: u64) -> Result<()> {
    info!(target: "airbitrage::soak", symbol, duration_secs, "Starting Binance continuous soak test");

    let (event_tx, mut event_rx) = mpsc::channel::<MarketEvent>(2048);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let client = BinanceClient::new(symbol);
    let client_spot = client.clone();
    let client_futures = client.clone();

    let tx1 = event_tx.clone();
    let rx1 = shutdown_rx.clone();
    let tx2 = event_tx.clone();
    let rx2 = shutdown_rx.clone();

    tokio::spawn(async move {
        client_spot.run_spot_stream(tx1, rx1).await;
    });
    tokio::spawn(async move {
        client_futures.run_futures_stream(tx2, rx2).await;
    });

    let mut spot_book = OrderBook::new(VenueId::Binance, MarketType::Spot, symbol);
    let mut futures_book = OrderBook::new(VenueId::Binance, MarketType::LinearPerpetual, symbol);
    let mut spot_updates = 0;
    let mut futures_updates = 0;
    let mut funding_updates = 0;
    let mut crossed_books_detected = 0;

    let start = Instant::now();
    let target_duration = Duration::from_secs(duration_secs);

    while start.elapsed() < target_duration {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                match event {
                    MarketEvent::OrderBookSnapshot { market_type, bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id, .. } => {
                        match market_type {
                            MarketType::Spot => {
                                if spot_book.set_snapshot(bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id).is_err() {
                                    crossed_books_detected += 1;
                                }
                                spot_updates += 1;
                            }
                            MarketType::LinearPerpetual => {
                                if futures_book.set_snapshot(bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id).is_err() {
                                    crossed_books_detected += 1;
                                }
                                futures_updates += 1;
                            }
                        }
                    }
                    MarketEvent::FundingRateUpdate { .. } => {
                        funding_updates += 1;
                    }
                    _ => {}
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }

    let _ = shutdown_tx.send(true);
    let elapsed = start.elapsed();
    let snap = client.metrics.snapshot();

    println!("\n================================================================================");
    println!("                          BINANCE SOAK TEST REPORT");
    println!("================================================================================");
    println!("Target Symbol:            {}", symbol.to_uppercase());
    println!("Soak Duration:            {:.2?}", elapsed);
    println!("Messages Received:        {}", snap.messages_received);
    println!("Messages Parsed:          {}", snap.messages_parsed);
    println!("Messages Rejected:        {}", snap.messages_rejected);
    println!("Parse Errors:             {}", snap.parse_errors);
    println!("Validation Errors:        {}", snap.validation_errors);
    println!("Backpressure Drops:       {}", snap.backpressure_drops);
    println!("Crossed Books Detected:   {}", crossed_books_detected);
    println!("Spot Snapshots Applied:   {}", spot_updates);
    println!("Futures Depth Applied:    {}", futures_updates);
    println!("Funding Updates Received: {}", funding_updates);
    println!(
        "Message Rate (avg):       {:.1} msgs/sec",
        (snap.messages_received as f64) / elapsed.as_secs_f64()
    );
    println!("================================================================================\n");

    info!(target: "airbitrage::soak", "Binance soak test finished cleanly");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config_path = parse_config_path();

    init_logging("info");

    if args.iter().any(|arg| arg == "--binance-smoke") {
        return run_binance_smoke_test("BTCUSDT").await;
    }

    if let Some(pos) = args.iter().position(|arg| arg == "--binance-soak") {
        let duration: u64 = args.get(pos + 1).and_then(|d| d.parse().ok()).unwrap_or(15);
        return run_binance_soak_test("BTCUSDT", duration).await;
    }

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
        milestone = "M1",
        status = "binance_ingestion_ready",
        "M1 Binance Market-Data Ingestion ready"
    );

    Ok(())
}
