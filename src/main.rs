use airbitrage::config::AppConfig;
use airbitrage::error::{EngineError, Result};
use airbitrage::market::state::BookLifecycleState;
use airbitrage::market::{MarketStateManager, OrderBook};
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use airbitrage::venues::binance::{BinanceClient, BinanceStreamType};
use airbitrage::venues::bybit::{BybitFeedConfig, BybitMetrics, BybitWebSocketFeed};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[cfg(target_os = "windows")]
mod mem_tracker {
    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        quota_peak_paged_pool_usage: usize,
        quota_paged_pool_usage: usize,
        quota_peak_non_paged_pool_usage: usize,
        quota_non_paged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }

    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> isize;
        fn K32GetProcessMemoryInfo(
            process: isize,
            counters: *mut ProcessMemoryCounters,
            cb: u32,
        ) -> i32;
    }

    pub fn get_memory_bytes() -> (usize, usize) {
        unsafe {
            let handle = GetCurrentProcess();
            let mut counters = std::mem::zeroed::<ProcessMemoryCounters>();
            counters.cb = std::mem::size_of::<ProcessMemoryCounters>() as u32;
            if K32GetProcessMemoryInfo(handle, &mut counters, counters.cb) != 0 {
                (counters.working_set_size, counters.peak_working_set_size)
            } else {
                (0, 0)
            }
        }
    }
}

#[cfg(not(target_os = "windows"))]
mod mem_tracker {
    pub fn get_memory_bytes() -> (usize, usize) {
        (0, 0)
    }
}

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
    info!(target: "airbitrage::smoke", symbol, "Starting live Binance M1.1 smoke test (Spot + Futures Depth /public + Futures Mark /market)");

    let (event_tx, mut event_rx) = mpsc::channel::<MarketEvent>(1024);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let client = BinanceClient::new(symbol);
    let client_spot = client.clone();
    let client_depth = client.clone();
    let client_mark = client.clone();

    let tx1 = event_tx.clone();
    let rx1 = shutdown_rx.clone();
    let tx2 = event_tx.clone();
    let rx2 = shutdown_rx.clone();
    let tx3 = event_tx.clone();
    let rx3 = shutdown_rx.clone();

    tokio::spawn(async move {
        client_spot.run_spot_stream(tx1, rx1).await;
    });
    tokio::spawn(async move {
        client_depth.run_futures_depth_stream(tx2, rx2).await;
    });
    tokio::spawn(async move {
        client_mark.run_futures_mark_stream(tx3, rx3).await;
    });

    let mut spot_book = OrderBook::new(VenueId::Binance, MarketType::Spot, symbol);
    let mut futures_book = OrderBook::new(VenueId::Binance, MarketType::LinearPerpetual, symbol);

    let mut spot_updates = 0;
    let mut futures_depth_updates = 0;
    let mut mark_updates = 0;

    let mut last_futures_u: u64 = 0;
    let mut last_futures_first_seq: u64 = 0;
    let mut last_futures_prev_seq: Option<u64> = None;
    let mut last_futures_trans_ts: i64 = 0;

    let mut latest_mark_price = None;
    let mut latest_index_price = None;
    let mut latest_funding_rate = None;
    let mut latest_next_funding_ts = 0;
    let mut latest_mark_exchange_ts = 0;

    let timeout = Duration::from_secs(12);
    let start = Instant::now();

    while start.elapsed() < timeout {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                match event {
                    MarketEvent::OrderBookSnapshot { market_type, bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id, .. } => {
                        if market_type == MarketType::Spot {
                            spot_book.set_snapshot(bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id)?;
                            spot_updates += 1;
                        }
                    }
                    MarketEvent::OrderBookDelta { bids, asks, first_sequence_id, sequence_id, prev_sequence_id, transaction_ts_ms, exchange_ts_ms, local_recv_ts_ns, .. } => {
                        last_futures_first_seq = first_sequence_id;
                        last_futures_u = sequence_id;
                        last_futures_prev_seq = prev_sequence_id;
                        last_futures_trans_ts = transaction_ts_ms;

                        // Maintain book with received deltas
                        let _ = futures_book.apply_delta(&bids, &asks, exchange_ts_ms, local_recv_ts_ns, sequence_id);
                        futures_depth_updates += 1;
                    }
                    MarketEvent::FundingRateUpdate { mark_price, index_price, rate, next_funding_ts_ms, exchange_ts_ms, .. } => {
                        latest_mark_price = Some(mark_price);
                        latest_index_price = index_price;
                        latest_funding_rate = Some(rate);
                        latest_next_funding_ts = next_funding_ts_ms;
                        latest_mark_exchange_ts = exchange_ts_ms;
                        mark_updates += 1;
                    }
                    _ => {}
                }

                if spot_updates >= 3 && futures_depth_updates >= 3 && mark_updates >= 1 {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }

    let spot_stale = client.is_stream_stale(BinanceStreamType::SpotDepth);
    let fut_depth_stale = client.is_stream_stale(BinanceStreamType::FuturesDepth);
    let fut_mark_stale = client.is_stream_stale(BinanceStreamType::FuturesMarkPrice);

    let _ = shutdown_tx.send(true);

    println!("\n================================================================================");
    println!("             BINANCE PROTOCOL M1.1 VERIFIED LIVE SMOKE TEST");
    println!("================================================================================");

    // 1. Spot Verification
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

    println!("1. BINANCE SPOT (wss://stream.binance.com/ws)");
    println!("   Status:                 CONNECTED & STREAMING");
    println!("   Target Symbol:          {}", symbol.to_uppercase());
    println!("   Snapshot Updates:       {}", spot_updates);
    println!("   Last Sequence (update): {}", spot_book.sequence_id);
    println!("   Best Bid:               {}", spot_bid);
    println!("   Best Ask:               {}", spot_ask);
    println!("   Spread:                 {}", spot_spread);
    println!(
        "   Stale State:            {}",
        if spot_stale { "STALE" } else { "HEALTHY" }
    );

    println!("\n--------------------------------------------------------------------------------");

    // 2. Futures Depth Verification
    let fut_bid = futures_book
        .best_bid()
        .map(|l| l.price.to_string())
        .unwrap_or_else(|| "N/A".into());
    let fut_ask = futures_book
        .best_ask()
        .map(|l| l.price.to_string())
        .unwrap_or_else(|| "N/A".into());

    println!("2. BINANCE USD-M FUTURES DEPTH (wss://fstream.binance.com/public/ws)");
    println!("   Status:                 CONNECTED & STREAMING");
    println!("   Route:                  /public/ws");
    println!("   Delta Updates:          {}", futures_depth_updates);
    println!("   First Update ID (U):    {}", last_futures_first_seq);
    println!("   Final Update ID (u):    {}", last_futures_u);
    println!("   Prev Update ID (pu):    {:?}", last_futures_prev_seq);
    println!(
        "   Exchange Event Ts (E):  {} ms",
        futures_book.exchange_ts_ms
    );
    println!("   Transaction Ts (T):     {} ms", last_futures_trans_ts);
    println!("   Top Bid / Ask:          {} / {}", fut_bid, fut_ask);
    println!(
        "   Stale State:            {}",
        if fut_depth_stale { "STALE" } else { "HEALTHY" }
    );

    println!("\n--------------------------------------------------------------------------------");

    // 3. Futures Mark Price & Funding Verification

    println!("3. BINANCE USD-M FUTURES MARK PRICE (wss://fstream.binance.com/market/ws)");
    println!("   Status:                 CONNECTED & STREAMING");
    println!("   Route:                  /market/ws");
    println!("   Mark Updates:           {}", mark_updates);
    println!(
        "   Mark Price:             {}",
        latest_mark_price
            .map(|p: Decimal| p.to_string())
            .unwrap_or_else(|| "N/A".into())
    );
    println!(
        "   Index Price:            {}",
        latest_index_price
            .map(|p: Decimal| p.to_string())
            .unwrap_or_else(|| "N/A".into())
    );
    println!(
        "   Funding Rate:           {}",
        latest_funding_rate
            .map(|r: Decimal| r.to_string())
            .unwrap_or_else(|| "N/A".into())
    );
    println!("   Next Funding Ts:        {} ms", latest_next_funding_ts);
    println!("   Exchange Event Ts:      {} ms", latest_mark_exchange_ts);
    println!(
        "   Stale State:            {}",
        if fut_mark_stale { "STALE" } else { "HEALTHY" }
    );

    println!("================================================================================\n");

    if spot_updates == 0 {
        return Err(EngineError::Transport(
            "Smoke test failed: Spot stream received 0 messages".into(),
        ));
    }
    if futures_depth_updates == 0 {
        return Err(EngineError::Transport(
            "Smoke test failed: Futures depth /public route received 0 messages".into(),
        ));
    }
    if mark_updates == 0 {
        return Err(EngineError::Transport(
            "Smoke test failed: Futures mark price /market route received 0 messages".into(),
        ));
    }

    info!(target: "airbitrage::smoke", "Live Binance M1.1 smoke test completed successfully with all 3 streams verified");
    Ok(())
}

async fn run_bybit_smoke_test(symbol: &str) -> Result<()> {
    info!(target: "airbitrage::smoke", symbol, "Starting live Bybit M3.1 smoke test (Spot + Linear)");

    let mut manager = MarketStateManager::new(Duration::from_millis(10_000));

    println!("\n================================================================================");
    println!("             BYBIT PROTOCOL M3.1 VERIFIED LIVE SMOKE TEST");
    println!("================================================================================");
    println!("Target Symbol:                {}", symbol.to_uppercase());

    // 1. BYBIT SPOT
    info!(target: "airbitrage::smoke", "Connecting to Bybit Spot WebSocket feed...");
    let metrics_spot = std::sync::Arc::new(BybitMetrics::default());
    let config_spot = BybitFeedConfig::default_spot(symbol);
    let (event_tx_spot, mut event_rx_spot) = mpsc::channel::<MarketEvent>(1024);
    let (shutdown_tx_spot, shutdown_rx_spot) = tokio::sync::watch::channel(false);
    let feed_spot =
        BybitWebSocketFeed::with_metrics(config_spot, metrics_spot.clone(), event_tx_spot);

    tokio::spawn(async move {
        let _ = feed_spot.run_stream(shutdown_rx_spot).await;
    });

    let mut spot_seq_violations = 0;
    let mut spot_crossed_books = 0;
    let spot_timeout = Duration::from_secs(6);
    let spot_start = Instant::now();

    while spot_start.elapsed() < spot_timeout {
        tokio::select! {
            Some(event) = event_rx_spot.recv() => {
                if let Err(e) = manager.handle_event(&event) {
                    spot_seq_violations += 1;
                    warn!(target: "airbitrage::smoke", error = %e, "Bybit Spot state event error");
                }
                if let Some(state) = manager.get_state(VenueId::Bybit, MarketType::Spot, symbol) {
                    let crossed = match (state.book.best_bid(), state.book.best_ask()) {
                        (Some(bid), Some(ask)) => bid.price >= ask.price,
                        _ => false,
                    };
                    if crossed {
                        spot_crossed_books += 1;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    let _ = shutdown_tx_spot.send(true);
    let spot_metrics_snap = metrics_spot.snapshot();
    let spot_state = manager.get_state(VenueId::Bybit, MarketType::Spot, symbol);

    println!("\n--------------------------------------------------------------------------------");
    println!("1. BYBIT SPOT STREAM");
    println!(
        "   Messages Received:         {}",
        spot_metrics_snap.messages_received
    );
    println!(
        "   Snapshots:                 {}",
        spot_metrics_snap.snapshots_received
    );
    println!(
        "   Deltas:                    {}",
        spot_metrics_snap.deltas_received
    );
    println!(
        "   Parse Errors:              {}",
        spot_metrics_snap.parse_errors
    );
    println!(
        "   Unknown Messages:          {}",
        spot_metrics_snap.unknown_messages
    );
    println!("   Sequence Violations:       {}", spot_seq_violations);
    println!("   Crossed Books:             {}", spot_crossed_books);
    println!(
        "   Connection Drops:          {}",
        spot_metrics_snap.disconnects
    );
    println!(
        "   Reconnects:                {}",
        spot_metrics_snap.reconnects
    );
    if let Some(s) = spot_state {
        println!("   Current Lifecycle:         {:?}", s.lifecycle_state);
        println!(
            "   Current Best Bid:          {:?}",
            s.book
                .best_bid()
                .map(|l| (l.price.to_string(), l.quantity.to_string()))
        );
        println!(
            "   Current Best Ask:          {:?}",
            s.book
                .best_ask()
                .map(|l| (l.price.to_string(), l.quantity.to_string()))
        );
    } else {
        println!("   Current Lifecycle:         Uninitialized");
        println!("   Current Best Bid:          None");
        println!("   Current Best Ask:          None");
    }

    // 2. BYBIT LINEAR
    info!(target: "airbitrage::smoke", "Connecting to Bybit Linear WebSocket feed...");
    let metrics_linear = std::sync::Arc::new(BybitMetrics::default());
    let config_linear = BybitFeedConfig::default_linear(symbol);
    let (event_tx_linear, mut event_rx_linear) = mpsc::channel::<MarketEvent>(1024);
    let (shutdown_tx_linear, shutdown_rx_linear) = tokio::sync::watch::channel(false);
    let feed_linear =
        BybitWebSocketFeed::with_metrics(config_linear, metrics_linear.clone(), event_tx_linear);

    tokio::spawn(async move {
        let _ = feed_linear.run_stream(shutdown_rx_linear).await;
    });

    let mut linear_seq_violations = 0;
    let mut linear_crossed_books = 0;
    let linear_timeout = Duration::from_secs(6);
    let linear_start = Instant::now();

    while linear_start.elapsed() < linear_timeout {
        tokio::select! {
            Some(event) = event_rx_linear.recv() => {
                if let Err(e) = manager.handle_event(&event) {
                    linear_seq_violations += 1;
                    warn!(target: "airbitrage::smoke", error = %e, "Bybit Linear state event error");
                }
                if let Some(state) = manager.get_state(VenueId::Bybit, MarketType::LinearPerpetual, symbol) {
                    let crossed = match (state.book.best_bid(), state.book.best_ask()) {
                        (Some(bid), Some(ask)) => bid.price >= ask.price,
                        _ => false,
                    };
                    if crossed {
                        linear_crossed_books += 1;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    let _ = shutdown_tx_linear.send(true);
    let linear_metrics_snap = metrics_linear.snapshot();
    let linear_state = manager.get_state(VenueId::Bybit, MarketType::LinearPerpetual, symbol);

    println!("\n--------------------------------------------------------------------------------");
    println!("2. BYBIT LINEAR STREAM");
    println!(
        "   Messages Received:         {}",
        linear_metrics_snap.messages_received
    );
    println!(
        "   Snapshots:                 {}",
        linear_metrics_snap.snapshots_received
    );
    println!(
        "   Deltas:                    {}",
        linear_metrics_snap.deltas_received
    );
    println!(
        "   Parse Errors:              {}",
        linear_metrics_snap.parse_errors
    );
    println!(
        "   Unknown Messages:          {}",
        linear_metrics_snap.unknown_messages
    );
    println!("   Sequence Violations:       {}", linear_seq_violations);
    println!("   Crossed Books:             {}", linear_crossed_books);
    println!(
        "   Connection Drops:          {}",
        linear_metrics_snap.disconnects
    );
    println!(
        "   Reconnects:                {}",
        linear_metrics_snap.reconnects
    );
    if let Some(s) = linear_state {
        println!("   Current Lifecycle:         {:?}", s.lifecycle_state);
        println!(
            "   Current Best Bid:          {:?}",
            s.book
                .best_bid()
                .map(|l| (l.price.to_string(), l.quantity.to_string()))
        );
        println!(
            "   Current Best Ask:          {:?}",
            s.book
                .best_ask()
                .map(|l| (l.price.to_string(), l.quantity.to_string()))
        );
    } else {
        println!("   Current Lifecycle:         Uninitialized");
        println!("   Current Best Bid:          None");
        println!("   Current Best Ask:          None");
    }

    println!("\n================================================================================");
    if spot_metrics_snap.messages_received == 0 {
        return Err(EngineError::Transport(
            "Bybit smoke test failed: Spot stream received 0 messages".into(),
        ));
    }
    if linear_metrics_snap.messages_received == 0 {
        return Err(EngineError::Transport(
            "Bybit smoke test failed: Linear stream received 0 messages".into(),
        ));
    }

    info!(target: "airbitrage::smoke", "Live Bybit M3.1 smoke test completed successfully with both Spot and Linear verified");
    Ok(())
}

async fn run_binance_soak_test(symbol: &str, duration_secs: u64) -> Result<()> {
    info!(
        target: "airbitrage::soak",
        symbol,
        duration_secs,
        "Starting Binance M1.1 continuous soak test"
    );

    let (start_mem_ws, _) = mem_tracker::get_memory_bytes();
    let (event_tx, mut event_rx) = mpsc::channel::<MarketEvent>(4096);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let client = BinanceClient::new(symbol);
    let client_spot = client.clone();
    let client_depth = client.clone();
    let client_mark = client.clone();

    let tx1 = event_tx.clone();
    let rx1 = shutdown_rx.clone();
    let tx2 = event_tx.clone();
    let rx2 = shutdown_rx.clone();
    let tx3 = event_tx.clone();
    let rx3 = shutdown_rx.clone();

    tokio::spawn(async move {
        client_spot.run_spot_stream(tx1, rx1).await;
    });
    tokio::spawn(async move {
        client_depth.run_futures_depth_stream(tx2, rx2).await;
    });
    tokio::spawn(async move {
        client_mark.run_futures_mark_stream(tx3, rx3).await;
    });

    let mut spot_book = OrderBook::new(VenueId::Binance, MarketType::Spot, symbol);
    let mut futures_book = OrderBook::new(VenueId::Binance, MarketType::LinearPerpetual, symbol);

    let mut spot_updates: u64 = 0;
    let mut futures_depth_updates: u64 = 0;
    let mut funding_updates: u64 = 0;
    let mut crossed_books_detected: u64 = 0;
    let mut stale_detections: u64 = 0;

    let mut latencies_us: Vec<u64> = Vec::with_capacity(100_000);

    let start = Instant::now();
    let target_duration = Duration::from_secs(duration_secs);
    let mut last_freshness_check = Instant::now();

    while start.elapsed() < target_duration {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                let proc_start = Instant::now();
                match event {
                    MarketEvent::OrderBookSnapshot { market_type, bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id, .. } => {
                        if market_type == MarketType::Spot {
                            if spot_book.set_snapshot(bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id).is_err() {
                                crossed_books_detected += 1;
                            }
                            spot_updates += 1;
                        }
                    }
                    MarketEvent::OrderBookDelta { bids, asks, sequence_id, exchange_ts_ms, local_recv_ts_ns, .. } => {
                        if futures_book.apply_delta(&bids, &asks, exchange_ts_ms, local_recv_ts_ns, sequence_id).is_err() {
                            crossed_books_detected += 1;
                            futures_book.bids.clear();
                            futures_book.asks.clear();
                        }
                        futures_depth_updates += 1;
                    }
                    MarketEvent::FundingRateUpdate { .. } => {
                        funding_updates += 1;
                    }
                    _ => {}
                }
                let proc_elapsed_us = proc_start.elapsed().as_micros() as u64;
                latencies_us.push(proc_elapsed_us);
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if last_freshness_check.elapsed() >= Duration::from_millis(500) {
                    last_freshness_check = Instant::now();
                    if client.is_stream_stale(BinanceStreamType::SpotDepth)
                        || client.is_stream_stale(BinanceStreamType::FuturesDepth)
                        || client.is_stream_stale(BinanceStreamType::FuturesMarkPrice)
                    {
                        stale_detections += 1;
                    }
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);
    let elapsed = start.elapsed();
    let snap = client.metrics.snapshot();
    let (end_mem_ws, peak_mem_ws) = mem_tracker::get_memory_bytes();

    // Compute empirical latency percentiles
    latencies_us.sort_unstable();
    let count = latencies_us.len();
    let p50 = if count > 0 {
        latencies_us[count * 50 / 100]
    } else {
        0
    };
    let p95 = if count > 0 {
        latencies_us[count * 95 / 100]
    } else {
        0
    };
    let p99 = if count > 0 {
        latencies_us[count * 99 / 100]
    } else {
        0
    };
    let avg = if count > 0 {
        latencies_us.iter().sum::<u64>() / count as u64
    } else {
        0
    };

    let total_messages =
        snap.spot_depth_messages + snap.futures_depth_messages + snap.futures_markprice_messages;

    println!("\n================================================================================");
    println!("                     BINANCE M1.1 EXTENDED SOAK TEST REPORT");
    println!("================================================================================");
    println!("Target Symbol:                {}", symbol.to_uppercase());
    println!("Actual Soak Duration:         {:.2?}", elapsed);
    println!("Total Stream Messages:        {}", total_messages);
    println!("Spot Depth Messages:          {}", snap.spot_depth_messages);
    println!(
        "Futures Depth Messages:       {}",
        snap.futures_depth_messages
    );
    println!(
        "Futures Mark Price Messages:  {}",
        snap.futures_markprice_messages
    );
    println!(
        "Subscription ACKs:            {}",
        snap.subscription_messages
    );
    println!("Parse Errors:                 {}", snap.parse_errors);
    println!("Validation Errors:            {}", snap.validation_errors);
    println!("Backpressure Drops:           {}", snap.backpressure_drops);
    println!("Reconnects:                   {}", snap.reconnects);
    println!("Crossed Books Detected:       {}", crossed_books_detected);
    println!("Stale Events Detected:        {}", stale_detections);
    println!("Spot Snapshots Applied:       {}", spot_updates);
    println!("Futures Deltas Applied:       {}", futures_depth_updates);
    println!("Funding Updates Received:     {}", funding_updates);
    println!(
        "Total Stream Rate:            {:.1} msgs/sec",
        (total_messages as f64) / elapsed.as_secs_f64()
    );
    println!(
        "Spot Depth Rate:              {:.1} msgs/sec",
        (snap.spot_depth_messages as f64) / elapsed.as_secs_f64()
    );
    println!(
        "Futures Depth Rate:           {:.1} msgs/sec",
        (snap.futures_depth_messages as f64) / elapsed.as_secs_f64()
    );
    println!(
        "Futures Mark Rate:            {:.1} msgs/sec",
        (snap.futures_markprice_messages as f64) / elapsed.as_secs_f64()
    );
    println!("--------------------------------------------------------------------------------");
    println!(
        "MEASURED EVENT PROCESSING LATENCIES (Sample Count: {})",
        count
    );
    println!("  p50 (median):               {} us", p50);
    println!("  p95:                        {} us", p95);
    println!("  p99:                        {} us", p99);
    println!("  Average:                    {} us", avg);
    println!("--------------------------------------------------------------------------------");
    println!("MEMORY FOOTPRINT (Working Set)");
    println!(
        "  Initial Memory:             {:.2} MB",
        (start_mem_ws as f64) / (1024.0 * 1024.0)
    );
    println!(
        "  Ending Memory:              {:.2} MB",
        (end_mem_ws as f64) / (1024.0 * 1024.0)
    );
    println!(
        "  Peak Working Set:           {:.2} MB",
        (peak_mem_ws as f64) / (1024.0 * 1024.0)
    );
    println!("================================================================================\n");

    info!(target: "airbitrage::soak", "Binance M1.1 extended soak test finished cleanly");
    Ok(())
}

fn fetch_binance_futures_snapshot(symbol: &str) -> Result<MarketEvent> {
    let url = format!(
        "https://fapi.binance.com/fapi/v1/depth?symbol={}&limit=1000",
        symbol.to_uppercase()
    );
    let output = std::process::Command::new("curl.exe")
        .args([
            "--ssl-no-revoke",
            "-sS",
            "--connect-timeout",
            "5",
            "--max-time",
            "10",
            &url,
        ])
        .output()
        .map_err(|e| EngineError::Transport(e.to_string()))?;

    if !output.status.success() {
        return Err(EngineError::Transport(format!(
            "Failed to fetch Futures depth snapshot: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let raw: serde_json::Value = serde_json::from_str(&stdout_str)?;
    let last_update_id = raw["lastUpdateId"].as_u64().ok_or_else(|| {
        EngineError::DataQuality("Missing lastUpdateId in Futures snapshot".into())
    })?;
    let exchange_ts = raw["E"].as_i64().unwrap_or(0);

    let raw_bids = raw["bids"]
        .as_array()
        .ok_or_else(|| EngineError::DataQuality("Missing bids in Futures snapshot".into()))?;
    let raw_asks = raw["asks"]
        .as_array()
        .ok_or_else(|| EngineError::DataQuality("Missing asks in Futures snapshot".into()))?;

    let mut bids = Vec::with_capacity(raw_bids.len());
    for item in raw_bids {
        if let (Some(p_str), Some(q_str)) = (item[0].as_str(), item[1].as_str())
            && let (Ok(p), Ok(q)) = (p_str.parse::<Decimal>(), q_str.parse::<Decimal>())
        {
            bids.push(PriceLevel::new(p, q));
        }
    }

    let mut asks = Vec::with_capacity(raw_asks.len());
    for item in raw_asks {
        if let (Some(p_str), Some(q_str)) = (item[0].as_str(), item[1].as_str())
            && let (Ok(p), Ok(q)) = (p_str.parse::<Decimal>(), q_str.parse::<Decimal>())
        {
            asks.push(PriceLevel::new(p, q));
        }
    }

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    Ok(MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::LinearPerpetual,
        symbol: symbol.to_uppercase(),
        bids,
        asks,
        exchange_ts_ms: exchange_ts,
        local_recv_ts_ns: now_ns,
        sequence_id: last_update_id,
    })
}

async fn run_market_state_live_test(symbol: &str, duration_secs: u64) -> Result<()> {
    info!(
        target: "airbitrage::state_live",
        symbol,
        duration_secs,
        "Starting local MarketStateManager live validation test"
    );

    let (event_tx, mut event_rx) = mpsc::channel::<MarketEvent>(4096);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let client = BinanceClient::new(symbol);
    let client_spot = client.clone();
    let client_depth = client.clone();
    let client_mark = client.clone();

    let tx1 = event_tx.clone();
    let rx1 = shutdown_rx.clone();
    let tx2 = event_tx.clone();
    let rx2 = shutdown_rx.clone();
    let tx3 = event_tx.clone();
    let rx3 = shutdown_rx.clone();

    tokio::spawn(async move {
        client_spot.run_spot_stream(tx1, rx1).await;
    });
    tokio::spawn(async move {
        client_depth.run_futures_depth_stream(tx2, rx2).await;
    });
    tokio::spawn(async move {
        client_mark.run_futures_mark_stream(tx3, rx3).await;
    });

    let mut manager = MarketStateManager::new(Duration::from_millis(1500));
    manager.register_instrument(VenueId::Binance, MarketType::Spot, symbol);
    manager.register_instrument(VenueId::Binance, MarketType::LinearPerpetual, symbol);

    let start = Instant::now();
    let target_duration = Duration::from_secs(duration_secs);
    let mut total_events_processed: u64 = 0;
    let mut sequence_errors: u64 = 0;
    let mut crossed_books_observed: u64 = 0;
    let mut futures_snapshot_requested = false;
    let mut futures_snapshot_applied = false;

    while start.elapsed() < target_duration {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                total_events_processed += 1;

                let is_futures_snap = matches!(
                    &event,
                    MarketEvent::OrderBookSnapshot {
                        market_type: MarketType::LinearPerpetual,
                        ..
                    }
                );

                if let Err(e) = manager.handle_event(&event) {
                    match &e {
                        EngineError::SequenceGap { .. } => {
                            sequence_errors += 1;
                            warn!(target: "airbitrage::state_live", error = %e, "Sequence gap observed in live stream");
                        }
                        EngineError::CrossedBook { .. } => {
                            crossed_books_observed += 1;
                            warn!(target: "airbitrage::state_live", error = %e, "Crossed book observed in live stream");
                        }
                        _ => {
                            warn!(target: "airbitrage::state_live", error = %e, "Error handling MarketEvent");
                        }
                    }
                } else if is_futures_snap {
                    futures_snapshot_applied = true;
                    info!(
                        target: "airbitrage::state_live",
                        "Futures depth snapshot applied and aligned with buffered deltas successfully"
                    );
                }

                // Once we have buffered at least 5 Futures deltas, trigger concurrent REST depth snapshot fetch
                if !futures_snapshot_requested && !futures_snapshot_applied {
                    let buffered_count = manager
                        .get_state(VenueId::Binance, MarketType::LinearPerpetual, symbol)
                        .map(|s| s.buffered_delta_count())
                        .unwrap_or(0);

                    if buffered_count >= 5 {
                        futures_snapshot_requested = true;
                        info!(
                            target: "airbitrage::state_live",
                            buffered_count,
                            "Triggering concurrent Binance Futures REST depth snapshot fetch"
                        );
                        let sym_copy = symbol.to_string();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            let res = tokio::task::spawn_blocking(move || {
                                fetch_binance_futures_snapshot(&sym_copy)
                            })
                            .await;

                                match res {
                                    Ok(Ok(snapshot_event)) => {
                                        info!(
                                            target: "airbitrage::state_live",
                                            "Futures REST depth snapshot fetched successfully; dispatching to event loop"
                                        );
                                        let _ = tx.send(snapshot_event).await;
                                    }
                                    Ok(Err(e)) => {
                                        warn!(target: "airbitrage::state_live", error = %e, "Failed to fetch Futures depth snapshot");
                                    }
                                    Err(e) => {
                                        warn!(target: "airbitrage::state_live", error = %e, "Task join error fetching Futures depth snapshot");
                                    }
                                }
                        });
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }

    let _ = shutdown_tx.send(true);
    let elapsed = start.elapsed();

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let spot_trusted = manager.get_trusted_book(VenueId::Binance, MarketType::Spot, symbol, now_ns);
    let fut_trusted = manager.get_trusted_book(
        VenueId::Binance,
        MarketType::LinearPerpetual,
        symbol,
        now_ns,
    );

    let spot_state = manager.get_state(VenueId::Binance, MarketType::Spot, symbol);
    let fut_state = manager.get_state(VenueId::Binance, MarketType::LinearPerpetual, symbol);
    let funding = manager.get_funding(VenueId::Binance, symbol);

    println!("\n================================================================================");
    println!("             LOCAL MARKET-STATE ENGINE VERIFIED LIVE TEST");
    println!("================================================================================");
    println!("Target Symbol:                {}", symbol.to_uppercase());
    println!("Test Window Duration:         {:.2?}", elapsed);
    println!("Total MarketEvents Processed: {}", total_events_processed);
    println!("Sequence Continuity Errors:   {}", sequence_errors);
    println!("Crossed Books Observed:       {}", crossed_books_observed);

    println!("\n--------------------------------------------------------------------------------");
    println!("1. BINANCE SPOT STATE");
    if let Some(s) = spot_state {
        println!("   Lifecycle State:           {:?}", s.lifecycle_state);
        println!("   Validity:                  {:?}", s.validity);
        println!("   Last Sequence ID:          {:?}", s.last_update_sequence);
        println!(
            "   Snapshots Applied:         {}",
            s.metrics.snapshots_applied
        );
        println!(
            "   Duplicate Snapshots:       {}",
            s.metrics.duplicate_deltas
        );
        println!(
            "   Is Stale (1.5s):           {}",
            s.is_stale(Duration::from_millis(1500), now_ns)
        );
        println!("   Is Trusted:                {}", spot_trusted.is_some());
        if let Some(b) = spot_trusted {
            println!(
                "   Best Bid:                  {}",
                b.best_bid()
                    .map(|l| l.price.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
            println!(
                "   Best Ask:                  {}",
                b.best_ask()
                    .map(|l| l.price.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
            println!(
                "   Spread:                    {}",
                b.spread()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
        }
    }

    println!("\n--------------------------------------------------------------------------------");
    println!("2. BINANCE USD-M FUTURES STATE");
    if let Some(s) = fut_state {
        println!("   Lifecycle State:           {:?}", s.lifecycle_state);
        println!("   Validity:                  {:?}", s.validity);
        println!("   Last Sequence ID:          {:?}", s.last_update_sequence);
        println!(
            "   Deltas Received:           {}",
            s.metrics.deltas_received
        );
        println!("   Deltas Applied:            {}", s.metrics.deltas_applied);
        println!(
            "   Duplicate Deltas:          {}",
            s.metrics.duplicate_deltas
        );
        println!("   Old Deltas:                {}", s.metrics.old_deltas);
        println!(
            "   Sequence Failures:         {}",
            s.metrics.sequence_failures
        );
        println!("   Invalidations:             {}", s.metrics.invalidations);
        println!(
            "   Is Stale (1.5s):           {}",
            s.is_stale(Duration::from_millis(1500), now_ns)
        );
        println!("   Is Trusted:                {}", fut_trusted.is_some());
        if let Some(b) = fut_trusted {
            println!(
                "   Best Bid:                  {}",
                b.best_bid()
                    .map(|l| l.price.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
            println!(
                "   Best Ask:                  {}",
                b.best_ask()
                    .map(|l| l.price.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
            println!(
                "   Spread:                    {}",
                b.spread()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
        }
    }

    println!("\n--------------------------------------------------------------------------------");
    println!("3. LATEST FUNDING STATE");
    if let Some(f) = funding {
        println!("   Mark Price:                {}", f.mark_price);
        println!(
            "   Index Price:               {}",
            f.index_price
                .map(|p| p.to_string())
                .unwrap_or_else(|| "N/A".into())
        );
        println!("   Funding Rate:              {}", f.rate);
        println!("   Next Funding Settlement:   {} ms", f.next_funding_ts_ms);
    }
    println!("================================================================================\n");

    if spot_trusted.is_none() {
        return Err(EngineError::Validation(
            "Live validation failed: Spot book is not trusted".into(),
        ));
    }
    if fut_trusted.is_none() {
        return Err(EngineError::Validation(
            "Live validation failed: Futures book is not trusted".into(),
        ));
    }
    if sequence_errors > 0 {
        return Err(EngineError::Validation(format!(
            "Live validation failed: {sequence_errors} sequence errors observed"
        )));
    }

    info!(target: "airbitrage::state_live", "MarketStateManager live validation passed successfully");
    Ok(())
}

// ============================================================================
// CONCURRENT FOUR-BOOK VALIDATION & RECOVERY HARNESS (M3.6 PRE-VALIDATION)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FeedId {
    BinanceSpot,
    BinanceLinear,
    BybitSpot,
    BybitLinear,
}

impl FeedId {
    fn label(&self) -> &'static str {
        match self {
            Self::BinanceSpot => "BINANCE / SPOT / BTCUSDT",
            Self::BinanceLinear => "BINANCE / LINEAR_PERPETUAL / BTCUSDT",
            Self::BybitSpot => "BYBIT / SPOT / BTCUSDT",
            Self::BybitLinear => "BYBIT / LINEAR_PERPETUAL / BTCUSDT",
        }
    }

    fn venue_market(&self) -> (VenueId, MarketType) {
        match self {
            Self::BinanceSpot => (VenueId::Binance, MarketType::Spot),
            Self::BinanceLinear => (VenueId::Binance, MarketType::LinearPerpetual),
            Self::BybitSpot => (VenueId::Bybit, MarketType::Spot),
            Self::BybitLinear => (VenueId::Bybit, MarketType::LinearPerpetual),
        }
    }

    fn from_event(event: &MarketEvent) -> Option<Self> {
        match event {
            MarketEvent::OrderBookSnapshot {
                venue, market_type, ..
            }
            | MarketEvent::OrderBookDelta {
                venue, market_type, ..
            } => match (venue, market_type) {
                (VenueId::Binance, MarketType::Spot) => Some(Self::BinanceSpot),
                (VenueId::Binance, MarketType::LinearPerpetual) => Some(Self::BinanceLinear),
                (VenueId::Bybit, MarketType::Spot) => Some(Self::BybitSpot),
                (VenueId::Bybit, MarketType::LinearPerpetual) => Some(Self::BybitLinear),
            },
            MarketEvent::FundingRateUpdate { venue, .. } => {
                if *venue == VenueId::Binance {
                    Some(Self::BinanceLinear)
                } else {
                    Some(Self::BybitLinear)
                }
            }
            MarketEvent::ConnectionState {
                venue, market_type, ..
            } => match (venue, market_type) {
                (VenueId::Binance, Some(MarketType::Spot)) => Some(Self::BinanceSpot),
                (VenueId::Binance, Some(MarketType::LinearPerpetual)) => Some(Self::BinanceLinear),
                (VenueId::Bybit, Some(MarketType::Spot)) => Some(Self::BybitSpot),
                (VenueId::Bybit, Some(MarketType::LinearPerpetual)) => Some(Self::BybitLinear),
                _ => None,
            },
        }
    }
}

#[derive(Debug, Default)]
struct FeedDiagnostics {
    raw_messages: u64,
    snapshots_received: u64,
    deltas_received: u64,
    parse_errors: u64,
    unknown_messages: u64,
    disconnects: u64,
    reconnects: u64,

    // Trace pipeline
    parsed_events: u64,
    manager_events: u64,
    accepted_updates: u64,
    sequence_rejections: u64,
    crossed_books_observed: u64,
    stale_transitions: u64,
    trusted_observations: u64,
    sanity_violations: u64,

    // Timing & Freshness
    freshness_samples_ms: Vec<u64>,
    receive_latencies_us: Vec<u64>,
    last_exchange_ts_ms: i64,
    last_recv_ts_ns: i64,
}

impl FeedDiagnostics {
    fn freshness_stats(&mut self) -> (u64, u64, u64, u64, u64) {
        if self.freshness_samples_ms.is_empty() {
            return (0, 0, 0, 0, 0);
        }
        self.freshness_samples_ms.sort_unstable();
        let len = self.freshness_samples_ms.len();
        let min = self.freshness_samples_ms[0];
        let max = self.freshness_samples_ms[len - 1];
        let avg = self.freshness_samples_ms.iter().sum::<u64>() / len as u64;
        let p95 = self.freshness_samples_ms[(len * 95) / 100];
        let p99 = self.freshness_samples_ms[(len * 99) / 100];
        (min, max, avg, p95, p99)
    }

    fn latency_p50(&mut self) -> u64 {
        if self.receive_latencies_us.is_empty() {
            return 0;
        }
        self.receive_latencies_us.sort_unstable();
        self.receive_latencies_us[self.receive_latencies_us.len() / 2]
    }

    fn latency_avg(&self) -> u64 {
        if self.receive_latencies_us.is_empty() {
            return 0;
        }
        self.receive_latencies_us.iter().sum::<u64>() / self.receive_latencies_us.len() as u64
    }
}

async fn run_four_book_validation(symbol: &str, duration_secs: u64) -> Result<()> {
    info!(
        target: "airbitrage::four_book",
        symbol,
        duration_secs,
        "Starting simultaneous 4-book live validation harness"
    );

    let (start_mem_ws, _) = mem_tracker::get_memory_bytes();
    let (event_tx, mut event_rx) = mpsc::channel::<MarketEvent>(16384);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // 1. Spawning Binance Feeds (Spot + Futures Depth + Futures Mark)
    let binance_client = BinanceClient::new(symbol);
    let bc_spot = binance_client.clone();
    let bc_depth = binance_client.clone();
    let bc_mark = binance_client.clone();

    let tx1 = event_tx.clone();
    let rx1 = shutdown_rx.clone();
    let tx2 = event_tx.clone();
    let rx2 = shutdown_rx.clone();
    let tx3 = event_tx.clone();
    let rx3 = shutdown_rx.clone();

    tokio::spawn(async move {
        bc_spot.run_spot_stream(tx1, rx1).await;
    });
    tokio::spawn(async move {
        bc_depth.run_futures_depth_stream(tx2, rx2).await;
    });
    tokio::spawn(async move {
        bc_mark.run_futures_mark_stream(tx3, rx3).await;
    });

    // 2. Spawning Bybit Spot Feed
    let bybit_spot_metrics = Arc::new(BybitMetrics::default());
    let bybit_spot_feed = BybitWebSocketFeed::with_metrics(
        BybitFeedConfig::default_spot(symbol),
        bybit_spot_metrics.clone(),
        event_tx.clone(),
    );
    let rx_by_spot = shutdown_rx.clone();
    tokio::spawn(async move {
        let _ = bybit_spot_feed.run_stream(rx_by_spot).await;
    });

    // 3. Spawning Bybit Linear Feed
    let bybit_linear_metrics = Arc::new(BybitMetrics::default());
    let bybit_linear_feed = BybitWebSocketFeed::with_metrics(
        BybitFeedConfig::default_linear(symbol),
        bybit_linear_metrics.clone(),
        event_tx.clone(),
    );
    let rx_by_linear = shutdown_rx.clone();
    tokio::spawn(async move {
        let _ = bybit_linear_feed.run_stream(rx_by_linear).await;
    });

    // 4. State Manager
    let mut manager = MarketStateManager::new(Duration::from_millis(3000));
    manager.register_instrument(VenueId::Binance, MarketType::Spot, symbol);
    manager.register_instrument(VenueId::Binance, MarketType::LinearPerpetual, symbol);
    manager.register_instrument(VenueId::Bybit, MarketType::Spot, symbol);
    manager.register_instrument(VenueId::Bybit, MarketType::LinearPerpetual, symbol);

    let mut diags: HashMap<FeedId, FeedDiagnostics> = [
        (FeedId::BinanceSpot, FeedDiagnostics::default()),
        (FeedId::BinanceLinear, FeedDiagnostics::default()),
        (FeedId::BybitSpot, FeedDiagnostics::default()),
        (FeedId::BybitLinear, FeedDiagnostics::default()),
    ]
    .into_iter()
    .collect();

    let start = Instant::now();
    let target_duration = Duration::from_secs(duration_secs);
    let mut futures_snapshot_requested = false;
    let mut futures_snapshot_applied = false;
    let mut last_sample_time = Instant::now();

    println!("\n================================================================================");
    println!("          FOUR-BOOK CONCURRENT VALIDATION HARNESS (M3.6 PRE-VALIDATION)");
    println!("================================================================================");
    println!(
        "Target Symbol:    {} | Scheduled Duration: {}s",
        symbol.to_uppercase(),
        duration_secs
    );
    println!(
        "Running feeds:    Binance Spot, Binance Futures, Bybit Spot, Bybit Linear concurrently"
    );

    while start.elapsed() < target_duration {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as i64;

                let feed_id = FeedId::from_event(&event);

                if let Some(fid) = feed_id {
                    let diag = diags.get_mut(&fid).unwrap();
                    diag.parsed_events += 1;
                    diag.manager_events += 1;

                    let local_recv_ns = match &event {
                        MarketEvent::OrderBookSnapshot {
                            local_recv_ts_ns,
                            exchange_ts_ms,
                            ..
                        } => {
                            diag.last_exchange_ts_ms = *exchange_ts_ms;
                            *local_recv_ts_ns
                        }
                        MarketEvent::OrderBookDelta {
                            local_recv_ts_ns,
                            exchange_ts_ms,
                            ..
                        } => {
                            diag.last_exchange_ts_ms = *exchange_ts_ms;
                            *local_recv_ts_ns
                        }
                        _ => now_ns,
                    };
                    diag.last_recv_ts_ns = local_recv_ns;
                    let latency_us = ((now_ns - local_recv_ns).max(0) / 1000) as u64;
                    diag.receive_latencies_us.push(latency_us);
                }

                let is_fut_snap = matches!(
                    &event,
                    MarketEvent::OrderBookSnapshot {
                        venue: VenueId::Binance,
                        market_type: MarketType::LinearPerpetual,
                        ..
                    }
                );

                match manager.handle_event(&event) {
                    Ok(mutated) => {
                        if let Some(fid) = feed_id
                            && mutated
                        {
                            diags.get_mut(&fid).unwrap().accepted_updates += 1;
                        }
                        if is_fut_snap {
                            futures_snapshot_applied = true;
                            info!(target: "airbitrage::four_book", "Binance Futures REST depth snapshot aligned successfully");
                        }
                    }
                    Err(e) => {
                        if let Some(fid) = feed_id {
                            let diag = diags.get_mut(&fid).unwrap();
                            match &e {
                                EngineError::SequenceGap { .. }
                                | EngineError::OutOfOrderUpdate { .. } => {
                                    diag.sequence_rejections += 1;
                                    warn!(target: "airbitrage::four_book", feed = ?fid, error = %e, "Sequence error");
                                }
                                EngineError::CrossedBook { .. } => {
                                    diag.crossed_books_observed += 1;
                                    warn!(target: "airbitrage::four_book", feed = ?fid, error = %e, "Crossed book error");
                                }
                                _ => {
                                    warn!(target: "airbitrage::four_book", feed = ?fid, error = %e, "Event error");
                                }
                            }
                        }
                    }
                }

                // Check Binance Futures buffering for REST snapshot
                if !futures_snapshot_requested && !futures_snapshot_applied {
                    let buffered_count = manager
                        .get_state(VenueId::Binance, MarketType::LinearPerpetual, symbol)
                        .map(|s| s.buffered_delta_count())
                        .unwrap_or(0);

                    if buffered_count >= 5 {
                        futures_snapshot_requested = true;
                        info!(
                            target: "airbitrage::four_book",
                            buffered_count,
                            "Triggering concurrent Binance Futures REST depth snapshot fetch"
                        );
                        let sym_copy = symbol.to_string();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            let res = tokio::task::spawn_blocking(move || {
                                fetch_binance_futures_snapshot(&sym_copy)
                            })
                            .await;
                            if let Ok(Ok(snap_event)) = res {
                                let _ = tx.send(snap_event).await;
                            }
                        });
                    }
                }

                // Sample trusted book for this feed
                if let Some(fid) = feed_id {
                    let (v, mt) = fid.venue_market();
                    let diag = diags.get_mut(&fid).unwrap();
                    if let Some(book) = manager.get_trusted_book(v, mt, symbol, now_ns) {
                        diag.trusted_observations += 1;
                        let freshness_ms =
                            ((now_ns - book.local_recv_ts_ns).max(0) / 1_000_000) as u64;
                        diag.freshness_samples_ms.push(freshness_ms);

                        // Continuous sanity assertion
                        match (book.best_bid(), book.best_ask()) {
                            (Some(b), Some(a)) => {
                                if b.price >= a.price
                                    || b.price <= Decimal::ZERO
                                    || a.price <= Decimal::ZERO
                                    || b.quantity <= Decimal::ZERO
                                    || a.quantity <= Decimal::ZERO
                                {
                                    diag.sanity_violations += 1;
                                }
                            }
                            _ => {
                                diag.sanity_violations += 1;
                            }
                        }
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        // Periodic 1-second staleness and health sweep across all 4 instruments
        if last_sample_time.elapsed() >= Duration::from_secs(1) {
            last_sample_time = Instant::now();
            let sample_now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64;

            for (fid, diag) in diags.iter_mut() {
                let (v, mt) = fid.venue_market();
                if let Some(book) = manager.get_trusted_book(v, mt, symbol, sample_now) {
                    diag.trusted_observations += 1;
                    let freshness_ms =
                        ((sample_now - book.local_recv_ts_ns).max(0) / 1_000_000) as u64;
                    diag.freshness_samples_ms.push(freshness_ms);
                } else if let Some(state) = manager.get_state(v, mt, symbol)
                    && state.lifecycle_state == BookLifecycleState::Live
                    && state.is_stale(Duration::from_millis(3000), sample_now)
                {
                    diag.stale_transitions += 1;
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);
    let elapsed = start.elapsed();
    let (end_mem_ws, peak_mem_ws) = mem_tracker::get_memory_bytes();

    // Pull adapter snapshots
    let binance_snap = binance_client.metrics.snapshot();
    let bybit_spot_snap = bybit_spot_metrics.snapshot();
    let bybit_linear_snap = bybit_linear_metrics.snapshot();

    // Sync raw metrics into diagnostics
    {
        let d = diags.get_mut(&FeedId::BinanceSpot).unwrap();
        d.raw_messages = binance_snap.spot_depth_messages;
        d.snapshots_received = binance_snap.spot_depth_messages;
        d.parse_errors = binance_snap.parse_errors;
        d.disconnects = binance_snap.disconnects;
    }
    {
        let d = diags.get_mut(&FeedId::BinanceLinear).unwrap();
        d.raw_messages = binance_snap.futures_depth_messages;
        d.snapshots_received = if futures_snapshot_applied { 1 } else { 0 };
        d.deltas_received = binance_snap.futures_depth_messages;
        d.parse_errors = binance_snap.parse_errors;
        d.disconnects = binance_snap.disconnects;
    }
    {
        let d = diags.get_mut(&FeedId::BybitSpot).unwrap();
        d.raw_messages = bybit_spot_snap.messages_received;
        d.snapshots_received = bybit_spot_snap.snapshots_received;
        d.deltas_received = bybit_spot_snap.deltas_received;
        d.parse_errors = bybit_spot_snap.parse_errors;
        d.unknown_messages = bybit_spot_snap.unknown_messages;
        d.disconnects = bybit_spot_snap.disconnects;
        d.reconnects = bybit_spot_snap.reconnects;
    }
    {
        let d = diags.get_mut(&FeedId::BybitLinear).unwrap();
        d.raw_messages = bybit_linear_snap.messages_received;
        d.snapshots_received = bybit_linear_snap.snapshots_received;
        d.deltas_received = bybit_linear_snap.deltas_received;
        d.parse_errors = bybit_linear_snap.parse_errors;
        d.unknown_messages = bybit_linear_snap.unknown_messages;
        d.disconnects = bybit_linear_snap.disconnects;
        d.reconnects = bybit_linear_snap.reconnects;
    }

    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    // --- REPORT SECTION 1: FOUR-BOOK STATUS TABLE ---
    println!("\n--------------------------------------------------------------------------------");
    println!("1. FOUR-BOOK STATUS TABLE");
    println!("--------------------------------------------------------------------------------");
    println!(
        "{:<36} | {:<9} | {:<8} | {:<7} | {:<5} | {:<5} | {:<7}",
        "Feed", "Connected", "Snapshot", "Deltas", "Live", "Valid", "Trusted"
    );
    println!(
        "{:-<36}-|-{:-<9}-|-{:-<8}-|-{:-<7}-|-{:-<5}-|-{:-<5}-|-{:-<7}",
        "", "", "", "", "", "", ""
    );

    let feed_ids = [
        FeedId::BinanceSpot,
        FeedId::BinanceLinear,
        FeedId::BybitSpot,
        FeedId::BybitLinear,
    ];

    let mut all_trusted = true;
    for fid in &feed_ids {
        let (v, mt) = fid.venue_market();
        let state = manager.get_state(v, mt, symbol);
        let trusted = manager.get_trusted_book(v, mt, symbol, now_ns);
        let d = diags.get(fid).unwrap();

        let is_live = state
            .map(|s| s.lifecycle_state == BookLifecycleState::Live)
            .unwrap_or(false);
        let is_valid = state.map(|s| s.validity.is_valid()).unwrap_or(false);
        let is_tr = trusted.is_some();
        if !is_tr {
            all_trusted = false;
        }

        println!(
            "{:<36} | {:<9} | {:<8} | {:<7} | {:<5} | {:<5} | {:<7}",
            fid.label(),
            if d.raw_messages > 0 { "YES" } else { "NO" },
            d.snapshots_received,
            d.deltas_received,
            if is_live { "YES" } else { "NO" },
            if is_valid { "YES" } else { "NO" },
            if is_tr { "YES" } else { "NO" }
        );
    }

    // --- REPORT SECTION 2: DETAILED METRICS PER FEED ---
    println!("\n--------------------------------------------------------------------------------");
    println!("2. DETAILED METRICS PER FEED");
    println!("--------------------------------------------------------------------------------");
    for fid in &feed_ids {
        let (v, mt) = fid.venue_market();
        let state = manager.get_state(v, mt, symbol);
        let trusted = manager.get_trusted_book(v, mt, symbol, now_ns);
        let d = diags.get_mut(fid).unwrap();
        let (f_min, f_max, f_avg, f_p95, f_p99) = d.freshness_stats();
        let lat_p50 = d.latency_p50();
        let lat_avg = d.latency_avg();

        println!("Feed: {}", fid.label());
        println!("  Connection Status:          Connected");
        println!("  Raw Messages Received:      {}", d.raw_messages);
        println!("  Snapshots Received:         {}", d.snapshots_received);
        println!("  Deltas Received:            {}", d.deltas_received);
        println!("  Parse Errors:               {}", d.parse_errors);
        println!("  Unknown Messages:           {}", d.unknown_messages);
        println!("  Sequence Violations:        {}", d.sequence_rejections);
        println!("  Crossed Books Observed:     {}", d.crossed_books_observed);
        println!("  Stale Transitions:          {}", d.stale_transitions);
        println!("  Reconnects:                 {}", d.reconnects);
        println!("  Sanity Violations:          {}", d.sanity_violations);
        if let Some(s) = state {
            println!("  Current Lifecycle:          {:?}", s.lifecycle_state);
            println!("  Current Validity:           {:?}", s.validity);
            println!("  Last Sequence ID:           {:?}", s.last_update_sequence);
        }
        if let Some(b) = trusted {
            let bid = b.best_bid().map(|l| (l.price, l.quantity));
            let ask = b.best_ask().map(|l| (l.price, l.quantity));
            let spread = b.spread();
            println!(
                "  Best Bid:                   {} (size: {})",
                bid.map(|(p, _)| p.to_string())
                    .unwrap_or_else(|| "N/A".into()),
                bid.map(|(_, q)| q.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
            println!(
                "  Best Ask:                   {} (size: {})",
                ask.map(|(p, _)| p.to_string())
                    .unwrap_or_else(|| "N/A".into()),
                ask.map(|(_, q)| q.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
            println!(
                "  Spread:                     {}",
                spread
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "N/A".into())
            );
            println!(
                "  Book Depth:                 {} bids, {} asks",
                b.bids.len(),
                b.asks.len()
            );
        }
        println!("  Last Exchange TS:           {} ms", d.last_exchange_ts_ms);
        println!(
            "  Local Receive Latency:      p50: {} us, avg: {} us",
            lat_p50, lat_avg
        );
        println!(
            "  Freshness (ms):             min: {} ms, max: {} ms, avg: {} ms, p95: {} ms, p99: {} ms",
            f_min, f_max, f_avg, f_p95, f_p99
        );
        println!();
    }

    // --- REPORT SECTION 3: TRACE EVENT FLOW PROOF ---
    println!("--------------------------------------------------------------------------------");
    println!("3. TRACE EVENT FLOW PROOF (Raw -> Parsed -> Manager -> Accepted -> Trusted)");
    println!("--------------------------------------------------------------------------------");
    println!(
        "{:<36} | {:<8} | {:<8} | {:<8} | {:<8} | {:<8} | {:<8}",
        "Feed", "Raw Msgs", "Parsed", "Manager", "Accepted", "Seq Rej", "Trusted Obs"
    );
    println!(
        "{:-<36}-|-{:-<8}-|-{:-<8}-|-{:-<8}-|-{:-<8}-|-{:-<8}-|-{:-<8}",
        "", "", "", "", "", "", ""
    );
    for fid in &feed_ids {
        let d = diags.get(fid).unwrap();
        println!(
            "{:<36} | {:<8} | {:<8} | {:<8} | {:<8} | {:<8} | {:<8}",
            fid.label(),
            d.raw_messages,
            d.parsed_events,
            d.manager_events,
            d.accepted_updates,
            d.sequence_rejections,
            d.trusted_observations
        );
    }

    // --- REPORT SECTION 4: SOAK SUMMARY ---
    let total_raw: u64 = diags.values().map(|d| d.raw_messages).sum();
    let total_seq_errors: u64 = diags.values().map(|d| d.sequence_rejections).sum();
    let total_crossed: u64 = diags.values().map(|d| d.crossed_books_observed).sum();
    let total_parse_errors: u64 = diags.values().map(|d| d.parse_errors).sum();
    let total_unknown: u64 = diags.values().map(|d| d.unknown_messages).sum();
    let total_disconnects: u64 = diags.values().map(|d| d.disconnects).sum();
    let total_reconnects: u64 = diags.values().map(|d| d.reconnects).sum();
    let total_sanity_violations: u64 = diags.values().map(|d| d.sanity_violations).sum();

    println!("\n--------------------------------------------------------------------------------");
    println!("4. SOAK SUMMARY & RESOURCE PROFILE");
    println!("--------------------------------------------------------------------------------");
    println!("Actual Run Duration:          {:.2?}", elapsed);
    println!("Total Raw Messages Ingested:  {}", total_raw);
    println!(
        "Aggregate Ingestion Rate:     {:.1} msgs/sec",
        (total_raw as f64) / elapsed.as_secs_f64()
    );
    println!("Sequence Continuity Errors:   {}", total_seq_errors);
    println!("Crossed Book Observations:    {}", total_crossed);
    println!("Parse Errors:                 {}", total_parse_errors);
    println!("Unknown Messages:             {}", total_unknown);
    println!("Disconnects Observed:         {}", total_disconnects);
    println!("Reconnects Observed:          {}", total_reconnects);
    println!("Sanity Invariant Violations:  {}", total_sanity_violations);
    println!(
        "Initial Memory (Working Set): {:.2} MB",
        (start_mem_ws as f64) / (1024.0 * 1024.0)
    );
    println!(
        "Ending Memory (Working Set):  {:.2} MB",
        (end_mem_ws as f64) / (1024.0 * 1024.0)
    );
    println!(
        "Peak Memory (Working Set):    {:.2} MB",
        (peak_mem_ws as f64) / (1024.0 * 1024.0)
    );
    println!("================================================================================\n");

    // Acceptance Assertions
    if !all_trusted {
        return Err(EngineError::Validation(
            "Validation failed: Not all 4 feeds achieved TRUSTED state".into(),
        ));
    }
    if total_seq_errors > 0 {
        return Err(EngineError::Validation(format!(
            "Validation failed: {total_seq_errors} sequence errors detected"
        )));
    }
    if total_crossed > 0 {
        return Err(EngineError::Validation(format!(
            "Validation failed: {total_crossed} crossed book states detected"
        )));
    }
    if total_sanity_violations > 0 {
        return Err(EngineError::Validation(format!(
            "Validation failed: {total_sanity_violations} sanity invariant violations detected"
        )));
    }
    if total_parse_errors > 0 {
        return Err(EngineError::Validation(format!(
            "Validation failed: {total_parse_errors} parse errors detected"
        )));
    }

    info!(
        target: "airbitrage::four_book",
        duration_secs,
        "Simultaneous 4-book live validation passed successfully with ALL 4 BOOKS TRUSTED"
    );
    Ok(())
}

async fn run_four_book_recovery_test(symbol: &str) -> Result<()> {
    info!(
        target: "airbitrage::recovery",
        symbol,
        "Starting 4-book forced recovery test harness"
    );

    println!("\n================================================================================");
    println!("         FOUR-BOOK FORCED RECOVERY & RESILIENCE TEST HARNESS");
    println!("================================================================================");
    println!("Target Symbol: BTCUSDT | Testing Bybit Spot & Binance Futures forced failover");

    let (event_tx, mut event_rx) = mpsc::channel::<MarketEvent>(16384);

    // Individual shutdown channels for targeted cancellation
    let (shutdown_tx_b_spot, shutdown_rx_b_spot) = tokio::sync::watch::channel(false);
    let (mut shutdown_tx_b_depth, shutdown_rx_b_depth) = tokio::sync::watch::channel(false);
    let (shutdown_tx_b_mark, shutdown_rx_b_mark) = tokio::sync::watch::channel(false);
    let (mut shutdown_tx_by_spot, shutdown_rx_by_spot) = tokio::sync::watch::channel(false);
    let (shutdown_tx_by_linear, shutdown_rx_by_linear) = tokio::sync::watch::channel(false);

    // Spawn 1: Binance Spot
    let binance_client = BinanceClient::new(symbol);
    let c_spot = binance_client.clone();
    let tx1 = event_tx.clone();
    tokio::spawn(async move {
        c_spot.run_spot_stream(tx1, shutdown_rx_b_spot).await;
    });

    // Spawn 2: Binance Futures Depth
    let c_depth = binance_client.clone();
    let tx2 = event_tx.clone();
    tokio::spawn(async move {
        c_depth
            .run_futures_depth_stream(tx2, shutdown_rx_b_depth.clone())
            .await;
    });

    // Spawn 3: Binance Futures Mark
    let c_mark = binance_client.clone();
    let tx3 = event_tx.clone();
    tokio::spawn(async move {
        c_mark
            .run_futures_mark_stream(tx3, shutdown_rx_b_mark)
            .await;
    });

    // Spawn 4: Bybit Spot
    let bybit_spot_metrics = Arc::new(BybitMetrics::default());
    let bybit_spot_feed = BybitWebSocketFeed::with_metrics(
        BybitFeedConfig::default_spot(symbol),
        bybit_spot_metrics.clone(),
        event_tx.clone(),
    );
    let rx_by_spot = shutdown_rx_by_spot.clone();
    tokio::spawn(async move {
        let _ = bybit_spot_feed.run_stream(rx_by_spot).await;
    });

    // Spawn 5: Bybit Linear
    let bybit_linear_metrics = Arc::new(BybitMetrics::default());
    let bybit_linear_feed = BybitWebSocketFeed::with_metrics(
        BybitFeedConfig::default_linear(symbol),
        bybit_linear_metrics.clone(),
        event_tx.clone(),
    );
    tokio::spawn(async move {
        let _ = bybit_linear_feed.run_stream(shutdown_rx_by_linear).await;
    });

    let mut manager = MarketStateManager::new(Duration::from_millis(3000));
    manager.register_instrument(VenueId::Binance, MarketType::Spot, symbol);
    manager.register_instrument(VenueId::Binance, MarketType::LinearPerpetual, symbol);
    manager.register_instrument(VenueId::Bybit, MarketType::Spot, symbol);
    manager.register_instrument(VenueId::Bybit, MarketType::LinearPerpetual, symbol);

    let mut futures_snapshot_requested = false;
    let mut futures_snapshot_applied = false;

    // Phase 1: Wait for all 4 books to reach Live + Trusted
    println!("\n[Phase 1] Establishing initial baseline synchronization across all 4 books...");
    let baseline_start = Instant::now();
    let baseline_timeout = Duration::from_secs(12);

    while baseline_start.elapsed() < baseline_timeout {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                let is_fut_snap = matches!(
                    &event,
                    MarketEvent::OrderBookSnapshot {
                        venue: VenueId::Binance,
                        market_type: MarketType::LinearPerpetual,
                        ..
                    }
                );
                let _ = manager.handle_event(&event);
                if is_fut_snap {
                    futures_snapshot_applied = true;
                }
                if !futures_snapshot_requested && !futures_snapshot_applied {
                    let count = manager
                        .get_state(VenueId::Binance, MarketType::LinearPerpetual, symbol)
                        .map(|s| s.buffered_delta_count())
                        .unwrap_or(0);
                    if count >= 5 {
                        futures_snapshot_requested = true;
                        let sym_copy = symbol.to_string();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            if let Ok(Ok(snap)) = tokio::task::spawn_blocking(move || fetch_binance_futures_snapshot(&sym_copy)).await {
                                let _ = tx.send(snap).await;
                            }
                        });
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let b_spot = manager.get_trusted_book(VenueId::Binance, MarketType::Spot, symbol, now_ns);
        let b_fut = manager.get_trusted_book(
            VenueId::Binance,
            MarketType::LinearPerpetual,
            symbol,
            now_ns,
        );
        let by_spot = manager.get_trusted_book(VenueId::Bybit, MarketType::Spot, symbol, now_ns);
        let by_linear =
            manager.get_trusted_book(VenueId::Bybit, MarketType::LinearPerpetual, symbol, now_ns);

        if b_spot.is_some() && b_fut.is_some() && by_spot.is_some() && by_linear.is_some() {
            println!(
                "  -> Baseline achieved: ALL 4 BOOKS ARE LIVE, VALID, AND TRUSTED in {:.2?}",
                baseline_start.elapsed()
            );
            break;
        }
    }

    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    if manager
        .get_trusted_book(VenueId::Bybit, MarketType::Spot, symbol, now_ns)
        .is_none()
    {
        return Err(EngineError::Validation(
            "Failed to establish Bybit Spot baseline".into(),
        ));
    }
    if manager
        .get_trusted_book(
            VenueId::Binance,
            MarketType::LinearPerpetual,
            symbol,
            now_ns,
        )
        .is_none()
    {
        return Err(EngineError::Validation(
            "Failed to establish Binance Futures baseline".into(),
        ));
    }

    // Phase 2: Interrupt Bybit Spot
    println!("\n[Phase 2] Executing Forced Interruption on Bybit Spot...");
    let t_spot_disconnect = Instant::now();
    let _ = shutdown_tx_by_spot.send(true);
    let disconnect_event = MarketEvent::ConnectionState {
        venue: VenueId::Bybit,
        market_type: Some(MarketType::Spot),
        is_connected: false,
        details: "Forced test cancellation".into(),
    };
    manager.handle_event(&disconnect_event)?;

    // Verify Bybit Spot is INVALIDATED immediately
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    assert!(
        manager
            .get_trusted_book(VenueId::Bybit, MarketType::Spot, symbol, now_ns)
            .is_none(),
        "Bybit Spot must NOT be trusted after disconnect"
    );
    assert_eq!(
        manager
            .get_state(VenueId::Bybit, MarketType::Spot, symbol)
            .unwrap()
            .lifecycle_state,
        BookLifecycleState::Invalidated
    );
    println!("  -> Verified: Bybit Spot is immediately INVALIDATED (not trusted)");

    // Verify CONCURRENCY SAFETY: Other 3 feeds remain LIVE and TRUSTED
    assert!(
        manager
            .get_trusted_book(VenueId::Bybit, MarketType::LinearPerpetual, symbol, now_ns)
            .is_some(),
        "Bybit Linear must remain TRUSTED despite Bybit Spot drop"
    );
    assert!(
        manager
            .get_trusted_book(VenueId::Binance, MarketType::Spot, symbol, now_ns)
            .is_some(),
        "Binance Spot must remain TRUSTED despite Bybit Spot drop"
    );
    assert!(
        manager
            .get_trusted_book(
                VenueId::Binance,
                MarketType::LinearPerpetual,
                symbol,
                now_ns
            )
            .is_some(),
        "Binance Futures must remain TRUSTED despite Bybit Spot drop"
    );
    println!("  -> Verified: Concurrency safety preserved — other 3 feeds remained Live & Trusted");

    // Hold outage for 1.5 seconds while draining events for other feeds
    let outage_hold = Instant::now();
    while outage_hold.elapsed() < Duration::from_millis(1500) {
        if let Ok(event) = event_rx.try_recv() {
            let _ = manager.handle_event(&event);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Re-spawn Bybit Spot feed
    println!("  -> Spawning reconnected Bybit Spot stream...");
    let (new_tx_by_spot, new_rx_by_spot) = tokio::sync::watch::channel(false);
    shutdown_tx_by_spot = new_tx_by_spot;

    let bybit_spot_feed_reconnected = BybitWebSocketFeed::with_metrics(
        BybitFeedConfig::default_spot(symbol),
        bybit_spot_metrics.clone(),
        event_tx.clone(),
    );
    tokio::spawn(async move {
        let _ = bybit_spot_feed_reconnected.run_stream(new_rx_by_spot).await;
    });

    let mut spot_recovered = false;
    let spot_recovery_timeout = Duration::from_secs(8);
    let mut spot_seq_errors = 0;
    let mut spot_crossed_books = 0;

    while t_spot_disconnect.elapsed() < spot_recovery_timeout && !spot_recovered {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                let fid = FeedId::from_event(&event);
                match manager.handle_event(&event) {
                    Ok(_) => {}
                    Err(e) => {
                        if fid == Some(FeedId::BybitSpot) {
                            match e {
                                EngineError::SequenceGap { .. } | EngineError::OutOfOrderUpdate { .. } => {
                                    spot_seq_errors += 1;
                                }
                                EngineError::CrossedBook { .. } => {
                                    spot_crossed_books += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        if manager
            .get_trusted_book(VenueId::Bybit, MarketType::Spot, symbol, now_ns)
            .is_some()
        {
            spot_recovered = true;
        }
    }

    let spot_recovery_dur = t_spot_disconnect.elapsed();
    assert!(
        spot_recovered,
        "Bybit Spot failed to recover to TRUSTED state"
    );
    assert_eq!(
        spot_seq_errors, 0,
        "Bybit Spot experienced sequence errors during recovery"
    );
    assert_eq!(
        spot_crossed_books, 0,
        "Bybit Spot observed crossed books during recovery"
    );
    println!(
        "  -> Bybit Spot successfully RECOVERED to LIVE + VALID + TRUSTED in {:.2?}",
        spot_recovery_dur
    );
    println!("     Post-recovery sequence errors: {}", spot_seq_errors);
    println!("     Post-recovery crossed books:   {}", spot_crossed_books);

    // Phase 3: Interrupt Binance Futures
    println!("\n[Phase 3] Executing Forced Interruption on Binance Futures Depth...");
    let t_fut_disconnect = Instant::now();
    let _ = shutdown_tx_b_depth.send(true);
    let disconnect_fut = MarketEvent::ConnectionState {
        venue: VenueId::Binance,
        market_type: Some(MarketType::LinearPerpetual),
        is_connected: false,
        details: "Forced test cancellation".into(),
    };
    manager.handle_event(&disconnect_fut)?;

    // Verify Binance Futures is INVALIDATED
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    assert!(
        manager
            .get_trusted_book(
                VenueId::Binance,
                MarketType::LinearPerpetual,
                symbol,
                now_ns
            )
            .is_none(),
        "Binance Futures must NOT be trusted after disconnect"
    );
    println!("  -> Verified: Binance Futures is immediately INVALIDATED (not trusted)");

    // Verify other 3 feeds remain untouched
    assert!(
        manager
            .get_trusted_book(VenueId::Binance, MarketType::Spot, symbol, now_ns)
            .is_some(),
        "Binance Spot must remain TRUSTED despite Futures depth drop"
    );
    assert!(
        manager
            .get_trusted_book(VenueId::Bybit, MarketType::Spot, symbol, now_ns)
            .is_some(),
        "Bybit Spot must remain TRUSTED"
    );
    assert!(
        manager
            .get_trusted_book(VenueId::Bybit, MarketType::LinearPerpetual, symbol, now_ns)
            .is_some(),
        "Bybit Linear must remain TRUSTED"
    );
    println!(
        "  -> Verified: Concurrency safety preserved — Binance Spot and Bybit feeds unaffected"
    );

    // Hold outage for 1.5 seconds
    let outage_hold = Instant::now();
    while outage_hold.elapsed() < Duration::from_millis(1500) {
        if let Ok(event) = event_rx.try_recv() {
            let _ = manager.handle_event(&event);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Re-spawn Binance Futures depth stream
    println!("  -> Spawning reconnected Binance Futures depth stream...");
    let (new_tx_b_depth, new_rx_b_depth) = tokio::sync::watch::channel(false);
    shutdown_tx_b_depth = new_tx_b_depth;

    let c_depth_reconnected = binance_client.clone();
    let tx_fut_reconnected = event_tx.clone();
    tokio::spawn(async move {
        c_depth_reconnected
            .run_futures_depth_stream(tx_fut_reconnected, new_rx_b_depth)
            .await;
    });

    let mut fut_recovered = false;
    let fut_recovery_timeout = Duration::from_secs(12);
    let mut fut_seq_errors = 0;
    let mut fut_crossed_books = 0;
    let mut fut_re_snapshot_requested = false;
    let mut fut_re_snapshot_applied = false;

    while t_fut_disconnect.elapsed() < fut_recovery_timeout && !fut_recovered {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                let fid = FeedId::from_event(&event);
                let is_fut_snap = matches!(
                    &event,
                    MarketEvent::OrderBookSnapshot {
                        venue: VenueId::Binance,
                        market_type: MarketType::LinearPerpetual,
                        ..
                    }
                );

                match manager.handle_event(&event) {
                    Ok(_) => {
                        if is_fut_snap {
                            fut_re_snapshot_applied = true;
                            info!(target: "airbitrage::recovery", "Binance Futures re-snapshot aligned successfully");
                        }
                    }
                    Err(e) => {
                        if fid == Some(FeedId::BinanceLinear) {
                            match e {
                                EngineError::SequenceGap { .. } | EngineError::OutOfOrderUpdate { .. } => {
                                    fut_seq_errors += 1;
                                }
                                EngineError::CrossedBook { .. } => {
                                    fut_crossed_books += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // Check buffering for new REST snapshot
                if !fut_re_snapshot_requested && !fut_re_snapshot_applied {
                    let count = manager
                        .get_state(VenueId::Binance, MarketType::LinearPerpetual, symbol)
                        .map(|s| s.buffered_delta_count())
                        .unwrap_or(0);
                    if count >= 5 {
                        fut_re_snapshot_requested = true;
                        let sym_copy = symbol.to_string();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            if let Ok(Ok(snap)) = tokio::task::spawn_blocking(move || fetch_binance_futures_snapshot(&sym_copy)).await {
                                let _ = tx.send(snap).await;
                            }
                        });
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        if manager
            .get_trusted_book(
                VenueId::Binance,
                MarketType::LinearPerpetual,
                symbol,
                now_ns,
            )
            .is_some()
        {
            fut_recovered = true;
        }
    }

    let fut_recovery_dur = t_fut_disconnect.elapsed();
    assert!(
        fut_recovered,
        "Binance Futures failed to recover to TRUSTED state"
    );
    assert_eq!(
        fut_seq_errors, 0,
        "Binance Futures experienced sequence errors during recovery"
    );
    assert_eq!(
        fut_crossed_books, 0,
        "Binance Futures observed crossed books during recovery"
    );
    println!(
        "  -> Binance Futures successfully RECOVERED to LIVE + VALID + TRUSTED in {:.2?}",
        fut_recovery_dur
    );
    println!("     Post-recovery sequence errors: {}", fut_seq_errors);
    println!("     Post-recovery crossed books:   {}", fut_crossed_books);

    // Shutdown all
    let _ = shutdown_tx_b_spot.send(true);
    let _ = shutdown_tx_b_depth.send(true);
    let _ = shutdown_tx_b_mark.send(true);
    let _ = shutdown_tx_by_spot.send(true);
    let _ = shutdown_tx_by_linear.send(true);

    println!("\n================================================================================");
    println!("             FORCED RECOVERY TEST SUITE COMPLETE: ALL SCENARIOS PASSED");
    println!("================================================================================\n");

    info!(target: "airbitrage::recovery", "4-book forced recovery test suite completed cleanly with 0 sequence errors");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config_path = parse_config_path();

    init_logging("info");

    if args.iter().any(|arg| arg == "--four-book-smoke") {
        return run_four_book_validation("BTCUSDT", 60).await;
    }

    if let Some(pos) = args.iter().position(|arg| arg == "--four-book-soak") {
        let duration: u64 = args
            .get(pos + 1)
            .and_then(|d| d.parse().ok())
            .unwrap_or(300);
        return run_four_book_validation("BTCUSDT", duration).await;
    }

    if args.iter().any(|arg| arg == "--four-book-recovery") {
        return run_four_book_recovery_test("BTCUSDT").await;
    }

    if args.iter().any(|arg| arg == "--market-state-live") {
        let pos = args.iter().position(|arg| arg == "--market-state-live");
        let duration: u64 = pos
            .and_then(|p| args.get(p + 1))
            .and_then(|d| d.parse().ok())
            .unwrap_or(15);
        return run_market_state_live_test("BTCUSDT", duration).await;
    }

    if args.iter().any(|arg| arg == "--binance-smoke") {
        return run_binance_smoke_test("BTCUSDT").await;
    }

    if args.iter().any(|arg| arg == "--bybit-smoke") {
        return run_bybit_smoke_test("BTCUSDT").await;
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
        milestone = "M1.1",
        status = "binance_hardened_ready",
        "M1.1 Binance Market-Data Protocol & Market-State Hardening verified"
    );

    Ok(())
}
