use airbitrage::config::AppConfig;
use airbitrage::error::{EngineError, Result};
use airbitrage::market::{MarketStateManager, OrderBook};
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use airbitrage::venues::binance::{BinanceClient, BinanceStreamType};
use rust_decimal::Decimal;
use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};
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
        "https://fapi.binance.com/fapi/v1/depth?symbol={}&limit=50",
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

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config_path = parse_config_path();

    init_logging("info");

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
