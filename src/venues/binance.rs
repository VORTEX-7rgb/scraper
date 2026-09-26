use crate::error::{EngineError, Result};
use crate::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, trace, warn};

// --- RAW BINANCE PAYLOAD SCHEMAS ---

#[derive(Debug, Deserialize)]
pub struct RawSpotDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct RawFuturesDepthUpdate {
    #[serde(default)]
    pub e: Option<String>,
    #[serde(rename = "E", default)]
    pub event_time: Option<i64>,
    #[serde(rename = "T", default)]
    pub transaction_time: Option<i64>,
    #[serde(default)]
    pub s: Option<String>,
    #[serde(rename = "U", default)]
    pub first_update_id: Option<u64>,
    #[serde(rename = "u", default)]
    pub final_update_id: Option<u64>,
    #[serde(rename = "pu", default)]
    pub prev_final_update_id: Option<u64>,
    #[serde(rename = "lastUpdateId", default)]
    pub last_update_id: Option<u64>,
    #[serde(default)]
    pub b: Vec<[String; 2]>,
    #[serde(default)]
    pub a: Vec<[String; 2]>,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct RawFuturesMarkPrice {
    #[serde(default)]
    pub e: Option<String>,
    #[serde(rename = "E", default)]
    pub event_time: i64,
    #[serde(default)]
    pub s: Option<String>,
    pub p: String,
    #[serde(default)]
    pub i: Option<String>,
    pub r: String,
    #[serde(rename = "T")]
    pub next_funding_time: i64,
}

#[derive(Debug, Deserialize)]
pub struct RawSubscriptionResult {
    pub result: Option<serde_json::Value>,
    pub id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct RawCombinedStream<T> {
    pub stream: String,
    pub data: T,
}

// --- OPERATIONAL METRICS ---

#[derive(Debug, Default)]
pub struct BinanceMetrics {
    pub connection_attempts: AtomicU64,
    pub successful_connections: AtomicU64,
    pub disconnects: AtomicU64,
    pub reconnects: AtomicU64,
    pub messages_received: AtomicU64,
    pub messages_parsed: AtomicU64,
    pub messages_rejected: AtomicU64,
    pub parse_errors: AtomicU64,
    pub validation_errors: AtomicU64,
    pub backpressure_drops: AtomicU64,
    pub last_exchange_ts_ms: AtomicI64,
    pub last_sequence_id: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceMetricsSnapshot {
    pub connection_attempts: u64,
    pub successful_connections: u64,
    pub disconnects: u64,
    pub reconnects: u64,
    pub messages_received: u64,
    pub messages_parsed: u64,
    pub messages_rejected: u64,
    pub parse_errors: u64,
    pub validation_errors: u64,
    pub backpressure_drops: u64,
    pub last_exchange_ts_ms: i64,
    pub last_sequence_id: u64,
}

impl BinanceMetrics {
    pub fn snapshot(&self) -> BinanceMetricsSnapshot {
        BinanceMetricsSnapshot {
            connection_attempts: self.connection_attempts.load(Ordering::Relaxed),
            successful_connections: self.successful_connections.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_parsed: self.messages_parsed.load(Ordering::Relaxed),
            messages_rejected: self.messages_rejected.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            validation_errors: self.validation_errors.load(Ordering::Relaxed),
            backpressure_drops: self.backpressure_drops.load(Ordering::Relaxed),
            last_exchange_ts_ms: self.last_exchange_ts_ms.load(Ordering::Relaxed),
            last_sequence_id: self.last_sequence_id.load(Ordering::Relaxed),
        }
    }
}

// --- PARSING & NORMALIZATION ENGINE ---

/// Parse raw string level arrays `[["64210.50", "0.45000000"], ...]` into canonical `PriceLevel`s.
pub fn parse_price_levels(raw_levels: &[[String; 2]], is_bid: bool) -> Result<Vec<PriceLevel>> {
    let mut levels = Vec::with_capacity(raw_levels.len());
    for item in raw_levels {
        let price = Decimal::from_str(&item[0])
            .map_err(|e| EngineError::Validation(format!("Invalid price '{}': {}", item[0], e)))?;
        let quantity = Decimal::from_str(&item[1]).map_err(|e| {
            EngineError::Validation(format!("Invalid quantity '{}': {}", item[1], e))
        })?;

        if price <= Decimal::ZERO {
            return Err(EngineError::Validation(format!(
                "Price must be strictly positive, got: {}",
                price
            )));
        }
        if quantity < Decimal::ZERO {
            return Err(EngineError::Validation(format!(
                "Quantity cannot be negative, got: {}",
                quantity
            )));
        }

        levels.push(PriceLevel::new(price, quantity));
    }

    if is_bid {
        levels.sort_by_key(|lvl| std::cmp::Reverse(lvl.price));
    } else {
        levels.sort_by_key(|lvl| lvl.price);
    }

    Ok(levels)
}

/// Parse raw Binance Spot partial depth frame (`@depth20@100ms`).
pub fn parse_spot_depth_payload(
    payload: &[u8],
    expected_symbol: &str,
    recv_ts_ns: i64,
) -> Result<Option<MarketEvent>> {
    // 1. Check for subscription responses: e.g. {"result":null,"id":1}
    if let Ok(sub) = serde_json::from_slice::<RawSubscriptionResult>(payload)
        && sub.id.is_some()
    {
        return Ok(None);
    }

    // 2. Parse direct or stream-wrapped payload
    let snapshot = if let Ok(wrapped) =
        serde_json::from_slice::<RawCombinedStream<RawSpotDepthSnapshot>>(payload)
    {
        let stream_lower = wrapped.stream.to_lowercase();
        let expected_lower = expected_symbol.to_lowercase();
        if !stream_lower.starts_with(&expected_lower) {
            return Err(EngineError::Validation(format!(
                "Mismatched stream symbol: expected '{}', got stream '{}'",
                expected_symbol, wrapped.stream
            )));
        }
        wrapped.data
    } else {
        serde_json::from_slice::<RawSpotDepthSnapshot>(payload)?
    };

    let bids = parse_price_levels(&snapshot.bids, true)?;
    let asks = parse_price_levels(&snapshot.asks, false)?;

    if bids.is_empty() && asks.is_empty() {
        return Err(EngineError::DataQuality("Empty order book received".into()));
    }

    // Order-book safety: verify best bid < best ask
    if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first())
        && best_bid.price >= best_ask.price
    {
        return Err(EngineError::CrossedBook {
            bid: best_bid.price,
            ask: best_ask.price,
        });
    }

    Ok(Some(MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        symbol: expected_symbol.to_uppercase(),
        bids,
        asks,
        exchange_ts_ms: 0, // Spot partial depth omits E; exchange_ts_ms is tracked in Futures
        local_recv_ts_ns: recv_ts_ns,
        sequence_id: snapshot.last_update_id,
    }))
}

/// Parse raw Binance USD-M Futures payload (handles both `depthUpdate` and `markPriceUpdate`).
pub fn parse_futures_payload(
    payload: &[u8],
    expected_symbol: &str,
    recv_ts_ns: i64,
) -> Result<Option<MarketEvent>> {
    // 1. Check for subscription responses
    if let Ok(sub) = serde_json::from_slice::<RawSubscriptionResult>(payload)
        && sub.id.is_some()
    {
        return Ok(None);
    }

    // 2. Inspect event type via generic Value inspection
    let val: serde_json::Value = serde_json::from_slice(payload)?;
    let root = if let Some(data) = val.get("data") {
        data
    } else {
        &val
    };

    let event_type = root.get("e").and_then(|v| v.as_str());

    match event_type {
        Some("depthUpdate") | None if root.get("bids").is_some() || root.get("b").is_some() => {
            let depth: RawFuturesDepthUpdate = serde_json::from_value(root.clone())?;

            if let Some(ref sym) = depth.s
                && !sym.eq_ignore_ascii_case(expected_symbol)
            {
                return Err(EngineError::Validation(format!(
                    "Symbol mismatch: expected '{}', got '{}'",
                    expected_symbol, sym
                )));
            }

            let raw_bids = if !depth.b.is_empty() {
                &depth.b
            } else {
                &depth.bids
            };
            let raw_asks = if !depth.a.is_empty() {
                &depth.a
            } else {
                &depth.asks
            };

            let bids = parse_price_levels(raw_bids, true)?;
            let asks = parse_price_levels(raw_asks, false)?;

            if bids.is_empty() && asks.is_empty() {
                return Err(EngineError::DataQuality(
                    "Empty futures depth received".into(),
                ));
            }

            if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first())
                && best_bid.price >= best_ask.price
            {
                return Err(EngineError::CrossedBook {
                    bid: best_bid.price,
                    ask: best_ask.price,
                });
            }

            let seq = depth
                .final_update_id
                .or(depth.last_update_id)
                .unwrap_or_default();
            let exchange_ts = depth.event_time.or(depth.transaction_time).unwrap_or(0);

            Ok(Some(MarketEvent::OrderBookSnapshot {
                venue: VenueId::Binance,
                market_type: MarketType::LinearPerpetual,
                symbol: expected_symbol.to_uppercase(),
                bids,
                asks,
                exchange_ts_ms: exchange_ts,
                local_recv_ts_ns: recv_ts_ns,
                sequence_id: seq,
            }))
        }
        Some("markPriceUpdate") => {
            let mark: RawFuturesMarkPrice = serde_json::from_value(root.clone())?;

            if let Some(ref sym) = mark.s
                && !sym.eq_ignore_ascii_case(expected_symbol)
            {
                return Err(EngineError::Validation(format!(
                    "Mark price symbol mismatch: expected '{}', got '{}'",
                    expected_symbol, sym
                )));
            }

            let rate = Decimal::from_str(&mark.r).map_err(|e| {
                EngineError::Validation(format!("Invalid funding rate '{}': {}", mark.r, e))
            })?;

            Ok(Some(MarketEvent::FundingRateUpdate {
                venue: VenueId::Binance,
                symbol: expected_symbol.to_uppercase(),
                rate,
                next_funding_ts_ms: mark.next_funding_time,
            }))
        }
        _ => {
            trace!(
                target: "airbitrage::binance",
                "Ignored unhandled Binance payload type: {:?}",
                event_type
            );
            Ok(None)
        }
    }
}

// --- CLIENT & STREAMING WORKER ---

/// Production-grade Binance WebSocket ingestion client.
#[derive(Debug, Clone)]
pub struct BinanceClient {
    pub symbol: String,
    pub spot_ws_url: String,
    pub futures_ws_url: String,
    pub metrics: Arc<BinanceMetrics>,
    pub max_reconnect_backoff_ms: u64,
}

impl BinanceClient {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            spot_ws_url: "wss://stream.binance.com/ws".into(),
            futures_ws_url: "wss://fstream.binance.com/ws".into(),
            metrics: Arc::new(BinanceMetrics::default()),
            max_reconnect_backoff_ms: 30_000,
        }
    }

    /// Run the Spot market data ingestion loop with reconnect backoff and backpressure handling.
    pub async fn run_spot_stream(
        &self,
        event_tx: Sender<MarketEvent>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let symbol_lower = self.symbol.to_lowercase();
        let stream_param = format!("{}@depth20@100ms", symbol_lower);
        let sub_payload = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": [stream_param],
            "id": 1
        })
        .to_string();

        let mut attempt: u32 = 0;

        while !*shutdown_rx.borrow() {
            self.metrics
                .connection_attempts
                .fetch_add(1, Ordering::Relaxed);
            info!(
                target: "airbitrage::binance",
                url = %self.spot_ws_url,
                symbol = %self.symbol,
                attempt,
                "Connecting to Binance Spot public WebSocket"
            );

            match connect_async(&self.spot_ws_url).await {
                Ok((mut ws_stream, _)) => {
                    self.metrics
                        .successful_connections
                        .fetch_add(1, Ordering::Relaxed);
                    attempt = 0;
                    info!(
                        target: "airbitrage::binance",
                        "Connected to Binance Spot WebSocket; sending subscription"
                    );

                    let _ = event_tx.try_send(MarketEvent::ConnectionState {
                        venue: VenueId::Binance,
                        is_connected: true,
                        details: "Connected to Spot WS".into(),
                    });

                    if let Err(e) = ws_stream.send(Message::Text(sub_payload.clone())).await {
                        error!(target: "airbitrage::binance", error = %e, "Failed to send Spot subscription payload");
                        continue;
                    }

                    while !*shutdown_rx.borrow() {
                        tokio::select! {
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    info!(target: "airbitrage::binance", "Shutdown signal received; closing Spot stream");
                                    break;
                                }
                            }
                            msg_opt = ws_stream.next() => {
                                let msg = match msg_opt {
                                    Some(Ok(m)) => m,
                                    Some(Err(e)) => {
                                        warn!(target: "airbitrage::binance", error = %e, "Error reading from Binance Spot WS");
                                        break;
                                    }
                                    None => {
                                        warn!(target: "airbitrage::binance", "Binance Spot WebSocket stream closed by server");
                                        break;
                                    }
                                };

                                let recv_instant = Instant::now();
                                let recv_ts_ns = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as i64;

                                self.metrics.messages_received.fetch_add(1, Ordering::Relaxed);

                                match msg {
                                    Message::Text(text) => {
                                        match parse_spot_depth_payload(text.as_bytes(), &self.symbol, recv_ts_ns) {
                                            Ok(Some(event)) => {
                                                self.metrics.messages_parsed.fetch_add(1, Ordering::Relaxed);
                                                if let MarketEvent::OrderBookSnapshot { sequence_id, .. } = &event {
                                                    self.metrics.last_sequence_id.store(*sequence_id, Ordering::Relaxed);
                                                }

                                                match event_tx.try_send(event) {
                                                    Ok(_) => {},
                                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                                        self.metrics.backpressure_drops.fetch_add(1, Ordering::Relaxed);
                                                        self.metrics.messages_rejected.fetch_add(1, Ordering::Relaxed);
                                                        warn!(target: "airbitrage::binance", "Channel buffer full; dropping Spot event to prevent unhedged stale lag");
                                                    },
                                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                        error!(target: "airbitrage::binance", "Event channel closed; terminating worker");
                                                        return;
                                                    }
                                                }
                                                trace!(target: "airbitrage::binance", elapsed_us = recv_instant.elapsed().as_micros(), "Dispatched Spot event");
                                            }
                                            Ok(None) => {},
                                            Err(e) => {
                                                self.metrics.parse_errors.fetch_add(1, Ordering::Relaxed);
                                                warn!(target: "airbitrage::binance", error = %e, "Failed to parse Binance Spot message");
                                            }
                                        }
                                    }
                                    Message::Ping(payload) => {
                                        let _ = ws_stream.send(Message::Pong(payload)).await;
                                    }
                                    Message::Close(_) => {
                                        info!(target: "airbitrage::binance", "Received close frame from Binance Spot WS");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    let _ = event_tx.try_send(MarketEvent::ConnectionState {
                        venue: VenueId::Binance,
                        is_connected: false,
                        details: "Disconnected from Spot WS".into(),
                    });
                }
                Err(e) => {
                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    warn!(target: "airbitrage::binance", error = %e, "Failed to connect to Binance Spot WS");
                }
            }

            if *shutdown_rx.borrow() {
                break;
            }

            attempt += 1;
            self.metrics.reconnects.fetch_add(1, Ordering::Relaxed);
            let base_ms =
                std::cmp::min(self.max_reconnect_backoff_ms, 1000 * (1 << attempt.min(5)));
            let jitter_ms = (attempt * 123) % 450;
            let backoff = Duration::from_millis(base_ms + jitter_ms as u64);
            warn!(target: "airbitrage::binance", backoff_ms = base_ms + jitter_ms as u64, "Sleeping before reconnecting to Spot WS");
            tokio::time::sleep(backoff).await;
        }
    }

    /// Run the USD-M Futures market data ingestion loop (depth20 + markPrice).
    pub async fn run_futures_stream(
        &self,
        event_tx: Sender<MarketEvent>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let symbol_lower = self.symbol.to_lowercase();
        let depth_param = format!("{}@depth20@100ms", symbol_lower);
        let mark_param = format!("{}@markPrice@1s", symbol_lower);
        let sub_payload = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": [depth_param, mark_param],
            "id": 2
        })
        .to_string();

        let mut attempt: u32 = 0;

        while !*shutdown_rx.borrow() {
            self.metrics
                .connection_attempts
                .fetch_add(1, Ordering::Relaxed);
            info!(
                target: "airbitrage::binance",
                url = %self.futures_ws_url,
                symbol = %self.symbol,
                attempt,
                "Connecting to Binance USD-M Futures public WebSocket"
            );

            match connect_async(&self.futures_ws_url).await {
                Ok((mut ws_stream, _)) => {
                    self.metrics
                        .successful_connections
                        .fetch_add(1, Ordering::Relaxed);
                    attempt = 0;
                    info!(
                        target: "airbitrage::binance",
                        "Connected to Binance Futures WebSocket; sending subscription"
                    );

                    let _ = event_tx.try_send(MarketEvent::ConnectionState {
                        venue: VenueId::Binance,
                        is_connected: true,
                        details: "Connected to Futures WS".into(),
                    });

                    if let Err(e) = ws_stream.send(Message::Text(sub_payload.clone())).await {
                        error!(target: "airbitrage::binance", error = %e, "Failed to send Futures subscription payload");
                        continue;
                    }

                    while !*shutdown_rx.borrow() {
                        tokio::select! {
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    info!(target: "airbitrage::binance", "Shutdown signal received; closing Futures stream");
                                    break;
                                }
                            }
                            msg_opt = ws_stream.next() => {
                                let msg = match msg_opt {
                                    Some(Ok(m)) => m,
                                    Some(Err(e)) => {
                                        warn!(target: "airbitrage::binance", error = %e, "Error reading from Binance Futures WS");
                                        break;
                                    }
                                    None => {
                                        warn!(target: "airbitrage::binance", "Binance Futures WebSocket stream closed by server");
                                        break;
                                    }
                                };

                                let recv_instant = Instant::now();
                                let recv_ts_ns = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos() as i64;

                                self.metrics.messages_received.fetch_add(1, Ordering::Relaxed);

                                match msg {
                                    Message::Text(text) => {
                                        match parse_futures_payload(text.as_bytes(), &self.symbol, recv_ts_ns) {
                                            Ok(Some(event)) => {
                                                self.metrics.messages_parsed.fetch_add(1, Ordering::Relaxed);
                                                if let MarketEvent::OrderBookSnapshot { sequence_id, exchange_ts_ms, .. } = &event {
                                                    self.metrics.last_sequence_id.store(*sequence_id, Ordering::Relaxed);
                                                    self.metrics.last_exchange_ts_ms.store(*exchange_ts_ms, Ordering::Relaxed);
                                                }

                                                match event_tx.try_send(event) {
                                                    Ok(_) => {},
                                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                                        self.metrics.backpressure_drops.fetch_add(1, Ordering::Relaxed);
                                                        self.metrics.messages_rejected.fetch_add(1, Ordering::Relaxed);
                                                        warn!(target: "airbitrage::binance", "Channel buffer full; dropping Futures event to enforce backpressure");
                                                    },
                                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                        error!(target: "airbitrage::binance", "Event channel closed; terminating worker");
                                                        return;
                                                    }
                                                }
                                                trace!(target: "airbitrage::binance", elapsed_us = recv_instant.elapsed().as_micros(), "Dispatched Futures event");
                                            }
                                            Ok(None) => {},
                                            Err(e) => {
                                                self.metrics.parse_errors.fetch_add(1, Ordering::Relaxed);
                                                warn!(target: "airbitrage::binance", error = %e, "Failed to parse Binance Futures message");
                                            }
                                        }
                                    }
                                    Message::Ping(payload) => {
                                        let _ = ws_stream.send(Message::Pong(payload)).await;
                                    }
                                    Message::Close(_) => {
                                        info!(target: "airbitrage::binance", "Received close frame from Binance Futures WS");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    let _ = event_tx.try_send(MarketEvent::ConnectionState {
                        venue: VenueId::Binance,
                        is_connected: false,
                        details: "Disconnected from Futures WS".into(),
                    });
                }
                Err(e) => {
                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    warn!(target: "airbitrage::binance", error = %e, "Failed to connect to Binance Futures WS");
                }
            }

            if *shutdown_rx.borrow() {
                break;
            }

            attempt += 1;
            self.metrics.reconnects.fetch_add(1, Ordering::Relaxed);
            let base_ms =
                std::cmp::min(self.max_reconnect_backoff_ms, 1000 * (1 << attempt.min(5)));
            let jitter_ms = (attempt * 123) % 450;
            let backoff = Duration::from_millis(base_ms + jitter_ms as u64);
            warn!(target: "airbitrage::binance", backoff_ms = base_ms + jitter_ms as u64, "Sleeping before reconnecting to Futures WS");
            tokio::time::sleep(backoff).await;
        }
    }
}

impl super::VenueConnector for BinanceClient {
    fn venue_id(&self) -> VenueId {
        VenueId::Binance
    }

    fn is_enabled(&self) -> bool {
        true
    }
}
