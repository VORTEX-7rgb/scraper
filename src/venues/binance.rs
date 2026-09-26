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

// --- STREAM IDENTIFIERS & FRESHNESS ---

/// Supported individual Binance stream types for per-stream freshness and health monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinanceStreamType {
    SpotDepth,
    FuturesDepth,
    FuturesMarkPrice,
}

impl std::fmt::Display for BinanceStreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SpotDepth => write!(f, "spot_depth"),
            Self::FuturesDepth => write!(f, "futures_depth"),
            Self::FuturesMarkPrice => write!(f, "futures_markprice"),
        }
    }
}

/// Explicit connection lifecycle state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionLifecycleState {
    Disconnected,
    Backoff,
    Reconnecting,
    Connected,
    Resubscribing,
    Streaming,
    Stale,
}

/// Per-stream freshness tracker recording wall-clock arrival epochs to detect stale feeds.
#[derive(Debug, Default)]
pub struct StreamFreshnessTracker {
    pub spot_depth_last_recv_ms: AtomicI64,
    pub futures_depth_last_recv_ms: AtomicI64,
    pub futures_markprice_last_recv_ms: AtomicI64,
}

impl StreamFreshnessTracker {
    pub fn record_event(&self, stream: BinanceStreamType, timestamp_ms: i64) {
        match stream {
            BinanceStreamType::SpotDepth => {
                self.spot_depth_last_recv_ms
                    .store(timestamp_ms, Ordering::Relaxed);
            }
            BinanceStreamType::FuturesDepth => {
                self.futures_depth_last_recv_ms
                    .store(timestamp_ms, Ordering::Relaxed);
            }
            BinanceStreamType::FuturesMarkPrice => {
                self.futures_markprice_last_recv_ms
                    .store(timestamp_ms, Ordering::Relaxed);
            }
        }
    }

    pub fn last_recv_ms(&self, stream: BinanceStreamType) -> i64 {
        match stream {
            BinanceStreamType::SpotDepth => self.spot_depth_last_recv_ms.load(Ordering::Relaxed),
            BinanceStreamType::FuturesDepth => {
                self.futures_depth_last_recv_ms.load(Ordering::Relaxed)
            }
            BinanceStreamType::FuturesMarkPrice => {
                self.futures_markprice_last_recv_ms.load(Ordering::Relaxed)
            }
        }
    }

    pub fn is_stale(&self, stream: BinanceStreamType, threshold: Duration, now_ms: i64) -> bool {
        let last = self.last_recv_ms(stream);
        if last <= 0 {
            return true;
        }
        (now_ms - last) > threshold.as_millis() as i64
    }

    pub fn reset(&self, stream: BinanceStreamType) {
        match stream {
            BinanceStreamType::SpotDepth => {
                self.spot_depth_last_recv_ms.store(0, Ordering::Relaxed)
            }
            BinanceStreamType::FuturesDepth => {
                self.futures_depth_last_recv_ms.store(0, Ordering::Relaxed)
            }
            BinanceStreamType::FuturesMarkPrice => self
                .futures_markprice_last_recv_ms
                .store(0, Ordering::Relaxed),
        }
    }
}

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
    #[serde(default)]
    pub b: Vec<[String; 2]>,
    #[serde(default)]
    pub a: Vec<[String; 2]>,
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
    #[serde(rename = "P", default)]
    pub index_price: Option<String>,
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

    pub spot_depth_messages: AtomicU64,
    pub futures_depth_messages: AtomicU64,
    pub futures_markprice_messages: AtomicU64,

    pub subscription_messages: AtomicU64,
    pub parse_errors: AtomicU64,
    pub validation_errors: AtomicU64,

    pub crossed_books: AtomicU64,
    pub stale_events: AtomicU64,
    pub backpressure_drops: AtomicU64,

    pub last_spot_sequence: AtomicU64,
    pub last_futures_sequence: AtomicU64,
    pub last_futures_exchange_ts: AtomicI64,
    pub last_markprice_exchange_ts: AtomicI64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceMetricsSnapshot {
    pub connection_attempts: u64,
    pub successful_connections: u64,
    pub disconnects: u64,
    pub reconnects: u64,
    pub spot_depth_messages: u64,
    pub futures_depth_messages: u64,
    pub futures_markprice_messages: u64,
    pub subscription_messages: u64,
    pub parse_errors: u64,
    pub validation_errors: u64,
    pub crossed_books: u64,
    pub stale_events: u64,
    pub backpressure_drops: u64,
    pub last_spot_sequence: u64,
    pub last_futures_sequence: u64,
    pub last_futures_exchange_ts: i64,
    pub last_markprice_exchange_ts: i64,
}

impl BinanceMetrics {
    pub fn snapshot(&self) -> BinanceMetricsSnapshot {
        BinanceMetricsSnapshot {
            connection_attempts: self.connection_attempts.load(Ordering::Relaxed),
            successful_connections: self.successful_connections.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            spot_depth_messages: self.spot_depth_messages.load(Ordering::Relaxed),
            futures_depth_messages: self.futures_depth_messages.load(Ordering::Relaxed),
            futures_markprice_messages: self.futures_markprice_messages.load(Ordering::Relaxed),
            subscription_messages: self.subscription_messages.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            validation_errors: self.validation_errors.load(Ordering::Relaxed),
            crossed_books: self.crossed_books.load(Ordering::Relaxed),
            stale_events: self.stale_events.load(Ordering::Relaxed),
            backpressure_drops: self.backpressure_drops.load(Ordering::Relaxed),
            last_spot_sequence: self.last_spot_sequence.load(Ordering::Relaxed),
            last_futures_sequence: self.last_futures_sequence.load(Ordering::Relaxed),
            last_futures_exchange_ts: self.last_futures_exchange_ts.load(Ordering::Relaxed),
            last_markprice_exchange_ts: self.last_markprice_exchange_ts.load(Ordering::Relaxed),
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
    // 1. Subscription response check: e.g. {"result":null,"id":1}
    if let Ok(sub) = serde_json::from_slice::<RawSubscriptionResult>(payload)
        && sub.id.is_some()
    {
        return Ok(None);
    }

    // 2. Direct or stream-wrapped payload parsing
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
        exchange_ts_ms: 0,
        local_recv_ts_ns: recv_ts_ns,
        sequence_id: snapshot.last_update_id,
    }))
}

/// Parse raw Binance USD-M Futures depth payload (`/public/ws` stream).
/// Emits `MarketEvent::OrderBookDelta` preserving `U`, `u`, `pu`, `E`, and `T`.
pub fn parse_futures_depth_payload(
    payload: &[u8],
    expected_symbol: &str,
    recv_ts_ns: i64,
) -> Result<Option<MarketEvent>> {
    // 1. Subscription ACK check
    if let Ok(sub) = serde_json::from_slice::<RawSubscriptionResult>(payload)
        && sub.id.is_some()
    {
        return Ok(None);
    }

    // 2. Direct or wrapped depth update parsing (strongly-typed, no serde_json::Value cloning)
    let depth = if let Ok(wrapped) =
        serde_json::from_slice::<RawCombinedStream<RawFuturesDepthUpdate>>(payload)
    {
        let stream_lower = wrapped.stream.to_lowercase();
        let expected_lower = expected_symbol.to_lowercase();
        if !stream_lower.starts_with(&expected_lower) {
            return Err(EngineError::Validation(format!(
                "Futures depth symbol mismatch: expected '{}', got stream '{}'",
                expected_symbol, wrapped.stream
            )));
        }
        wrapped.data
    } else {
        serde_json::from_slice::<RawFuturesDepthUpdate>(payload)?
    };

    if let Some(ref sym) = depth.s
        && !sym.eq_ignore_ascii_case(expected_symbol)
    {
        return Err(EngineError::Validation(format!(
            "Symbol mismatch: expected '{}', got '{}'",
            expected_symbol, sym
        )));
    }

    let first_seq = depth.first_update_id.unwrap_or(0);
    let final_seq = depth.final_update_id.unwrap_or(0);

    if first_seq > final_seq && final_seq > 0 {
        return Err(EngineError::Validation(format!(
            "Invalid sequence ID ordering: U ({first_seq}) > u ({final_seq})"
        )));
    }

    let bids = parse_price_levels(&depth.b, true)?;
    let asks = parse_price_levels(&depth.a, false)?;

    if bids.is_empty() && asks.is_empty() {
        return Err(EngineError::DataQuality(
            "Empty futures depth received".into(),
        ));
    }

    let exchange_ts = depth.event_time.unwrap_or(0);
    let trans_ts = depth.transaction_time.unwrap_or(exchange_ts);

    Ok(Some(MarketEvent::OrderBookDelta {
        venue: VenueId::Binance,
        market_type: MarketType::LinearPerpetual,
        symbol: expected_symbol.to_uppercase(),
        bids,
        asks,
        first_sequence_id: first_seq,
        sequence_id: final_seq,
        prev_sequence_id: depth.prev_final_update_id,
        transaction_ts_ms: trans_ts,
        exchange_ts_ms: exchange_ts,
        local_recv_ts_ns: recv_ts_ns,
    }))
}

/// Parse raw Binance USD-M Futures mark price & funding payload (`/market/ws` stream).
/// Emits `MarketEvent::FundingRateUpdate` including mark price, index price, and timestamps.
pub fn parse_futures_mark_price_payload(
    payload: &[u8],
    expected_symbol: &str,
    recv_ts_ns: i64,
) -> Result<Option<MarketEvent>> {
    // 1. Subscription ACK check
    if let Ok(sub) = serde_json::from_slice::<RawSubscriptionResult>(payload)
        && sub.id.is_some()
    {
        return Ok(None);
    }

    // 2. Direct or wrapped mark price parsing
    let mark = if let Ok(wrapped) =
        serde_json::from_slice::<RawCombinedStream<RawFuturesMarkPrice>>(payload)
    {
        let stream_lower = wrapped.stream.to_lowercase();
        let expected_lower = expected_symbol.to_lowercase();
        if !stream_lower.starts_with(&expected_lower) {
            return Err(EngineError::Validation(format!(
                "Mark price symbol mismatch: expected '{}', got stream '{}'",
                expected_symbol, wrapped.stream
            )));
        }
        wrapped.data
    } else {
        serde_json::from_slice::<RawFuturesMarkPrice>(payload)?
    };

    if let Some(ref sym) = mark.s
        && !sym.eq_ignore_ascii_case(expected_symbol)
    {
        return Err(EngineError::Validation(format!(
            "Mark price symbol mismatch: expected '{}', got '{}'",
            expected_symbol, sym
        )));
    }

    let mark_price = Decimal::from_str(&mark.p)
        .map_err(|e| EngineError::Validation(format!("Invalid mark price '{}': {}", mark.p, e)))?;

    if mark_price <= Decimal::ZERO {
        return Err(EngineError::Validation(format!(
            "Mark price must be positive, got: {}",
            mark_price
        )));
    }

    let index_price = if let Some(ref idx_str) = mark.index_price {
        Some(Decimal::from_str(idx_str).map_err(|e| {
            EngineError::Validation(format!("Invalid index price '{}': {}", idx_str, e))
        })?)
    } else {
        None
    };

    let rate = Decimal::from_str(&mark.r).map_err(|e| {
        EngineError::Validation(format!("Invalid funding rate '{}': {}", mark.r, e))
    })?;

    Ok(Some(MarketEvent::FundingRateUpdate {
        venue: VenueId::Binance,
        symbol: expected_symbol.to_uppercase(),
        mark_price,
        index_price,
        rate,
        next_funding_ts_ms: mark.next_funding_time,
        exchange_ts_ms: mark.event_time,
        local_recv_ts_ns: recv_ts_ns,
    }))
}

/// Unified generic parser for Binance Futures payloads (dispatches depth or mark price without cloning).
pub fn parse_futures_payload(
    payload: &[u8],
    expected_symbol: &str,
    recv_ts_ns: i64,
) -> Result<Option<MarketEvent>> {
    // 1. Subscription ACK check
    if let Ok(sub) = serde_json::from_slice::<RawSubscriptionResult>(payload)
        && sub.id.is_some()
    {
        return Ok(None);
    }

    // 2. Try parsing as depth update
    if let Ok(depth_event) = parse_futures_depth_payload(payload, expected_symbol, recv_ts_ns) {
        return Ok(depth_event);
    }

    // 3. Try parsing as mark price update
    if let Ok(mark_event) = parse_futures_mark_price_payload(payload, expected_symbol, recv_ts_ns) {
        return Ok(mark_event);
    }

    trace!(
        target: "airbitrage::binance",
        "Ignored unhandled or unrecognized Binance payload"
    );
    Ok(None)
}

// --- CLIENT & STREAMING WORKER ---

/// Production-grade Binance WebSocket ingestion client implementing the 2026 routed architecture.
#[derive(Debug, Clone)]
pub struct BinanceClient {
    pub symbol: String,
    pub spot_ws_url: String,
    pub futures_public_ws_url: String,
    pub futures_market_ws_url: String,
    pub metrics: Arc<BinanceMetrics>,
    pub freshness: Arc<StreamFreshnessTracker>,
    pub max_reconnect_backoff_ms: u64,
    pub freshness_threshold: Duration,
}

impl BinanceClient {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            spot_ws_url: "wss://stream.binance.com/ws".into(),
            futures_public_ws_url: "wss://fstream.binance.com/public/ws".into(),
            futures_market_ws_url: "wss://fstream.binance.com/market/ws".into(),
            metrics: Arc::new(BinanceMetrics::default()),
            freshness: Arc::new(StreamFreshnessTracker::default()),
            max_reconnect_backoff_ms: 30_000,
            freshness_threshold: Duration::from_millis(500),
        }
    }

    /// Freshness threshold tailored to each stream's nominal publication cadence.
    pub fn stream_freshness_threshold(&self, stream: BinanceStreamType) -> Duration {
        match stream {
            BinanceStreamType::SpotDepth => self.freshness_threshold, // 500ms (5 missed 100ms frames)
            BinanceStreamType::FuturesDepth => self.freshness_threshold, // 500ms (5 missed 100ms frames)
            BinanceStreamType::FuturesMarkPrice => Duration::from_millis(3000), // 3000ms (3 missed 1s ticks)
        }
    }

    /// Check if a particular Binance stream is currently stale based on its nominal cadence.
    pub fn is_stream_stale(&self, stream: BinanceStreamType) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.freshness
            .is_stale(stream, self.stream_freshness_threshold(stream), now_ms)
    }

    /// Run the Spot market data ingestion loop (`@depth20@100ms`).
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
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
                                let recv_ts_ns = now.as_nanos() as i64;
                                let recv_ts_ms = now.as_millis() as i64;

                                match msg {
                                    Message::Text(text) => {
                                        match parse_spot_depth_payload(text.as_bytes(), &self.symbol, recv_ts_ns) {
                                            Ok(Some(event)) => {
                                                self.metrics.spot_depth_messages.fetch_add(1, Ordering::Relaxed);
                                                self.freshness.record_event(BinanceStreamType::SpotDepth, recv_ts_ms);

                                                if let MarketEvent::OrderBookSnapshot { sequence_id, .. } = &event {
                                                    self.metrics.last_spot_sequence.store(*sequence_id, Ordering::Relaxed);
                                                }

                                                match event_tx.try_send(event) {
                                                    Ok(_) => {},
                                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                                        self.metrics.backpressure_drops.fetch_add(1, Ordering::Relaxed);
                                                        warn!(target: "airbitrage::binance", "Buffer full; dropping Spot event to prevent stale lag");
                                                    },
                                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                        error!(target: "airbitrage::binance", "Event channel closed; terminating Spot worker");
                                                        return;
                                                    }
                                                }
                                                trace!(target: "airbitrage::binance", elapsed_us = recv_instant.elapsed().as_micros(), "Dispatched Spot event");
                                            }
                                            Ok(None) => {
                                                self.metrics.subscription_messages.fetch_add(1, Ordering::Relaxed);
                                            }
                                            Err(e) => {
                                                match &e {
                                                    EngineError::CrossedBook { .. } => {
                                                        self.metrics.crossed_books.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                    EngineError::Validation(_) => {
                                                        self.metrics.validation_errors.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                    _ => {
                                                        self.metrics.parse_errors.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                }
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
                    self.freshness.reset(BinanceStreamType::SpotDepth);
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

    /// Run the USD-M Futures Depth ingestion loop using the `/public` route (`@depth20@100ms`).
    pub async fn run_futures_depth_stream(
        &self,
        event_tx: Sender<MarketEvent>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let symbol_lower = self.symbol.to_lowercase();
        let depth_param = format!("{}@depth20@100ms", symbol_lower);
        let sub_payload = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": [depth_param],
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
                url = %self.futures_public_ws_url,
                symbol = %self.symbol,
                attempt,
                "Connecting to Binance Futures /public WebSocket for depth"
            );

            match connect_async(&self.futures_public_ws_url).await {
                Ok((mut ws_stream, _)) => {
                    self.metrics
                        .successful_connections
                        .fetch_add(1, Ordering::Relaxed);
                    attempt = 0;
                    info!(
                        target: "airbitrage::binance",
                        "Connected to Binance Futures /public WebSocket; sending depth subscription"
                    );

                    let _ = event_tx.try_send(MarketEvent::ConnectionState {
                        venue: VenueId::Binance,
                        is_connected: true,
                        details: "Connected to Futures /public WS".into(),
                    });

                    if let Err(e) = ws_stream.send(Message::Text(sub_payload.clone())).await {
                        error!(target: "airbitrage::binance", error = %e, "Failed to send Futures depth subscription");
                        continue;
                    }

                    while !*shutdown_rx.borrow() {
                        tokio::select! {
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    info!(target: "airbitrage::binance", "Shutdown signal received; closing Futures depth stream");
                                    break;
                                }
                            }
                            msg_opt = ws_stream.next() => {
                                let msg = match msg_opt {
                                    Some(Ok(m)) => m,
                                    Some(Err(e)) => {
                                        warn!(target: "airbitrage::binance", error = %e, "Error reading from Binance Futures depth WS");
                                        break;
                                    }
                                    None => {
                                        warn!(target: "airbitrage::binance", "Binance Futures depth WebSocket stream closed by server");
                                        break;
                                    }
                                };

                                let recv_instant = Instant::now();
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
                                let recv_ts_ns = now.as_nanos() as i64;
                                let recv_ts_ms = now.as_millis() as i64;

                                match msg {
                                    Message::Text(text) => {
                                        match parse_futures_depth_payload(text.as_bytes(), &self.symbol, recv_ts_ns) {
                                            Ok(Some(event)) => {
                                                self.metrics.futures_depth_messages.fetch_add(1, Ordering::Relaxed);
                                                self.freshness.record_event(BinanceStreamType::FuturesDepth, recv_ts_ms);

                                                if let MarketEvent::OrderBookDelta { sequence_id, exchange_ts_ms, .. } = &event {
                                                    self.metrics.last_futures_sequence.store(*sequence_id, Ordering::Relaxed);
                                                    self.metrics.last_futures_exchange_ts.store(*exchange_ts_ms, Ordering::Relaxed);
                                                }

                                                match event_tx.try_send(event) {
                                                    Ok(_) => {},
                                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                                        self.metrics.backpressure_drops.fetch_add(1, Ordering::Relaxed);
                                                        warn!(target: "airbitrage::binance", "Buffer full; dropping Futures depth event to enforce backpressure");
                                                    },
                                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                        error!(target: "airbitrage::binance", "Event channel closed; terminating Futures depth worker");
                                                        return;
                                                    }
                                                }
                                                trace!(target: "airbitrage::binance", elapsed_us = recv_instant.elapsed().as_micros(), "Dispatched Futures depth delta");
                                            }
                                            Ok(None) => {
                                                self.metrics.subscription_messages.fetch_add(1, Ordering::Relaxed);
                                            }
                                            Err(e) => {
                                                match &e {
                                                    EngineError::Validation(_) => {
                                                        self.metrics.validation_errors.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                    _ => {
                                                        self.metrics.parse_errors.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                }
                                                warn!(target: "airbitrage::binance", error = %e, "Failed to parse Binance Futures depth message");
                                            }
                                        }
                                    }
                                    Message::Ping(payload) => {
                                        let _ = ws_stream.send(Message::Pong(payload)).await;
                                    }
                                    Message::Close(_) => {
                                        info!(target: "airbitrage::binance", "Received close frame from Binance Futures depth WS");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    self.freshness.reset(BinanceStreamType::FuturesDepth);
                    let _ = event_tx.try_send(MarketEvent::ConnectionState {
                        venue: VenueId::Binance,
                        is_connected: false,
                        details: "Disconnected from Futures /public WS".into(),
                    });
                }
                Err(e) => {
                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    warn!(target: "airbitrage::binance", error = %e, "Failed to connect to Binance Futures /public WS");
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
            warn!(target: "airbitrage::binance", backoff_ms = base_ms + jitter_ms as u64, "Sleeping before reconnecting to Futures /public WS");
            tokio::time::sleep(backoff).await;
        }
    }

    /// Run the USD-M Futures Mark Price & Funding ingestion loop using the `/market` route (`@markPrice@1s`).
    pub async fn run_futures_mark_stream(
        &self,
        event_tx: Sender<MarketEvent>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let symbol_lower = self.symbol.to_lowercase();
        let mark_param = format!("{}@markPrice@1s", symbol_lower);
        let sub_payload = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": [mark_param],
            "id": 3
        })
        .to_string();

        let mut attempt: u32 = 0;

        while !*shutdown_rx.borrow() {
            self.metrics
                .connection_attempts
                .fetch_add(1, Ordering::Relaxed);
            info!(
                target: "airbitrage::binance",
                url = %self.futures_market_ws_url,
                symbol = %self.symbol,
                attempt,
                "Connecting to Binance Futures /market WebSocket for mark price"
            );

            match connect_async(&self.futures_market_ws_url).await {
                Ok((mut ws_stream, _)) => {
                    self.metrics
                        .successful_connections
                        .fetch_add(1, Ordering::Relaxed);
                    attempt = 0;
                    info!(
                        target: "airbitrage::binance",
                        "Connected to Binance Futures /market WebSocket; sending markPrice subscription"
                    );

                    let _ = event_tx.try_send(MarketEvent::ConnectionState {
                        venue: VenueId::Binance,
                        is_connected: true,
                        details: "Connected to Futures /market WS".into(),
                    });

                    if let Err(e) = ws_stream.send(Message::Text(sub_payload.clone())).await {
                        error!(target: "airbitrage::binance", error = %e, "Failed to send Futures markPrice subscription");
                        continue;
                    }

                    while !*shutdown_rx.borrow() {
                        tokio::select! {
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    info!(target: "airbitrage::binance", "Shutdown signal received; closing Futures mark stream");
                                    break;
                                }
                            }
                            msg_opt = ws_stream.next() => {
                                let msg = match msg_opt {
                                    Some(Ok(m)) => m,
                                    Some(Err(e)) => {
                                        warn!(target: "airbitrage::binance", error = %e, "Error reading from Binance Futures mark WS");
                                        break;
                                    }
                                    None => {
                                        warn!(target: "airbitrage::binance", "Binance Futures mark WebSocket stream closed by server");
                                        break;
                                    }
                                };

                                let recv_instant = Instant::now();
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
                                let recv_ts_ns = now.as_nanos() as i64;
                                let recv_ts_ms = now.as_millis() as i64;

                                match msg {
                                    Message::Text(text) => {
                                        match parse_futures_mark_price_payload(text.as_bytes(), &self.symbol, recv_ts_ns) {
                                            Ok(Some(event)) => {
                                                self.metrics.futures_markprice_messages.fetch_add(1, Ordering::Relaxed);
                                                self.freshness.record_event(BinanceStreamType::FuturesMarkPrice, recv_ts_ms);

                                                if let MarketEvent::FundingRateUpdate { exchange_ts_ms, .. } = &event {
                                                    self.metrics.last_markprice_exchange_ts.store(*exchange_ts_ms, Ordering::Relaxed);
                                                }

                                                match event_tx.try_send(event) {
                                                    Ok(_) => {},
                                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                                        self.metrics.backpressure_drops.fetch_add(1, Ordering::Relaxed);
                                                        warn!(target: "airbitrage::binance", "Buffer full; dropping Futures mark event");
                                                    },
                                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                        error!(target: "airbitrage::binance", "Event channel closed; terminating Futures mark worker");
                                                        return;
                                                    }
                                                }
                                                trace!(target: "airbitrage::binance", elapsed_us = recv_instant.elapsed().as_micros(), "Dispatched Futures mark price event");
                                            }
                                            Ok(None) => {
                                                self.metrics.subscription_messages.fetch_add(1, Ordering::Relaxed);
                                            }
                                            Err(e) => {
                                                match &e {
                                                    EngineError::Validation(_) => {
                                                        self.metrics.validation_errors.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                    _ => {
                                                        self.metrics.parse_errors.fetch_add(1, Ordering::Relaxed);
                                                    }
                                                }
                                                warn!(target: "airbitrage::binance", error = %e, "Failed to parse Binance Futures mark price message");
                                            }
                                        }
                                    }
                                    Message::Ping(payload) => {
                                        let _ = ws_stream.send(Message::Pong(payload)).await;
                                    }
                                    Message::Close(_) => {
                                        info!(target: "airbitrage::binance", "Received close frame from Binance Futures mark WS");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    self.freshness.reset(BinanceStreamType::FuturesMarkPrice);
                    let _ = event_tx.try_send(MarketEvent::ConnectionState {
                        venue: VenueId::Binance,
                        is_connected: false,
                        details: "Disconnected from Futures /market WS".into(),
                    });
                }
                Err(e) => {
                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    warn!(target: "airbitrage::binance", error = %e, "Failed to connect to Binance Futures /market WS");
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
            warn!(target: "airbitrage::binance", backoff_ms = base_ms + jitter_ms as u64, "Sleeping before reconnecting to Futures /market WS");
            tokio::time::sleep(backoff).await;
        }
    }

    /// Run both USD-M Futures streams (depth on `/public` and mark price on `/market`) concurrently.
    pub async fn run_futures_stream(
        &self,
        event_tx: Sender<MarketEvent>,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let (tx1, rx1) = (event_tx.clone(), shutdown_rx.clone());
        let (tx2, rx2) = (event_tx, shutdown_rx);

        tokio::join!(
            self.run_futures_depth_stream(tx1, rx1),
            self.run_futures_mark_stream(tx2, rx2),
        );
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
