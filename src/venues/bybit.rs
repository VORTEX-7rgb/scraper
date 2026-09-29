use crate::error::{EngineError, Result};
use crate::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use crate::venues::VenueConnector;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, error, info, warn};

// --- DEFAULT ENDPOINTS ---

pub const BYBIT_SPOT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";
pub const BYBIT_LINEAR_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";

// --- CONFIGURATION ---

/// Configuration for Bybit V5 public WebSocket feed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BybitFeedConfig {
    pub venue: VenueId,
    pub market_type: MarketType,
    pub symbol: String,
    pub ws_url: String,
    pub depth: u32,
    pub heartbeat_interval: Duration,
    pub reconnect_backoff: Duration,
    pub max_reconnect_backoff: Duration,
}

impl BybitFeedConfig {
    pub fn default_spot(symbol: impl Into<String>) -> Self {
        Self {
            venue: VenueId::Bybit,
            market_type: MarketType::Spot,
            symbol: symbol.into().to_uppercase(),
            ws_url: BYBIT_SPOT_WS_URL.to_string(),
            depth: 50,
            heartbeat_interval: Duration::from_secs(20),
            reconnect_backoff: Duration::from_millis(500),
            max_reconnect_backoff: Duration::from_secs(10),
        }
    }

    pub fn default_linear(symbol: impl Into<String>) -> Self {
        Self {
            venue: VenueId::Bybit,
            market_type: MarketType::LinearPerpetual,
            symbol: symbol.into().to_uppercase(),
            ws_url: BYBIT_LINEAR_WS_URL.to_string(),
            depth: 50,
            heartbeat_interval: Duration::from_secs(20),
            reconnect_backoff: Duration::from_millis(500),
            max_reconnect_backoff: Duration::from_secs(10),
        }
    }

    pub fn orderbook_topic(&self) -> String {
        format!("orderbook.{}.{}", self.depth, self.symbol)
    }
}

// --- CONNECTOR WRAPPER ---

/// Connector representing Bybit configuration.
#[derive(Debug, Clone)]
pub struct BybitConnector {
    pub enabled: bool,
    pub symbols: Vec<String>,
}

impl BybitConnector {
    pub fn new(enabled: bool, symbols: Vec<String>) -> Self {
        Self { enabled, symbols }
    }
}

impl VenueConnector for BybitConnector {
    fn venue_id(&self) -> VenueId {
        VenueId::Bybit
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

// --- REQUESTS & RAW WIRE MESSAGES ---

/// Outgoing request envelope sent to Bybit V5 WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BybitRequest {
    pub op: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub args: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub req_id: Option<String>,
}

impl BybitRequest {
    pub fn subscribe(topics: Vec<String>, req_id: Option<String>) -> Self {
        Self {
            op: "subscribe".to_string(),
            args: topics,
            req_id,
        }
    }

    pub fn ping() -> Self {
        Self {
            op: "ping".to_string(),
            args: Vec::new(),
            req_id: None,
        }
    }
}

/// Generic response envelope received from Bybit V5 WebSocket.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BybitResponseEnvelope {
    pub op: Option<String>,
    pub args: Option<Vec<String>>,
    pub success: Option<bool>,
    pub ret_msg: Option<String>,
    pub conn_id: Option<String>,
    pub req_id: Option<String>,
    pub topic: Option<String>,
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub ts: Option<i64>,
    pub cts: Option<i64>,
    pub data: Option<serde_json::Value>,
}

/// Raw order book payload structure inside Bybit `data`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BybitRawOrderBookData {
    pub s: String,
    pub b: Vec<[String; 2]>,
    pub a: Vec<[String; 2]>,
    pub u: u64,
    pub seq: Option<u64>,
}

// --- PARSED MESSAGE CLASSIFICATION ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BybitParsedMessage {
    SubscriptionAck {
        success: bool,
        ret_msg: String,
        req_id: Option<String>,
        conn_id: Option<String>,
    },
    Pong {
        args: Vec<String>,
        conn_id: Option<String>,
    },
    OrderBookSnapshot {
        symbol: String,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        sequence_id: u64,
        seq: Option<u64>,
        exchange_ts_ms: i64,
    },
    OrderBookDelta {
        symbol: String,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        sequence_id: u64,
        seq: Option<u64>,
        transaction_ts_ms: i64,
        exchange_ts_ms: i64,
    },
    Unknown(String),
}

// --- PARSING HELPERS ---

/// Parse raw string level arrays `[["65000.50", "1.500"], ...]` into canonical `PriceLevel`s.
pub fn parse_price_levels(raw_levels: &[[String; 2]]) -> Result<Vec<PriceLevel>> {
    let mut levels = Vec::with_capacity(raw_levels.len());
    for item in raw_levels {
        let price = Decimal::from_str(&item[0])
            .map_err(|e| EngineError::Validation(format!("Invalid price '{}': {}", item[0], e)))?;
        let quantity = Decimal::from_str(&item[1]).map_err(|e| {
            EngineError::Validation(format!("Invalid quantity '{}': {}", item[1], e))
        })?;
        levels.push(PriceLevel::new(price, quantity));
    }
    Ok(levels)
}

/// Parse a raw JSON text string into a classified `BybitParsedMessage`.
pub fn parse_bybit_message(raw_text: &str) -> Result<BybitParsedMessage> {
    let envelope: BybitResponseEnvelope =
        serde_json::from_str(raw_text).map_err(EngineError::Json)?;

    // 1. Operation responses (subscribe, ping/pong)
    if let Some(op) = envelope.op.as_deref() {
        match op {
            "subscribe" => {
                let success = envelope.success.unwrap_or(false);
                let ret_msg = envelope.ret_msg.unwrap_or_default();
                return Ok(BybitParsedMessage::SubscriptionAck {
                    success,
                    ret_msg,
                    req_id: envelope.req_id,
                    conn_id: envelope.conn_id,
                });
            }
            "pong" => {
                let args = envelope.args.unwrap_or_default();
                return Ok(BybitParsedMessage::Pong {
                    args,
                    conn_id: envelope.conn_id,
                });
            }
            "ping" if envelope.ret_msg.as_deref() == Some("pong") => {
                let args = envelope.args.unwrap_or_default();
                return Ok(BybitParsedMessage::Pong {
                    args,
                    conn_id: envelope.conn_id,
                });
            }
            _ => {}
        }
    }

    // 2. Stream message topics (orderbook.*)
    if envelope
        .topic
        .as_deref()
        .is_some_and(|t| t.starts_with("orderbook."))
    {
        let msg_type = envelope.type_.as_deref().unwrap_or_default();
        let exchange_ts = envelope.ts.unwrap_or(0);
        let transaction_ts = envelope.cts.unwrap_or(exchange_ts);

        let Some(data_val) = envelope.data else {
            return Err(EngineError::Validation(
                "Missing 'data' in Bybit orderbook message".into(),
            ));
        };

        let ob_data: BybitRawOrderBookData = serde_json::from_value(data_val)
            .map_err(|e| EngineError::Validation(format!("Invalid orderbook data: {e}")))?;

        let bids = parse_price_levels(&ob_data.b)?;
        let asks = parse_price_levels(&ob_data.a)?;

        match msg_type {
            "snapshot" => {
                return Ok(BybitParsedMessage::OrderBookSnapshot {
                    symbol: ob_data.s,
                    bids,
                    asks,
                    sequence_id: ob_data.u,
                    seq: ob_data.seq,
                    exchange_ts_ms: exchange_ts,
                });
            }
            "delta" => {
                return Ok(BybitParsedMessage::OrderBookDelta {
                    symbol: ob_data.s,
                    bids,
                    asks,
                    sequence_id: ob_data.u,
                    seq: ob_data.seq,
                    transaction_ts_ms: transaction_ts,
                    exchange_ts_ms: exchange_ts,
                });
            }
            other => {
                return Ok(BybitParsedMessage::Unknown(format!(
                    "Unknown orderbook message type: {other}"
                )));
            }
        }
    }

    Ok(BybitParsedMessage::Unknown(raw_text.to_string()))
}

/// Convert a classified `BybitParsedMessage` into an optional canonical `MarketEvent`.
pub fn into_canonical_event(
    parsed: BybitParsedMessage,
    market_type: MarketType,
    local_recv_ts_ns: i64,
) -> Option<MarketEvent> {
    match parsed {
        BybitParsedMessage::OrderBookSnapshot {
            symbol,
            bids,
            asks,
            sequence_id,
            exchange_ts_ms,
            ..
        } => Some(MarketEvent::OrderBookSnapshot {
            venue: VenueId::Bybit,
            market_type,
            symbol,
            bids,
            asks,
            exchange_ts_ms,
            local_recv_ts_ns,
            sequence_id,
        }),
        BybitParsedMessage::OrderBookDelta {
            symbol,
            bids,
            asks,
            sequence_id,
            transaction_ts_ms,
            exchange_ts_ms,
            ..
        } => Some(MarketEvent::OrderBookDelta {
            venue: VenueId::Bybit,
            market_type,
            symbol,
            bids,
            asks,
            first_sequence_id: sequence_id,
            sequence_id,
            prev_sequence_id: None,
            transaction_ts_ms,
            exchange_ts_ms,
            local_recv_ts_ns,
        }),
        _ => None,
    }
}

// --- OPERATIONAL METRICS ---

#[derive(Debug, Default)]
pub struct BybitMetrics {
    pub connection_attempts: AtomicU64,
    pub successful_connections: AtomicU64,
    pub disconnects: AtomicU64,
    pub reconnects: AtomicU64,

    pub messages_received: AtomicU64,
    pub snapshots_received: AtomicU64,
    pub deltas_received: AtomicU64,

    pub subscription_messages: AtomicU64,
    pub subscription_errors: AtomicU64,
    pub heartbeat_events: AtomicU64,

    pub parse_errors: AtomicU64,
    pub unknown_messages: AtomicU64,
    pub sequence_errors: AtomicU64,

    pub last_sequence_id: AtomicU64,
    pub last_exchange_ts_ms: AtomicI64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitMetricsSnapshot {
    pub connection_attempts: u64,
    pub successful_connections: u64,
    pub disconnects: u64,
    pub reconnects: u64,
    pub messages_received: u64,
    pub snapshots_received: u64,
    pub deltas_received: u64,
    pub subscription_messages: u64,
    pub subscription_errors: u64,
    pub heartbeat_events: u64,
    pub parse_errors: u64,
    pub unknown_messages: u64,
    pub sequence_errors: u64,
    pub last_sequence_id: u64,
    pub last_exchange_ts_ms: i64,
}

impl BybitMetrics {
    pub fn snapshot(&self) -> BybitMetricsSnapshot {
        BybitMetricsSnapshot {
            connection_attempts: self.connection_attempts.load(Ordering::Relaxed),
            successful_connections: self.successful_connections.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            snapshots_received: self.snapshots_received.load(Ordering::Relaxed),
            deltas_received: self.deltas_received.load(Ordering::Relaxed),
            subscription_messages: self.subscription_messages.load(Ordering::Relaxed),
            subscription_errors: self.subscription_errors.load(Ordering::Relaxed),
            heartbeat_events: self.heartbeat_events.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            unknown_messages: self.unknown_messages.load(Ordering::Relaxed),
            sequence_errors: self.sequence_errors.load(Ordering::Relaxed),
            last_sequence_id: self.last_sequence_id.load(Ordering::Relaxed),
            last_exchange_ts_ms: self.last_exchange_ts_ms.load(Ordering::Relaxed),
        }
    }
}

// --- WEBSOCKET CLIENT & TRANSPORT FEED ---

/// High-performance public WebSocket transport for Bybit V5 market data.
pub struct BybitWebSocketFeed {
    config: BybitFeedConfig,
    metrics: Arc<BybitMetrics>,
    event_tx: Sender<MarketEvent>,
}

impl BybitWebSocketFeed {
    pub fn new(config: BybitFeedConfig, event_tx: Sender<MarketEvent>) -> Self {
        Self {
            config,
            metrics: Arc::new(BybitMetrics::default()),
            event_tx,
        }
    }

    pub fn with_metrics(
        config: BybitFeedConfig,
        metrics: Arc<BybitMetrics>,
        event_tx: Sender<MarketEvent>,
    ) -> Self {
        Self {
            config,
            metrics,
            event_tx,
        }
    }

    pub fn metrics(&self) -> Arc<BybitMetrics> {
        Arc::clone(&self.metrics)
    }

    pub fn config(&self) -> &BybitFeedConfig {
        &self.config
    }

    /// Run the persistent WebSocket connection loop with automatic reconnect and ping/pong.
    pub async fn run_stream(&self, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
        let mut backoff = self.config.reconnect_backoff;

        while !*shutdown_rx.borrow() {
            self.metrics
                .connection_attempts
                .fetch_add(1, Ordering::Relaxed);
            info!(
                venue = "bybit",
                market = %self.config.market_type,
                symbol = %self.config.symbol,
                url = %self.config.ws_url,
                "Connecting to Bybit public WebSocket"
            );

            let _ = self
                .event_tx
                .send(MarketEvent::ConnectionState {
                    venue: VenueId::Bybit,
                    market_type: Some(self.config.market_type),
                    is_connected: false,
                    details: "Connecting".into(),
                })
                .await;

            match connect_async(&self.config.ws_url).await {
                Ok((ws_stream, _)) => {
                    self.metrics
                        .successful_connections
                        .fetch_add(1, Ordering::Relaxed);
                    backoff = self.config.reconnect_backoff; // Reset backoff on success
                    info!(
                        venue = "bybit",
                        market = %self.config.market_type,
                        symbol = %self.config.symbol,
                        "Connected to Bybit public WebSocket"
                    );

                    let _ = self
                        .event_tx
                        .send(MarketEvent::ConnectionState {
                            venue: VenueId::Bybit,
                            market_type: Some(self.config.market_type),
                            is_connected: true,
                            details: "Connected".into(),
                        })
                        .await;

                    if let Err(err) = self
                        .handle_connected_stream(ws_stream, &mut shutdown_rx)
                        .await
                    {
                        warn!(
                            venue = "bybit",
                            market = %self.config.market_type,
                            symbol = %self.config.symbol,
                            error = %err,
                            "Bybit stream closed or failed"
                        );
                    }
                }
                Err(err) => {
                    self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                    error!(
                        venue = "bybit",
                        market = %self.config.market_type,
                        symbol = %self.config.symbol,
                        error = %err,
                        "Failed to connect to Bybit WebSocket"
                    );
                }
            }

            if *shutdown_rx.borrow() {
                break;
            }

            self.metrics.reconnects.fetch_add(1, Ordering::Relaxed);
            let _ = self
                .event_tx
                .send(MarketEvent::ConnectionState {
                    venue: VenueId::Bybit,
                    market_type: Some(self.config.market_type),
                    is_connected: false,
                    details: format!("Reconnecting in {:?}", backoff),
                })
                .await;

            tokio::select! {
                _ = tokio::time::sleep(backoff) => {},
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }

            backoff = (backoff * 2).min(self.config.max_reconnect_backoff);
        }

        info!(
            venue = "bybit",
            market = %self.config.market_type,
            symbol = %self.config.symbol,
            "Bybit feed loop shut down gracefully"
        );
        Ok(())
    }

    /// Manage subscription, incoming messages, and ping/pong keep-alive for an active stream.
    async fn handle_connected_stream<S>(
        &self,
        mut ws_stream: S,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Result<()>
    where
        S: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error>
            + Unpin,
    {
        // 1. Subscribe to orderbook topic
        let sub_topic = self.config.orderbook_topic();
        let sub_req = BybitRequest::subscribe(vec![sub_topic.clone()], Some("sub_1".into()));
        let sub_json = serde_json::to_string(&sub_req).map_err(EngineError::Json)?;

        debug!(topic = %sub_topic, "Sending Bybit subscription request");
        ws_stream
            .send(Message::Text(sub_json))
            .await
            .map_err(|e| EngineError::WebSocket(e.to_string()))?;

        // 2. Setup keep-alive ping interval
        let mut ping_interval = tokio::time::interval(self.config.heartbeat_interval);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        let _ = ws_stream.close().await;
                        return Ok(());
                    }
                }
                _ = ping_interval.tick() => {
                    let ping_req = BybitRequest::ping();
                    let ping_json = serde_json::to_string(&ping_req).map_err(EngineError::Json)?;
                    if let Err(e) = ws_stream.send(Message::Text(ping_json)).await {
                        self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                        return Err(EngineError::WebSocket(format!("Ping failed: {e}")));
                    }
                }
                msg_opt = ws_stream.next() => {
                    let Some(msg_res) = msg_opt else {
                        self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                        return Err(EngineError::WebSocket("WebSocket stream EOF".into()));
                    };

                    let msg = msg_res.map_err(|e| {
                        self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                        EngineError::WebSocket(e.to_string())
                    })?;

                    let local_recv_ns = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0);

                    match msg {
                        Message::Text(raw_text) => {
                            self.metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                            match parse_bybit_message(&raw_text) {
                                Ok(parsed) => {
                                    match &parsed {
                                        BybitParsedMessage::SubscriptionAck { success, ret_msg, .. } => {
                                            self.metrics.subscription_messages.fetch_add(1, Ordering::Relaxed);
                                            if *success {
                                                info!(topic = %sub_topic, "Bybit subscription acknowledged successfully");
                                            } else {
                                                self.metrics.subscription_errors.fetch_add(1, Ordering::Relaxed);
                                                error!(topic = %sub_topic, reason = %ret_msg, "Bybit subscription rejected");
                                            }
                                        }
                                        BybitParsedMessage::Pong { .. } => {
                                            self.metrics.heartbeat_events.fetch_add(1, Ordering::Relaxed);
                                        }
                                        BybitParsedMessage::OrderBookSnapshot { sequence_id, exchange_ts_ms, .. } => {
                                            self.metrics.snapshots_received.fetch_add(1, Ordering::Relaxed);
                                            self.metrics.last_sequence_id.store(*sequence_id, Ordering::Relaxed);
                                            self.metrics.last_exchange_ts_ms.store(*exchange_ts_ms, Ordering::Relaxed);

                                            if let Some(event) = into_canonical_event(parsed, self.config.market_type, local_recv_ns) {
                                                let _ = self.event_tx.send(event).await;
                                            }
                                        }
                                        BybitParsedMessage::OrderBookDelta { sequence_id, exchange_ts_ms, .. } => {
                                            self.metrics.deltas_received.fetch_add(1, Ordering::Relaxed);
                                            self.metrics.last_sequence_id.store(*sequence_id, Ordering::Relaxed);
                                            self.metrics.last_exchange_ts_ms.store(*exchange_ts_ms, Ordering::Relaxed);

                                            if let Some(event) = into_canonical_event(parsed, self.config.market_type, local_recv_ns) {
                                                let _ = self.event_tx.send(event).await;
                                            }
                                        }
                                        BybitParsedMessage::Unknown(raw) => {
                                            self.metrics.unknown_messages.fetch_add(1, Ordering::Relaxed);
                                            info!(raw = %raw, "Received unhandled Bybit message");
                                        }
                                    }
                                }
                                Err(err) => {
                                    self.metrics.parse_errors.fetch_add(1, Ordering::Relaxed);
                                    warn!(error = %err, raw = %raw_text, "Failed to parse Bybit message");
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                return Err(EngineError::WebSocket(format!("Protocol pong failed: {e}")));
                            }
                        }
                        Message::Close(_) => {
                            self.metrics.disconnects.fetch_add(1, Ordering::Relaxed);
                            return Err(EngineError::WebSocket("Bybit server closed WebSocket connection".into()));
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
