use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Unique identifier for supported market data venues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VenueId {
    Binance,
    Bybit,
}

impl std::fmt::Display for VenueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Binance => write!(f, "binance"),
            Self::Bybit => write!(f, "bybit"),
        }
    }
}

/// Category of market instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    Spot,
    LinearPerpetual,
}

impl std::fmt::Display for MarketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spot => write!(f, "spot"),
            Self::LinearPerpetual => write!(f, "linear_perpetual"),
        }
    }
}

/// Order book quote side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

/// Discrete price level in a limit order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

impl PriceLevel {
    pub fn new(price: Decimal, quantity: Decimal) -> Self {
        Self { price, quantity }
    }
}

/// Normalized canonical market event emitted by venue ingestion adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum MarketEvent {
    /// Full snapshot of top N price levels.
    OrderBookSnapshot {
        venue: VenueId,
        market_type: MarketType,
        symbol: String,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        exchange_ts_ms: i64,
        local_recv_ts_ns: i64,
        sequence_id: u64,
    },
    /// Incremental level updates. Quantity == 0 denotes level deletion.
    OrderBookDelta {
        venue: VenueId,
        market_type: MarketType,
        symbol: String,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        exchange_ts_ms: i64,
        local_recv_ts_ns: i64,
        sequence_id: u64,
    },
    /// Perpetual funding rate update.
    FundingRateUpdate {
        venue: VenueId,
        symbol: String,
        rate: Decimal,
        next_funding_ts_ms: i64,
    },
    /// Connection state lifecycle event.
    ConnectionState {
        venue: VenueId,
        is_connected: bool,
        details: String,
    },
}
