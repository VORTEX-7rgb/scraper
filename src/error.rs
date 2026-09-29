use thiserror::Error;

/// Core error taxonomy for Airbitrage.
/// Categorizes errors cleanly without dynamic stringly-typed propagation.
#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML deserialization error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("JSON deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Order book invariant violation: {0}")]
    OrderBookInvariant(String),

    #[error("Crossed order book detected: best bid {bid} >= best ask {ask}")]
    CrossedBook {
        bid: rust_decimal::Decimal,
        ask: rust_decimal::Decimal,
    },

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Invalid execution quantity: {0}. Quantity must be strictly positive.")]
    InvalidQuantity(rust_decimal::Decimal),

    #[error("Data quality error: {0}")]
    DataQuality(String),

    #[error(
        "Sequence gap detected: expected previous {expected_prev}, received previous {received_prev:?}, first update {first_seq}, final update {final_seq}"
    )]
    SequenceGap {
        expected_prev: u64,
        received_prev: Option<u64>,
        first_seq: u64,
        final_seq: u64,
    },

    #[error("Out of order update: last accepted sequence {last_seq}, received {received_seq}")]
    OutOfOrderUpdate { last_seq: u64, received_seq: u64 },

    #[error("Invalid market state transition from {from} to {to}: {reason}")]
    InvalidStateTransition {
        from: String,
        to: String,
        reason: String,
    },

    #[error("Invalid book state: {0}")]
    InvalidBookState(String),

    #[error("Snapshot synchronization failure: {0}")]
    SnapshotSyncFailure(String),

    #[error("Stale market state: age {age_ms}ms exceeds maximum {max_age_ms}ms")]
    StaleMarketState { age_ms: u64, max_age_ms: u64 },

    #[error("Transport/Network error: {0}")]
    Transport(String),

    #[error("WebSocket error: {0}")]
    WebSocket(String),
}

pub type Result<T> = std::result::Result<T, EngineError>;
