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

    #[error("Data quality error: {0}")]
    DataQuality(String),

    #[error("Transport/Network error: {0}")]
    Transport(String),

    #[error("WebSocket error: {0}")]
    WebSocket(String),
}

pub type Result<T> = std::result::Result<T, EngineError>;
