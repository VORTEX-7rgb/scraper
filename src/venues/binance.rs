use super::VenueConnector;
use crate::types::VenueId;

/// Placeholder connector for Binance public market data.
/// Concrete WebSocket client implementation is scheduled for Milestone M1.
#[derive(Debug, Clone)]
pub struct BinanceConnector {
    pub enabled: bool,
    pub symbols: Vec<String>,
}

impl BinanceConnector {
    pub fn new(enabled: bool, symbols: Vec<String>) -> Self {
        Self { enabled, symbols }
    }
}

impl VenueConnector for BinanceConnector {
    fn venue_id(&self) -> VenueId {
        VenueId::Binance
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }
}
