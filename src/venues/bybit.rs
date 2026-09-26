use super::VenueConnector;
use crate::types::VenueId;

/// Placeholder connector for Bybit public market data.
/// Concrete WebSocket client implementation is scheduled for Milestone M2.
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
