pub mod binance;
pub mod bybit;

use crate::types::VenueId;

/// Marker trait defining the lifecycle contract for venue ingestion connectors.
/// Concrete WebSocket network ingestion is implemented in Milestones M1 (Binance) and M2 (Bybit).
pub trait VenueConnector {
    /// Return the unique venue identifier.
    fn venue_id(&self) -> VenueId;

    /// Return whether this connector is currently configured as enabled.
    fn is_enabled(&self) -> bool;
}
