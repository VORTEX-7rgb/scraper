pub mod observer;
pub mod persistence;
pub mod types;

pub use observer::{ObservationConfig, ObservationEngine};
pub use persistence::OpportunityTracker;
pub use types::{
    ActiveOpportunity, DislocationObservation, MarketRelationship, ObservationRejectionReason,
    OpportunityEndReason, OpportunityKey, OpportunityRecord, PersistenceTransition,
};
