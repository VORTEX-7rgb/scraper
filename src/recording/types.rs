use crate::error::{EngineError, Result};
use crate::observatory::types::{
    DislocationObservation, OpportunityKey, OpportunityRecord, PersistenceTransition,
};
use crate::types::MarketEvent;
use serde::{Deserialize, Serialize};

/// Current canonical research schema version.
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Discriminant tag indicating the specific type of event stored in the envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchEventType {
    /// Ingested raw market event (snapshot, delta, funding rate, connection state).
    MarketEvent,
    /// Derived cross-book executable dislocation observation.
    DislocationObservation,
    /// Temporal state machine transition (Started, Continued, Ended).
    PersistenceTransition,
    /// Finalized opportunity historical record.
    OpportunityRecord,
}

impl std::fmt::Display for ResearchEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MarketEvent => write!(f, "market_event"),
            Self::DislocationObservation => write!(f, "dislocation_observation"),
            Self::PersistenceTransition => write!(f, "persistence_transition"),
            Self::OpportunityRecord => write!(f, "opportunity_record"),
        }
    }
}

/// Payload carrying the typed event data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ResearchPayload {
    MarketEvent(MarketEvent),
    DislocationObservation(Box<DislocationObservation>),
    PersistenceTransition(PersistenceTransitionEvent),
    OpportunityRecord(OpportunityRecord),
}

/// Explicit event capturing an opportunity persistence transition with its associated key and timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceTransitionEvent {
    pub key: OpportunityKey,
    pub timestamp_ns: i64,
    pub transition: PersistenceTransition,
}

/// Self-describing canonical research event envelope.
///
/// Encapsulates schema versioning, event classification, capture timestamp, and typed payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchEvent {
    /// Schema version for format evolution and backward/forward compatibility.
    pub schema_version: u32,
    /// Explicit event classification tag for indexed filtering and stream routing.
    pub event_type: ResearchEventType,
    /// Canonical local timestamp in nanoseconds when this record was generated.
    pub timestamp_ns: i64,
    /// Event-specific typed payload.
    pub payload: ResearchPayload,
}

impl ResearchEvent {
    /// Create a new raw market event record.
    pub fn new_market_event(timestamp_ns: i64, event: MarketEvent) -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            event_type: ResearchEventType::MarketEvent,
            timestamp_ns,
            payload: ResearchPayload::MarketEvent(event),
        }
    }

    /// Create a new dislocation observation record.
    pub fn new_dislocation(observation: DislocationObservation) -> Self {
        let timestamp_ns = observation.observation_ts_ns;
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            event_type: ResearchEventType::DislocationObservation,
            timestamp_ns,
            payload: ResearchPayload::DislocationObservation(Box::new(observation)),
        }
    }

    /// Create a new persistence transition event record.
    pub fn new_transition(
        key: OpportunityKey,
        timestamp_ns: i64,
        transition: PersistenceTransition,
    ) -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            event_type: ResearchEventType::PersistenceTransition,
            timestamp_ns,
            payload: ResearchPayload::PersistenceTransition(PersistenceTransitionEvent {
                key,
                timestamp_ns,
                transition,
            }),
        }
    }

    /// Create a new finalized opportunity record.
    pub fn new_opportunity(record: OpportunityRecord) -> Self {
        let timestamp_ns = record.end_ts_ns;
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            event_type: ResearchEventType::OpportunityRecord,
            timestamp_ns,
            payload: ResearchPayload::OpportunityRecord(record),
        }
    }

    /// Perform structural invariant validation on the envelope and its payload.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != CURRENT_SCHEMA_VERSION {
            return Err(EngineError::UnsupportedSchemaVersion {
                found: self.schema_version,
                supported: CURRENT_SCHEMA_VERSION,
            });
        }
        if self.timestamp_ns <= 0 {
            return Err(EngineError::Validation(format!(
                "Invalid research event timestamp_ns: {}",
                self.timestamp_ns
            )));
        }

        let matches = matches!(
            (&self.event_type, &self.payload),
            (
                ResearchEventType::MarketEvent,
                ResearchPayload::MarketEvent(_)
            ) | (
                ResearchEventType::DislocationObservation,
                ResearchPayload::DislocationObservation(_),
            ) | (
                ResearchEventType::PersistenceTransition,
                ResearchPayload::PersistenceTransition(_),
            ) | (
                ResearchEventType::OpportunityRecord,
                ResearchPayload::OpportunityRecord(_),
            )
        );

        if !matches {
            return Err(EngineError::Validation(format!(
                "ResearchEvent event_type {:?} does not match payload variant",
                self.event_type
            )));
        }

        Ok(())
    }
}
