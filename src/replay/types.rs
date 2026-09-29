use crate::config::AppConfig;
use crate::execution::VenueFeeRegistry;
use crate::market::state::BookLifecycleState;
use crate::observatory::ObservationConfig;
use crate::observatory::types::{DislocationObservation, MarketRelationship};
use crate::recording::types::ResearchEventType;
use crate::types::{MarketType, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration governing historical deterministic replay execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayConfig {
    /// Maximum allowed staleness for order books before marking them untrusted.
    pub max_staleness: Duration,
    /// Cross-book dislocation observation settings (quantities, max book age, max skew).
    pub observation_config: ObservationConfig,
    /// Venue taker fee registry.
    pub fee_registry: VenueFeeRegistry,
    /// Minimum net edge threshold in basis points for persistence opportunity tracking.
    pub min_net_edge_bps: Decimal,
    /// If true, sequence breaks, invariant failures, or invalid deltas in market events fail loudly.
    pub fail_on_market_error: bool,
    /// If true, terminates replay immediately on the first observation or transition mismatch.
    pub stop_on_first_mismatch: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            max_staleness: Duration::from_millis(5_000),
            observation_config: ObservationConfig::default(),
            fee_registry: VenueFeeRegistry::default(),
            min_net_edge_bps: Decimal::ZERO,
            fail_on_market_error: true,
            stop_on_first_mismatch: false,
        }
    }
}

impl ReplayConfig {
    /// Create a replay configuration from loaded `AppConfig`.
    pub fn from_app_config(app_config: &AppConfig) -> Self {
        Self {
            max_staleness: Duration::from_millis(app_config.engine.max_book_age_ms),
            observation_config: ObservationConfig::from(&app_config.observatory),
            fee_registry: VenueFeeRegistry::default(),
            min_net_edge_bps: app_config.observatory.min_net_edge_bps,
            fail_on_market_error: app_config.replay.fail_on_market_error,
            stop_on_first_mismatch: app_config.replay.stop_on_first_mismatch,
        }
    }

    pub fn with_max_staleness(mut self, max_staleness: Duration) -> Self {
        self.max_staleness = max_staleness;
        self
    }

    pub fn with_observation_config(mut self, config: ObservationConfig) -> Self {
        self.observation_config = config;
        self
    }

    pub fn with_fee_registry(mut self, fee_registry: VenueFeeRegistry) -> Self {
        self.fee_registry = fee_registry;
        self
    }

    pub fn with_min_net_edge_bps(mut self, min_net_edge_bps: Decimal) -> Self {
        self.min_net_edge_bps = min_net_edge_bps;
        self
    }

    pub fn with_fail_on_market_error(mut self, fail_on_market_error: bool) -> Self {
        self.fail_on_market_error = fail_on_market_error;
        self
    }

    pub fn with_stop_on_first_mismatch(mut self, stop_on_first_mismatch: bool) -> Self {
        self.stop_on_first_mismatch = stop_on_first_mismatch;
        self
    }
}

/// Structured, highly diagnostic record of a discrepancy between recorded and replayed state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayMismatch {
    pub record_index: u64,
    pub event_timestamp_ns: i64,
    pub event_type: ResearchEventType,
    pub field_name: String,
    pub recorded_value: String,
    pub replayed_value: String,
    pub buy_venue: Option<VenueId>,
    pub buy_market: Option<MarketType>,
    pub sell_venue: Option<VenueId>,
    pub sell_market: Option<MarketType>,
    pub symbol: String,
    pub market_relationship: Option<MarketRelationship>,
    pub reference_quantity: Option<Decimal>,
}

impl std::fmt::Display for ReplayMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pair_str = match (
            self.buy_venue,
            self.buy_market,
            self.sell_venue,
            self.sell_market,
        ) {
            (Some(bv), Some(bm), Some(sv), Some(sm)) => {
                format!("{bv} {bm} -> {sv} {sm}")
            }
            _ => "unknown pair".to_string(),
        };

        let qty_str = self
            .reference_quantity
            .map(|q| format!(" (qty {q})"))
            .unwrap_or_default();

        write!(
            f,
            "Mismatch at record #{} (ts: {}ns) [{:?}]: field '{}' expected '{}', got '{}' for {} {}{}",
            self.record_index,
            self.event_timestamp_ns,
            self.event_type,
            self.field_name,
            self.recorded_value,
            self.replayed_value,
            pair_str,
            self.symbol,
            qty_str,
        )
    }
}

/// Outcome of processing a single research event during replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayStepOutcome {
    MarketEventApplied {
        mutated: bool,
    },
    ObservationCompared {
        replayed: Box<DislocationObservation>,
        mismatches: Vec<ReplayMismatch>,
    },
    TransitionCompared {
        mismatches: Vec<ReplayMismatch>,
    },
    OpportunityCompared {
        mismatches: Vec<ReplayMismatch>,
    },
}

/// Comprehensive summary of historical replay execution and comparison metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayResult {
    pub total_records_read: u64,
    pub market_events_consumed: u64,
    pub market_events_mutated: u64,
    pub derived_observations_found: u64,
    pub replayed_observations: u64,
    pub matched_observations: u64,
    pub derived_transitions_found: u64,
    pub matched_transitions: u64,
    pub derived_opportunities_found: u64,
    pub matched_opportunities: u64,
    pub mismatches: Vec<ReplayMismatch>,
    pub last_event_timestamp_ns: Option<i64>,
    pub final_books: HashMap<String, BookLifecycleState>,
}

impl ReplayResult {
    /// Return true if zero mismatches occurred and replay perfectly reproduced reference outputs.
    pub fn is_clean(&self) -> bool {
        self.mismatches.is_empty()
    }

    /// Return true if replay is deterministic and successful.
    pub fn is_deterministic(&self) -> bool {
        self.mismatches.is_empty()
    }
}

impl std::fmt::Display for ReplayResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "============================================================"
        )?;
        writeln!(
            f,
            "             HISTORICAL REPLAY REPORT                      "
        )?;
        writeln!(
            f,
            "============================================================"
        )?;
        writeln!(
            f,
            "Total Records Read:             {}",
            self.total_records_read
        )?;
        writeln!(
            f,
            "Market Events Consumed:         {}",
            self.market_events_consumed
        )?;
        writeln!(
            f,
            "Order Books Mutated:            {}",
            self.market_events_mutated
        )?;
        writeln!(
            f,
            "Derived Observations Found:     {}",
            self.derived_observations_found
        )?;
        writeln!(
            f,
            "Replayed Observations Computed: {}",
            self.replayed_observations
        )?;
        writeln!(
            f,
            "Matched Observations:           {}",
            self.matched_observations
        )?;
        writeln!(
            f,
            "Derived Transitions Found:      {}",
            self.derived_transitions_found
        )?;
        writeln!(
            f,
            "Matched Transitions:            {}",
            self.matched_transitions
        )?;
        writeln!(
            f,
            "Derived Opportunities Found:    {}",
            self.derived_opportunities_found
        )?;
        writeln!(
            f,
            "Matched Opportunities:          {}",
            self.matched_opportunities
        )?;
        writeln!(
            f,
            "Total Mismatches Detected:      {}",
            self.mismatches.len()
        )?;
        writeln!(
            f,
            "Replay Status:                  {}",
            if self.is_clean() {
                "VERIFIED DETERMINISTIC"
            } else {
                "MISMATCH DETECTED"
            }
        )?;
        writeln!(
            f,
            "------------------------------------------------------------"
        )?;
        writeln!(f, "Final Book Lifecycle States:")?;
        for (instrument, state) in &self.final_books {
            writeln!(f, "  - {instrument}: {state:?}")?;
        }
        if !self.mismatches.is_empty() {
            writeln!(
                f,
                "------------------------------------------------------------"
            )?;
            writeln!(f, "Mismatches Detail (first 10):")?;
            for (idx, mismatch) in self.mismatches.iter().take(10).enumerate() {
                writeln!(f, "  [{}] {}", idx + 1, mismatch)?;
            }
            if self.mismatches.len() > 10 {
                writeln!(
                    f,
                    "  ... and {} more mismatches",
                    self.mismatches.len() - 10
                )?;
            }
        }
        writeln!(
            f,
            "============================================================"
        )?;
        Ok(())
    }
}
