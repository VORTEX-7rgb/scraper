use crate::observatory::types::{
    ActiveOpportunity, DislocationObservation, ObservationRejectionReason, OpportunityEndReason,
    OpportunityKey, OpportunityRecord, PersistenceTransition,
};
use crate::types::{MarketType, VenueId};
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};

/// State machine tracking the temporal emergence, continuation, and termination of executable dislocations.
#[derive(Debug, Clone)]
pub struct OpportunityTracker {
    min_net_edge_bps: Decimal,
    active_opportunities: HashMap<OpportunityKey, ActiveOpportunity>,
    completed_history: VecDeque<OpportunityRecord>,
    max_history_capacity: usize,
}

impl OpportunityTracker {
    /// Create a new opportunity tracker with a minimum net edge in basis points.
    pub fn new(min_net_edge_bps: Decimal) -> Self {
        Self::with_capacity(min_net_edge_bps, 10_000)
    }

    /// Create a tracker with explicit history buffer capacity.
    pub fn with_capacity(min_net_edge_bps: Decimal, max_history_capacity: usize) -> Self {
        Self {
            min_net_edge_bps,
            active_opportunities: HashMap::new(),
            completed_history: VecDeque::new(),
            max_history_capacity,
        }
    }

    /// Configured minimum net edge threshold in basis points.
    pub fn min_net_edge_bps(&self) -> Decimal {
        self.min_net_edge_bps
    }

    /// Number of currently active, continuous opportunities.
    pub fn active_count(&self) -> usize {
        self.active_opportunities.len()
    }

    /// Retrieve an in-flight active opportunity if currently present.
    pub fn active_opportunity(&self, key: &OpportunityKey) -> Option<&ActiveOpportunity> {
        self.active_opportunities.get(key)
    }

    /// Process a dislocation observation through the persistence state machine.
    pub fn process_observation(
        &mut self,
        observation: &DislocationObservation,
    ) -> PersistenceTransition {
        let key = OpportunityKey::new(
            observation.buy_venue,
            observation.buy_market,
            observation.sell_venue,
            observation.sell_market,
            &observation.symbol,
            observation.reference_quantity,
            observation.market_relationship,
        );

        let meets_criteria = observation.is_valid
            && observation.both_books_trusted
            && observation.both_books_fresh
            && observation.fully_executable
            && observation
                .net_edge_bps
                .is_some_and(|bps| bps >= self.min_net_edge_bps);

        if let Some(active) = self.active_opportunities.get_mut(&key) {
            // State: ACTIVE
            if meets_criteria {
                // ACTIVE -> ACTIVE (Continuation)
                let bps = observation.net_edge_bps.unwrap_or(Decimal::ZERO);
                active.sample_count += 1;
                active.last_observed_ts_ns = observation.observation_ts_ns;
                active.last_observed_edge_bps = bps;
                active.peak_net_edge_bps = active.peak_net_edge_bps.max(bps);
                active.min_net_edge_bps = active.min_net_edge_bps.min(bps);
                active.sum_net_edge_bps += bps;
                active.max_common_executable_quantity = active
                    .max_common_executable_quantity
                    .max(observation.common_executable_quantity);

                PersistenceTransition::Continued
            } else {
                // ACTIVE -> ENDED (Termination)
                let active = self
                    .active_opportunities
                    .remove(&key)
                    .expect("Active opportunity must exist");

                let termination_reason = self.classify_termination_reason(observation);
                let end_ts_ns = observation.observation_ts_ns;
                let duration_ms = ((end_ts_ns - active.start_ts_ns).max(0) / 1_000_000) as u64;

                let average_net_edge_bps = if active.sample_count == 0 {
                    Decimal::ZERO
                } else {
                    active.sum_net_edge_bps / Decimal::from(active.sample_count)
                };

                let record = OpportunityRecord {
                    key: active.key,
                    start_ts_ns: active.start_ts_ns,
                    end_ts_ns,
                    duration_ms,
                    sample_count: active.sample_count,
                    first_observed_edge_bps: active.first_observed_edge_bps,
                    last_observed_edge_bps: active.last_observed_edge_bps,
                    peak_net_edge_bps: active.peak_net_edge_bps,
                    min_net_edge_bps: active.min_net_edge_bps,
                    average_net_edge_bps,
                    max_common_executable_quantity: active.max_common_executable_quantity,
                    termination_reason,
                };

                self.push_completed(record.clone());
                PersistenceTransition::Ended(Box::new(record))
            }
        } else {
            // State: INACTIVE
            if meets_criteria {
                // INACTIVE -> ACTIVE (Activation)
                let bps = observation.net_edge_bps.unwrap_or(Decimal::ZERO);
                let active = ActiveOpportunity {
                    key: key.clone(),
                    start_ts_ns: observation.observation_ts_ns,
                    last_observed_ts_ns: observation.observation_ts_ns,
                    sample_count: 1,
                    first_observed_edge_bps: bps,
                    last_observed_edge_bps: bps,
                    peak_net_edge_bps: bps,
                    min_net_edge_bps: bps,
                    sum_net_edge_bps: bps,
                    max_common_executable_quantity: observation.common_executable_quantity,
                };
                self.active_opportunities.insert(key, active);
                PersistenceTransition::Started
            } else {
                PersistenceTransition::None
            }
        }
    }

    /// Classify why an active opportunity was terminated given an invalid or substandard observation.
    fn classify_termination_reason(
        &self,
        observation: &DislocationObservation,
    ) -> OpportunityEndReason {
        if !observation.both_books_trusted {
            return OpportunityEndReason::UntrustedBook;
        }

        if let Some(ref rejection) = observation.rejection_reason {
            match rejection {
                ObservationRejectionReason::UntrustedBook { .. } => {
                    OpportunityEndReason::UntrustedBook
                }
                ObservationRejectionReason::StaleBook { .. } => OpportunityEndReason::StaleBook,
                ObservationRejectionReason::TimestampSkewExceeded { .. } => {
                    OpportunityEndReason::TimestampSkewExceeded
                }
                ObservationRejectionReason::CrossedBook { .. }
                | ObservationRejectionReason::EmptyBook { .. } => OpportunityEndReason::InvalidBook,
                ObservationRejectionReason::MissingBook { .. } => OpportunityEndReason::MissingBook,
                ObservationRejectionReason::SelfComparison
                | ObservationRejectionReason::ExecutionEngineError(_) => {
                    OpportunityEndReason::InvalidBook
                }
            }
        } else if !observation.both_books_fresh {
            OpportunityEndReason::StaleBook
        } else if !observation.is_valid {
            OpportunityEndReason::InvalidBook
        } else if !observation.fully_executable {
            OpportunityEndReason::InsufficientLiquidity
        } else if observation.net_edge_bps.is_none()
            || observation.net_edge_bps.unwrap() < self.min_net_edge_bps
        {
            OpportunityEndReason::NetEdgeBelowThreshold
        } else {
            OpportunityEndReason::InvalidBook
        }
    }

    /// Terminate any active opportunities affected by a disconnected feed.
    pub fn handle_feed_disconnect(
        &mut self,
        venue: VenueId,
        market_type: Option<MarketType>,
        disconnect_ts_ns: i64,
    ) -> Vec<OpportunityRecord> {
        let mut terminated = Vec::new();
        let keys_to_terminate: Vec<OpportunityKey> = self
            .active_opportunities
            .keys()
            .filter(|k| {
                let matches_buy = k.buy_venue == venue
                    && (market_type.is_none() || Some(k.buy_market) == market_type);
                let matches_sell = k.sell_venue == venue
                    && (market_type.is_none() || Some(k.sell_market) == market_type);
                matches_buy || matches_sell
            })
            .cloned()
            .collect();

        for key in keys_to_terminate {
            if let Some(active) = self.active_opportunities.remove(&key) {
                let duration_ms =
                    ((disconnect_ts_ns - active.start_ts_ns).max(0) / 1_000_000) as u64;
                let average_net_edge_bps = if active.sample_count == 0 {
                    Decimal::ZERO
                } else {
                    active.sum_net_edge_bps / Decimal::from(active.sample_count)
                };

                let record = OpportunityRecord {
                    key: active.key,
                    start_ts_ns: active.start_ts_ns,
                    end_ts_ns: disconnect_ts_ns,
                    duration_ms,
                    sample_count: active.sample_count,
                    first_observed_edge_bps: active.first_observed_edge_bps,
                    last_observed_edge_bps: active.last_observed_edge_bps,
                    peak_net_edge_bps: active.peak_net_edge_bps,
                    min_net_edge_bps: active.min_net_edge_bps,
                    average_net_edge_bps,
                    max_common_executable_quantity: active.max_common_executable_quantity,
                    termination_reason: OpportunityEndReason::FeedDisconnected,
                };

                self.push_completed(record.clone());
                terminated.push(record);
            }
        }

        terminated
    }

    /// Check for active opportunities that have ceased receiving updates beyond a timeout.
    pub fn check_staleness(&mut self, now_ns: i64, max_age_ms: u64) -> Vec<OpportunityRecord> {
        let max_age_ns = (max_age_ms as i64) * 1_000_000;
        let mut terminated = Vec::new();

        let stale_keys: Vec<OpportunityKey> = self
            .active_opportunities
            .iter()
            .filter(|(_, active)| now_ns - active.last_observed_ts_ns > max_age_ns)
            .map(|(key, _)| key.clone())
            .collect();

        for key in stale_keys {
            if let Some(active) = self.active_opportunities.remove(&key) {
                let duration_ms =
                    ((active.last_observed_ts_ns - active.start_ts_ns).max(0) / 1_000_000) as u64;
                let average_net_edge_bps = if active.sample_count == 0 {
                    Decimal::ZERO
                } else {
                    active.sum_net_edge_bps / Decimal::from(active.sample_count)
                };

                let record = OpportunityRecord {
                    key: active.key,
                    start_ts_ns: active.start_ts_ns,
                    end_ts_ns: active.last_observed_ts_ns,
                    duration_ms,
                    sample_count: active.sample_count,
                    first_observed_edge_bps: active.first_observed_edge_bps,
                    last_observed_edge_bps: active.last_observed_edge_bps,
                    peak_net_edge_bps: active.peak_net_edge_bps,
                    min_net_edge_bps: active.min_net_edge_bps,
                    average_net_edge_bps,
                    max_common_executable_quantity: active.max_common_executable_quantity,
                    termination_reason: OpportunityEndReason::StaleBook,
                };

                self.push_completed(record.clone());
                terminated.push(record);
            }
        }

        terminated
    }

    /// Push a completed record into the internal ring buffer.
    fn push_completed(&mut self, record: OpportunityRecord) {
        if self.completed_history.len() >= self.max_history_capacity {
            self.completed_history.pop_front();
        }
        self.completed_history.push_back(record);
    }

    /// Reference to completed opportunity records.
    pub fn completed_records(&self) -> &VecDeque<OpportunityRecord> {
        &self.completed_history
    }

    /// Drain all completed opportunity records from the buffer.
    pub fn drain_completed_records(&mut self) -> Vec<OpportunityRecord> {
        self.completed_history.drain(..).collect()
    }
}
