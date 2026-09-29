use crate::error::{EngineError, Result};
use crate::market::MarketStateManager;
use crate::market::state::SequencePolicy;
use crate::observatory::types::{
    DislocationObservation, OpportunityKey, OpportunityRecord, PersistenceTransition,
};
use crate::observatory::{ObservationEngine, OpportunityTracker};
use crate::recording::reader::ResearchReader;
use crate::recording::types::{
    CURRENT_SCHEMA_VERSION, ResearchEvent, ResearchEventType, ResearchPayload,
};
use crate::replay::comparator::{
    compare_observations, compare_opportunity_records, compare_transitions,
};
use crate::replay::types::{ReplayConfig, ReplayMismatch, ReplayResult, ReplayStepOutcome};
use crate::types::{MarketEvent, MarketType, VenueId};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::io::Read;

/// Deterministic Historical Replay Engine.
///
/// Reconstructs local limit order books step-by-step from recorded raw `MarketEvent`s,
/// drives the M4 pricing engine and M5 cross-book observatory over replayed state,
/// and verifies replayed observations against recorded reference outputs using exact Decimal math.
pub struct ReplayEngine {
    manager: MarketStateManager,
    observation_engine: ObservationEngine,
    opportunity_tracker: Option<OpportunityTracker>,
    config: ReplayConfig,
    last_transitions: HashMap<OpportunityKey, PersistenceTransition>,
    last_opportunity_records: HashMap<OpportunityKey, OpportunityRecord>,
    last_replayed_observation: Option<DislocationObservation>,
    current_record_index: u64,
    latest_market_ts_ns: Option<i64>,
}

impl ReplayEngine {
    /// Create a new replay engine with explicit configuration.
    pub fn new(config: ReplayConfig) -> Self {
        let manager = MarketStateManager::new(config.max_staleness);
        let observation_engine = ObservationEngine::new(
            config.observation_config.clone(),
            config.fee_registry.clone(),
        );
        let opportunity_tracker = Some(OpportunityTracker::new(config.min_net_edge_bps));

        Self {
            manager,
            observation_engine,
            opportunity_tracker,
            config,
            last_transitions: HashMap::new(),
            last_opportunity_records: HashMap::new(),
            last_replayed_observation: None,
            current_record_index: 0,
            latest_market_ts_ns: None,
        }
    }

    /// Create an engine with canonical default settings.
    pub fn with_defaults() -> Self {
        Self::new(ReplayConfig::default())
    }

    /// Access the underlying `MarketStateManager`.
    pub fn manager(&self) -> &MarketStateManager {
        &self.manager
    }

    /// Mutably access the underlying `MarketStateManager`.
    pub fn manager_mut(&mut self) -> &mut MarketStateManager {
        &mut self.manager
    }

    /// Access the underlying `ObservationEngine`.
    pub fn observation_engine(&self) -> &ObservationEngine {
        &self.observation_engine
    }

    /// Access the opportunity persistence tracker if present.
    pub fn tracker(&self) -> Option<&OpportunityTracker> {
        self.opportunity_tracker.as_ref()
    }

    /// Mutably access the opportunity persistence tracker if present.
    pub fn tracker_mut(&mut self) -> Option<&mut OpportunityTracker> {
        self.opportunity_tracker.as_mut()
    }

    /// Register an instrument with an explicit sequence policy.
    pub fn register_instrument_with_policy(
        &mut self,
        venue: VenueId,
        market_type: MarketType,
        symbol: impl Into<String>,
        policy: SequencePolicy,
    ) {
        self.manager
            .register_instrument_with_policy(venue, market_type, symbol, policy);
    }

    /// Observe a single pair's dislocation under the current replayed market state.
    #[allow(clippy::too_many_arguments)]
    pub fn observe_pair(
        &self,
        buy_venue: VenueId,
        buy_market: MarketType,
        sell_venue: VenueId,
        sell_market: MarketType,
        symbol: &str,
        quantity: Decimal,
        now_ns: i64,
    ) -> DislocationObservation {
        self.observation_engine.observe_pair(
            &self.manager,
            buy_venue,
            buy_market,
            sell_venue,
            sell_market,
            symbol,
            quantity,
            now_ns,
        )
    }

    /// Sweep across all configured reference quantities for a single pair under replayed state.
    pub fn observe_sweep(
        &self,
        buy_venue: VenueId,
        buy_market: MarketType,
        sell_venue: VenueId,
        sell_market: MarketType,
        symbol: &str,
        now_ns: i64,
    ) -> Vec<DislocationObservation> {
        self.observation_engine.observe_sweep(
            &self.manager,
            buy_venue,
            buy_market,
            sell_venue,
            sell_market,
            symbol,
            now_ns,
        )
    }

    /// Sweep across all endpoints and configured reference quantities under replayed state.
    pub fn observe_all_pairs_sweep(
        &self,
        symbol: &str,
        endpoints: &[(VenueId, MarketType)],
        now_ns: i64,
    ) -> Vec<DislocationObservation> {
        self.observation_engine
            .observe_all_pairs_sweep(&self.manager, symbol, endpoints, now_ns)
    }

    /// Process a single `ResearchEvent` through historical replay.
    ///
    /// - `MarketEvent` is applied directly to `MarketStateManager` as input.
    /// - `DislocationObservation` is recomputed from state and compared against recorded observation.
    /// - `PersistenceTransition` is compared against internal tracker state.
    /// - `OpportunityRecord` is compared against internal tracker history.
    pub fn replay_step(
        &mut self,
        record_index: u64,
        event: &ResearchEvent,
    ) -> Result<ReplayStepOutcome> {
        event.validate()?;
        if event.schema_version != CURRENT_SCHEMA_VERSION {
            return Err(EngineError::UnsupportedSchemaVersion {
                found: event.schema_version,
                supported: CURRENT_SCHEMA_VERSION,
            });
        }

        self.current_record_index = record_index;

        match &event.payload {
            ResearchPayload::MarketEvent(market_event) => {
                // Update latest market timestamp from internal event timestamps
                if let Some(ts) = extract_market_event_timestamp(market_event) {
                    self.latest_market_ts_ns = Some(ts);
                }

                // Apply to MarketStateManager (never bypasses verification)
                let mutation_result = self.manager.handle_event(market_event);
                match mutation_result {
                    Ok(mutated) => Ok(ReplayStepOutcome::MarketEventApplied { mutated }),
                    Err(err) => {
                        if self.config.fail_on_market_error {
                            Err(err)
                        } else {
                            Ok(ReplayStepOutcome::MarketEventApplied { mutated: false })
                        }
                    }
                }
            }

            ResearchPayload::DislocationObservation(recorded_obs) => {
                // Recompute observation strictly from current reconstructed order books
                let replayed = self.observation_engine.observe_pair(
                    &self.manager,
                    recorded_obs.buy_venue,
                    recorded_obs.buy_market,
                    recorded_obs.sell_venue,
                    recorded_obs.sell_market,
                    &recorded_obs.symbol,
                    recorded_obs.reference_quantity,
                    recorded_obs.observation_ts_ns,
                );

                // Diagnostic comparison
                let mismatches = compare_observations(record_index, recorded_obs, &replayed);

                // Track opportunity persistence if tracker enabled
                if let Some(tracker) = self.opportunity_tracker.as_mut() {
                    let key = OpportunityKey::new(
                        replayed.buy_venue,
                        replayed.buy_market,
                        replayed.sell_venue,
                        replayed.sell_market,
                        &replayed.symbol,
                        replayed.reference_quantity,
                        replayed.market_relationship,
                    );
                    let transition = tracker.process_observation(&replayed);
                    if let PersistenceTransition::Ended(ref rec) = transition {
                        self.last_opportunity_records
                            .insert(key.clone(), (**rec).clone());
                    }
                    self.last_transitions.insert(key, transition);
                }

                self.last_replayed_observation = Some(replayed.clone());
                Ok(ReplayStepOutcome::ObservationCompared {
                    replayed: Box::new(replayed),
                    mismatches,
                })
            }

            ResearchPayload::PersistenceTransition(recorded_trans) => {
                let mismatches = match self.last_transitions.get(&recorded_trans.key) {
                    Some(last_trans) => {
                        compare_transitions(record_index, recorded_trans, last_trans)
                    }
                    None => vec![ReplayMismatch {
                        record_index,
                        event_timestamp_ns: recorded_trans.timestamp_ns,
                        event_type: ResearchEventType::PersistenceTransition,
                        field_name: "transition".to_string(),
                        recorded_value: format!("{:?}", recorded_trans.transition),
                        replayed_value: "None (no replayed transition for key)".to_string(),
                        buy_venue: Some(recorded_trans.key.buy_venue),
                        buy_market: Some(recorded_trans.key.buy_market),
                        sell_venue: Some(recorded_trans.key.sell_venue),
                        sell_market: Some(recorded_trans.key.sell_market),
                        symbol: recorded_trans.key.symbol.clone(),
                        market_relationship: Some(recorded_trans.key.market_relationship),
                        reference_quantity: Some(recorded_trans.key.reference_quantity),
                    }],
                };

                Ok(ReplayStepOutcome::TransitionCompared { mismatches })
            }

            ResearchPayload::OpportunityRecord(recorded_rec) => {
                let mismatches = match self.last_opportunity_records.get(&recorded_rec.key) {
                    Some(last_rec) => {
                        compare_opportunity_records(record_index, recorded_rec, last_rec)
                    }
                    None => vec![ReplayMismatch {
                        record_index,
                        event_timestamp_ns: recorded_rec.end_ts_ns,
                        event_type: ResearchEventType::OpportunityRecord,
                        field_name: "opportunity_record".to_string(),
                        recorded_value: format!("{:?}", recorded_rec),
                        replayed_value: "None (no completed opportunity for key)".to_string(),
                        buy_venue: Some(recorded_rec.key.buy_venue),
                        buy_market: Some(recorded_rec.key.buy_market),
                        sell_venue: Some(recorded_rec.key.sell_venue),
                        sell_market: Some(recorded_rec.key.sell_market),
                        symbol: recorded_rec.key.symbol.clone(),
                        market_relationship: Some(recorded_rec.key.market_relationship),
                        reference_quantity: Some(recorded_rec.key.reference_quantity),
                    }],
                };

                Ok(ReplayStepOutcome::OpportunityCompared { mismatches })
            }
        }
    }

    /// Stream a complete research dataset through the replay engine.
    ///
    /// Reads records sequentially from `reader`, maintaining deterministic ordering.
    /// Returns a structured `ReplayResult` containing full execution and comparison statistics.
    pub fn replay<R: Read>(&mut self, reader: &mut ResearchReader<R>) -> Result<ReplayResult> {
        let mut total_records_read = 0u64;
        let mut market_events_consumed = 0u64;
        let mut market_events_mutated = 0u64;
        let mut derived_observations_found = 0u64;
        let mut replayed_observations = 0u64;
        let mut matched_observations = 0u64;
        let mut derived_transitions_found = 0u64;
        let mut matched_transitions = 0u64;
        let mut derived_opportunities_found = 0u64;
        let mut matched_opportunities = 0u64;
        let mut all_mismatches = Vec::new();

        while let Some(event) = reader.next_event()? {
            total_records_read += 1;
            let record_index = total_records_read;

            let outcome = self.replay_step(record_index, &event)?;

            match outcome {
                ReplayStepOutcome::MarketEventApplied { mutated } => {
                    market_events_consumed += 1;
                    if mutated {
                        market_events_mutated += 1;
                    }
                }
                ReplayStepOutcome::ObservationCompared { mismatches, .. } => {
                    derived_observations_found += 1;
                    replayed_observations += 1;
                    if mismatches.is_empty() {
                        matched_observations += 1;
                    } else {
                        all_mismatches.extend(mismatches);
                    }
                }
                ReplayStepOutcome::TransitionCompared { mismatches } => {
                    derived_transitions_found += 1;
                    if mismatches.is_empty() {
                        matched_transitions += 1;
                    } else {
                        all_mismatches.extend(mismatches);
                    }
                }
                ReplayStepOutcome::OpportunityCompared { mismatches } => {
                    derived_opportunities_found += 1;
                    if mismatches.is_empty() {
                        matched_opportunities += 1;
                    } else {
                        all_mismatches.extend(mismatches);
                    }
                }
            }

            if self.config.stop_on_first_mismatch && !all_mismatches.is_empty() {
                break;
            }
        }

        // Build snapshot of final book states
        let mut final_books = HashMap::new();
        for (key, state) in self.manager.iter_states() {
            let label = format!("{} {} {}", key.venue, key.market_type, key.symbol);
            final_books.insert(label, state.lifecycle_state);
        }

        Ok(ReplayResult {
            total_records_read,
            market_events_consumed,
            market_events_mutated,
            derived_observations_found,
            replayed_observations,
            matched_observations,
            derived_transitions_found,
            matched_transitions,
            derived_opportunities_found,
            matched_opportunities,
            mismatches: all_mismatches,
            last_event_timestamp_ns: self.latest_market_ts_ns,
            final_books,
        })
    }
}

/// Helper to extract internal market event timestamp (not the host recording timestamp).
fn extract_market_event_timestamp(event: &MarketEvent) -> Option<i64> {
    match event {
        MarketEvent::OrderBookSnapshot {
            local_recv_ts_ns, ..
        } => Some(*local_recv_ts_ns),
        MarketEvent::OrderBookDelta {
            local_recv_ts_ns, ..
        } => Some(*local_recv_ts_ns),
        MarketEvent::FundingRateUpdate {
            local_recv_ts_ns, ..
        } => Some(*local_recv_ts_ns),
        MarketEvent::ConnectionState { .. } => None,
    }
}
