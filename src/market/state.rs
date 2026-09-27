use crate::error::{EngineError, Result};
use crate::market::OrderBook;
use crate::types::{MarketType, PriceLevel, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;

/// Explicit lifecycle states for a managed local limit order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookLifecycleState {
    /// Initial uninitialized state. No snapshot or delta applied.
    Empty,
    /// Waiting for a baseline snapshot. Incoming deltas are buffered.
    AwaitingSnapshot,
    /// Baseline snapshot received; aligning and replaying buffered deltas.
    Synchronizing,
    /// Fully synchronized and receiving continuous updates. Only this state is trusted.
    Live,
    /// Invariant failure, sequence break, or crossed book detected. State cannot be trusted.
    Invalidated,
    /// Explicit recovery/resynchronization initiated; preparing for fresh baseline snapshot.
    Resyncing,
}

impl BookLifecycleState {
    /// Verify whether a state transition from `self` to `target` is permitted.
    pub fn can_transition_to(&self, target: BookLifecycleState) -> bool {
        match (self, target) {
            // Same state is always a no-op transition
            (a, b) if *a == b => true,

            // From Empty
            (Self::Empty, Self::AwaitingSnapshot) => true,

            // From AwaitingSnapshot
            (Self::AwaitingSnapshot, Self::Synchronizing) => true,
            (Self::AwaitingSnapshot, Self::Live) => true, // Snapshot-only streams (e.g. Spot depth20)
            (Self::AwaitingSnapshot, Self::Invalidated) => true,

            // From Synchronizing
            (Self::Synchronizing, Self::Live) => true,
            (Self::Synchronizing, Self::Invalidated) => true,

            // From Live
            (Self::Live, Self::Invalidated) => true,
            (Self::Live, Self::Resyncing) => true,
            (Self::Live, Self::Live) => true, // Ongoing stream updates

            // From Invalidated
            (Self::Invalidated, Self::Resyncing) => true,

            // From Resyncing
            (Self::Resyncing, Self::AwaitingSnapshot) => true,
            (Self::Resyncing, Self::Synchronizing) => true,
            (Self::Resyncing, Self::Live) => true,

            // All other direct transitions are disallowed (e.g. Empty -> Live directly without snapshot)
            _ => false,
        }
    }

    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }
}

/// Explicit taxonomy of invalidation causes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvalidationReason {
    CrossedBook {
        bid: Decimal,
        ask: Decimal,
    },
    SequenceGap {
        expected_prev: u64,
        received_prev: Option<u64>,
        first_seq: u64,
        final_seq: u64,
    },
    OutOfOrderUpdate {
        last_seq: u64,
        received_seq: u64,
    },
    EmptyBook,
    SnapshotSyncError(String),
    ManualInvalidation(String),
    VenueDisconnected(String),
}

impl std::fmt::Display for InvalidationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CrossedBook { bid, ask } => {
                write!(f, "Crossed book: best bid {bid} >= best ask {ask}")
            }
            Self::SequenceGap {
                expected_prev,
                received_prev,
                first_seq,
                final_seq,
            } => {
                write!(
                    f,
                    "Sequence gap: expected {expected_prev}, received prev {received_prev:?}, U={first_seq}, u={final_seq}"
                )
            }
            Self::OutOfOrderUpdate {
                last_seq,
                received_seq,
            } => {
                write!(
                    f,
                    "Out of order update: last {last_seq}, received {received_seq}"
                )
            }
            Self::EmptyBook => write!(f, "Order book contains zero bids and zero asks"),
            Self::SnapshotSyncError(err) => write!(f, "Snapshot sync error: {err}"),
            Self::ManualInvalidation(reason) => write!(f, "Manual invalidation: {reason}"),
            Self::VenueDisconnected(details) => write!(f, "Venue disconnected: {details}"),
        }
    }
}

/// Order book validity assessment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookValidity {
    Valid,
    Invalid(InvalidationReason),
}

impl BookValidity {
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}

/// Diagnostic counters for local market state monitoring.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketStateMetrics {
    pub snapshots_applied: u64,
    pub deltas_received: u64,
    pub deltas_applied: u64,
    pub duplicate_deltas: u64,
    pub old_deltas: u64,
    pub sequence_failures: u64,
    pub invalidations: u64,
    pub resync_attempts: u64,
    pub resync_successes: u64,
    pub crossed_books: u64,
    pub stale_events: u64,
}

/// Canonical delta update payload for order book mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaUpdate {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub first_sequence_id: u64,
    pub sequence_id: u64,
    pub prev_sequence_id: Option<u64>,
    pub transaction_ts_ms: i64,
    pub exchange_ts_ms: i64,
    pub local_recv_ts_ns: i64,
}

impl DeltaUpdate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        first_sequence_id: u64,
        sequence_id: u64,
        prev_sequence_id: Option<u64>,
        transaction_ts_ms: i64,
        exchange_ts_ms: i64,
        local_recv_ts_ns: i64,
    ) -> Self {
        Self {
            bids,
            asks,
            first_sequence_id,
            sequence_id,
            prev_sequence_id,
            transaction_ts_ms,
            exchange_ts_ms,
            local_recv_ts_ns,
        }
    }
}

/// Complete, verified local market state for a single instrument on a single venue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketState {
    pub venue: VenueId,
    pub market_type: MarketType,
    pub symbol: String,

    pub book: OrderBook,
    pub lifecycle_state: BookLifecycleState,
    pub validity: BookValidity,

    pub last_update_sequence: Option<u64>,
    pub last_exchange_ts_ms: Option<i64>,
    pub last_local_recv_ts_ns: Option<i64>,

    delta_buffer: VecDeque<DeltaUpdate>,
    max_buffered_deltas: usize,

    pub metrics: MarketStateMetrics,
}

impl MarketState {
    pub fn new(venue: VenueId, market_type: MarketType, symbol: impl Into<String>) -> Self {
        let sym = symbol.into();
        Self {
            venue,
            market_type,
            symbol: sym.clone(),
            book: OrderBook::new(venue, market_type, sym),
            lifecycle_state: BookLifecycleState::AwaitingSnapshot,
            validity: BookValidity::Invalid(InvalidationReason::ManualInvalidation(
                "Awaiting initial baseline snapshot".into(),
            )),
            last_update_sequence: None,
            last_exchange_ts_ms: None,
            last_local_recv_ts_ns: None,
            delta_buffer: VecDeque::with_capacity(256),
            max_buffered_deltas: 1000,
            metrics: MarketStateMetrics::default(),
        }
    }

    /// Transition to another lifecycle state, enforcing state-machine graph invariants.
    pub fn transition_to(&mut self, target: BookLifecycleState, reason: &str) -> Result<()> {
        if !self.lifecycle_state.can_transition_to(target) {
            return Err(EngineError::InvalidStateTransition {
                from: format!("{:?}", self.lifecycle_state),
                to: format!("{:?}", target),
                reason: reason.to_string(),
            });
        }
        self.lifecycle_state = target;
        Ok(())
    }

    /// Invalidate the local market state with a concrete actionable reason.
    pub fn invalidate(&mut self, reason: InvalidationReason) {
        self.lifecycle_state = BookLifecycleState::Invalidated;
        self.validity = BookValidity::Invalid(reason);
        self.metrics.invalidations += 1;
    }

    /// Request resynchronization. Resets book levels and buffer, transitioning to `AwaitingSnapshot`.
    pub fn request_resync(&mut self) -> Result<()> {
        self.transition_to(
            BookLifecycleState::Resyncing,
            "Resynchronization cycle initiated",
        )?;
        self.metrics.resync_attempts += 1;

        // Clear book state and buffer
        self.book = OrderBook::new(self.venue, self.market_type, &self.symbol);
        self.delta_buffer.clear();
        self.last_update_sequence = None;
        self.validity = BookValidity::Invalid(InvalidationReason::ManualInvalidation(
            "Resync in progress; awaiting snapshot".into(),
        ));

        self.transition_to(
            BookLifecycleState::AwaitingSnapshot,
            "Ready for new snapshot",
        )?;
        Ok(())
    }

    /// Apply an order book snapshot.
    ///
    /// For snapshot-only streams (e.g. Binance Spot depth20):
    /// - Directly updates and validates order book state.
    ///
    /// For delta-streams (e.g. Binance USD-M Futures):
    /// - Sets baseline snapshot, then drains and aligns all valid buffered deltas per the
    ///   official Binance synchronization protocol.
    pub fn apply_snapshot(
        &mut self,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        exchange_ts_ms: i64,
        local_recv_ts_ns: i64,
        sequence_id: u64,
    ) -> Result<bool> {
        // In Live state, check for duplicate or old snapshots
        if self.lifecycle_state == BookLifecycleState::Live
            && let Some(last_seq) = self.last_update_sequence
        {
            if sequence_id == last_seq {
                self.metrics.duplicate_deltas += 1;
                return Ok(false);
            }
            if sequence_id < last_seq {
                self.metrics.old_deltas += 1;
                return Ok(false);
            }
        }

        // If in Invalidated state, an explicit resync or snapshot transition is required
        if self.lifecycle_state == BookLifecycleState::Invalidated {
            self.request_resync()?;
        }

        // Apply snapshot to internal order book
        if let Err(err) =
            self.book
                .set_snapshot(bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id)
        {
            match err {
                EngineError::CrossedBook { bid, ask } => {
                    self.invalidate(InvalidationReason::CrossedBook { bid, ask });
                    self.metrics.crossed_books += 1;
                    return Err(EngineError::CrossedBook { bid, ask });
                }
                other => {
                    self.invalidate(InvalidationReason::SnapshotSyncError(format!("{other}")));
                    return Err(other);
                }
            }
        }

        // Validate non-empty invariant
        if self.book.bids.is_empty() && self.book.asks.is_empty() {
            self.invalidate(InvalidationReason::EmptyBook);
            return Err(EngineError::DataQuality("Empty snapshot received".into()));
        }

        let was_resyncing = self.metrics.resync_attempts > self.metrics.resync_successes;

        // If deltas were buffered during AwaitingSnapshot, align them per Binance protocol
        if !self.delta_buffer.is_empty() {
            self.transition_to(
                BookLifecycleState::Synchronizing,
                "Aligning buffered deltas with snapshot",
            )?;

            // 1. Drop obsolete buffered events where final update u < snapshot sequence S
            while let Some(front) = self.delta_buffer.front() {
                if front.sequence_id < sequence_id
                    && front.prev_sequence_id.unwrap_or(0) < sequence_id
                {
                    self.delta_buffer.pop_front();
                } else {
                    break;
                }
            }

            // 2. Identify the first valid event that covers or immediately follows sequence_id:
            // Condition: (U <= S && u >= S) OR (pu == Some(S) || U == S + 1)
            let mut current_u = sequence_id;

            if let Some(first_match_idx) = self.delta_buffer.iter().position(|d| {
                (d.first_sequence_id <= sequence_id && d.sequence_id >= sequence_id)
                    || (d.prev_sequence_id == Some(sequence_id)
                        || d.first_sequence_id == sequence_id + 1)
            }) {
                // Drop any unaligned events preceding the first match
                for _ in 0..first_match_idx {
                    self.delta_buffer.pop_front();
                }

                // Apply remaining buffered deltas in strict sequential order
                while let Some(delta) = self.delta_buffer.pop_front() {
                    // Continuity check:
                    // If delta is the first applied and covers S, or pu == current_u
                    let is_first_covering = delta.first_sequence_id <= sequence_id
                        && delta.sequence_id >= sequence_id
                        && current_u == sequence_id;

                    if !is_first_covering {
                        let pu_matches = delta.prev_sequence_id == Some(current_u);
                        let contiguous_u = delta.first_sequence_id <= current_u + 1
                            && delta.sequence_id > current_u;

                        if !pu_matches && !contiguous_u {
                            let gap = InvalidationReason::SequenceGap {
                                expected_prev: current_u,
                                received_prev: delta.prev_sequence_id,
                                first_seq: delta.first_sequence_id,
                                final_seq: delta.sequence_id,
                            };
                            self.invalidate(gap);
                            self.metrics.sequence_failures += 1;
                            return Err(EngineError::SequenceGap {
                                expected_prev: current_u,
                                received_prev: delta.prev_sequence_id,
                                first_seq: delta.first_sequence_id,
                                final_seq: delta.sequence_id,
                            });
                        }
                    }

                    if let Err(err) = self.book.apply_delta(
                        &delta.bids,
                        &delta.asks,
                        delta.exchange_ts_ms,
                        delta.local_recv_ts_ns,
                        delta.sequence_id,
                    ) {
                        if let EngineError::CrossedBook { bid, ask } = err {
                            self.invalidate(InvalidationReason::CrossedBook { bid, ask });
                            self.metrics.crossed_books += 1;
                        }
                        return Err(err);
                    }

                    current_u = delta.sequence_id;
                    self.metrics.deltas_applied += 1;
                }
            } else if !self.delta_buffer.is_empty() {
                // Remaining deltas have a sequence gap beyond the snapshot
                let (first_seq, final_seq, prev_seq) = {
                    let front = &self.delta_buffer[0];
                    (
                        front.first_sequence_id,
                        front.sequence_id,
                        front.prev_sequence_id,
                    )
                };
                let gap = InvalidationReason::SequenceGap {
                    expected_prev: sequence_id,
                    received_prev: prev_seq,
                    first_seq,
                    final_seq,
                };
                self.invalidate(gap);
                self.metrics.sequence_failures += 1;
                self.delta_buffer.clear();
                return Err(EngineError::SequenceGap {
                    expected_prev: sequence_id,
                    received_prev: prev_seq,
                    first_seq,
                    final_seq,
                });
            }

            self.last_update_sequence = Some(current_u);
        } else {
            self.last_update_sequence = Some(sequence_id);
        }

        self.last_exchange_ts_ms = Some(exchange_ts_ms);
        self.last_local_recv_ts_ns = Some(local_recv_ts_ns);
        self.delta_buffer.clear();

        self.lifecycle_state = BookLifecycleState::Live;
        self.validity = BookValidity::Valid;
        self.metrics.snapshots_applied += 1;

        if was_resyncing {
            self.metrics.resync_successes += 1;
        }

        Ok(true)
    }

    /// Apply an incremental order book delta update.
    ///
    /// - If `AwaitingSnapshot` or `Synchronizing`: buffers the delta.
    /// - If `Invalidated`: rejects mutation until resynchronization.
    /// - If `Live`: strictly checks sequence continuity (`pu == previous.u`).
    ///   Detects duplicate/old events, mutates the order book, and checks crossed-book invariants.
    pub fn apply_delta(&mut self, delta: DeltaUpdate) -> Result<bool> {
        let DeltaUpdate {
            bids,
            asks,
            first_sequence_id,
            sequence_id,
            prev_sequence_id,
            transaction_ts_ms,
            exchange_ts_ms,
            local_recv_ts_ns,
        } = delta;

        // Basic message payload validation
        if first_sequence_id > sequence_id && sequence_id > 0 {
            return Err(EngineError::Validation(format!(
                "Sequence ID inversion: U ({first_sequence_id}) > u ({sequence_id})"
            )));
        }

        match self.lifecycle_state {
            BookLifecycleState::Empty
            | BookLifecycleState::AwaitingSnapshot
            | BookLifecycleState::Synchronizing => {
                self.metrics.deltas_received += 1;
                if self.delta_buffer.len() >= self.max_buffered_deltas {
                    self.delta_buffer.pop_front();
                }
                self.delta_buffer.push_back(DeltaUpdate {
                    bids,
                    asks,
                    first_sequence_id,
                    sequence_id,
                    prev_sequence_id,
                    transaction_ts_ms,
                    exchange_ts_ms,
                    local_recv_ts_ns,
                });
                Ok(false)
            }
            BookLifecycleState::Invalidated | BookLifecycleState::Resyncing => {
                self.metrics.deltas_received += 1;
                Err(EngineError::InvalidBookState(format!(
                    "Cannot apply delta in {:?} state; resynchronization required",
                    self.lifecycle_state
                )))
            }
            BookLifecycleState::Live => {
                self.metrics.deltas_received += 1;
                let last_u = self.last_update_sequence.unwrap_or(0);

                // Duplicate check
                if sequence_id == last_u {
                    self.metrics.duplicate_deltas += 1;
                    return Ok(false);
                }

                // Old / already applied update check
                if sequence_id < last_u {
                    self.metrics.old_deltas += 1;
                    return Ok(false);
                }

                // Stream continuity check:
                // Official Binance USD-M Futures protocol:
                // 1. The first event following a snapshot satisfies: U <= lastUpdateId && u >= lastUpdateId
                // 2. Each subsequent event's pu MUST correspond to previous event's u
                let is_first_covering_snapshot =
                    first_sequence_id <= last_u && sequence_id >= last_u;

                let continuity_ok = if is_first_covering_snapshot {
                    true
                } else {
                    match prev_sequence_id {
                        Some(pu) => pu == last_u,
                        None => first_sequence_id <= last_u + 1 && sequence_id > last_u,
                    }
                };

                if !continuity_ok {
                    let gap = InvalidationReason::SequenceGap {
                        expected_prev: last_u,
                        received_prev: prev_sequence_id,
                        first_seq: first_sequence_id,
                        final_seq: sequence_id,
                    };
                    self.invalidate(gap);
                    self.metrics.sequence_failures += 1;
                    return Err(EngineError::SequenceGap {
                        expected_prev: last_u,
                        received_prev: prev_sequence_id,
                        first_seq: first_sequence_id,
                        final_seq: sequence_id,
                    });
                }

                // Apply delta to internal order book
                if let Err(err) = self.book.apply_delta(
                    &bids,
                    &asks,
                    exchange_ts_ms,
                    local_recv_ts_ns,
                    sequence_id,
                ) {
                    if let EngineError::CrossedBook { bid, ask } = err {
                        self.invalidate(InvalidationReason::CrossedBook { bid, ask });
                        self.metrics.crossed_books += 1;
                    }
                    return Err(err);
                }

                // Verify non-empty invariant
                if self.book.bids.is_empty() && self.book.asks.is_empty() {
                    self.invalidate(InvalidationReason::EmptyBook);
                    return Err(EngineError::DataQuality("Book emptied by delta".into()));
                }

                self.last_update_sequence = Some(sequence_id);
                self.last_exchange_ts_ms = Some(exchange_ts_ms);
                self.last_local_recv_ts_ns = Some(local_recv_ts_ns);
                self.metrics.deltas_applied += 1;

                Ok(true)
            }
        }
    }

    /// Check if the market state is stale relative to a given duration threshold.
    pub fn is_stale(&self, threshold: Duration, now_ns: i64) -> bool {
        if !self.lifecycle_state.is_live() {
            return true;
        }
        let Some(recv_ns) = self.last_local_recv_ts_ns else {
            return true;
        };
        let age_ns = now_ns - recv_ns;
        age_ns > threshold.as_nanos() as i64
    }

    /// Return a reference to the order book ONLY if the state is fully trusted:
    /// - State is strictly `BookLifecycleState::Live`
    /// - Validity is `BookValidity::Valid`
    /// - Book is not crossed (`!book.is_crossed()`)
    /// - Not stale (if `max_staleness` is provided)
    pub fn trusted_book(&self, max_staleness: Option<Duration>, now_ns: i64) -> Option<&OrderBook> {
        if !self.lifecycle_state.is_live() {
            return None;
        }
        if !self.validity.is_valid() {
            return None;
        }
        if self.book.is_crossed() {
            return None;
        }
        if let Some(threshold) = max_staleness
            && self.is_stale(threshold, now_ns)
        {
            return None;
        }
        Some(&self.book)
    }

    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.book.best_bid()
    }

    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.book.best_ask()
    }

    pub fn spread(&self) -> Option<Decimal> {
        self.book.spread()
    }

    pub fn buffered_delta_count(&self) -> usize {
        self.delta_buffer.len()
    }
}
