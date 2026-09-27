use crate::error::{EngineError, Result};
use crate::market::OrderBook;
use crate::market::state::{DeltaUpdate, InvalidationReason, MarketState, MarketStateMetrics};
use crate::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::Duration;

/// Key identifying a specific instrument on a specific venue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstrumentKey {
    pub venue: VenueId,
    pub market_type: MarketType,
    pub symbol: String,
}

impl InstrumentKey {
    pub fn new(venue: VenueId, market_type: MarketType, symbol: impl Into<String>) -> Self {
        Self {
            venue,
            market_type,
            symbol: symbol.into().to_uppercase(),
        }
    }
}

/// Latest observed funding rate snapshot for perpetual instruments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingState {
    pub venue: VenueId,
    pub symbol: String,
    pub mark_price: Decimal,
    pub index_price: Option<Decimal>,
    pub rate: Decimal,
    pub next_funding_ts_ms: i64,
    pub exchange_ts_ms: i64,
    pub local_recv_ts_ns: i64,
}

/// Centralized manager for all local market states across venues and instruments.
///
/// Ensures all mutations from incoming `MarketEvent`s pass through sequence continuity,
/// order book invariant checks, and cadence-aware freshness verification before being
/// exposed as trusted state.
#[derive(Debug)]
pub struct MarketStateManager {
    books: HashMap<InstrumentKey, MarketState>,
    funding_rates: HashMap<(VenueId, String), FundingState>,
    max_staleness: Duration,
}

impl MarketStateManager {
    pub fn new(max_staleness: Duration) -> Self {
        Self {
            books: HashMap::new(),
            funding_rates: HashMap::new(),
            max_staleness,
        }
    }

    /// Register an instrument to be managed. Initializes an empty state awaiting snapshot.
    pub fn register_instrument(
        &mut self,
        venue: VenueId,
        market_type: MarketType,
        symbol: impl Into<String>,
    ) {
        let key = InstrumentKey::new(venue, market_type, symbol);
        self.books
            .entry(key.clone())
            .or_insert_with(|| MarketState::new(key.venue, key.market_type, key.symbol));
    }

    /// Ingest and route an incoming canonical `MarketEvent`.
    ///
    /// Returns `Ok(true)` if an order book was mutated, `Ok(false)` if ignored (duplicate/buffer),
    /// or `Err(...)` if a sequence break, invariant failure, or validation error occurred.
    pub fn handle_event(&mut self, event: &MarketEvent) -> Result<bool> {
        match event {
            MarketEvent::OrderBookSnapshot {
                venue,
                market_type,
                symbol,
                bids,
                asks,
                exchange_ts_ms,
                local_recv_ts_ns,
                sequence_id,
            } => {
                let key = InstrumentKey::new(*venue, *market_type, symbol);
                let state = self
                    .books
                    .entry(key.clone())
                    .or_insert_with(|| MarketState::new(key.venue, key.market_type, key.symbol));

                state.apply_snapshot(
                    bids.clone(),
                    asks.clone(),
                    *exchange_ts_ms,
                    *local_recv_ts_ns,
                    *sequence_id,
                )
            }
            MarketEvent::OrderBookDelta {
                venue,
                market_type,
                symbol,
                bids,
                asks,
                first_sequence_id,
                sequence_id,
                prev_sequence_id,
                transaction_ts_ms,
                exchange_ts_ms,
                local_recv_ts_ns,
            } => {
                let key = InstrumentKey::new(*venue, *market_type, symbol);
                let state = self
                    .books
                    .entry(key.clone())
                    .or_insert_with(|| MarketState::new(key.venue, key.market_type, key.symbol));

                state.apply_delta(DeltaUpdate {
                    bids: bids.clone(),
                    asks: asks.clone(),
                    first_sequence_id: *first_sequence_id,
                    sequence_id: *sequence_id,
                    prev_sequence_id: *prev_sequence_id,
                    transaction_ts_ms: *transaction_ts_ms,
                    exchange_ts_ms: *exchange_ts_ms,
                    local_recv_ts_ns: *local_recv_ts_ns,
                })
            }
            MarketEvent::FundingRateUpdate {
                venue,
                symbol,
                mark_price,
                index_price,
                rate,
                next_funding_ts_ms,
                exchange_ts_ms,
                local_recv_ts_ns,
            } => {
                let sym_upper = symbol.to_uppercase();
                self.funding_rates.insert(
                    (*venue, sym_upper.clone()),
                    FundingState {
                        venue: *venue,
                        symbol: sym_upper,
                        mark_price: *mark_price,
                        index_price: *index_price,
                        rate: *rate,
                        next_funding_ts_ms: *next_funding_ts_ms,
                        exchange_ts_ms: *exchange_ts_ms,
                        local_recv_ts_ns: *local_recv_ts_ns,
                    },
                );
                Ok(false)
            }
            MarketEvent::ConnectionState {
                venue,
                is_connected,
                details,
            } => {
                if !*is_connected {
                    // When a venue drops, immediately invalidate all books for that venue
                    for (key, state) in self.books.iter_mut() {
                        if key.venue == *venue {
                            state
                                .invalidate(InvalidationReason::VenueDisconnected(details.clone()));
                        }
                    }
                }
                Ok(false)
            }
        }
    }

    /// Retrieve an immutable reference to the `MarketState` of a given instrument.
    pub fn get_state(
        &self,
        venue: VenueId,
        market_type: MarketType,
        symbol: &str,
    ) -> Option<&MarketState> {
        let key = InstrumentKey::new(venue, market_type, symbol);
        self.books.get(&key)
    }

    /// Retrieve a mutable reference to the `MarketState` of a given instrument.
    pub fn get_state_mut(
        &mut self,
        venue: VenueId,
        market_type: MarketType,
        symbol: &str,
    ) -> Option<&mut MarketState> {
        let key = InstrumentKey::new(venue, market_type, symbol);
        self.books.get_mut(&key)
    }

    /// Retrieve a reference to the verified, continuous, non-crossed order book ONLY if it is trusted.
    ///
    /// Returns `None` if the state is uninitialized, synchronizing, invalidated, resyncing, crossed, or stale.
    pub fn get_trusted_book(
        &self,
        venue: VenueId,
        market_type: MarketType,
        symbol: &str,
        now_ns: i64,
    ) -> Option<&OrderBook> {
        let state = self.get_state(venue, market_type, symbol)?;
        state.trusted_book(Some(self.max_staleness), now_ns)
    }

    /// Check if a given instrument's state is currently Live and Valid.
    pub fn is_live(&self, venue: VenueId, market_type: MarketType, symbol: &str) -> bool {
        self.get_state(venue, market_type, symbol)
            .map(|s| s.lifecycle_state.is_live() && s.validity.is_valid())
            .unwrap_or(false)
    }

    /// Check if a given instrument's state is currently stale.
    pub fn is_stale(
        &self,
        venue: VenueId,
        market_type: MarketType,
        symbol: &str,
        now_ns: i64,
    ) -> bool {
        self.get_state(venue, market_type, symbol)
            .map(|s| s.is_stale(self.max_staleness, now_ns))
            .unwrap_or(true)
    }

    /// Return the best bid price level of a given instrument if available.
    pub fn best_bid(
        &self,
        venue: VenueId,
        market_type: MarketType,
        symbol: &str,
    ) -> Option<PriceLevel> {
        self.get_state(venue, market_type, symbol)
            .and_then(|s| s.best_bid())
    }

    /// Return the best ask price level of a given instrument if available.
    pub fn best_ask(
        &self,
        venue: VenueId,
        market_type: MarketType,
        symbol: &str,
    ) -> Option<PriceLevel> {
        self.get_state(venue, market_type, symbol)
            .and_then(|s| s.best_ask())
    }

    /// Return the top-of-book spread if available.
    pub fn spread(&self, venue: VenueId, market_type: MarketType, symbol: &str) -> Option<Decimal> {
        self.get_state(venue, market_type, symbol)
            .and_then(|s| s.spread())
    }

    /// Return diagnostic metrics for a specific instrument.
    pub fn get_metrics(
        &self,
        venue: VenueId,
        market_type: MarketType,
        symbol: &str,
    ) -> Option<&MarketStateMetrics> {
        self.get_state(venue, market_type, symbol)
            .map(|s| &s.metrics)
    }

    /// Return the latest funding rate state for a perpetual instrument.
    pub fn get_funding(&self, venue: VenueId, symbol: &str) -> Option<&FundingState> {
        self.funding_rates.get(&(venue, symbol.to_uppercase()))
    }

    /// Explicitly request a resync cycle for a specific instrument.
    pub fn request_resync(
        &mut self,
        venue: VenueId,
        market_type: MarketType,
        symbol: &str,
    ) -> Result<()> {
        let key = InstrumentKey::new(venue, market_type, symbol);
        let state = self.books.get_mut(&key).ok_or_else(|| {
            EngineError::Validation(format!("Unregistered instrument: {:?}", key))
        })?;
        state.request_resync()
    }

    /// Get current configured max staleness duration.
    pub fn max_staleness(&self) -> Duration {
        self.max_staleness
    }

    /// Set max staleness duration.
    pub fn set_max_staleness(&mut self, duration: Duration) {
        self.max_staleness = duration;
    }
}
