use crate::config::ObservatorySettings;
use crate::execution::VenueFeeRegistry;
use crate::market::MarketStateManager;
use crate::observatory::types::{
    DislocationObservation, MarketRelationship, ObservationRejectionReason,
};
use crate::types::{MarketType, VenueId};
use rust_decimal::Decimal;

/// Configuration parameters governing the cross-book observation engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationConfig {
    /// Ordered list of reference quantities to evaluate (e.g. 0.001, 0.01, 0.1 BTC).
    pub reference_quantities: Vec<Decimal>,
    /// Maximum allowed age in milliseconds for an order book before it is considered stale.
    pub max_book_age_ms: u64,
    /// Maximum allowed exchange timestamp skew in milliseconds between two order books.
    pub max_timestamp_skew_ms: u64,
    /// Additional proportional cost rate (e.g. operational buffer) applied to both sides.
    pub other_cost_rate: Decimal,
}

impl Default for ObservationConfig {
    fn default() -> Self {
        Self {
            reference_quantities: vec![
                Decimal::new(1, 3),  // 0.001
                Decimal::new(5, 3),  // 0.005
                Decimal::new(1, 2),  // 0.01
                Decimal::new(25, 3), // 0.025
                Decimal::new(5, 2),  // 0.05
                Decimal::new(1, 1),  // 0.10
            ],
            max_book_age_ms: 1_000,
            max_timestamp_skew_ms: 2_000,
            other_cost_rate: Decimal::ZERO,
        }
    }
}

impl From<&ObservatorySettings> for ObservationConfig {
    fn from(settings: &ObservatorySettings) -> Self {
        Self {
            reference_quantities: settings.reference_quantities.clone(),
            max_book_age_ms: settings.max_book_age_ms,
            max_timestamp_skew_ms: settings.max_timestamp_skew_ms,
            other_cost_rate: Decimal::ZERO,
        }
    }
}

/// Orchestration engine for observing executable cross-book dislocations.
///
/// Evaluates trusted order books against M4 pricing primitives across
/// configurable reference quantities, enforcing trust gates, freshness, and
/// timestamp skew boundaries.
#[derive(Debug, Clone)]
pub struct ObservationEngine {
    config: ObservationConfig,
    fee_registry: VenueFeeRegistry,
}

impl ObservationEngine {
    /// Create a new observation engine with explicit configuration and fee registry.
    pub fn new(config: ObservationConfig, fee_registry: VenueFeeRegistry) -> Self {
        Self {
            config,
            fee_registry,
        }
    }

    /// Create an engine with canonical exchange baseline defaults.
    pub fn with_defaults() -> Self {
        Self {
            config: ObservationConfig::default(),
            fee_registry: VenueFeeRegistry::default(),
        }
    }

    /// Create an engine from configuration settings and fee registry.
    pub fn from_settings(settings: &ObservatorySettings, fee_registry: VenueFeeRegistry) -> Self {
        Self {
            config: ObservationConfig::from(settings),
            fee_registry,
        }
    }

    /// Access current engine configuration.
    pub fn config(&self) -> &ObservationConfig {
        &self.config
    }

    /// Access fee registry.
    pub fn fee_registry(&self) -> &VenueFeeRegistry {
        &self.fee_registry
    }

    /// Update the additional cost rate.
    pub fn set_other_cost_rate(&mut self, rate: Decimal) {
        self.config.other_cost_rate = rate;
    }

    /// Observe executable dislocation for a directed pair: Buy on `buy_venue`, Sell on `sell_venue`.
    #[allow(clippy::too_many_arguments)]
    pub fn observe_pair(
        &self,
        manager: &MarketStateManager,
        buy_venue: VenueId,
        buy_market: MarketType,
        sell_venue: VenueId,
        sell_market: MarketType,
        symbol: &str,
        quantity: Decimal,
        now_ns: i64,
    ) -> DislocationObservation {
        let relationship = MarketRelationship::classify(buy_market, sell_market);

        // 1. Guard against self-comparison
        if buy_venue == sell_venue && buy_market == sell_market {
            return DislocationObservation::rejected(
                buy_venue,
                buy_market,
                sell_venue,
                sell_market,
                symbol,
                relationship,
                now_ns,
                quantity,
                ObservationRejectionReason::SelfComparison,
            );
        }

        // 2. Fetch underlying MarketState references
        let buy_state = match manager.get_state(buy_venue, buy_market, symbol) {
            Some(s) => s,
            None => {
                return DislocationObservation::rejected(
                    buy_venue,
                    buy_market,
                    sell_venue,
                    sell_market,
                    symbol,
                    relationship,
                    now_ns,
                    quantity,
                    ObservationRejectionReason::MissingBook {
                        venue: buy_venue,
                        market_type: buy_market,
                    },
                );
            }
        };

        let sell_state = match manager.get_state(sell_venue, sell_market, symbol) {
            Some(s) => s,
            None => {
                return DislocationObservation::rejected(
                    buy_venue,
                    buy_market,
                    sell_venue,
                    sell_market,
                    symbol,
                    relationship,
                    now_ns,
                    quantity,
                    ObservationRejectionReason::MissingBook {
                        venue: sell_venue,
                        market_type: sell_market,
                    },
                );
            }
        };

        // 3. Extract timestamps and metrics
        let buy_recv_ns = buy_state.last_local_recv_ts_ns;
        let sell_recv_ns = sell_state.last_local_recv_ts_ns;
        let buy_exchange_ms = buy_state.last_exchange_ts_ms;
        let sell_exchange_ms = sell_state.last_exchange_ts_ms;

        let buy_age_ms = buy_recv_ns.map(|ns| ((now_ns - ns).max(0) / 1_000_000) as u64);
        let sell_age_ms = sell_recv_ns.map(|ns| ((now_ns - ns).max(0) / 1_000_000) as u64);
        let skew_ms = match (buy_exchange_ms, sell_exchange_ms) {
            (Some(b), Some(s)) => Some((b - s).abs()),
            _ => None,
        };

        // Helper closure to build rejected observation preserving captured timing
        let make_rejected = |reason: ObservationRejectionReason,
                             both_books_trusted: bool,
                             both_books_fresh: bool,
                             no_crossed_books: bool| {
            DislocationObservation {
                buy_venue,
                buy_market,
                sell_venue,
                sell_market,
                symbol: symbol.to_uppercase(),
                market_relationship: relationship,
                observation_ts_ns: now_ns,
                buy_book_exchange_ts_ms: buy_exchange_ms,
                sell_book_exchange_ts_ms: sell_exchange_ms,
                buy_book_recv_ts_ns: buy_recv_ns,
                sell_book_recv_ts_ns: sell_recv_ns,
                timestamp_skew_ms: skew_ms,
                buy_book_age_ms: buy_age_ms,
                sell_book_age_ms: sell_age_ms,
                reference_quantity: quantity,
                buy_available_quantity: Decimal::ZERO,
                sell_available_quantity: Decimal::ZERO,
                common_executable_quantity: Decimal::ZERO,
                fully_executable: false,
                buy_vwap: None,
                sell_vwap: None,
                buy_worst_fill_price: None,
                sell_worst_fill_price: None,
                buy_best_ask: None,
                sell_best_bid: None,
                gross_spread: None,
                gross_spread_bps: None,
                gross_edge: None,
                buy_fee: Decimal::ZERO,
                sell_fee: Decimal::ZERO,
                total_fees: Decimal::ZERO,
                net_edge: None,
                net_edge_bps: None,
                both_books_trusted,
                both_books_fresh,
                no_crossed_books,
                is_valid: false,
                rejection_reason: Some(reason),
            }
        };

        // 4. Invariant: Crossed books check
        if buy_state.book.is_crossed() {
            return make_rejected(
                ObservationRejectionReason::CrossedBook {
                    venue: buy_venue,
                    market_type: buy_market,
                },
                false,
                false,
                false,
            );
        }
        if sell_state.book.is_crossed() {
            return make_rejected(
                ObservationRejectionReason::CrossedBook {
                    venue: sell_venue,
                    market_type: sell_market,
                },
                false,
                false,
                false,
            );
        }

        // 5. Invariant: Empty order books
        if buy_state.book.asks.is_empty() || buy_state.book.bids.is_empty() {
            return make_rejected(
                ObservationRejectionReason::EmptyBook {
                    venue: buy_venue,
                    market_type: buy_market,
                },
                false,
                false,
                true,
            );
        }
        if sell_state.book.bids.is_empty() || sell_state.book.asks.is_empty() {
            return make_rejected(
                ObservationRejectionReason::EmptyBook {
                    venue: sell_venue,
                    market_type: sell_market,
                },
                false,
                false,
                true,
            );
        }

        // 6. Freshness gate: Book age threshold
        let buy_is_stale = match buy_age_ms {
            Some(age) => age > self.config.max_book_age_ms,
            None => true,
        };
        if buy_is_stale {
            return make_rejected(
                ObservationRejectionReason::StaleBook {
                    venue: buy_venue,
                    market_type: buy_market,
                    age_ms: buy_age_ms.unwrap_or(u64::MAX),
                    max_age_ms: self.config.max_book_age_ms,
                },
                false,
                false,
                true,
            );
        }

        let sell_is_stale = match sell_age_ms {
            Some(age) => age > self.config.max_book_age_ms,
            None => true,
        };
        if sell_is_stale {
            return make_rejected(
                ObservationRejectionReason::StaleBook {
                    venue: sell_venue,
                    market_type: sell_market,
                    age_ms: sell_age_ms.unwrap_or(u64::MAX),
                    max_age_ms: self.config.max_book_age_ms,
                },
                false,
                false,
                true,
            );
        }

        // 7. Freshness gate: Timestamp skew threshold
        if let Some(skew) = skew_ms
            && skew as u64 > self.config.max_timestamp_skew_ms
        {
            return make_rejected(
                ObservationRejectionReason::TimestampSkewExceeded {
                    skew_ms: skew as u64,
                    max_skew_ms: self.config.max_timestamp_skew_ms,
                },
                false,
                false,
                true,
            );
        }

        // 8. Trust Gate: Must satisfy MarketStateManager trust verification
        let buy_trusted = manager.get_trusted_book(buy_venue, buy_market, symbol, now_ns);
        let buy_book = match buy_trusted {
            Some(b) => b,
            None => {
                return make_rejected(
                    ObservationRejectionReason::UntrustedBook {
                        venue: buy_venue,
                        market_type: buy_market,
                        reason: format!(
                            "Lifecycle: {:?}, Validity: {:?}",
                            buy_state.lifecycle_state, buy_state.validity
                        ),
                    },
                    false,
                    true,
                    true,
                );
            }
        };

        let sell_trusted = manager.get_trusted_book(sell_venue, sell_market, symbol, now_ns);
        let sell_book = match sell_trusted {
            Some(b) => b,
            None => {
                return make_rejected(
                    ObservationRejectionReason::UntrustedBook {
                        venue: sell_venue,
                        market_type: sell_market,
                        reason: format!(
                            "Lifecycle: {:?}, Validity: {:?}",
                            sell_state.lifecycle_state, sell_state.validity
                        ),
                    },
                    false,
                    true,
                    true,
                );
            }
        };

        // 9. Pricing execution via M4 primitive
        let buy_fees = self.fee_registry.get_schedule(buy_venue, buy_market);
        let sell_fees = self.fee_registry.get_schedule(sell_venue, sell_market);

        let comparison = match crate::execution::compare_cross_book(
            buy_book,
            sell_book,
            quantity,
            buy_fees,
            sell_fees,
            self.config.other_cost_rate,
        ) {
            Ok(c) => c,
            Err(err) => {
                return make_rejected(
                    ObservationRejectionReason::ExecutionEngineError(err.to_string()),
                    true,
                    true,
                    true,
                );
            }
        };

        // 10. Construct canonical valid observation
        DislocationObservation {
            buy_venue,
            buy_market,
            sell_venue,
            sell_market,
            symbol: symbol.to_uppercase(),
            market_relationship: relationship,
            observation_ts_ns: now_ns,
            buy_book_exchange_ts_ms: buy_exchange_ms,
            sell_book_exchange_ts_ms: sell_exchange_ms,
            buy_book_recv_ts_ns: buy_recv_ns,
            sell_book_recv_ts_ns: sell_recv_ns,
            timestamp_skew_ms: skew_ms,
            buy_book_age_ms: buy_age_ms,
            sell_book_age_ms: sell_age_ms,
            reference_quantity: quantity,
            buy_available_quantity: comparison.buy_estimate.filled_quantity,
            sell_available_quantity: comparison.sell_estimate.filled_quantity,
            common_executable_quantity: comparison.matched_quantity,
            fully_executable: comparison.fully_executable,
            buy_vwap: comparison.buy_estimate.vwap,
            sell_vwap: comparison.sell_estimate.vwap,
            buy_worst_fill_price: comparison.buy_estimate.worst_fill_price,
            sell_worst_fill_price: comparison.sell_estimate.worst_fill_price,
            buy_best_ask: buy_book.best_ask().map(|l| l.price),
            sell_best_bid: sell_book.best_bid().map(|l| l.price),
            gross_spread: comparison.gross_spread,
            gross_spread_bps: comparison.gross_spread_bps,
            gross_edge: comparison.gross_pnl,
            buy_fee: comparison.buy_fee,
            sell_fee: comparison.sell_fee,
            total_fees: comparison.total_costs,
            net_edge: comparison.net_pnl,
            net_edge_bps: comparison.net_spread_bps,
            both_books_trusted: true,
            both_books_fresh: true,
            no_crossed_books: true,
            is_valid: true,
            rejection_reason: None,
        }
    }

    /// Sweep across all configured reference quantities for a given pair.
    #[allow(clippy::too_many_arguments)]
    pub fn observe_sweep(
        &self,
        manager: &MarketStateManager,
        buy_venue: VenueId,
        buy_market: MarketType,
        sell_venue: VenueId,
        sell_market: MarketType,
        symbol: &str,
        now_ns: i64,
    ) -> Vec<DislocationObservation> {
        self.config
            .reference_quantities
            .iter()
            .map(|&qty| {
                self.observe_pair(
                    manager,
                    buy_venue,
                    buy_market,
                    sell_venue,
                    sell_market,
                    symbol,
                    qty,
                    now_ns,
                )
            })
            .collect()
    }

    /// Sweep across all configured reference quantities for all distinct directed pairs.
    pub fn observe_all_pairs_sweep(
        &self,
        manager: &MarketStateManager,
        symbol: &str,
        endpoints: &[(VenueId, MarketType)],
        now_ns: i64,
    ) -> Vec<DislocationObservation> {
        let mut observations = Vec::new();

        for &(buy_venue, buy_market) in endpoints {
            for &(sell_venue, sell_market) in endpoints {
                if buy_venue == sell_venue && buy_market == sell_market {
                    continue;
                }
                let sweep = self.observe_sweep(
                    manager,
                    buy_venue,
                    buy_market,
                    sell_venue,
                    sell_market,
                    symbol,
                    now_ns,
                );
                observations.extend(sweep);
            }
        }

        observations
    }
}
