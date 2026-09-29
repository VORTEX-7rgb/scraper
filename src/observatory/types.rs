use crate::types::{MarketType, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Economic classification of the instrument relationship between two order books.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRelationship {
    /// Same-instrument comparison between spot markets (e.g. Binance Spot vs Bybit Spot).
    SpotSpot,
    /// Same-instrument comparison between perpetual linear futures markets (e.g. Binance Linear vs Bybit Linear).
    PerpPerp,
    /// Cross-instrument basis comparison (e.g. Spot vs Perpetual).
    /// Distinct economic mechanism from same-instrument dislocations!
    CrossInstrumentBasis,
}

impl MarketRelationship {
    /// Classify the economic relationship between two market instruments.
    pub fn classify(buy_market: MarketType, sell_market: MarketType) -> Self {
        match (buy_market, sell_market) {
            (MarketType::Spot, MarketType::Spot) => Self::SpotSpot,
            (MarketType::LinearPerpetual, MarketType::LinearPerpetual) => Self::PerpPerp,
            (MarketType::Spot, MarketType::LinearPerpetual)
            | (MarketType::LinearPerpetual, MarketType::Spot) => Self::CrossInstrumentBasis,
        }
    }

    /// Check if this is a direct same-instrument comparison (Spot-Spot or Perp-Perp).
    pub fn is_direct_dislocation(&self) -> bool {
        matches!(self, Self::SpotSpot | Self::PerpPerp)
    }

    /// Check if this is a cross-instrument basis comparison.
    pub fn is_basis(&self) -> bool {
        matches!(self, Self::CrossInstrumentBasis)
    }
}

impl std::fmt::Display for MarketRelationship {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SpotSpot => write!(f, "spot_spot"),
            Self::PerpPerp => write!(f, "perp_perp"),
            Self::CrossInstrumentBasis => write!(f, "cross_instrument_basis"),
        }
    }
}

/// Explicit taxonomy of reasons why a cross-book observation could not be evaluated.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationRejectionReason {
    /// Buy and sell endpoints are identical.
    SelfComparison,
    /// Missing book state for one or both venues.
    MissingBook {
        venue: VenueId,
        market_type: MarketType,
    },
    /// Underlying order book is not currently in trusted state.
    UntrustedBook {
        venue: VenueId,
        market_type: MarketType,
        reason: String,
    },
    /// Order book age exceeds maximum staleness threshold.
    StaleBook {
        venue: VenueId,
        market_type: MarketType,
        age_ms: u64,
        max_age_ms: u64,
    },
    /// Order book is crossed (best bid >= best ask).
    CrossedBook {
        venue: VenueId,
        market_type: MarketType,
    },
    /// Order book has empty bids or asks.
    EmptyBook {
        venue: VenueId,
        market_type: MarketType,
    },
    /// Inter-book timestamp skew exceeds maximum threshold.
    TimestampSkewExceeded { skew_ms: u64, max_skew_ms: u64 },
    /// Execution engine error during depth walking.
    ExecutionEngineError(String),
}

impl std::fmt::Display for ObservationRejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SelfComparison => {
                write!(f, "Self-comparison: buy and sell endpoints are identical")
            }
            Self::MissingBook { venue, market_type } => {
                write!(f, "Missing book state for {venue} {market_type}")
            }
            Self::UntrustedBook {
                venue,
                market_type,
                reason,
            } => {
                write!(
                    f,
                    "Untrusted book state for {venue} {market_type}: {reason}"
                )
            }
            Self::StaleBook {
                venue,
                market_type,
                age_ms,
                max_age_ms,
            } => {
                write!(
                    f,
                    "Stale book for {venue} {market_type}: age {age_ms}ms > max {max_age_ms}ms"
                )
            }
            Self::CrossedBook { venue, market_type } => {
                write!(f, "Crossed book detected for {venue} {market_type}")
            }
            Self::EmptyBook { venue, market_type } => {
                write!(f, "Empty order book for {venue} {market_type}")
            }
            Self::TimestampSkewExceeded {
                skew_ms,
                max_skew_ms,
            } => {
                write!(
                    f,
                    "Timestamp skew {skew_ms}ms exceeds maximum {max_skew_ms}ms"
                )
            }
            Self::ExecutionEngineError(msg) => write!(f, "Execution pricing error: {msg}"),
        }
    }
}

/// Canonical cross-book dislocation observation snapshot.
///
/// Contains complete observed market depth data, configured assumptions,
/// derived financial economics, and trust/quality metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DislocationObservation {
    // Identity
    pub buy_venue: VenueId,
    pub buy_market: MarketType,
    pub sell_venue: VenueId,
    pub sell_market: MarketType,
    pub symbol: String,
    pub market_relationship: MarketRelationship,

    // Timing
    pub observation_ts_ns: i64,
    pub buy_book_exchange_ts_ms: Option<i64>,
    pub sell_book_exchange_ts_ms: Option<i64>,
    pub buy_book_recv_ts_ns: Option<i64>,
    pub sell_book_recv_ts_ns: Option<i64>,
    pub timestamp_skew_ms: Option<i64>,
    pub buy_book_age_ms: Option<u64>,
    pub sell_book_age_ms: Option<u64>,

    // Requested & Executable Quantities
    pub reference_quantity: Decimal,
    pub buy_available_quantity: Decimal,
    pub sell_available_quantity: Decimal,
    pub common_executable_quantity: Decimal,
    pub fully_executable: bool,

    // Pricing
    pub buy_vwap: Option<Decimal>,
    pub sell_vwap: Option<Decimal>,
    pub buy_worst_fill_price: Option<Decimal>,
    pub sell_worst_fill_price: Option<Decimal>,
    pub buy_best_ask: Option<Decimal>,
    pub sell_best_bid: Option<Decimal>,

    // Economics
    pub gross_spread: Option<Decimal>,
    pub gross_spread_bps: Option<Decimal>,
    pub gross_edge: Option<Decimal>,
    pub buy_fee: Decimal,
    pub sell_fee: Decimal,
    pub total_fees: Decimal,
    pub net_edge: Option<Decimal>,
    pub net_edge_bps: Option<Decimal>,

    // Quality & Trust
    pub both_books_trusted: bool,
    pub both_books_fresh: bool,
    pub no_crossed_books: bool,
    pub is_valid: bool,
    pub rejection_reason: Option<ObservationRejectionReason>,
}

impl DislocationObservation {
    /// Construct a rejected observation when trust, freshness, or sanity checks fail.
    #[allow(clippy::too_many_arguments)]
    pub fn rejected(
        buy_venue: VenueId,
        buy_market: MarketType,
        sell_venue: VenueId,
        sell_market: MarketType,
        symbol: impl Into<String>,
        market_relationship: MarketRelationship,
        observation_ts_ns: i64,
        reference_quantity: Decimal,
        rejection_reason: ObservationRejectionReason,
    ) -> Self {
        Self {
            buy_venue,
            buy_market,
            sell_venue,
            sell_market,
            symbol: symbol.into().to_uppercase(),
            market_relationship,
            observation_ts_ns,
            buy_book_exchange_ts_ms: None,
            sell_book_exchange_ts_ms: None,
            buy_book_recv_ts_ns: None,
            sell_book_recv_ts_ns: None,
            timestamp_skew_ms: None,
            buy_book_age_ms: None,
            sell_book_age_ms: None,
            reference_quantity,
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
            both_books_trusted: false,
            both_books_fresh: false,
            no_crossed_books: true,
            is_valid: false,
            rejection_reason: Some(rejection_reason),
        }
    }

    /// Check if this observation represents an executable opportunity with positive net edge.
    pub fn is_positive_executable_edge(&self) -> bool {
        self.is_valid
            && self.fully_executable
            && self.net_edge_bps.is_some_and(|bps| bps > Decimal::ZERO)
    }
}

/// Unique key identifying an observation stream for persistence tracking.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OpportunityKey {
    pub buy_venue: VenueId,
    pub buy_market: MarketType,
    pub sell_venue: VenueId,
    pub sell_market: MarketType,
    pub symbol: String,
    pub reference_quantity: Decimal,
    pub market_relationship: MarketRelationship,
}

impl OpportunityKey {
    pub fn new(
        buy_venue: VenueId,
        buy_market: MarketType,
        sell_venue: VenueId,
        sell_market: MarketType,
        symbol: impl Into<String>,
        reference_quantity: Decimal,
        market_relationship: MarketRelationship,
    ) -> Self {
        Self {
            buy_venue,
            buy_market,
            sell_venue,
            sell_market,
            symbol: symbol.into().to_uppercase(),
            reference_quantity,
            market_relationship,
        }
    }
}

/// Explicit taxonomy of opportunity termination causes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpportunityEndReason {
    /// Net executable edge fell below the required threshold.
    NetEdgeBelowThreshold,
    /// Underlying order book became untrusted.
    UntrustedBook,
    /// Order book exceeded maximum staleness threshold.
    StaleBook,
    /// Order book became crossed or invalid.
    InvalidBook,
    /// Inter-book timestamp skew exceeded maximum.
    TimestampSkewExceeded,
    /// Depth degraded: requested quantity is no longer fully executable.
    InsufficientLiquidity,
    /// Exchange feed connection dropped.
    FeedDisconnected,
    /// Order book state was removed or missing.
    MissingBook,
}

impl std::fmt::Display for OpportunityEndReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NetEdgeBelowThreshold => write!(f, "Net executable edge fell below threshold"),
            Self::UntrustedBook => write!(f, "Underlying order book became untrusted"),
            Self::StaleBook => write!(f, "Order book exceeded maximum staleness threshold"),
            Self::InvalidBook => write!(f, "Order book became crossed or invalid"),
            Self::TimestampSkewExceeded => write!(f, "Inter-book timestamp skew exceeded maximum"),
            Self::InsufficientLiquidity => {
                write!(
                    f,
                    "Depth degraded: requested quantity no longer fully executable"
                )
            }
            Self::FeedDisconnected => write!(f, "Exchange feed connection dropped"),
            Self::MissingBook => write!(f, "Order book state removed or missing"),
        }
    }
}

/// Historical record of a completed, continuous dislocation opportunity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpportunityRecord {
    pub key: OpportunityKey,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
    pub duration_ms: u64,
    pub sample_count: u64,
    pub first_observed_edge_bps: Decimal,
    pub last_observed_edge_bps: Decimal,
    pub peak_net_edge_bps: Decimal,
    pub min_net_edge_bps: Decimal,
    pub average_net_edge_bps: Decimal,
    pub max_common_executable_quantity: Decimal,
    pub termination_reason: OpportunityEndReason,
}

/// In-flight state of a currently active persistent opportunity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveOpportunity {
    pub key: OpportunityKey,
    pub start_ts_ns: i64,
    pub last_observed_ts_ns: i64,
    pub sample_count: u64,
    pub first_observed_edge_bps: Decimal,
    pub last_observed_edge_bps: Decimal,
    pub peak_net_edge_bps: Decimal,
    pub min_net_edge_bps: Decimal,
    pub sum_net_edge_bps: Decimal,
    pub max_common_executable_quantity: Decimal,
}

impl ActiveOpportunity {
    /// Current duration in milliseconds since start.
    pub fn current_duration_ms(&self) -> u64 {
        ((self.last_observed_ts_ns - self.start_ts_ns).max(0) / 1_000_000) as u64
    }

    /// Running average net edge in basis points.
    pub fn running_average_bps(&self) -> Decimal {
        if self.sample_count == 0 {
            Decimal::ZERO
        } else {
            self.sum_net_edge_bps / Decimal::from(self.sample_count)
        }
    }
}

/// Result of evaluating an observation against the persistence state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transition", content = "record", rename_all = "snake_case")]
pub enum PersistenceTransition {
    /// Inactive opportunity; conditions for activation not satisfied.
    None,
    /// Opportunity met all criteria and transitioned INACTIVE -> ACTIVE.
    Started,
    /// Active opportunity remained valid and updated metrics (ACTIVE -> ACTIVE).
    Continued,
    /// Active opportunity terminated due to a failed criterion (ACTIVE -> ENDED).
    Ended(Box<OpportunityRecord>),
}
