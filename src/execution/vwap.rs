use crate::execution::depth::ExecutionEstimate;
use crate::types::Side;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Calculate Volume-Weighted Average Price (VWAP) across a collection of (price, quantity) pairs.
///
/// Formula:
/// ```text
/// VWAP = Σ(price_i × quantity_i) / Σ(quantity_i)
/// ```
///
/// Returns `None` if the total filled quantity is zero or empty.
pub fn calculate_vwap(levels: &[(Decimal, Decimal)]) -> Option<Decimal> {
    let mut total_notional = Decimal::ZERO;
    let mut total_quantity = Decimal::ZERO;

    for &(price, qty) in levels {
        if qty > Decimal::ZERO && price > Decimal::ZERO {
            total_notional += price * qty;
            total_quantity += qty;
        }
    }

    if total_quantity > Decimal::ZERO {
        Some(total_notional / total_quantity)
    } else {
        None
    }
}

/// Strongly typed representation of execution price impact against top-of-book quotes.
///
/// Formulas:
/// - BUY:
///   - `impact_ratio = (VWAP - best_ask) / best_ask`
///   - `worst_impact_ratio = (worst_fill_price - best_ask) / best_ask`
/// - SELL:
///   - `impact_ratio = (best_bid - VWAP) / best_bid`
///   - `worst_impact_ratio = (best_bid - worst_fill_price) / best_bid`
///
/// Units:
/// - `impact_ratio`: Decimal fraction (e.g. `0.0005` represents 0.05% or 5 bps of slippage/impact)
/// - `impact_bps`: Basis points (`impact_ratio * 10000`)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceImpact {
    pub side: Side,
    pub best_price: Decimal,
    pub vwap: Decimal,
    pub worst_fill_price: Decimal,
    pub impact_ratio: Decimal,
    pub worst_impact_ratio: Decimal,
    pub fully_filled: bool,
}

impl PriceImpact {
    /// Calculate price impact from an `ExecutionEstimate` and the starting top-of-book quote (`best_ask` for Buy, `best_bid` for Sell).
    ///
    /// Returns `None` if:
    /// - `estimate.vwap` is `None` (0 quantity filled)
    /// - `estimate.worst_fill_price` is `None`
    /// - `best_price <= 0` (invalid quote)
    pub fn calculate(estimate: &ExecutionEstimate, best_price: Decimal) -> Option<Self> {
        if best_price <= Decimal::ZERO {
            return None;
        }
        let vwap = estimate.vwap?;
        let worst_price = estimate.worst_fill_price?;

        let (impact_ratio, worst_impact_ratio) = match estimate.side {
            Side::Buy => {
                let diff = vwap - best_price;
                let worst_diff = worst_price - best_price;
                (diff / best_price, worst_diff / best_price)
            }
            Side::Sell => {
                let diff = best_price - vwap;
                let worst_diff = best_price - worst_price;
                (diff / best_price, worst_diff / best_price)
            }
        };

        Some(Self {
            side: estimate.side,
            best_price,
            vwap,
            worst_fill_price: worst_price,
            impact_ratio,
            worst_impact_ratio,
            fully_filled: estimate.fully_filled,
        })
    }

    /// Price impact expressed in basis points (1 bp = 0.01% = 0.0001).
    pub fn impact_bps(&self) -> Decimal {
        self.impact_ratio * Decimal::from(10_000)
    }

    /// Worst-level price impact expressed in basis points.
    pub fn worst_impact_bps(&self) -> Decimal {
        self.worst_impact_ratio * Decimal::from(10_000)
    }
}
