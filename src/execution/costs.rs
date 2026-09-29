use crate::error::{EngineError, Result};
use crate::execution::depth::{ExecutionEstimate, walk_depth};
use crate::execution::fees::FeeSchedule;
use crate::market::OrderBook;
use crate::types::{MarketType, Side, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Detailed breakdown of visible depth execution combined with explicit configured fee and cost assumptions.
///
/// Disentangles purely observed market facts (`estimate`) from operational cost assumptions (`fee_amount`, `other_cost_amount`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCostBreakdown {
    /// Pure observed execution estimate from visible depth
    pub estimate: ExecutionEstimate,
    /// Configured trading fee in quote currency
    pub fee_amount: Decimal,
    /// Fee rate applied (decimal fraction, e.g. 0.001 = 0.1%)
    pub fee_rate: Decimal,
    /// Additional configured proportional costs (e.g. slippage buffer, regulatory, exchange charges)
    pub other_cost_amount: Decimal,
    /// Rate for other configured costs (decimal fraction)
    pub other_cost_rate: Decimal,
    /// Total operational costs (fee_amount + other_cost_amount)
    pub total_cost_amount: Decimal,
    /// Effective unit price realized after all configured costs:
    /// - For BUY: `(notional + total_cost) / filled_quantity` (higher effective purchase price)
    /// - For SELL: `(notional - total_cost) / filled_quantity` (lower effective net proceeds)
    pub effective_price: Option<Decimal>,
}

impl ExecutionCostBreakdown {
    /// Apply a fee schedule and optional additional cost rate to an execution estimate.
    pub fn new(
        estimate: ExecutionEstimate,
        fee_schedule: &FeeSchedule,
        is_taker: bool,
        other_cost_rate: Decimal,
    ) -> Result<Self> {
        if other_cost_rate < Decimal::ZERO {
            return Err(EngineError::Validation(format!(
                "other_cost_rate cannot be negative: {other_cost_rate}"
            )));
        }

        let (fee_amount, fee_rate) = if is_taker {
            (
                fee_schedule.calculate_taker_fee(estimate.notional),
                fee_schedule.taker_rate,
            )
        } else {
            (
                fee_schedule.calculate_maker_fee(estimate.notional),
                fee_schedule.maker_rate,
            )
        };

        let other_cost_amount = if estimate.notional > Decimal::ZERO {
            estimate.notional * other_cost_rate
        } else {
            Decimal::ZERO
        };

        let total_cost_amount = fee_amount + other_cost_amount;

        let effective_price = if estimate.filled_quantity > Decimal::ZERO {
            match estimate.side {
                Side::Buy => {
                    let total_gross_and_costs = estimate.notional + total_cost_amount;
                    Some(total_gross_and_costs / estimate.filled_quantity)
                }
                Side::Sell => {
                    let net_proceeds = estimate.notional - total_cost_amount;
                    Some(net_proceeds / estimate.filled_quantity)
                }
            }
        } else {
            None
        };

        Ok(Self {
            estimate,
            fee_amount,
            fee_rate,
            other_cost_amount,
            other_cost_rate,
            total_cost_amount,
            effective_price,
        })
    }
}

/// Generic mathematical primitive comparing executable prices between two trusted order books for the SAME requested quantity.
///
/// Convention:
/// - Buy on `buy_book` (consuming asks)
/// - Sell on `sell_book` (consuming bids)
///
/// Invariants:
/// - `fully_executable == true` IF AND ONLY IF both books possess sufficient depth to completely fill `requested_quantity`.
/// - If either side is partially filled or empty, `fully_executable == false`.
/// - Never invents liquidity or extrapolates beyond visible depth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossBookComparison {
    pub buy_venue: VenueId,
    pub buy_market_type: MarketType,
    pub sell_venue: VenueId,
    pub sell_market_type: MarketType,
    pub symbol: String,
    pub requested_quantity: Decimal,
    pub buy_estimate: ExecutionEstimate,
    pub sell_estimate: ExecutionEstimate,
    pub fully_executable: bool,
    pub matched_quantity: Decimal,
    pub gross_spread: Option<Decimal>,
    pub gross_spread_bps: Option<Decimal>,
    pub buy_notional_matched: Decimal,
    pub sell_notional_matched: Decimal,
    pub gross_pnl: Option<Decimal>,
    pub buy_fee: Decimal,
    pub sell_fee: Decimal,
    pub other_costs: Decimal,
    pub total_costs: Decimal,
    pub net_pnl: Option<Decimal>,
    pub net_spread_bps: Option<Decimal>,
}

/// Compare executable pricing across two trusted order books: Buy on `buy_book`, Sell on `sell_book`.
pub fn compare_cross_book(
    buy_book: &OrderBook,
    sell_book: &OrderBook,
    requested_quantity: Decimal,
    buy_fee_schedule: &FeeSchedule,
    sell_fee_schedule: &FeeSchedule,
    other_cost_rate: Decimal,
) -> Result<CrossBookComparison> {
    if requested_quantity <= Decimal::ZERO {
        return Err(EngineError::InvalidQuantity(requested_quantity));
    }
    if other_cost_rate < Decimal::ZERO {
        return Err(EngineError::Validation(format!(
            "other_cost_rate cannot be negative: {other_cost_rate}"
        )));
    }

    let buy_estimate = walk_depth(buy_book, Side::Buy, requested_quantity)?;
    let sell_estimate = walk_depth(sell_book, Side::Sell, requested_quantity)?;

    let fully_executable = buy_estimate.fully_filled && sell_estimate.fully_filled;
    let matched_quantity = buy_estimate
        .filled_quantity
        .min(sell_estimate.filled_quantity);

    let (
        gross_spread,
        gross_spread_bps,
        buy_notional_matched,
        sell_notional_matched,
        gross_pnl,
        buy_fee,
        sell_fee,
        other_costs,
        total_costs,
        net_pnl,
        net_spread_bps,
    ) = match (buy_estimate.vwap, sell_estimate.vwap) {
        (Some(buy_vwap), Some(sell_vwap)) if matched_quantity > Decimal::ZERO => {
            let spread = sell_vwap - buy_vwap;
            let spread_bps = (spread / buy_vwap) * Decimal::from(10_000);

            let buy_notional_m = buy_vwap * matched_quantity;
            let sell_notional_m = sell_vwap * matched_quantity;
            let gross_profit = sell_notional_m - buy_notional_m;

            let b_fee = buy_fee_schedule.calculate_taker_fee(buy_notional_m);
            let s_fee = sell_fee_schedule.calculate_taker_fee(sell_notional_m);
            let o_cost = (buy_notional_m + sell_notional_m) * other_cost_rate;
            let tot_costs = b_fee + s_fee + o_cost;

            let net_profit = gross_profit - tot_costs;
            let net_bps = (net_profit / buy_notional_m) * Decimal::from(10_000);

            (
                Some(spread),
                Some(spread_bps),
                buy_notional_m,
                sell_notional_m,
                Some(gross_profit),
                b_fee,
                s_fee,
                o_cost,
                tot_costs,
                Some(net_profit),
                Some(net_bps),
            )
        }
        _ => (
            None,
            None,
            Decimal::ZERO,
            Decimal::ZERO,
            None,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            None,
            None,
        ),
    };

    Ok(CrossBookComparison {
        buy_venue: buy_book.venue,
        buy_market_type: buy_book.market_type,
        sell_venue: sell_book.venue,
        sell_market_type: sell_book.market_type,
        symbol: buy_book.symbol.clone(),
        requested_quantity,
        buy_estimate,
        sell_estimate,
        fully_executable,
        matched_quantity,
        gross_spread,
        gross_spread_bps,
        buy_notional_matched,
        sell_notional_matched,
        gross_pnl,
        buy_fee,
        sell_fee,
        other_costs,
        total_costs,
        net_pnl,
        net_spread_bps,
    })
}
