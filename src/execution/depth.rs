use crate::error::{EngineError, Result};
use crate::market::OrderBook;
use crate::types::{PriceLevel, Side};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Strongly typed result of walking visible limit order book depth for a requested quantity.
///
/// Units:
/// - `requested_quantity`: Base asset units (e.g. BTC)
/// - `filled_quantity`: Base asset units actually filled across visible depth (e.g. BTC)
/// - `remaining_quantity`: Base asset units unfilled due to depth depletion (e.g. BTC)
/// - `notional`: Quote asset units consumed or received (e.g. USDT)
/// - `vwap`: Volume-Weighted Average Price in quote currency per base unit (e.g. USDT/BTC)
/// - `worst_fill_price`: Price of the deepest level consumed (e.g. USDT/BTC)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEstimate {
    pub side: Side,
    pub requested_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub notional: Decimal,
    pub vwap: Option<Decimal>,
    pub worst_fill_price: Option<Decimal>,
    pub fully_filled: bool,
    pub levels_consumed: usize,
}

impl ExecutionEstimate {
    /// True if the order book possessed sufficient visible depth to satisfy 100% of the requested quantity.
    #[inline]
    pub fn is_fully_executable(&self) -> bool {
        self.fully_filled
    }

    /// True if only a portion of the requested quantity could be executed against visible depth.
    #[inline]
    pub fn is_partially_executable(&self) -> bool {
        !self.fully_filled && self.filled_quantity > Decimal::ZERO
    }

    /// True if zero quantity could be executed (e.g. empty order book).
    #[inline]
    pub fn is_not_executable(&self) -> bool {
        self.filled_quantity == Decimal::ZERO
    }
}

/// Walk visible order-book depth for a requested quantity and side.
///
/// - For `Side::Buy`: Consumes asks from lowest price upward.
/// - For `Side::Sell`: Consumes bids from highest price downward.
///
/// Invariants:
/// - `requested_quantity` must be strictly positive (`> 0`). Non-positive quantities return `Err(EngineError::InvalidQuantity)`.
/// - Operates immutably as a read-only projection over the trusted book.
/// - Any malformed price level (`price <= 0` or `quantity < 0`) triggers an immediate error.
/// - O(K) complexity where K is the number of levels consumed (at most available depth).
pub fn walk_depth(
    book: &OrderBook,
    side: Side,
    requested_quantity: Decimal,
) -> Result<ExecutionEstimate> {
    let levels = match side {
        Side::Buy => &book.asks,
        Side::Sell => &book.bids,
    };
    walk_levels(levels, side, requested_quantity)
}

/// Walk a pre-sorted slice of price levels (`asks` ascending or `bids` descending).
pub fn walk_levels(
    levels: &[PriceLevel],
    side: Side,
    requested_quantity: Decimal,
) -> Result<ExecutionEstimate> {
    if requested_quantity <= Decimal::ZERO {
        return Err(EngineError::InvalidQuantity(requested_quantity));
    }

    let mut remaining = requested_quantity;
    let mut filled = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut worst_fill_price: Option<Decimal> = None;
    let mut levels_consumed = 0usize;

    for level in levels {
        if level.price <= Decimal::ZERO {
            return Err(EngineError::OrderBookInvariant(format!(
                "Malformed order book level: price {} must be positive",
                level.price
            )));
        }
        if level.quantity < Decimal::ZERO {
            return Err(EngineError::OrderBookInvariant(format!(
                "Malformed order book level: quantity {} cannot be negative",
                level.quantity
            )));
        }
        if level.quantity == Decimal::ZERO {
            continue;
        }

        let fill_qty = remaining.min(level.quantity);
        notional += fill_qty * level.price;
        filled += fill_qty;
        remaining -= fill_qty;
        worst_fill_price = Some(level.price);
        levels_consumed += 1;

        if remaining == Decimal::ZERO {
            break;
        }
    }

    let fully_filled = remaining == Decimal::ZERO;
    let vwap = if filled > Decimal::ZERO {
        Some(notional / filled)
    } else {
        None
    };

    Ok(ExecutionEstimate {
        side,
        requested_quantity,
        filled_quantity: filled,
        remaining_quantity: remaining,
        notional,
        vwap,
        worst_fill_price,
        fully_filled,
        levels_consumed,
    })
}
