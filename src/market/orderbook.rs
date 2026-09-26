use crate::error::{EngineError, Result};
use crate::types::{MarketType, PriceLevel, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Canonical in-memory limit order book representing the top N levels.
/// Bids are sorted descending (highest price first).
/// Asks are sorted ascending (lowest price first).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBook {
    pub venue: VenueId,
    pub market_type: MarketType,
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts_ms: i64,
    pub local_recv_ts_ns: i64,
    pub sequence_id: u64,
}

impl OrderBook {
    /// Construct an empty order book for a given instrument.
    pub fn new(venue: VenueId, market_type: MarketType, symbol: impl Into<String>) -> Self {
        Self {
            venue,
            market_type,
            symbol: symbol.into(),
            bids: Vec::with_capacity(50),
            asks: Vec::with_capacity(50),
            exchange_ts_ms: 0,
            local_recv_ts_ns: 0,
            sequence_id: 0,
        }
    }

    /// Best available bid quote.
    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids.first().copied()
    }

    /// Best available ask quote.
    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks.first().copied()
    }

    /// Absolute top-of-book spread (best_ask - best_bid).
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.price - bid.price),
            _ => None,
        }
    }

    /// Check if the order book is in an invalid crossed or locked state.
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid.price >= ask.price,
            _ => false,
        }
    }

    /// Overwrite order book levels with a full snapshot.
    /// Enforces sorting: bids descending, asks ascending.
    pub fn set_snapshot(
        &mut self,
        mut bids: Vec<PriceLevel>,
        mut asks: Vec<PriceLevel>,
        exchange_ts_ms: i64,
        local_recv_ts_ns: i64,
        sequence_id: u64,
    ) -> Result<()> {
        // Retain only positive quantities
        bids.retain(|lvl| lvl.quantity > Decimal::ZERO);
        asks.retain(|lvl| lvl.quantity > Decimal::ZERO);

        // Sort bids descending by price
        bids.sort_by_key(|a| std::cmp::Reverse(a.price));
        // Sort asks ascending by price
        asks.sort_by_key(|a| a.price);

        self.bids = bids;
        self.asks = asks;
        self.exchange_ts_ms = exchange_ts_ms;
        self.local_recv_ts_ns = local_recv_ts_ns;
        self.sequence_id = sequence_id;

        if self.is_crossed() {
            let bid = self.best_bid().map(|b| b.price).unwrap_or(Decimal::ZERO);
            let ask = self.best_ask().map(|a| a.price).unwrap_or(Decimal::ZERO);
            return Err(EngineError::CrossedBook { bid, ask });
        }

        Ok(())
    }

    /// Apply incremental delta updates.
    /// If quantity == 0, the price level is deleted.
    /// Otherwise, the level is updated or inserted maintaining sorted order.
    pub fn apply_delta(
        &mut self,
        delta_bids: &[PriceLevel],
        delta_asks: &[PriceLevel],
        exchange_ts_ms: i64,
        local_recv_ts_ns: i64,
        sequence_id: u64,
    ) -> Result<()> {
        Self::mutate_levels(&mut self.bids, delta_bids, true);
        Self::mutate_levels(&mut self.asks, delta_asks, false);

        self.exchange_ts_ms = exchange_ts_ms;
        self.local_recv_ts_ns = local_recv_ts_ns;
        self.sequence_id = sequence_id;

        if self.is_crossed() {
            let bid = self.best_bid().map(|b| b.price).unwrap_or(Decimal::ZERO);
            let ask = self.best_ask().map(|a| a.price).unwrap_or(Decimal::ZERO);
            return Err(EngineError::CrossedBook { bid, ask });
        }

        Ok(())
    }

    fn mutate_levels(book_side: &mut Vec<PriceLevel>, deltas: &[PriceLevel], is_bid: bool) {
        for delta in deltas {
            if let Some(pos) = book_side.iter().position(|lvl| lvl.price == delta.price) {
                if delta.quantity <= Decimal::ZERO {
                    book_side.remove(pos);
                } else {
                    book_side[pos].quantity = delta.quantity;
                }
            } else if delta.quantity > Decimal::ZERO {
                book_side.push(*delta);
            }
        }

        if is_bid {
            book_side.sort_by_key(|a| std::cmp::Reverse(a.price));
        } else {
            book_side.sort_by_key(|a| a.price);
        }
    }
}
