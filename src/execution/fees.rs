use crate::error::{EngineError, Result};
use crate::types::{MarketType, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configurable fee schedule for an exchange instrument.
///
/// Units:
/// - `taker_rate`: Proportional fee rate as a decimal fraction (e.g. `0.0010` = 0.10% = 10 bps, `0.0005` = 0.05% = 5 bps).
/// - `maker_rate`: Proportional fee rate as a decimal fraction (e.g. `0.0002` = 0.02% = 2 bps).
/// - `fixed_fee`: Absolute fixed transaction cost denominated in the quote asset (e.g. `0.0` for crypto exchanges).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeSchedule {
    pub taker_rate: Decimal,
    pub maker_rate: Decimal,
    pub fixed_fee: Decimal,
}

impl Default for FeeSchedule {
    fn default() -> Self {
        Self {
            taker_rate: Decimal::ZERO,
            maker_rate: Decimal::ZERO,
            fixed_fee: Decimal::ZERO,
        }
    }
}

impl FeeSchedule {
    /// Construct a new fee schedule with strict non-negative validation.
    pub fn new(taker_rate: Decimal, maker_rate: Decimal, fixed_fee: Decimal) -> Result<Self> {
        if taker_rate < Decimal::ZERO {
            return Err(EngineError::Validation(format!(
                "Taker fee rate cannot be negative: {taker_rate}"
            )));
        }
        if maker_rate < Decimal::ZERO {
            return Err(EngineError::Validation(format!(
                "Maker fee rate cannot be negative: {maker_rate}"
            )));
        }
        if fixed_fee < Decimal::ZERO {
            return Err(EngineError::Validation(format!(
                "Fixed fee cannot be negative: {fixed_fee}"
            )));
        }
        Ok(Self {
            taker_rate,
            maker_rate,
            fixed_fee,
        })
    }

    /// Construct a purely proportional taker fee schedule with zero fixed cost.
    pub fn taker_only(taker_rate: Decimal) -> Result<Self> {
        Self::new(taker_rate, Decimal::ZERO, Decimal::ZERO)
    }

    /// Construct a zero-fee schedule (for testing or fee-free pairs).
    pub fn zero() -> Self {
        Self::default()
    }

    /// Calculate total taker fee in quote currency for a given notional amount.
    ///
    /// Formula: `(notional * taker_rate) + fixed_fee`
    #[inline]
    pub fn calculate_taker_fee(&self, notional: Decimal) -> Decimal {
        if notional <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            (notional * self.taker_rate) + self.fixed_fee
        }
    }

    /// Calculate total maker fee in quote currency for a given notional amount.
    ///
    /// Formula: `(notional * maker_rate) + fixed_fee`
    #[inline]
    pub fn calculate_maker_fee(&self, notional: Decimal) -> Decimal {
        if notional <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            (notional * self.maker_rate) + self.fixed_fee
        }
    }
}

/// Key identifying a specific venue and market type for fee resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FeeKey {
    pub venue: VenueId,
    pub market_type: MarketType,
}

impl FeeKey {
    pub fn new(venue: VenueId, market_type: MarketType) -> Self {
        Self { venue, market_type }
    }
}

/// Registry mapping `(VenueId, MarketType)` to specific configured fee schedules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueFeeRegistry {
    pub schedules: HashMap<FeeKey, FeeSchedule>,
    pub fallback: FeeSchedule,
}

impl Default for VenueFeeRegistry {
    /// Initialize with verified standard Tier-0 exchange baseline taker fees:
    /// - Binance Spot: 10 bps (0.0010)
    /// - Binance Linear Perpetual: 5 bps (0.0005)
    /// - Bybit Spot: 10 bps (0.0010)
    /// - Bybit Linear Perpetual: 5.5 bps (0.00055)
    fn default() -> Self {
        let mut schedules = HashMap::new();

        // Binance Spot: 10 bps taker, 10 bps maker
        schedules.insert(
            FeeKey::new(VenueId::Binance, MarketType::Spot),
            FeeSchedule {
                taker_rate: Decimal::new(10, 4), // 0.0010
                maker_rate: Decimal::new(10, 4), // 0.0010
                fixed_fee: Decimal::ZERO,
            },
        );

        // Binance Linear Perpetual: 5 bps taker, 2 bps maker
        schedules.insert(
            FeeKey::new(VenueId::Binance, MarketType::LinearPerpetual),
            FeeSchedule {
                taker_rate: Decimal::new(5, 4), // 0.0005
                maker_rate: Decimal::new(2, 4), // 0.0002
                fixed_fee: Decimal::ZERO,
            },
        );

        // Bybit Spot: 10 bps taker, 10 bps maker
        schedules.insert(
            FeeKey::new(VenueId::Bybit, MarketType::Spot),
            FeeSchedule {
                taker_rate: Decimal::new(10, 4), // 0.0010
                maker_rate: Decimal::new(10, 4), // 0.0010
                fixed_fee: Decimal::ZERO,
            },
        );

        // Bybit Linear Perpetual: 5.5 bps taker, 2 bps maker
        schedules.insert(
            FeeKey::new(VenueId::Bybit, MarketType::LinearPerpetual),
            FeeSchedule {
                taker_rate: Decimal::new(55, 5), // 0.00055
                maker_rate: Decimal::new(20, 5), // 0.00020
                fixed_fee: Decimal::ZERO,
            },
        );

        Self {
            schedules,
            fallback: FeeSchedule::default(),
        }
    }
}

impl VenueFeeRegistry {
    /// Construct an empty registry with a custom fallback fee schedule.
    pub fn empty(fallback: FeeSchedule) -> Self {
        Self {
            schedules: HashMap::new(),
            fallback,
        }
    }

    /// Register or override a fee schedule for a specific venue and market type.
    pub fn set_schedule(&mut self, venue: VenueId, market_type: MarketType, schedule: FeeSchedule) {
        self.schedules
            .insert(FeeKey::new(venue, market_type), schedule);
    }

    /// Resolve the fee schedule for a venue and market type, falling back to the default if unconfigured.
    pub fn get_schedule(&self, venue: VenueId, market_type: MarketType) -> &FeeSchedule {
        self.schedules
            .get(&FeeKey::new(venue, market_type))
            .unwrap_or(&self.fallback)
    }
}
