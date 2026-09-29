pub mod costs;
pub mod depth;
pub mod fees;
pub mod vwap;

pub use costs::{CrossBookComparison, ExecutionCostBreakdown, compare_cross_book};
pub use depth::{ExecutionEstimate, walk_depth, walk_levels};
pub use fees::{FeeKey, FeeSchedule, VenueFeeRegistry};
pub use vwap::{PriceImpact, calculate_vwap};
