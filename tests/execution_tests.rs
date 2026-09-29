use airbitrage::error::EngineError;
use airbitrage::execution::{
    ExecutionCostBreakdown, ExecutionEstimate, FeeSchedule, PriceImpact, VenueFeeRegistry,
    calculate_vwap, compare_cross_book, walk_depth, walk_levels,
};
use airbitrage::market::state::{BookLifecycleState, InvalidationReason};
use airbitrage::market::{MarketStateManager, OrderBook};
use airbitrage::types::{MarketType, PriceLevel, Side, VenueId};
use rust_decimal_macros::dec;
use std::time::Duration;

// Helper to construct a synthetic OrderBook
fn make_test_book(
    venue: VenueId,
    market_type: MarketType,
    symbol: &str,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
) -> OrderBook {
    let mut book = OrderBook::new(venue, market_type, symbol);
    book.set_snapshot(bids, asks, 1000, 1000, 1).unwrap();
    book
}

// ============================================================================
// 1. BASIC BUY EXECUTION
// ============================================================================

#[test]
fn test_basic_buy_single_level() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(101.00), dec!(1.0)),
        ],
    );

    let estimate = walk_depth(&book, Side::Buy, dec!(0.5)).unwrap();

    assert_eq!(estimate.side, Side::Buy);
    assert_eq!(estimate.requested_quantity, dec!(0.5));
    assert_eq!(estimate.filled_quantity, dec!(0.5));
    assert_eq!(estimate.remaining_quantity, dec!(0.0));
    assert_eq!(estimate.notional, dec!(50.00));
    assert_eq!(estimate.vwap, Some(dec!(100.00)));
    assert_eq!(estimate.worst_fill_price, Some(dec!(100.00)));
    assert!(estimate.is_fully_executable());
    assert!(!estimate.is_partially_executable());
    assert!(!estimate.is_not_executable());
    assert_eq!(estimate.levels_consumed, 1);
}

// ============================================================================
// 2. MULTI-LEVEL BUY EXECUTION
// ============================================================================

#[test]
fn test_multi_level_buy() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(101.00), dec!(2.0)),
            PriceLevel::new(dec!(102.00), dec!(5.0)),
        ],
    );

    // Request 2.0 BTC: 1.0 @ 100.00 + 1.0 @ 101.00 = 201.00 notional, VWAP = 100.50
    let estimate = walk_depth(&book, Side::Buy, dec!(2.0)).unwrap();

    assert_eq!(estimate.filled_quantity, dec!(2.0));
    assert_eq!(estimate.remaining_quantity, dec!(0.0));
    assert_eq!(estimate.notional, dec!(201.00));
    assert_eq!(estimate.vwap, Some(dec!(100.50)));
    assert_eq!(estimate.worst_fill_price, Some(dec!(101.00)));
    assert!(estimate.is_fully_executable());
    assert_eq!(estimate.levels_consumed, 2);
}

// ============================================================================
// 3. BASIC SELL EXECUTION
// ============================================================================

#[test]
fn test_basic_sell_multi_level() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(99.00), dec!(2.0)),
        ],
        vec![PriceLevel::new(dec!(101.00), dec!(1.0))],
    );

    // Request 1.5 BTC: 1.0 @ 100.00 + 0.5 @ 99.00 = 100 + 49.50 = 149.50 notional
    let estimate = walk_depth(&book, Side::Sell, dec!(1.5)).unwrap();

    assert_eq!(estimate.side, Side::Sell);
    assert_eq!(estimate.filled_quantity, dec!(1.5));
    assert_eq!(estimate.remaining_quantity, dec!(0.0));
    assert_eq!(estimate.notional, dec!(149.50));
    // 149.50 / 1.5 = 99.66666666666666666666666667
    assert_eq!(estimate.vwap, Some(dec!(149.50) / dec!(1.5)));
    assert_eq!(estimate.worst_fill_price, Some(dec!(99.00)));
    assert!(estimate.is_fully_executable());
    assert_eq!(estimate.levels_consumed, 2);
}

// ============================================================================
// 4. EXACT LEVEL BOUNDARY
// ============================================================================

#[test]
fn test_exact_level_boundary() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(101.00), dec!(2.0)),
        ],
    );

    // Exactly 1.0 BTC matches level 1 quantity exactly
    let estimate = walk_depth(&book, Side::Buy, dec!(1.0)).unwrap();

    assert_eq!(estimate.filled_quantity, dec!(1.0));
    assert_eq!(estimate.remaining_quantity, dec!(0.0));
    assert_eq!(estimate.notional, dec!(100.00));
    assert_eq!(estimate.vwap, Some(dec!(100.00)));
    assert_eq!(estimate.worst_fill_price, Some(dec!(100.00)));
    assert_eq!(estimate.levels_consumed, 1);
    assert!(estimate.is_fully_executable());
}

// ============================================================================
// 5. MULTI-LEVEL BOUNDARY
// ============================================================================

#[test]
fn test_multi_level_exact_boundary() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(101.00), dec!(2.0)),
            PriceLevel::new(dec!(102.00), dec!(5.0)),
        ],
    );

    // Exactly 3.0 BTC matches level 1 (1.0) + level 2 (2.0)
    let estimate = walk_depth(&book, Side::Buy, dec!(3.0)).unwrap();

    assert_eq!(estimate.filled_quantity, dec!(3.0));
    assert_eq!(estimate.remaining_quantity, dec!(0.0));
    // 1*100 + 2*101 = 302
    assert_eq!(estimate.notional, dec!(302.00));
    assert_eq!(estimate.vwap, Some(dec!(302.00) / dec!(3.0)));
    assert_eq!(estimate.worst_fill_price, Some(dec!(101.00)));
    assert_eq!(estimate.levels_consumed, 2);
    assert!(estimate.is_fully_executable());
}

// ============================================================================
// 6. PARTIAL LIQUIDITY / FILL HANDLING
// ============================================================================

#[test]
fn test_partial_fill_insufficient_depth() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(101.00), dec!(2.0)),
        ],
    );

    // Total available is 3.0 BTC. Request is 5.0 BTC.
    let estimate = walk_depth(&book, Side::Buy, dec!(5.0)).unwrap();

    assert_eq!(estimate.requested_quantity, dec!(5.0));
    assert_eq!(estimate.filled_quantity, dec!(3.0));
    assert_eq!(estimate.remaining_quantity, dec!(2.0));
    assert_eq!(estimate.notional, dec!(302.00));
    assert_eq!(estimate.vwap, Some(dec!(302.00) / dec!(3.0)));
    assert_eq!(estimate.worst_fill_price, Some(dec!(101.00)));
    assert!(!estimate.is_fully_executable());
    assert!(estimate.is_partially_executable());
    assert!(!estimate.is_not_executable());
    assert_eq!(estimate.levels_consumed, 2);
}

// ============================================================================
// 7. EMPTY BOOK HANDLING
// ============================================================================

#[test]
fn test_empty_book_non_executable() {
    let empty_book = OrderBook::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    let buy_estimate = walk_depth(&empty_book, Side::Buy, dec!(1.0)).unwrap();
    assert_eq!(buy_estimate.requested_quantity, dec!(1.0));
    assert_eq!(buy_estimate.filled_quantity, dec!(0.0));
    assert_eq!(buy_estimate.remaining_quantity, dec!(1.0));
    assert_eq!(buy_estimate.notional, dec!(0.0));
    assert_eq!(buy_estimate.vwap, None);
    assert_eq!(buy_estimate.worst_fill_price, None);
    assert!(buy_estimate.is_not_executable());
    assert!(!buy_estimate.is_fully_executable());
    assert!(!buy_estimate.is_partially_executable());
    assert_eq!(buy_estimate.levels_consumed, 0);

    let sell_estimate = walk_depth(&empty_book, Side::Sell, dec!(1.0)).unwrap();
    assert!(sell_estimate.is_not_executable());
    assert_eq!(sell_estimate.vwap, None);
}

// ============================================================================
// 8. ZERO QUANTITY REJECTION
// ============================================================================

#[test]
fn test_zero_quantity_rejected() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![PriceLevel::new(dec!(100.00), dec!(1.0))],
    );

    let res = walk_depth(&book, Side::Buy, dec!(0.0));
    assert!(res.is_err());
    match res.unwrap_err() {
        EngineError::InvalidQuantity(q) => assert_eq!(q, dec!(0.0)),
        other => panic!("Expected InvalidQuantity, got: {:?}", other),
    }
}

// ============================================================================
// 9. NEGATIVE QUANTITY REJECTION
// ============================================================================

#[test]
fn test_negative_quantity_rejected() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![PriceLevel::new(dec!(100.00), dec!(1.0))],
    );

    let res = walk_depth(&book, Side::Sell, dec!(-2.5));
    assert!(res.is_err());
    match res.unwrap_err() {
        EngineError::InvalidQuantity(q) => assert_eq!(q, dec!(-2.5)),
        other => panic!("Expected InvalidQuantity, got: {:?}", other),
    }
}

// ============================================================================
// 10. INVALID PRICE LEVEL REJECTION
// ============================================================================

#[test]
fn test_invalid_level_price_rejected() {
    let bad_levels = vec![
        PriceLevel::new(dec!(0.0), dec!(1.0)),
        PriceLevel::new(dec!(100.0), dec!(1.0)),
    ];

    let res = walk_levels(&bad_levels, Side::Buy, dec!(1.0));
    assert!(res.is_err());
    match res.unwrap_err() {
        EngineError::OrderBookInvariant(msg) => assert!(msg.contains("must be positive")),
        other => panic!("Expected OrderBookInvariant, got: {:?}", other),
    }
}

// ============================================================================
// 11. INVALID LEVEL QUANTITY REJECTION
// ============================================================================

#[test]
fn test_invalid_level_quantity_rejected() {
    let bad_levels = vec![PriceLevel::new(dec!(100.0), dec!(-1.0))];

    let res = walk_levels(&bad_levels, Side::Buy, dec!(1.0));
    assert!(res.is_err());
    match res.unwrap_err() {
        EngineError::OrderBookInvariant(msg) => assert!(msg.contains("cannot be negative")),
        other => panic!("Expected OrderBookInvariant, got: {:?}", other),
    }
}

// ============================================================================
// 12. DECIMAL PRECISION & ACCURACY
// ============================================================================

#[test]
fn test_decimal_precision_exactness() {
    // Exact Satoshi and micro-USD values that lose precision in f64
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![],
        vec![
            PriceLevel::new(dec!(83125.12345678), dec!(0.12345678)),
            PriceLevel::new(dec!(83125.87654321), dec!(0.87654322)),
        ],
    );

    let estimate = walk_depth(&book, Side::Buy, dec!(1.0)).unwrap();
    assert_eq!(estimate.filled_quantity, dec!(1.0));
    assert!(estimate.is_fully_executable());

    // Verify exact notional sum: (83125.12345678 * 0.12345678) + (83125.87654321 * 0.87654322)
    let expected_notional =
        (dec!(83125.12345678) * dec!(0.12345678)) + (dec!(83125.87654321) * dec!(0.87654322));
    assert_eq!(estimate.notional, expected_notional);
    assert_eq!(estimate.vwap, Some(expected_notional));
}

// ============================================================================
// 13. VWAP STANDALONE FORMULA
// ============================================================================

#[test]
fn test_vwap_standalone_calculation() {
    let levels = vec![
        (dec!(100.00), dec!(1.0)),
        (dec!(105.00), dec!(2.0)),
        (dec!(110.00), dec!(1.0)),
    ];

    // (100*1 + 105*2 + 110*1) / 4 = (100 + 210 + 110) / 4 = 420 / 4 = 105.00
    let vwap = calculate_vwap(&levels);
    assert_eq!(vwap, Some(dec!(105.00)));

    // Empty levels -> None
    assert_eq!(calculate_vwap(&[]), None);
}

// ============================================================================
// 14. WORST FILL PRICE TRACKING
// ============================================================================

#[test]
fn test_worst_fill_price_tracking() {
    let book = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(101.00), dec!(2.0)),
            PriceLevel::new(dec!(105.00), dec!(3.0)),
        ],
    );

    // Fill partially into level 3
    let estimate = walk_depth(&book, Side::Buy, dec!(4.0)).unwrap();
    // Consumes 1.0 @ 100, 2.0 @ 101, 1.0 @ 105 -> worst fill price is 105.00
    assert_eq!(estimate.worst_fill_price, Some(dec!(105.00)));
    assert_eq!(estimate.levels_consumed, 3);
}

// ============================================================================
// 15. PRICE IMPACT CALCULATIONS (BUY & SELL)
// ============================================================================

#[test]
fn test_price_impact_buy_and_sell() {
    // BUY side impact: (VWAP - best_ask) / best_ask
    let buy_estimate = ExecutionEstimate {
        side: Side::Buy,
        requested_quantity: dec!(2.0),
        filled_quantity: dec!(2.0),
        remaining_quantity: dec!(0.0),
        notional: dec!(201.00),
        vwap: Some(dec!(100.50)),
        worst_fill_price: Some(dec!(101.00)),
        fully_filled: true,
        levels_consumed: 2,
    };
    let best_ask = dec!(100.00);
    let buy_impact = PriceImpact::calculate(&buy_estimate, best_ask).unwrap();

    // impact = (100.50 - 100.00) / 100.00 = 0.50 / 100.00 = 0.0050 (50 bps)
    assert_eq!(buy_impact.impact_ratio, dec!(0.0050));
    assert_eq!(buy_impact.impact_bps(), dec!(50.0));
    // worst impact = (101.00 - 100.00) / 100.00 = 0.0100 (100 bps)
    assert_eq!(buy_impact.worst_impact_ratio, dec!(0.0100));
    assert_eq!(buy_impact.worst_impact_bps(), dec!(100.0));

    // SELL side impact: (best_bid - VWAP) / best_bid
    let sell_estimate = ExecutionEstimate {
        side: Side::Sell,
        requested_quantity: dec!(2.0),
        filled_quantity: dec!(2.0),
        remaining_quantity: dec!(0.0),
        notional: dec!(199.00),
        vwap: Some(dec!(99.50)),
        worst_fill_price: Some(dec!(99.00)),
        fully_filled: true,
        levels_consumed: 2,
    };
    let best_bid = dec!(100.00);
    let sell_impact = PriceImpact::calculate(&sell_estimate, best_bid).unwrap();

    // impact = (100.00 - 99.50) / 100.00 = 0.50 / 100.00 = 0.0050 (50 bps)
    assert_eq!(sell_impact.impact_ratio, dec!(0.0050));
    assert_eq!(sell_impact.impact_bps(), dec!(50.0));
}

// ============================================================================
// 16. CONFIGURABLE FEE MODEL
// ============================================================================

#[test]
fn test_fee_schedule_and_cost_breakdown() {
    let fee_schedule = FeeSchedule::new(
        dec!(0.0005), // 5 bps taker
        dec!(0.0002), // 2 bps maker
        dec!(1.50),   // $1.50 fixed fee
    )
    .unwrap();

    let notional = dec!(10_000.00);
    let taker_fee = fee_schedule.calculate_taker_fee(notional);
    // (10,000 * 0.0005) + 1.50 = 5.00 + 1.50 = 6.50
    assert_eq!(taker_fee, dec!(6.50));

    let maker_fee = fee_schedule.calculate_maker_fee(notional);
    // (10,000 * 0.0002) + 1.50 = 2.00 + 1.50 = 3.50
    assert_eq!(maker_fee, dec!(3.50));

    // Execution Cost Breakdown on BUY
    let estimate = ExecutionEstimate {
        side: Side::Buy,
        requested_quantity: dec!(1.0),
        filled_quantity: dec!(1.0),
        remaining_quantity: dec!(0.0),
        notional: dec!(10_000.00),
        vwap: Some(dec!(10_000.00)),
        worst_fill_price: Some(dec!(10_000.00)),
        fully_filled: true,
        levels_consumed: 1,
    };

    let breakdown =
        ExecutionCostBreakdown::new(estimate, &fee_schedule, true, dec!(0.0001)).unwrap();
    assert_eq!(breakdown.fee_amount, dec!(6.50));
    assert_eq!(breakdown.other_cost_amount, dec!(1.00)); // 10,000 * 0.0001
    assert_eq!(breakdown.total_cost_amount, dec!(7.50));
    // Effective purchase price per unit = (10,000 + 7.50) / 1.0 = 10,007.50
    assert_eq!(breakdown.effective_price, Some(dec!(10007.50)));
}

#[test]
fn test_venue_fee_registry_defaults() {
    let registry = VenueFeeRegistry::default();

    let binance_spot = registry.get_schedule(VenueId::Binance, MarketType::Spot);
    assert_eq!(binance_spot.taker_rate, dec!(0.0010)); // 10 bps

    let binance_perp = registry.get_schedule(VenueId::Binance, MarketType::LinearPerpetual);
    assert_eq!(binance_perp.taker_rate, dec!(0.0005)); // 5 bps

    let bybit_spot = registry.get_schedule(VenueId::Bybit, MarketType::Spot);
    assert_eq!(bybit_spot.taker_rate, dec!(0.0010)); // 10 bps

    let bybit_perp = registry.get_schedule(VenueId::Bybit, MarketType::LinearPerpetual);
    assert_eq!(bybit_perp.taker_rate, dec!(0.00055)); // 5.5 bps
}

// ============================================================================
// 17. CROSS-BOOK COMPARISON PRIMITIVE
// ============================================================================

#[test]
fn test_cross_book_comparison_full_execution() {
    let book_a = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(101.00), dec!(2.0)),
        ],
    );

    let book_b = make_test_book(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![
            PriceLevel::new(dec!(103.00), dec!(1.0)),
            PriceLevel::new(dec!(102.00), dec!(2.0)),
        ],
        vec![PriceLevel::new(dec!(104.00), dec!(1.0))],
    );

    let fee_a = FeeSchedule::taker_only(dec!(0.0010)).unwrap(); // 10 bps
    let fee_b = FeeSchedule::taker_only(dec!(0.0010)).unwrap(); // 10 bps

    // Buy 2 BTC on Book A: 1 @ 100 + 1 @ 101 = 201 notional, VWAP = 100.50
    // Sell 2 BTC on Book B: 1 @ 103 + 1 @ 102 = 205 notional, VWAP = 102.50
    let cmp = compare_cross_book(&book_a, &book_b, dec!(2.0), &fee_a, &fee_b, dec!(0.0)).unwrap();

    assert!(cmp.fully_executable);
    assert_eq!(cmp.matched_quantity, dec!(2.0));
    assert_eq!(cmp.buy_estimate.vwap, Some(dec!(100.50)));
    assert_eq!(cmp.sell_estimate.vwap, Some(dec!(102.50)));
    assert_eq!(cmp.gross_spread, Some(dec!(2.00)));
    assert_eq!(cmp.buy_notional_matched, dec!(201.00));
    assert_eq!(cmp.sell_notional_matched, dec!(205.00));
    assert_eq!(cmp.gross_pnl, Some(dec!(4.00)));

    // Fees: 201 * 0.001 = 0.201; 205 * 0.001 = 0.205; total fees = 0.406
    assert_eq!(cmp.buy_fee, dec!(0.201));
    assert_eq!(cmp.sell_fee, dec!(0.205));
    assert_eq!(cmp.total_costs, dec!(0.406));

    // Net PnL = 4.00 - 0.406 = 3.594
    assert_eq!(cmp.net_pnl, Some(dec!(3.594)));
}

// ============================================================================
// 18. PARTIAL CROSS-BOOK EXECUTION
// ============================================================================

#[test]
fn test_cross_book_comparison_partial_fill() {
    let book_a = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![],
        // Only 1.0 BTC available on Book A
        vec![PriceLevel::new(dec!(100.00), dec!(1.0))],
    );

    let book_b = make_test_book(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        // 5.0 BTC available on Book B
        vec![PriceLevel::new(dec!(103.00), dec!(5.0))],
        vec![],
    );

    let fee = FeeSchedule::taker_only(dec!(0.0010)).unwrap();

    // Request 2.0 BTC: Book A can only fill 1.0 BTC!
    let cmp = compare_cross_book(&book_a, &book_b, dec!(2.0), &fee, &fee, dec!(0.0)).unwrap();

    // MUST NOT be marked fully executable!
    assert!(!cmp.fully_executable);
    assert_eq!(cmp.matched_quantity, dec!(1.0));
    assert_eq!(cmp.buy_estimate.filled_quantity, dec!(1.0));
    assert_eq!(cmp.sell_estimate.filled_quantity, dec!(2.0));
    assert_eq!(cmp.buy_estimate.remaining_quantity, dec!(1.0));
    assert_eq!(cmp.sell_estimate.remaining_quantity, dec!(0.0));
}

// ============================================================================
// 19. TRUSTED BOOK INTEGRATION & UNTRUSTED GATING
// ============================================================================

#[test]
fn test_market_state_manager_trusted_book_gating() {
    let mut manager = MarketStateManager::new(Duration::from_millis(1000));
    manager.register_instrument(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    let now_ns = 1_000_000_000i64;

    // 1. Untrusted: initially AwaitingSnapshot -> estimate_execution must return None!
    let uninit = manager
        .estimate_execution(
            VenueId::Binance,
            MarketType::Spot,
            "BTCUSDT",
            Side::Buy,
            dec!(1.0),
            now_ns,
        )
        .unwrap();
    assert!(
        uninit.is_none(),
        "Uninitialized state must return None for execution"
    );

    // 2. Apply fresh valid snapshot -> becomes Live & Trusted
    let snap = airbitrage::types::MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(dec!(99.00), dec!(2.0))],
        asks: vec![PriceLevel::new(dec!(100.00), dec!(2.0))],
        exchange_ts_ms: 1000,
        local_recv_ts_ns: now_ns,
        sequence_id: 100,
    };
    manager.handle_event(&snap).unwrap();
    assert!(manager.is_live(VenueId::Binance, MarketType::Spot, "BTCUSDT"));

    // Now trusted -> estimate_execution returns valid estimate!
    let live_est = manager
        .estimate_execution(
            VenueId::Binance,
            MarketType::Spot,
            "BTCUSDT",
            Side::Buy,
            dec!(1.0),
            now_ns,
        )
        .unwrap();
    assert!(live_est.is_some());
    assert_eq!(live_est.unwrap().vwap, Some(dec!(100.00)));

    // 3. Stale check -> age exceeds 1000ms threshold -> trusted_book returns None!
    let stale_now_ns = now_ns + 2_000_000_000i64; // +2000ms
    let stale_est = manager
        .estimate_execution(
            VenueId::Binance,
            MarketType::Spot,
            "BTCUSDT",
            Side::Buy,
            dec!(1.0),
            stale_now_ns,
        )
        .unwrap();
    assert!(
        stale_est.is_none(),
        "Stale state must return None for execution"
    );

    // 4. Invalidation check -> manual invalidation -> returns None!
    let state = manager
        .get_state_mut(VenueId::Binance, MarketType::Spot, "BTCUSDT")
        .unwrap();
    state.invalidate(InvalidationReason::EmptyBook);
    assert_eq!(state.lifecycle_state, BookLifecycleState::Invalidated);

    let invalid_est = manager
        .estimate_execution(
            VenueId::Binance,
            MarketType::Spot,
            "BTCUSDT",
            Side::Buy,
            dec!(1.0),
            now_ns,
        )
        .unwrap();
    assert!(
        invalid_est.is_none(),
        "Invalidated state must return None for execution"
    );
}

// ============================================================================
// 20. ACCEPTANCE TEST (SECTION 27 REQUIREMENT)
// ============================================================================

#[test]
fn test_m4_acceptance_criteria_section_27() {
    // Construct synthetic Book A
    // ASK: 100 × 1, 101 × 2, 102 × 5
    let book_a = make_test_book(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(dec!(99.00), dec!(1.0))],
        vec![
            PriceLevel::new(dec!(100.00), dec!(1.0)),
            PriceLevel::new(dec!(101.00), dec!(2.0)),
            PriceLevel::new(dec!(102.00), dec!(5.0)),
        ],
    );

    // Construct synthetic Book B
    // BID: 103 × 1, 102 × 2, 101 × 5
    let book_b = make_test_book(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![
            PriceLevel::new(dec!(103.00), dec!(1.0)),
            PriceLevel::new(dec!(102.00), dec!(2.0)),
            PriceLevel::new(dec!(101.00), dec!(5.0)),
        ],
        vec![PriceLevel::new(dec!(104.00), dec!(1.0))],
    );

    // Requested: 2.0 BTC
    let fee_schedule = FeeSchedule::taker_only(dec!(0.0010)).unwrap(); // 10 bps
    let cmp = compare_cross_book(
        &book_a,
        &book_b,
        dec!(2.0),
        &fee_schedule,
        &fee_schedule,
        dec!(0.0),
    )
    .unwrap();

    // 1. Verify BUY VWAP on A: 1.0 @ 100 + 1.0 @ 101 = 201 / 2 = 100.50
    assert_eq!(cmp.buy_estimate.vwap, Some(dec!(100.50)));
    assert_eq!(cmp.buy_estimate.notional, dec!(201.00));
    assert_eq!(cmp.buy_estimate.worst_fill_price, Some(dec!(101.00)));

    // 2. Verify SELL VWAP on B: 1.0 @ 103 + 1.0 @ 102 = 205 / 2 = 102.50
    assert_eq!(cmp.sell_estimate.vwap, Some(dec!(102.50)));
    assert_eq!(cmp.sell_estimate.notional, dec!(205.00));
    assert_eq!(cmp.sell_estimate.worst_fill_price, Some(dec!(102.00)));

    // 3. Gross executable difference: 102.50 - 100.50 = 2.00
    assert_eq!(cmp.gross_spread, Some(dec!(2.00)));
    // Gross PnL: 205 - 201 = 4.00
    assert_eq!(cmp.gross_pnl, Some(dec!(4.00)));

    // 4. Fees: Buy fee = 201 * 0.001 = 0.201; Sell fee = 205 * 0.001 = 0.205
    assert_eq!(cmp.buy_fee, dec!(0.201));
    assert_eq!(cmp.sell_fee, dec!(0.205));
    assert_eq!(cmp.total_costs, dec!(0.406));

    // 5. Net result: 4.00 - 0.406 = 3.594
    assert_eq!(cmp.net_pnl, Some(dec!(3.594)));
    assert!(cmp.fully_executable);

    // 6. Test exceeding available liquidity: Book A total ask liquidity = 1 + 2 + 5 = 8 BTC.
    // Request 10 BTC:
    let cmp_exceed = compare_cross_book(
        &book_a,
        &book_b,
        dec!(10.0),
        &fee_schedule,
        &fee_schedule,
        dec!(0.0),
    )
    .unwrap();

    // Refuses to label full quantity executable!
    assert!(!cmp_exceed.fully_executable);
    assert_eq!(cmp_exceed.buy_estimate.filled_quantity, dec!(8.0));
    assert_eq!(cmp_exceed.buy_estimate.remaining_quantity, dec!(2.0));
    assert_eq!(cmp_exceed.matched_quantity, dec!(8.0));
}
