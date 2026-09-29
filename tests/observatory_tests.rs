use airbitrage::execution::VenueFeeRegistry;
use airbitrage::market::MarketStateManager;
use airbitrage::market::state::SequencePolicy;
use airbitrage::observatory::{
    DislocationObservation, MarketRelationship, ObservationConfig, ObservationEngine,
    ObservationRejectionReason, OpportunityEndReason, OpportunityKey, OpportunityTracker,
    PersistenceTransition,
};
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use rust_decimal::Decimal;
use std::time::Duration;

fn setup_manager() -> MarketStateManager {
    MarketStateManager::new(Duration::from_millis(5_000))
}

#[allow(clippy::too_many_arguments)]
fn inject_trusted_book(
    manager: &mut MarketStateManager,
    venue: VenueId,
    market_type: MarketType,
    symbol: &str,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
    exchange_ts_ms: i64,
    local_recv_ts_ns: i64,
) {
    manager.register_instrument_with_policy(
        venue,
        market_type,
        symbol,
        SequencePolicy::SnapshotOnly,
    );
    let event = MarketEvent::OrderBookSnapshot {
        venue,
        market_type,
        symbol: symbol.to_string(),
        bids,
        asks,
        exchange_ts_ms,
        local_recv_ts_ns,
        sequence_id: 1,
    };
    manager
        .handle_event(&event)
        .expect("Snapshot must apply cleanly");
}

#[test]
fn test_01_no_edge_after_fees_recorded_correctly() {
    let mut manager = setup_manager();
    let now_ns = 1_000_000_000_i64;
    let ex_ms = 1_000_i64;

    // Binance Spot: Ask = 100.00
    inject_trusted_book(
        &mut manager,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(99), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    // Bybit Spot: Bid = 100.00 (Zero gross spread, negative net after taker fees)
    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(101), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(obs.is_valid);
    assert!(obs.both_books_trusted);
    assert!(obs.both_books_fresh);
    assert!(obs.fully_executable);
    assert_eq!(obs.gross_spread, Some(Decimal::ZERO));
    assert!(obs.net_edge_bps.is_some());
    // Both sides charged 10 bps taker -> total ~20 bps negative
    assert!(obs.net_edge_bps.unwrap() < Decimal::ZERO);
    assert!(!obs.is_positive_executable_edge());
}

#[test]
fn test_02_positive_executable_edge() {
    let mut manager = setup_manager();
    let now_ns = 1_000_000_000_i64;
    let ex_ms = 1_000_i64;

    // Buy on Binance Spot at 100.00
    inject_trusted_book(
        &mut manager,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(99), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    // Sell on Bybit Spot at 102.00 (Gross spread = 2.00, or 200 bps, exceeds 20 bps total fees)
    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(102), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(103), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(obs.is_valid);
    assert!(obs.fully_executable);
    assert_eq!(obs.gross_spread, Some(Decimal::from(2)));
    assert!(obs.net_edge_bps.unwrap() > Decimal::ZERO);
    assert!(obs.is_positive_executable_edge());
}

#[test]
fn test_03_fees_eliminate_apparent_edge() {
    let mut manager = setup_manager();
    let now_ns = 1_000_000_000_i64;
    let ex_ms = 1_000_i64;

    // Buy on Binance Spot at 100.00
    inject_trusted_book(
        &mut manager,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(99), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    // Sell on Bybit Spot at 100.05 (Gross spread = +0.05 or +5 bps, but fees are 10 + 10 = 20 bps)
    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::new(10005, 2), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(101), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(obs.is_valid);
    assert!(obs.gross_spread.unwrap() > Decimal::ZERO);
    assert!(obs.net_edge.unwrap() < Decimal::ZERO);
    assert!(obs.net_edge_bps.unwrap() < Decimal::ZERO);
    assert!(!obs.is_positive_executable_edge());
}

#[test]
fn test_04_partial_depth_liquidity() {
    let mut manager = setup_manager();
    let now_ns = 1_000_000_000_i64;
    let ex_ms = 1_000_i64;

    // Binance Spot only has 2.0 BTC depth on asks
    inject_trusted_book(
        &mut manager,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(99), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(100), Decimal::from(2))],
        ex_ms,
        now_ns,
    );

    // Bybit Spot has 5.0 BTC depth on bids
    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(102), Decimal::from(5))],
        vec![PriceLevel::new(Decimal::from(103), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    let engine = ObservationEngine::with_defaults();
    // Request 5.0 BTC -> Buy side only has 2.0 BTC
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::from(5),
        now_ns,
    );

    assert!(obs.is_valid);
    assert!(!obs.fully_executable);
    assert_eq!(obs.buy_available_quantity, Decimal::from(2));
    assert_eq!(obs.sell_available_quantity, Decimal::from(5));
    assert_eq!(obs.common_executable_quantity, Decimal::from(2));
    assert!(!obs.is_positive_executable_edge());
}

#[test]
fn test_05_stale_book_rejected() {
    let mut manager = setup_manager();
    let now_ns = 10_000_000_000_i64; // 10s
    let old_ns = 1_000_000_000_i64; // 1s -> age = 9s > max_book_age_ms (1s)

    inject_trusted_book(
        &mut manager,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(99), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
        1_000,
        old_ns,
    );

    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(102), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(103), Decimal::from(10))],
        10_000,
        now_ns,
    );

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(!obs.is_valid);
    assert!(!obs.both_books_fresh);
    match obs.rejection_reason {
        Some(ObservationRejectionReason::StaleBook { venue, .. }) => {
            assert_eq!(venue, VenueId::Binance);
        }
        other => panic!("Expected StaleBook, got {other:?}"),
    }
}

#[test]
fn test_06_timestamp_skew_exceeded() {
    let mut manager = setup_manager();
    let now_ns = 10_000_000_000_i64;

    // Both books received freshly locally, but exchange timestamps differ by 5 seconds (5,000 ms > 2,000 ms max skew)
    inject_trusted_book(
        &mut manager,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(99), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
        5_000, // 5s exchange time
        now_ns,
    );

    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(102), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(103), Decimal::from(10))],
        10_500, // 10.5s exchange time -> skew = 5,500 ms
        now_ns,
    );

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(!obs.is_valid);
    match obs.rejection_reason {
        Some(ObservationRejectionReason::TimestampSkewExceeded { skew_ms, .. }) => {
            assert_eq!(skew_ms, 5_500);
        }
        other => panic!("Expected TimestampSkewExceeded, got {other:?}"),
    }
}

#[test]
fn test_07_untrusted_book_rejected() {
    let mut manager = setup_manager();
    let now_ns = 1_000_000_000_i64;

    // Binance Spot registered but never received snapshot (remains AwaitingSnapshot -> untrusted)
    manager.register_instrument(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(102), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(103), Decimal::from(10))],
        1_000,
        now_ns,
    );

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(!obs.is_valid);
    assert!(!obs.both_books_trusted);
    assert!(obs.rejection_reason.is_some());
}

#[test]
fn test_08_crossed_book_rejected() {
    let mut manager = setup_manager();
    let now_ns = 1_000_000_000_i64;

    // Create an invalid/crossed book
    let mut state =
        airbitrage::market::state::MarketState::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");
    // Directly inject crossed levels
    let _ = state.apply_snapshot(
        vec![PriceLevel::new(Decimal::from(105), Decimal::ONE)],
        vec![PriceLevel::new(Decimal::from(100), Decimal::ONE)], // best ask 100 < best bid 105 -> crossed
        1_000,
        now_ns,
        1,
    );
    assert!(state.book.is_crossed());

    // Inject valid book on Bybit
    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(102), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(103), Decimal::from(10))],
        1_000,
        now_ns,
    );

    // Register corrupted state into manager
    manager.register_instrument(VenueId::Binance, MarketType::Spot, "BTCUSDT");
    *manager
        .get_state_mut(VenueId::Binance, MarketType::Spot, "BTCUSDT")
        .unwrap() = state;

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(!obs.is_valid);
    assert!(!obs.no_crossed_books);
    match obs.rejection_reason {
        Some(ObservationRejectionReason::CrossedBook { venue, .. }) => {
            assert_eq!(venue, VenueId::Binance);
        }
        other => panic!("Expected CrossedBook, got {other:?}"),
    }
}

#[test]
fn test_09_self_comparison_rejected() {
    let manager = setup_manager();
    let now_ns = 1_000_000_000_i64;

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(!obs.is_valid);
    assert_eq!(
        obs.rejection_reason,
        Some(ObservationRejectionReason::SelfComparison)
    );
}

#[test]
fn test_10_reference_quantity_sweep() {
    let mut manager = setup_manager();
    let now_ns = 1_000_000_000_i64;
    let ex_ms = 1_000_i64;

    // Multi-level depth:
    // Binance Spot asks: 1.0 @ 100, 2.0 @ 101, 5.0 @ 102
    inject_trusted_book(
        &mut manager,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(99), Decimal::from(10))],
        vec![
            PriceLevel::new(Decimal::from(100), Decimal::from(1)),
            PriceLevel::new(Decimal::from(101), Decimal::from(2)),
            PriceLevel::new(Decimal::from(102), Decimal::from(5)),
        ],
        ex_ms,
        now_ns,
    );

    // Bybit Spot bids: 1.0 @ 105, 2.0 @ 104, 5.0 @ 103
    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![
            PriceLevel::new(Decimal::from(105), Decimal::from(1)),
            PriceLevel::new(Decimal::from(104), Decimal::from(2)),
            PriceLevel::new(Decimal::from(103), Decimal::from(5)),
        ],
        vec![PriceLevel::new(Decimal::from(106), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    let config = ObservationConfig {
        reference_quantities: vec![Decimal::from(1), Decimal::from(2), Decimal::from(5)],
        ..Default::default()
    };
    let engine = ObservationEngine::new(config, VenueFeeRegistry::default());

    let sweep = engine.observe_sweep(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        now_ns,
    );

    assert_eq!(sweep.len(), 3);
    assert_eq!(sweep[0].reference_quantity, Decimal::from(1));
    assert_eq!(sweep[1].reference_quantity, Decimal::from(2));
    assert_eq!(sweep[2].reference_quantity, Decimal::from(5));

    // As quantity increases, VWAP degrades (buy VWAP increases, sell VWAP decreases -> spread narrows)
    assert!(sweep[0].gross_spread.unwrap() > sweep[1].gross_spread.unwrap());
    assert!(sweep[1].gross_spread.unwrap() > sweep[2].gross_spread.unwrap());
}

#[test]
fn test_11_negative_edge_preserved() {
    let mut manager = setup_manager();
    let now_ns = 1_000_000_000_i64;
    let ex_ms = 1_000_i64;

    // Buy on Binance Spot at 105.00
    inject_trusted_book(
        &mut manager,
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(104), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(105), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    // Sell on Bybit Spot at 100.00 (Gross spread = -5.00)
    inject_trusted_book(
        &mut manager,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
        vec![PriceLevel::new(Decimal::from(101), Decimal::from(10))],
        ex_ms,
        now_ns,
    );

    let engine = ObservationEngine::with_defaults();
    let obs = engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert!(obs.is_valid);
    assert_eq!(obs.gross_spread, Some(Decimal::from(-5)));
    assert!(obs.net_edge_bps.unwrap() < Decimal::ZERO);
    assert!(obs.net_edge.is_some());
}

#[test]
fn test_12_spot_spot_classification() {
    let rel = MarketRelationship::classify(MarketType::Spot, MarketType::Spot);
    assert_eq!(rel, MarketRelationship::SpotSpot);
    assert!(rel.is_direct_dislocation());
    assert!(!rel.is_basis());
}

#[test]
fn test_13_perp_perp_classification() {
    let rel =
        MarketRelationship::classify(MarketType::LinearPerpetual, MarketType::LinearPerpetual);
    assert_eq!(rel, MarketRelationship::PerpPerp);
    assert!(rel.is_direct_dislocation());
    assert!(!rel.is_basis());
}

#[test]
fn test_14_spot_perp_basis_classification() {
    let rel1 = MarketRelationship::classify(MarketType::Spot, MarketType::LinearPerpetual);
    assert_eq!(rel1, MarketRelationship::CrossInstrumentBasis);
    assert!(!rel1.is_direct_dislocation());
    assert!(rel1.is_basis());

    let rel2 = MarketRelationship::classify(MarketType::LinearPerpetual, MarketType::Spot);
    assert_eq!(rel2, MarketRelationship::CrossInstrumentBasis);
    assert!(!rel2.is_direct_dislocation());
    assert!(rel2.is_basis());
}

// -----------------------------------------------------------------------
// Persistence Tracking Tests
// -----------------------------------------------------------------------

fn make_dummy_observation(
    now_ns: i64,
    net_edge_bps: Decimal,
    fully_executable: bool,
    both_trusted: bool,
    both_fresh: bool,
    rejection: Option<ObservationRejectionReason>,
) -> DislocationObservation {
    DislocationObservation {
        buy_venue: VenueId::Binance,
        buy_market: MarketType::Spot,
        sell_venue: VenueId::Bybit,
        sell_market: MarketType::Spot,
        symbol: "BTCUSDT".into(),
        market_relationship: MarketRelationship::SpotSpot,
        observation_ts_ns: now_ns,
        buy_book_exchange_ts_ms: Some(1_000),
        sell_book_exchange_ts_ms: Some(1_000),
        buy_book_recv_ts_ns: Some(now_ns),
        sell_book_recv_ts_ns: Some(now_ns),
        timestamp_skew_ms: Some(0),
        buy_book_age_ms: Some(0),
        sell_book_age_ms: Some(0),
        reference_quantity: Decimal::ONE,
        buy_available_quantity: Decimal::ONE,
        sell_available_quantity: Decimal::ONE,
        common_executable_quantity: Decimal::ONE,
        fully_executable,
        buy_vwap: Some(Decimal::from(100)),
        sell_vwap: Some(Decimal::from(102)),
        buy_worst_fill_price: Some(Decimal::from(100)),
        sell_worst_fill_price: Some(Decimal::from(102)),
        buy_best_ask: Some(Decimal::from(100)),
        sell_best_bid: Some(Decimal::from(102)),
        gross_spread: Some(Decimal::from(2)),
        gross_spread_bps: Some(Decimal::from(200)),
        gross_edge: Some(Decimal::from(2)),
        buy_fee: Decimal::new(10, 2),
        sell_fee: Decimal::new(10, 2),
        total_fees: Decimal::new(20, 2),
        net_edge: Some(Decimal::new(180, 2)),
        net_edge_bps: Some(net_edge_bps),
        both_books_trusted: both_trusted,
        both_books_fresh: both_fresh,
        no_crossed_books: true,
        is_valid: rejection.is_none(),
        rejection_reason: rejection,
    }
}

#[test]
fn test_persistence_a_start_condition() {
    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let t0 = 1_000_000_000_i64;

    let obs = make_dummy_observation(t0, Decimal::from(15), true, true, true, None);
    let transition = tracker.process_observation(&obs);

    assert_eq!(transition, PersistenceTransition::Started);
    assert_eq!(tracker.active_count(), 1);

    let key = OpportunityKey::new(
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        MarketRelationship::SpotSpot,
    );
    let active = tracker.active_opportunity(&key).unwrap();
    assert_eq!(active.start_ts_ns, t0);
    assert_eq!(active.sample_count, 1);
    assert_eq!(active.first_observed_edge_bps, Decimal::from(15));
}

#[test]
fn test_persistence_b_continuation() {
    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let t0 = 1_000_000_000_i64;
    let t1 = 1_500_000_000_i64; // +500ms

    let obs0 = make_dummy_observation(t0, Decimal::from(10), true, true, true, None);
    assert_eq!(
        tracker.process_observation(&obs0),
        PersistenceTransition::Started
    );

    let obs1 = make_dummy_observation(t1, Decimal::from(25), true, true, true, None);
    let transition = tracker.process_observation(&obs1);

    assert_eq!(transition, PersistenceTransition::Continued);
    assert_eq!(tracker.active_count(), 1);

    let key = OpportunityKey::new(
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        MarketRelationship::SpotSpot,
    );
    let active = tracker.active_opportunity(&key).unwrap();
    assert_eq!(active.sample_count, 2);
    assert_eq!(active.current_duration_ms(), 500);
    assert_eq!(active.peak_net_edge_bps, Decimal::from(25));
    assert_eq!(active.min_net_edge_bps, Decimal::from(10));
}

#[test]
fn test_persistence_c_edge_disappears_termination() {
    let mut tracker = OpportunityTracker::new(Decimal::from(5));
    let t0 = 1_000_000_000_i64;
    let t1 = 2_000_000_000_i64; // +1000ms

    let obs0 = make_dummy_observation(t0, Decimal::from(10), true, true, true, None);
    assert_eq!(
        tracker.process_observation(&obs0),
        PersistenceTransition::Started
    );

    // Edge drops to 2 bps (< 5 bps threshold)
    let obs1 = make_dummy_observation(t1, Decimal::from(2), true, true, true, None);
    let transition = tracker.process_observation(&obs1);

    match transition {
        PersistenceTransition::Ended(record) => {
            assert_eq!(record.duration_ms, 1_000);
            assert_eq!(record.sample_count, 1);
            assert_eq!(
                record.termination_reason,
                OpportunityEndReason::NetEdgeBelowThreshold
            );
        }
        other => panic!("Expected Ended, got {other:?}"),
    }

    assert_eq!(tracker.active_count(), 0);
    assert_eq!(tracker.completed_records().len(), 1);
}

#[test]
fn test_persistence_d_untrusted_book_termination() {
    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let t0 = 1_000_000_000_i64;
    let t1 = 1_200_000_000_i64;

    let obs0 = make_dummy_observation(t0, Decimal::from(10), true, true, true, None);
    tracker.process_observation(&obs0);

    // Book becomes untrusted
    let obs1 = make_dummy_observation(
        t1,
        Decimal::from(10),
        true,
        false, // both_trusted = false
        true,
        Some(ObservationRejectionReason::UntrustedBook {
            venue: VenueId::Binance,
            market_type: MarketType::Spot,
            reason: "Resyncing".into(),
        }),
    );
    let transition = tracker.process_observation(&obs1);

    match transition {
        PersistenceTransition::Ended(record) => {
            assert_eq!(
                record.termination_reason,
                OpportunityEndReason::UntrustedBook
            );
        }
        other => panic!("Expected Ended with UntrustedBook, got {other:?}"),
    }
}

#[test]
fn test_persistence_e_stale_book_termination() {
    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let t0 = 1_000_000_000_i64;
    let t1 = 1_300_000_000_i64;

    let obs0 = make_dummy_observation(t0, Decimal::from(10), true, true, true, None);
    tracker.process_observation(&obs0);

    // Book becomes stale
    let obs1 = make_dummy_observation(
        t1,
        Decimal::from(10),
        true,
        true,
        false, // fresh = false
        Some(ObservationRejectionReason::StaleBook {
            venue: VenueId::Binance,
            market_type: MarketType::Spot,
            age_ms: 1500,
            max_age_ms: 1000,
        }),
    );
    let transition = tracker.process_observation(&obs1);

    match transition {
        PersistenceTransition::Ended(record) => {
            assert_eq!(record.termination_reason, OpportunityEndReason::StaleBook);
        }
        other => panic!("Expected Ended with StaleBook, got {other:?}"),
    }
}

#[test]
fn test_persistence_f_liquidity_degradation_termination() {
    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let t0 = 1_000_000_000_i64;
    let t1 = 1_400_000_000_i64;

    let obs0 = make_dummy_observation(t0, Decimal::from(10), true, true, true, None);
    tracker.process_observation(&obs0);

    // Depth degraded: requested quantity no longer fully executable
    let obs1 = make_dummy_observation(
        t1,
        Decimal::from(10),
        false, // fully_executable = false
        true,
        true,
        None,
    );
    let transition = tracker.process_observation(&obs1);

    match transition {
        PersistenceTransition::Ended(record) => {
            assert_eq!(
                record.termination_reason,
                OpportunityEndReason::InsufficientLiquidity
            );
        }
        other => panic!("Expected Ended with InsufficientLiquidity, got {other:?}"),
    }
}

#[test]
fn test_persistence_g_negative_edge_termination() {
    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let t0 = 1_000_000_000_i64;
    let t1 = 1_800_000_000_i64;

    let obs0 = make_dummy_observation(t0, Decimal::from(10), true, true, true, None);
    tracker.process_observation(&obs0);

    // Spread inverted -> net edge becomes -15 bps
    let obs1 = make_dummy_observation(t1, Decimal::from(-15), true, true, true, None);
    let transition = tracker.process_observation(&obs1);

    match transition {
        PersistenceTransition::Ended(record) => {
            assert_eq!(
                record.termination_reason,
                OpportunityEndReason::NetEdgeBelowThreshold
            );
        }
        other => panic!("Expected Ended with NetEdgeBelowThreshold, got {other:?}"),
    }
}

#[test]
fn test_persistence_h_feed_disconnect() {
    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let t0 = 1_000_000_000_i64;
    let t_drop = 1_750_000_000_i64;

    let obs0 = make_dummy_observation(t0, Decimal::from(10), true, true, true, None);
    tracker.process_observation(&obs0);
    assert_eq!(tracker.active_count(), 1);

    // Feed drops for Binance Spot
    let terminated =
        tracker.handle_feed_disconnect(VenueId::Binance, Some(MarketType::Spot), t_drop);
    assert_eq!(terminated.len(), 1);
    assert_eq!(
        terminated[0].termination_reason,
        OpportunityEndReason::FeedDisconnected
    );
    assert_eq!(terminated[0].duration_ms, 750);
    assert_eq!(tracker.active_count(), 0);
}
