use airbitrage::error::EngineError;
use airbitrage::market::MarketStateManager;
use airbitrage::market::state::{
    BookLifecycleState, BookValidity, DeltaUpdate, InvalidationReason, MarketState,
};
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use rust_decimal_macros::dec;
use std::time::Duration;

fn make_delta(
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
    first_seq: u64,
    final_seq: u64,
    prev_seq: Option<u64>,
    exchange_ts: i64,
) -> DeltaUpdate {
    DeltaUpdate::new(
        bids,
        asks,
        first_seq,
        final_seq,
        prev_seq,
        exchange_ts,
        exchange_ts,
        exchange_ts,
    )
}

// ============================================================================
// 1. FRESH SNAPSHOT TEST
// ============================================================================

#[test]
fn test_state_fresh_snapshot_initialization() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");
    assert_eq!(state.lifecycle_state, BookLifecycleState::AwaitingSnapshot);
    assert!(!state.validity.is_valid());

    let bids = vec![
        PriceLevel::new(dec!(65000.00), dec!(1.5)),
        PriceLevel::new(dec!(64990.00), dec!(2.0)),
    ];
    let asks = vec![
        PriceLevel::new(dec!(65005.00), dec!(0.8)),
        PriceLevel::new(dec!(65010.00), dec!(3.1)),
    ];

    let applied = state
        .apply_snapshot(bids, asks, 1727400000000, 1000, 1001)
        .expect("Snapshot application should succeed");

    assert!(applied);
    assert_eq!(state.lifecycle_state, BookLifecycleState::Live);
    assert!(state.validity.is_valid());
    assert_eq!(state.last_update_sequence, Some(1001));
    assert_eq!(state.last_exchange_ts_ms, Some(1727400000000));
    assert_eq!(state.last_local_recv_ts_ns, Some(1000));

    // Verify quotes & sorting
    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(dec!(65000.00), dec!(1.5)))
    );
    assert_eq!(
        state.best_ask(),
        Some(PriceLevel::new(dec!(65005.00), dec!(0.8)))
    );
    assert_eq!(state.spread(), Some(dec!(5.00)));

    // Trusted book accessor should return book
    assert!(state.trusted_book(None, 1000).is_some());
}

// ============================================================================
// 2. VALID DELTA CHAIN TEST
// ============================================================================

#[test]
fn test_state_valid_delta_chain() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");

    // Initialize with snapshot at sequence 100
    let bids = vec![PriceLevel::new(dec!(65000.00), dec!(1.0))];
    let asks = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    state
        .apply_snapshot(bids, asks, 1000, 1000, 100)
        .expect("Snapshot should succeed");

    // Delta 1: U=101, u=105, pu=100
    let delta1_bids = vec![PriceLevel::new(dec!(65002.00), dec!(0.5))];
    let applied1 = state
        .apply_delta(make_delta(delta1_bids, vec![], 101, 105, Some(100), 1010))
        .expect("Delta 1 should succeed");
    assert!(applied1);
    assert_eq!(state.last_update_sequence, Some(105));
    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(dec!(65002.00), dec!(0.5)))
    );

    // Delta 2: U=106, u=110, pu=105
    let delta2_asks = vec![PriceLevel::new(dec!(65008.00), dec!(1.2))];
    let applied2 = state
        .apply_delta(make_delta(vec![], delta2_asks, 106, 110, Some(105), 1020))
        .expect("Delta 2 should succeed");
    assert!(applied2);
    assert_eq!(state.last_update_sequence, Some(110));
    assert_eq!(
        state.best_ask(),
        Some(PriceLevel::new(dec!(65008.00), dec!(1.2)))
    );

    // Delta 3: U=111, u=115, pu=110
    let delta3_bids = vec![PriceLevel::new(dec!(65003.00), dec!(2.0))];
    let applied3 = state
        .apply_delta(make_delta(delta3_bids, vec![], 111, 115, Some(110), 1030))
        .expect("Delta 3 should succeed");
    assert!(applied3);
    assert_eq!(state.last_update_sequence, Some(115));
    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(dec!(65003.00), dec!(2.0)))
    );

    assert_eq!(state.metrics.deltas_applied, 3);
}

// ============================================================================
// 3. QUANTITY UPDATE TEST
// ============================================================================

#[test]
fn test_state_quantity_update() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");
    let bids = vec![PriceLevel::new(dec!(65000.00), dec!(1.0))];
    let asks = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    state.apply_snapshot(bids, asks, 1000, 1000, 100).unwrap();

    // Update quantity of 65000.00 to 5.5
    let delta_bids = vec![PriceLevel::new(dec!(65000.00), dec!(5.5))];
    state
        .apply_delta(make_delta(delta_bids, vec![], 101, 102, Some(100), 1010))
        .unwrap();

    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(dec!(65000.00), dec!(5.5)))
    );
    assert_eq!(state.book.bids.len(), 1);
}

// ============================================================================
// 4. LEVEL INSERTION TEST
// ============================================================================

#[test]
fn test_state_level_insertion() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");
    let bids = vec![
        PriceLevel::new(dec!(65000.00), dec!(1.0)),
        PriceLevel::new(dec!(64990.00), dec!(2.0)),
    ];
    let asks = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    state.apply_snapshot(bids, asks, 1000, 1000, 100).unwrap();

    // Insert intermediate level 64995.00
    let delta_bids = vec![PriceLevel::new(dec!(64995.00), dec!(3.0))];
    state
        .apply_delta(make_delta(delta_bids, vec![], 101, 102, Some(100), 1010))
        .unwrap();

    assert_eq!(state.book.bids.len(), 3);
    assert_eq!(state.book.bids[0].price, dec!(65000.00));
    assert_eq!(state.book.bids[1].price, dec!(64995.00));
    assert_eq!(state.book.bids[2].price, dec!(64990.00));
}

// ============================================================================
// 5. LEVEL DELETION TEST
// ============================================================================

#[test]
fn test_state_level_deletion() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");
    let bids = vec![
        PriceLevel::new(dec!(65000.00), dec!(1.0)),
        PriceLevel::new(dec!(64990.00), dec!(2.0)),
    ];
    let asks = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    state.apply_snapshot(bids, asks, 1000, 1000, 100).unwrap();

    // Delete best bid 65000.00 by setting quantity to 0
    let delta_bids = vec![PriceLevel::new(dec!(65000.00), dec!(0.0))];
    state
        .apply_delta(make_delta(delta_bids, vec![], 101, 102, Some(100), 1010))
        .unwrap();

    assert_eq!(state.book.bids.len(), 1);
    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(dec!(64990.00), dec!(2.0)))
    );
}

// ============================================================================
// 6. DUPLICATE UPDATE TEST
// ============================================================================

#[test]
fn test_state_duplicate_update_ignored() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");
    let bids = vec![PriceLevel::new(dec!(65000.00), dec!(1.0))];
    let asks = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    state.apply_snapshot(bids, asks, 1000, 1000, 100).unwrap();

    // Apply delta u=105
    let delta = vec![PriceLevel::new(dec!(65001.00), dec!(0.5))];
    let res1 = state
        .apply_delta(make_delta(delta.clone(), vec![], 101, 105, Some(100), 1010))
        .unwrap();
    assert!(res1);

    // Apply identical delta u=105 again
    let res2 = state
        .apply_delta(make_delta(delta, vec![], 101, 105, Some(100), 1010))
        .unwrap();
    assert!(!res2); // Not applied, duplicate
    assert_eq!(state.metrics.duplicate_deltas, 1);
    assert_eq!(state.last_update_sequence, Some(105));
}

// ============================================================================
// 7. OLD UPDATE TEST
// ============================================================================

#[test]
fn test_state_old_update_ignored() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");
    let bids = vec![PriceLevel::new(dec!(65000.00), dec!(1.0))];
    let asks = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    state.apply_snapshot(bids, asks, 1000, 1000, 100).unwrap();

    // Advance to sequence 110
    state
        .apply_delta(make_delta(vec![], vec![], 101, 110, Some(100), 1010))
        .unwrap();

    // Incoming event with final sequence 105 (already behind 110)
    let old_delta = vec![PriceLevel::new(dec!(65002.00), dec!(0.5))];
    let applied = state
        .apply_delta(make_delta(old_delta, vec![], 101, 105, Some(100), 1005))
        .unwrap();

    assert!(!applied); // Ignored safely
    assert_eq!(state.metrics.old_deltas, 1);
    assert_eq!(state.last_update_sequence, Some(110));
}

// ============================================================================
// 8. SEQUENCE GAP TEST
// ============================================================================

#[test]
fn test_state_sequence_gap_invalidates_book() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");
    let bids = vec![PriceLevel::new(dec!(65000.00), dec!(1.0))];
    let asks = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    state.apply_snapshot(bids, asks, 1000, 1000, 100).unwrap();

    // Expected next delta to have pu=100. Instead pu=105 (gap from missing 101..105!)
    let gap_delta = vec![PriceLevel::new(dec!(65005.00), dec!(1.0))];
    let result = state.apply_delta(make_delta(gap_delta, vec![], 106, 110, Some(105), 1020));

    assert!(result.is_err());
    match result {
        Err(EngineError::SequenceGap {
            expected_prev,
            received_prev,
            first_seq,
            final_seq,
        }) => {
            assert_eq!(expected_prev, 100);
            assert_eq!(received_prev, Some(105));
            assert_eq!(first_seq, 106);
            assert_eq!(final_seq, 110);
        }
        other => panic!("Expected SequenceGap, got: {:?}", other),
    }

    // Verify book is invalidated and NO LONGER trusted
    assert_eq!(state.lifecycle_state, BookLifecycleState::Invalidated);
    assert!(!state.validity.is_valid());
    assert!(state.trusted_book(None, 1020).is_none());
    assert_eq!(state.metrics.sequence_failures, 1);
    assert_eq!(state.metrics.invalidations, 1);
}

// ============================================================================
// 9. OUT-OF-ORDER UPDATE (U > u)
// ============================================================================

#[test]
fn test_state_out_of_order_sequence_inversion() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");
    let bids = vec![PriceLevel::new(dec!(65000.00), dec!(1.0))];
    let asks = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    state.apply_snapshot(bids, asks, 1000, 1000, 100).unwrap();

    // U=120 > u=110
    let invalid_delta = state.apply_delta(make_delta(vec![], vec![], 120, 110, Some(100), 1010));
    assert!(invalid_delta.is_err());
    match invalid_delta {
        Err(EngineError::Validation(msg)) => assert!(msg.contains("Sequence ID inversion")),
        other => panic!("Expected Validation error, got: {:?}", other),
    }
}

// ============================================================================
// 10. CROSSED BOOK TEST
// ============================================================================

#[test]
fn test_state_crossed_book_invalidates_and_rejects() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    // Snapshot where bid >= ask
    let bids = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    let asks = vec![PriceLevel::new(dec!(65005.00), dec!(1.0))];

    let result = state.apply_snapshot(bids, asks, 1000, 1000, 1);
    assert!(result.is_err());
    match result {
        Err(EngineError::CrossedBook { bid, ask }) => {
            assert_eq!(bid, dec!(65010.00));
            assert_eq!(ask, dec!(65005.00));
        }
        other => panic!("Expected CrossedBook, got: {:?}", other),
    }

    assert_eq!(state.lifecycle_state, BookLifecycleState::Invalidated);
    assert!(!state.validity.is_valid());
    assert!(state.trusted_book(None, 1000).is_none());
    assert_eq!(state.metrics.crossed_books, 1);
}

// ============================================================================
// 11. RECOVERY / RESYNC TEST
// ============================================================================

#[test]
fn test_state_recovery_cycle() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");

    // 1. Initial snapshot -> LIVE
    state
        .apply_snapshot(
            vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
            vec![PriceLevel::new(dec!(65010.00), dec!(1.0))],
            1000,
            1000,
            100,
        )
        .unwrap();
    assert_eq!(state.lifecycle_state, BookLifecycleState::Live);

    // 2. Trigger gap -> INVALIDATED
    let _ = state.apply_delta(make_delta(vec![], vec![], 110, 115, Some(109), 1010));
    assert_eq!(state.lifecycle_state, BookLifecycleState::Invalidated);
    assert!(state.trusted_book(None, 1010).is_none());

    // 3. Subsequent deltas during Invalidated state are rejected
    let rejected = state.apply_delta(make_delta(vec![], vec![], 116, 120, Some(115), 1020));
    assert!(rejected.is_err());

    // 4. Request Resync -> AwaitingSnapshot
    state.request_resync().expect("Resync should succeed");
    assert_eq!(state.lifecycle_state, BookLifecycleState::AwaitingSnapshot);
    assert_eq!(state.metrics.resync_attempts, 1);

    // 5. Apply fresh baseline snapshot at sequence 200 -> LIVE
    state
        .apply_snapshot(
            vec![PriceLevel::new(dec!(65005.00), dec!(2.0))],
            vec![PriceLevel::new(dec!(65015.00), dec!(2.0))],
            2000,
            2000,
            200,
        )
        .unwrap();

    assert_eq!(state.lifecycle_state, BookLifecycleState::Live);
    assert!(state.validity.is_valid());
    assert_eq!(state.last_update_sequence, Some(200));
    assert_eq!(state.metrics.resync_successes, 1);

    // 6. Next valid contiguous delta applies cleanly
    let delta_res = state.apply_delta(make_delta(
        vec![PriceLevel::new(dec!(65006.00), dec!(0.5))],
        vec![],
        201,
        205,
        Some(200),
        2010,
    ));
    assert!(delta_res.is_ok());
    assert_eq!(state.last_update_sequence, Some(205));
    assert!(state.trusted_book(None, 2010).is_some());
}

// ============================================================================
// 12. STALE BOOK TEST
// ============================================================================

#[test]
fn test_state_staleness_detection() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");
    let threshold = Duration::from_millis(500);

    // Before snapshot: stale
    assert!(state.is_stale(threshold, 1_000_000_000));

    // Snapshot at t = 1,000,000,000 ns (1.000s)
    state
        .apply_snapshot(
            vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
            vec![PriceLevel::new(dec!(65010.00), dec!(1.0))],
            1000,
            1_000_000_000,
            100,
        )
        .unwrap();

    // At t = 1,300,000,000 ns (300ms elapsed < 500ms): not stale
    assert!(!state.is_stale(threshold, 1_300_000_000));
    assert!(state.trusted_book(Some(threshold), 1_300_000_000).is_some());

    // At t = 1,600,000,000 ns (600ms elapsed > 500ms): STALE!
    assert!(state.is_stale(threshold, 1_600_000_000));
    // Trusted book accessor MUST return None when stale!
    assert!(state.trusted_book(Some(threshold), 1_600_000_000).is_none());
}

// ============================================================================
// 13. WRONG SYMBOL ISOLATION TEST
// ============================================================================

#[test]
fn test_manager_symbol_isolation() {
    let mut manager = MarketStateManager::new(Duration::from_millis(1000));
    manager.register_instrument(VenueId::Binance, MarketType::Spot, "BTCUSDT");
    manager.register_instrument(VenueId::Binance, MarketType::Spot, "ETHUSDT");

    // Snapshot for BTCUSDT
    let btc_snap = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
        asks: vec![PriceLevel::new(dec!(65010.00), dec!(1.0))],
        exchange_ts_ms: 1000,
        local_recv_ts_ns: 1000,
        sequence_id: 100,
    };
    manager.handle_event(&btc_snap).unwrap();

    assert!(manager.is_live(VenueId::Binance, MarketType::Spot, "BTCUSDT"));
    // ETHUSDT has received no events -> must NOT be live!
    assert!(!manager.is_live(VenueId::Binance, MarketType::Spot, "ETHUSDT"));
    assert_eq!(
        manager
            .get_state(VenueId::Binance, MarketType::Spot, "ETHUSDT")
            .unwrap()
            .lifecycle_state,
        BookLifecycleState::AwaitingSnapshot
    );
}

// ============================================================================
// 14. VENUE AND MARKET TYPE ISOLATION TEST
// ============================================================================

#[test]
fn test_manager_market_type_isolation() {
    let mut manager = MarketStateManager::new(Duration::from_millis(1000));

    // Event for Binance Spot BTCUSDT
    let spot_event = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
        asks: vec![PriceLevel::new(dec!(65010.00), dec!(1.0))],
        exchange_ts_ms: 1000,
        local_recv_ts_ns: 1000,
        sequence_id: 100,
    };
    manager.handle_event(&spot_event).unwrap();

    assert!(manager.is_live(VenueId::Binance, MarketType::Spot, "BTCUSDT"));
    // Binance LinearPerpetual BTCUSDT must NOT be affected
    assert!(!manager.is_live(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT"));
}

// ============================================================================
// 15. EMPTY / INVALID BOOK HANDLING
// ============================================================================

#[test]
fn test_state_empty_snapshot_rejected() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");
    let result = state.apply_snapshot(vec![], vec![], 1000, 1000, 1);
    assert!(result.is_err());
    assert_eq!(state.lifecycle_state, BookLifecycleState::Invalidated);
}

// ============================================================================
// 16. STATE MACHINE TRANSITIONS TEST
// ============================================================================

#[test]
fn test_state_machine_illegal_transitions_rejected() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    // Illegal: Empty -> Live directly without snapshot
    state.lifecycle_state = BookLifecycleState::Empty;
    let res = state.transition_to(BookLifecycleState::Live, "test");
    assert!(res.is_err());
    match res {
        Err(EngineError::InvalidStateTransition { from, to, .. }) => {
            assert_eq!(from, "Empty");
            assert_eq!(to, "Live");
        }
        other => panic!("Expected InvalidStateTransition, got: {:?}", other),
    }

    // Illegal: Invalidated -> Live directly without resynchronizing and snapshot
    state.lifecycle_state = BookLifecycleState::Invalidated;
    let res2 = state.transition_to(BookLifecycleState::Live, "test");
    assert!(res2.is_err());
}

// ============================================================================
// 17. BINANCE FUTURES BUFFERED DELTA SYNCHRONIZATION
// ============================================================================

#[test]
fn test_binance_futures_buffered_delta_alignment() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");
    assert_eq!(state.lifecycle_state, BookLifecycleState::AwaitingSnapshot);

    // 1. Delta updates arrive before REST snapshot and are buffered
    // Delta 1: U=100, u=105, pu=99
    state
        .apply_delta(make_delta(
            vec![PriceLevel::new(dec!(65001.00), dec!(1.0))],
            vec![PriceLevel::new(dec!(65011.00), dec!(1.0))],
            100,
            105,
            Some(99),
            1005,
        ))
        .unwrap();

    // Delta 2: U=106, u=110, pu=105
    state
        .apply_delta(make_delta(
            vec![PriceLevel::new(dec!(65002.00), dec!(1.5))],
            vec![],
            106,
            110,
            Some(105),
            1010,
        ))
        .unwrap();

    assert_eq!(state.buffered_delta_count(), 2);
    assert_eq!(state.lifecycle_state, BookLifecycleState::AwaitingSnapshot);

    // 2. Snapshot arrives with lastUpdateId = 103 (covered by Delta 1: U(100) <= 103 && u(105) >= 103)
    let snapshot_bids = vec![PriceLevel::new(dec!(65000.00), dec!(2.0))];
    let snapshot_asks = vec![PriceLevel::new(dec!(65012.00), dec!(2.0))];

    let applied = state
        .apply_snapshot(snapshot_bids, snapshot_asks, 1003, 1003, 103)
        .expect("Snapshot alignment should succeed");

    assert!(applied);
    // State must now be LIVE!
    assert_eq!(state.lifecycle_state, BookLifecycleState::Live);
    assert!(state.validity.is_valid());

    // Final sequence should be advanced to the latest buffered delta (u=110)
    assert_eq!(state.last_update_sequence, Some(110));
    assert_eq!(state.buffered_delta_count(), 0);

    // Verify order book state reflects the replayed deltas
    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(dec!(65002.00), dec!(1.5)))
    );
    assert_eq!(
        state.best_ask(),
        Some(PriceLevel::new(dec!(65011.00), dec!(1.0)))
    );
}

// ============================================================================
// 18. BINANCE FUTURES BUFFERED DELTA SEQUENCE GAP
// ============================================================================

#[test]
fn test_binance_futures_buffered_delta_gap_rejected() {
    let mut state = MarketState::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");

    // Delta arriving has U=150, u=160, pu=149
    state
        .apply_delta(make_delta(
            vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
            vec![],
            150,
            160,
            Some(149),
            1000,
        ))
        .unwrap();

    // Snapshot arrives with lastUpdateId = 100 (gap between 100 and 150!)
    let result = state.apply_snapshot(
        vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
        vec![PriceLevel::new(dec!(65010.00), dec!(1.0))],
        1000,
        1000,
        100,
    );

    assert!(result.is_err());
    assert_eq!(state.lifecycle_state, BookLifecycleState::Invalidated);
    assert_eq!(state.metrics.sequence_failures, 1);
}

// ============================================================================
// 19. DISCONNECTION INVALIDATION TEST
// ============================================================================

#[test]
fn test_manager_disconnection_invalidates_books() {
    let mut manager = MarketStateManager::new(Duration::from_millis(1000));
    manager.register_instrument(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    // Bring Spot to Live
    let snap = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
        asks: vec![PriceLevel::new(dec!(65010.00), dec!(1.0))],
        exchange_ts_ms: 1000,
        local_recv_ts_ns: 1000,
        sequence_id: 100,
    };
    manager.handle_event(&snap).unwrap();
    assert!(manager.is_live(VenueId::Binance, MarketType::Spot, "BTCUSDT"));

    // Venue drops
    let disconnect = MarketEvent::ConnectionState {
        venue: VenueId::Binance,
        is_connected: false,
        details: "TCP reset".into(),
    };
    manager.handle_event(&disconnect).unwrap();

    // Must be invalidated immediately
    assert!(!manager.is_live(VenueId::Binance, MarketType::Spot, "BTCUSDT"));
    let state = manager
        .get_state(VenueId::Binance, MarketType::Spot, "BTCUSDT")
        .unwrap();
    assert_eq!(state.lifecycle_state, BookLifecycleState::Invalidated);
    assert_eq!(
        state.validity,
        BookValidity::Invalid(InvalidationReason::VenueDisconnected("TCP reset".into()))
    );
}

// ============================================================================
// 20. FUNDING RATE ROUTING TEST
// ============================================================================

#[test]
fn test_manager_funding_rate_routing() {
    let mut manager = MarketStateManager::new(Duration::from_millis(1000));
    let funding = MarketEvent::FundingRateUpdate {
        venue: VenueId::Binance,
        symbol: "BTCUSDT".into(),
        mark_price: dec!(65010.50),
        index_price: Some(dec!(65008.00)),
        rate: dec!(0.00010000),
        next_funding_ts_ms: 1727414400000,
        exchange_ts_ms: 1727400000000,
        local_recv_ts_ns: 1000,
    };
    manager.handle_event(&funding).unwrap();

    let fund_state = manager.get_funding(VenueId::Binance, "BTCUSDT").unwrap();
    assert_eq!(fund_state.mark_price, dec!(65010.50));
    assert_eq!(fund_state.rate, dec!(0.00010000));
    assert_eq!(fund_state.next_funding_ts_ms, 1727414400000);
}
