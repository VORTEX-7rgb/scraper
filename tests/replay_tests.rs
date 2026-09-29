use airbitrage::market::MarketStateManager;
use airbitrage::market::state::{BookLifecycleState, SequencePolicy};
use airbitrage::observatory::types::{
    DislocationObservation, MarketRelationship, ObservationRejectionReason, OpportunityKey,
    PersistenceTransition,
};
use airbitrage::observatory::{ObservationEngine, OpportunityTracker};
use airbitrage::recording::{ResearchReader, ResearchRecorder};
use airbitrage::replay::ReplayEngine;
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use rust_decimal::Decimal;
use std::io::Cursor;
use std::time::Duration;

#[allow(clippy::too_many_arguments)]
fn make_book_snapshot(
    venue: VenueId,
    market_type: MarketType,
    symbol: &str,
    bids: Vec<(i64, i64)>,
    asks: Vec<(i64, i64)>,
    exchange_ts_ms: i64,
    local_recv_ts_ns: i64,
    sequence_id: u64,
) -> MarketEvent {
    MarketEvent::OrderBookSnapshot {
        venue,
        market_type,
        symbol: symbol.to_string(),
        bids: bids
            .into_iter()
            .map(|(p, q)| PriceLevel::new(Decimal::from(p), Decimal::from(q)))
            .collect(),
        asks: asks
            .into_iter()
            .map(|(p, q)| PriceLevel::new(Decimal::from(p), Decimal::from(q)))
            .collect(),
        exchange_ts_ms,
        local_recv_ts_ns,
        sequence_id,
    }
}

#[allow(clippy::too_many_arguments)]
fn make_book_delta(
    venue: VenueId,
    market_type: MarketType,
    symbol: &str,
    bids: Vec<(i64, i64)>,
    asks: Vec<(i64, i64)>,
    first_seq: u64,
    seq: u64,
    prev_seq: Option<u64>,
    exchange_ts_ms: i64,
    local_recv_ts_ns: i64,
) -> MarketEvent {
    MarketEvent::OrderBookDelta {
        venue,
        market_type,
        symbol: symbol.to_string(),
        bids: bids
            .into_iter()
            .map(|(p, q)| PriceLevel::new(Decimal::from(p), Decimal::from(q)))
            .collect(),
        asks: asks
            .into_iter()
            .map(|(p, q)| PriceLevel::new(Decimal::from(p), Decimal::from(q)))
            .collect(),
        first_sequence_id: first_seq,
        sequence_id: seq,
        prev_sequence_id: prev_seq,
        transaction_ts_ms: exchange_ts_ms,
        exchange_ts_ms,
        local_recv_ts_ns,
    }
}

#[test]
fn test_01_replay_empty_dataset() {
    let mut reader = ResearchReader::new(Cursor::new(b""));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine
        .replay(&mut reader)
        .expect("Empty replay must succeed");

    assert_eq!(result.total_records_read, 0);
    assert_eq!(result.market_events_consumed, 0);
    assert_eq!(result.replayed_observations, 0);
    assert!(result.is_clean());
    assert!(result.is_deterministic());
}

#[test]
fn test_02_replay_single_snapshot() {
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snapshot = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65000, 10)],
        vec![(65100, 10)],
        1_000,
        1_000_000_000,
        1,
    );
    recorder
        .record_market_event(1_000_000_000, snapshot)
        .unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.total_records_read, 1);
    assert_eq!(result.market_events_consumed, 1);
    assert_eq!(result.market_events_mutated, 1);
    assert!(result.is_clean());

    let state = engine
        .manager()
        .get_state(VenueId::Binance, MarketType::Spot, "BTCUSDT")
        .expect("State must exist");
    assert_eq!(state.lifecycle_state, BookLifecycleState::Live);
    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(Decimal::from(65000), Decimal::from(10)))
    );
    assert_eq!(
        state.best_ask(),
        Some(PriceLevel::new(Decimal::from(65100), Decimal::from(10)))
    );
}

#[test]
fn test_03_replay_snapshot_plus_delta() {
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snapshot = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65000, 10)],
        vec![(65100, 10)],
        1_000,
        1_000_000_000,
        1,
    );
    let delta = make_book_delta(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65050, 5)], // New best bid
        vec![],
        2,
        2,
        Some(1),
        1_100,
        1_100_000_000,
    );

    recorder
        .record_market_event(1_000_000_000, snapshot)
        .unwrap();
    recorder.record_market_event(1_100_000_000, delta).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.total_records_read, 2);
    assert_eq!(result.market_events_consumed, 2);
    assert_eq!(result.market_events_mutated, 2);
    assert!(result.is_clean());

    let state = engine
        .manager()
        .get_state(VenueId::Bybit, MarketType::Spot, "BTCUSDT")
        .unwrap();
    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(Decimal::from(65050), Decimal::from(5)))
    );
}

#[test]
fn test_04_replay_multiple_deltas() {
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snapshot = make_book_snapshot(
        VenueId::Bybit,
        MarketType::LinearPerpetual,
        "BTCUSDT",
        vec![(65000, 10)],
        vec![(65100, 10)],
        1_000,
        1_000_000_000,
        100,
    );
    recorder
        .record_market_event(1_000_000_000, snapshot)
        .unwrap();

    for i in 1..=5 {
        let delta = make_book_delta(
            VenueId::Bybit,
            MarketType::LinearPerpetual,
            "BTCUSDT",
            vec![(65000 + i, 1)],
            vec![(65100 - i, 1)],
            100 + i as u64,
            100 + i as u64,
            Some(100 + i as u64 - 1),
            1_000 + i * 100,
            1_000_000_000 + i * 100_000_000,
        );
        recorder
            .record_market_event(1_000_000_000 + i * 100_000_000, delta)
            .unwrap();
    }

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.total_records_read, 6);
    assert_eq!(result.market_events_consumed, 6);
    assert_eq!(result.market_events_mutated, 6);
    assert!(result.is_clean());

    let state = engine
        .manager()
        .get_state(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT")
        .unwrap();
    assert_eq!(
        state.best_bid(),
        Some(PriceLevel::new(Decimal::from(65005), Decimal::from(1)))
    );
    assert_eq!(
        state.best_ask(),
        Some(PriceLevel::new(Decimal::from(65095), Decimal::from(1)))
    );
}

#[test]
fn test_05_sequence_continuity() {
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        1_000_000,
        1,
    );
    let delta1 = make_book_delta(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 2)],
        vec![],
        2,
        2,
        Some(1),
        1001,
        1_001_000,
    );
    let delta2 = make_book_delta(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 3)],
        vec![],
        3,
        3,
        Some(2),
        1002,
        1_002_000,
    );

    recorder.record_market_event(1_000_000, snap).unwrap();
    recorder.record_market_event(1_001_000, delta1).unwrap();
    recorder.record_market_event(1_002_000, delta2).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.market_events_consumed, 3);
    assert!(result.is_clean());
}

#[test]
fn test_06_invalid_sequence_handling() {
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap = make_book_snapshot(
        VenueId::Binance,
        MarketType::LinearPerpetual,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        1_000_000,
        100,
    );
    // Gap: last_u was 100, incoming delta has prev_u = 105 (gap)
    let bad_delta = make_book_delta(
        VenueId::Binance,
        MarketType::LinearPerpetual,
        "BTCUSDT",
        vec![(100, 2)],
        vec![],
        106,
        107,
        Some(105),
        1001,
        1_001_000,
    );

    recorder.record_market_event(1_000_000, snap).unwrap();
    recorder.record_market_event(1_001_000, bad_delta).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader);

    assert!(
        result.is_err(),
        "Sequence gap must fail replay when fail_on_market_error is true"
    );
}

#[test]
fn test_07_snapshot_recovery_behavior() {
    let mut manager = MarketStateManager::new(Duration::from_millis(5000));
    manager.register_instrument_with_policy(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        SequencePolicy::SnapshotOnly,
    );

    let snap1 = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        1_000_000,
        1,
    );
    manager.handle_event(&snap1).unwrap();
    assert_eq!(
        manager
            .get_state(VenueId::Binance, MarketType::Spot, "BTCUSDT")
            .unwrap()
            .lifecycle_state,
        BookLifecycleState::Live
    );

    manager
        .request_resync(VenueId::Binance, MarketType::Spot, "BTCUSDT")
        .unwrap();

    let snap2 = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(102, 1)],
        vec![(103, 1)],
        2000,
        2_000_000,
        2,
    );
    manager.handle_event(&snap2).unwrap();
    assert_eq!(
        manager
            .get_state(VenueId::Binance, MarketType::Spot, "BTCUSDT")
            .unwrap()
            .lifecycle_state,
        BookLifecycleState::Live
    );
}

#[test]
fn test_08_trusted_state_transition() {
    let mut manager = MarketStateManager::new(Duration::from_millis(5000));
    let now_ns = 1_000_000_i64;

    assert!(
        manager
            .get_trusted_book(VenueId::Binance, MarketType::Spot, "BTCUSDT", now_ns)
            .is_none()
    );

    let snap = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        now_ns,
        1,
    );
    manager.handle_event(&snap).unwrap();
    assert!(
        manager
            .get_trusted_book(VenueId::Binance, MarketType::Spot, "BTCUSDT", now_ns)
            .is_some()
    );
}

#[test]
fn test_09_untrusted_state_behavior() {
    let mut manager = MarketStateManager::new(Duration::from_millis(5000));
    let now_ns = 1_000_000_i64;

    let crossed_snap = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 1)],
        vec![(100, 1)], // Crossed
        1000,
        now_ns,
        1,
    );
    let res = manager.handle_event(&crossed_snap);
    assert!(res.is_err());
    assert!(
        manager
            .get_trusted_book(VenueId::Binance, MarketType::Spot, "BTCUSDT", now_ns)
            .is_none()
    );
}

#[test]
fn test_10_deterministic_replay_twice() {
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65000, 10)],
        vec![(65100, 10)],
        1000,
        1_000_000_000,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65050, 10)],
        vec![(65150, 10)],
        1000,
        1_000_000_000,
        1,
    );

    recorder.record_market_event(1_000_000_000, snap_a).unwrap();
    recorder.record_market_event(1_000_000_000, snap_b).unwrap();

    let bytes = recorder.into_inner().unwrap();

    let mut reader1 = ResearchReader::new(Cursor::new(&bytes));
    let mut engine1 = ReplayEngine::with_defaults();
    let result1 = engine1.replay(&mut reader1).unwrap();

    let mut reader2 = ResearchReader::new(Cursor::new(&bytes));
    let mut engine2 = ReplayEngine::with_defaults();
    let result2 = engine2.replay(&mut reader2).unwrap();

    assert_eq!(result1, result2);
}

#[test]
fn test_11_exact_decimal_equality() {
    let mut manager = MarketStateManager::new(Duration::from_millis(5000));
    let now_ns = 1_000_000_000_i64;

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65000, 5)],
        vec![(65100, 5)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65200, 5)],
        vec![(65300, 5)],
        1000,
        now_ns,
        1,
    );
    manager.handle_event(&snap_a).unwrap();
    manager.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs = obs_engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );

    assert_eq!(obs.buy_vwap, Some(Decimal::from(65100)));
    assert_eq!(obs.sell_vwap, Some(Decimal::from(65200)));
    assert_eq!(obs.gross_spread, Some(Decimal::from(100)));
}

#[test]
fn test_12_replayed_vwap_equality() {
    let mut recorder = ResearchRecorder::new(Vec::new());
    let now_ns = 1_000_000_000_i64;

    let snap_binance = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        bids: vec![PriceLevel::new(Decimal::from(90), Decimal::from(10))],
        asks: vec![
            PriceLevel::new(Decimal::from(100), Decimal::from(1)),
            PriceLevel::new(Decimal::from(103), Decimal::from(2)),
        ],
        exchange_ts_ms: 1000,
        local_recv_ts_ns: now_ns,
        sequence_id: 1,
    };
    let snap_bybit = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Bybit,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        bids: vec![PriceLevel::new(Decimal::from(110), Decimal::from(3))],
        asks: vec![PriceLevel::new(Decimal::from(120), Decimal::from(10))],
        exchange_ts_ms: 1000,
        local_recv_ts_ns: now_ns,
        sequence_id: 1,
    };

    recorder
        .record_market_event(now_ns, snap_binance.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_bybit.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_binance).unwrap();
    temp_mgr.handle_event(&snap_bybit).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let expected_obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::from(3),
        now_ns,
    );
    assert_eq!(expected_obs.buy_vwap, Some(Decimal::from(102)));

    recorder.record_dislocation(expected_obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_13_replayed_fee_equality() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(99, 10)],
        vec![(100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 10)],
        vec![(101, 10)],
        1000,
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let expected_obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    recorder.record_dislocation(expected_obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_14_replayed_gross_edge_equality() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(90, 10)],
        vec![(100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 10)],
        vec![(110, 10)],
        1000,
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let expected_obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert_eq!(expected_obs.gross_edge, Some(Decimal::from(5))); // 105 - 100
    recorder.record_dislocation(expected_obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_15_replayed_net_edge_equality() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(90, 10)],
        vec![(100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 10)],
        vec![(110, 10)],
        1000,
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let expected_obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert!(expected_obs.net_edge.is_some());
    recorder.record_dislocation(expected_obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_16_replayed_quantity_sweep_equality() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(90, 10)],
        vec![(100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 10)],
        vec![(110, 10)],
        1000,
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let sweep = obs_engine.observe_sweep(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        now_ns,
    );
    let sweep_len = sweep.len();
    for obs in sweep {
        recorder.record_dislocation(obs).unwrap();
    }

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, sweep_len as u64);
    assert!(result.is_clean());
}

#[test]
fn test_17_replayed_freshness_equality() {
    let now_ns = 1_500_000_000_i64;
    let recv_ns = 1_000_000_000_i64; // age = 500ms
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        recv_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        recv_ns,
        1,
    );
    recorder
        .record_market_event(recv_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(recv_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert_eq!(obs.buy_book_age_ms, Some(500));
    assert_eq!(obs.sell_book_age_ms, Some(500));
    recorder.record_dislocation(obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_18_replayed_timestamp_skew_equality() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1050, // 50ms skew
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert_eq!(obs.timestamp_skew_ms, Some(50));
    recorder.record_dislocation(obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_19_replayed_persistence_start() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(90, 10)],
        vec![(100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 10)],
        vec![(110, 10)],
        1000,
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    recorder.record_dislocation(obs.clone()).unwrap();

    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let transition = tracker.process_observation(&obs);
    assert_eq!(transition, PersistenceTransition::Started);

    let key = OpportunityKey::new(
        obs.buy_venue,
        obs.buy_market,
        obs.sell_venue,
        obs.sell_market,
        &obs.symbol,
        obs.reference_quantity,
        obs.market_relationship,
    );
    recorder.record_transition(key, now_ns, transition).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert_eq!(result.matched_transitions, 1);
    assert!(result.is_clean());
}

#[test]
fn test_20_replayed_persistence_continue() {
    let now_ns1 = 1_000_000_000_i64;
    let now_ns2 = 1_100_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(90, 10)],
        vec![(100, 10)],
        1000,
        now_ns1,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 10)],
        vec![(110, 10)],
        1000,
        now_ns1,
        1,
    );
    recorder
        .record_market_event(now_ns1, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns1, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs1 = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns1,
    );
    recorder.record_dislocation(obs1.clone()).unwrap();

    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let trans1 = tracker.process_observation(&obs1);
    let key = OpportunityKey::new(
        obs1.buy_venue,
        obs1.buy_market,
        obs1.sell_venue,
        obs1.sell_market,
        &obs1.symbol,
        obs1.reference_quantity,
        obs1.market_relationship,
    );
    recorder
        .record_transition(key.clone(), now_ns1, trans1)
        .unwrap();

    let obs2 = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns2,
    );
    recorder.record_dislocation(obs2.clone()).unwrap();
    let trans2 = tracker.process_observation(&obs2);
    assert_eq!(trans2, PersistenceTransition::Continued);
    recorder.record_transition(key, now_ns2, trans2).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 2);
    assert_eq!(result.matched_transitions, 2);
    assert!(result.is_clean());
}

#[test]
fn test_21_replayed_persistence_end() {
    let now_ns1 = 1_000_000_000_i64;
    let now_ns2 = 1_200_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(90, 10)],
        vec![(100, 10)],
        1000,
        now_ns1,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 10)],
        vec![(110, 10)],
        1000,
        now_ns1,
        1,
    );
    recorder
        .record_market_event(now_ns1, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns1, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs1 = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns1,
    );
    recorder.record_dislocation(obs1.clone()).unwrap();

    let mut tracker = OpportunityTracker::new(Decimal::ZERO);
    let trans1 = tracker.process_observation(&obs1);
    let key = OpportunityKey::new(
        obs1.buy_venue,
        obs1.buy_market,
        obs1.sell_venue,
        obs1.sell_market,
        &obs1.symbol,
        obs1.reference_quantity,
        obs1.market_relationship,
    );
    recorder
        .record_transition(key.clone(), now_ns1, trans1)
        .unwrap();

    let delta = make_book_delta(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 0), (99, 10)], // Delete 105, add 99
        vec![],
        2,
        2,
        Some(1),
        1200,
        now_ns2,
    );
    recorder
        .record_market_event(now_ns2, delta.clone())
        .unwrap();
    temp_mgr.handle_event(&delta).unwrap();

    let obs2 = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns2,
    );
    recorder.record_dislocation(obs2.clone()).unwrap();
    let trans2 = tracker.process_observation(&obs2);
    assert!(matches!(trans2, PersistenceTransition::Ended(_)));

    if let PersistenceTransition::Ended(ref rec) = trans2 {
        recorder.record_opportunity((**rec).clone()).unwrap();
    }
    recorder.record_transition(key, now_ns2, trans2).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 2);
    assert_eq!(result.matched_transitions, 2);
    assert_eq!(result.matched_opportunities, 1);
    assert!(result.is_clean());
}

#[test]
fn test_22_observation_mismatch_detection() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(90, 10)],
        vec![(100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 10)],
        vec![(110, 10)],
        1000,
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let mut tampered_obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    tampered_obs.net_edge = Some(Decimal::from(9999));
    recorder.record_dislocation(tampered_obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert!(!result.is_clean());
    assert_eq!(result.mismatches.len(), 1);
    assert_eq!(result.mismatches[0].field_name, "net_edge");
}

#[test]
fn test_23_mismatch_diagnostics() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(90, 10)],
        vec![(100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(105, 10)],
        vec![(110, 10)],
        1000,
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let mut tampered = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    tampered.buy_vwap = Some(Decimal::from(42));
    recorder.record_dislocation(tampered).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.mismatches.len(), 1);
    let m = &result.mismatches[0];
    assert_eq!(m.record_index, 3);
    assert_eq!(m.field_name, "buy_vwap");
    assert_eq!(m.recorded_value, "Some(42)");
    assert_eq!(m.replayed_value, "Some(100)");
    assert_eq!(m.symbol, "BTCUSDT");

    let disp = m.to_string();
    assert!(disp.contains("buy_vwap"));
    assert!(disp.contains("expected 'Some(42)'"));
    assert!(disp.contains("got 'Some(100)'"));
}

#[test]
fn test_24_derived_events_not_used_as_replay_inputs() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let obs = DislocationObservation::rejected(
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        MarketRelationship::SpotSpot,
        now_ns,
        Decimal::ONE,
        ObservationRejectionReason::MissingBook {
            venue: VenueId::Binance,
            market_type: MarketType::Spot,
        },
    );
    recorder.record_dislocation(obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.market_events_consumed, 0);
    assert!(
        engine
            .manager()
            .get_state(VenueId::Binance, MarketType::Spot, "BTCUSDT")
            .is_none()
    );
}

#[test]
fn test_25_recorded_at_timestamp_does_not_replace_market_timestamp() {
    let mut recorder = ResearchRecorder::new(Vec::new());

    let market_recv_ts = 123_456_789_i64;
    let envelope_ts = 999_999_999_i64;

    let snap = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        market_recv_ts,
        1,
    );
    recorder.record_market_event(envelope_ts, snap).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    engine.replay(&mut reader).unwrap();

    let state = engine
        .manager()
        .get_state(VenueId::Binance, MarketType::Spot, "BTCUSDT")
        .unwrap();
    assert_eq!(state.last_local_recv_ts_ns, Some(market_recv_ts));
    assert_ne!(state.last_local_recv_ts_ns, Some(envelope_ts));
}

#[test]
fn test_26_multiple_venues() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let venues = [
        (VenueId::Binance, MarketType::Spot),
        (VenueId::Binance, MarketType::LinearPerpetual),
        (VenueId::Bybit, MarketType::Spot),
        (VenueId::Bybit, MarketType::LinearPerpetual),
    ];

    for &(ven, mkt) in &venues {
        let snap = make_book_snapshot(
            ven,
            mkt,
            "BTCUSDT",
            vec![(100, 1)],
            vec![(101, 1)],
            1000,
            now_ns,
            1,
        );
        recorder.record_market_event(now_ns, snap).unwrap();
    }

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.market_events_consumed, 4);
    assert_eq!(result.final_books.len(), 4);
    for &(ven, mkt) in &venues {
        let state = engine.manager().get_state(ven, mkt, "BTCUSDT").unwrap();
        assert_eq!(state.lifecycle_state, BookLifecycleState::Live);
    }
}

#[test]
fn test_27_spot_spot_comparison() {
    let mut manager = MarketStateManager::new(Duration::from_millis(5000));
    let now_ns = 1_000_000_000_i64;

    let s1 = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        now_ns,
        1,
    );
    let s2 = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        now_ns,
        1,
    );
    manager.handle_event(&s1).unwrap();
    manager.handle_event(&s2).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs = obs_engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert_eq!(obs.market_relationship, MarketRelationship::SpotSpot);
}

#[test]
fn test_28_perp_perp_comparison() {
    let mut manager = MarketStateManager::new(Duration::from_millis(5000));
    let now_ns = 1_000_000_000_i64;

    let s1 = make_book_snapshot(
        VenueId::Binance,
        MarketType::LinearPerpetual,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        now_ns,
        1,
    );
    let s2 = make_book_snapshot(
        VenueId::Bybit,
        MarketType::LinearPerpetual,
        "BTCUSDT",
        vec![(100, 1)],
        vec![(101, 1)],
        1000,
        now_ns,
        1,
    );
    manager.handle_event(&s1).unwrap();
    manager.handle_event(&s2).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs = obs_engine.observe_pair(
        &manager,
        VenueId::Binance,
        MarketType::LinearPerpetual,
        VenueId::Bybit,
        MarketType::LinearPerpetual,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert_eq!(obs.market_relationship, MarketRelationship::PerpPerp);
}

#[test]
fn test_29_rejected_observation_comparison() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let obs_engine = ObservationEngine::with_defaults();
    let empty_mgr = MarketStateManager::new(Duration::from_millis(5000));
    let rejected_obs = obs_engine.observe_pair(
        &empty_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert!(!rejected_obs.is_valid);
    recorder.record_dislocation(rejected_obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_30_partial_executable_quantity() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        bids: vec![PriceLevel::new(Decimal::from(90), Decimal::from(10))],
        asks: vec![PriceLevel::new(Decimal::from(100), Decimal::new(5, 1))], // 0.5 BTC
        exchange_ts_ms: 1000,
        local_recv_ts_ns: now_ns,
        sequence_id: 1,
    };
    let snap_b = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Bybit,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        bids: vec![PriceLevel::new(Decimal::from(105), Decimal::from(10))],
        asks: vec![PriceLevel::new(Decimal::from(110), Decimal::from(10))],
        exchange_ts_ms: 1000,
        local_recv_ts_ns: now_ns,
        sequence_id: 1,
    };
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert!(!obs.fully_executable);
    assert_eq!(obs.common_executable_quantity, Decimal::new(5, 1));
    recorder.record_dislocation(obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_31_negative_net_edge() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(99, 10)],
        vec![(100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(100, 10)],
        vec![(101, 10)],
        1000,
        now_ns,
        1,
    );
    recorder
        .record_market_event(now_ns, snap_a.clone())
        .unwrap();
    recorder
        .record_market_event(now_ns, snap_b.clone())
        .unwrap();

    let mut temp_mgr = MarketStateManager::new(Duration::from_millis(5000));
    temp_mgr.handle_event(&snap_a).unwrap();
    temp_mgr.handle_event(&snap_b).unwrap();

    let obs_engine = ObservationEngine::with_defaults();
    let obs = obs_engine.observe_pair(
        &temp_mgr,
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        now_ns,
    );
    assert!(obs.net_edge.is_some());
    assert!(obs.net_edge.unwrap() < Decimal::ZERO);
    recorder.record_dislocation(obs).unwrap();

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader).unwrap();

    assert_eq!(result.matched_observations, 1);
    assert!(result.is_clean());
}

#[test]
fn test_32_repeated_replay_determinism() {
    let now_ns = 1_000_000_000_i64;
    let mut recorder = ResearchRecorder::new(Vec::new());

    let snap_a = make_book_snapshot(
        VenueId::Binance,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65000, 10)],
        vec![(65100, 10)],
        1000,
        now_ns,
        1,
    );
    let snap_b = make_book_snapshot(
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        vec![(65050, 10)],
        vec![(65150, 10)],
        1000,
        now_ns,
        1,
    );
    recorder.record_market_event(now_ns, snap_a).unwrap();
    recorder.record_market_event(now_ns, snap_b).unwrap();

    let bytes = recorder.into_inner().unwrap();

    let mut base_result = None;
    for _ in 0..50 {
        let mut reader = ResearchReader::new(Cursor::new(&bytes));
        let mut engine = ReplayEngine::with_defaults();
        let result = engine.replay(&mut reader).unwrap();

        if let Some(ref base) = base_result {
            assert_eq!(base, &result);
        } else {
            base_result = Some(result);
        }
    }
}
