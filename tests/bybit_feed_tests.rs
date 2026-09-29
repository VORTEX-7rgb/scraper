use airbitrage::error::EngineError;
use airbitrage::market::MarketStateManager;
use airbitrage::market::state::{BookLifecycleState, SequencePolicy};
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use airbitrage::venues::bybit::{
    BYBIT_LINEAR_WS_URL, BYBIT_SPOT_WS_URL, BybitFeedConfig, BybitParsedMessage, BybitRequest,
    into_canonical_event, parse_bybit_message, parse_price_levels,
};
use rust_decimal_macros::dec;
use std::time::Duration;

// ============================================================================
// TEST 1: VALID BYBIT CONNECTION CONFIGURATION
// ============================================================================

#[test]
fn test_m3_1_connection_configuration() {
    let spot_cfg = BybitFeedConfig::default_spot("BTCUSDT");
    assert_eq!(spot_cfg.venue, VenueId::Bybit);
    assert_eq!(spot_cfg.market_type, MarketType::Spot);
    assert_eq!(spot_cfg.symbol, "BTCUSDT");
    assert_eq!(spot_cfg.ws_url, BYBIT_SPOT_WS_URL);
    assert_eq!(spot_cfg.depth, 50);
    assert_eq!(spot_cfg.orderbook_topic(), "orderbook.50.BTCUSDT");
    assert_eq!(spot_cfg.heartbeat_interval, Duration::from_secs(20));

    let linear_cfg = BybitFeedConfig::default_linear("btcusdt");
    assert_eq!(linear_cfg.venue, VenueId::Bybit);
    assert_eq!(linear_cfg.market_type, MarketType::LinearPerpetual);
    assert_eq!(linear_cfg.symbol, "BTCUSDT");
    assert_eq!(linear_cfg.ws_url, BYBIT_LINEAR_WS_URL);
    assert_eq!(linear_cfg.depth, 50);
    assert_eq!(linear_cfg.orderbook_topic(), "orderbook.50.BTCUSDT");
}

// ============================================================================
// TEST 2: VALID SUBSCRIPTION PAYLOAD GENERATION
// ============================================================================

#[test]
fn test_m3_1_subscription_payload_generation() {
    let req = BybitRequest::subscribe(
        vec!["orderbook.50.BTCUSDT".to_string()],
        Some("req_spot_101".to_string()),
    );
    let json_str = serde_json::to_string(&req).expect("Serialization should succeed");
    assert!(json_str.contains("\"op\":\"subscribe\""));
    assert!(json_str.contains("\"args\":[\"orderbook.50.BTCUSDT\"]"));
    assert!(json_str.contains("\"req_id\":\"req_spot_101\""));
}

// ============================================================================
// TEST 3: SUBSCRIPTION ACKNOWLEDGEMENT PARSING
// ============================================================================

#[test]
fn test_m3_1_subscription_acknowledgement_parsing() {
    // Success ack
    let success_json = r#"{
        "success": true,
        "ret_msg": "subscribe",
        "conn_id": "2324d924-aa4d-45b0-a858-7b8be29ab52b",
        "req_id": "req_spot_101",
        "op": "subscribe"
    }"#;
    let parsed = parse_bybit_message(success_json).expect("Should parse success ack");
    match parsed {
        BybitParsedMessage::SubscriptionAck {
            success,
            ret_msg,
            req_id,
            conn_id,
        } => {
            assert!(success);
            assert_eq!(ret_msg, "subscribe");
            assert_eq!(req_id, Some("req_spot_101".into()));
            assert_eq!(conn_id, Some("2324d924-aa4d-45b0-a858-7b8be29ab52b".into()));
        }
        other => panic!("Expected SubscriptionAck, got: {:?}", other),
    }

    // Failure ack
    let failure_json = r#"{
        "success": false,
        "ret_msg": "unknown topic invalid_topic",
        "conn_id": "2324d924-aa4d-45b0-a858-7b8be29ab52b",
        "req_id": "req_spot_102",
        "op": "subscribe"
    }"#;
    let parsed_fail = parse_bybit_message(failure_json).expect("Should parse failure ack");
    match parsed_fail {
        BybitParsedMessage::SubscriptionAck {
            success, ret_msg, ..
        } => {
            assert!(!success);
            assert!(ret_msg.contains("unknown topic"));
        }
        other => panic!("Expected SubscriptionAck, got: {:?}", other),
    }
}

// ============================================================================
// TEST 4: SNAPSHOT PAYLOAD PARSING
// ============================================================================

#[test]
fn test_m3_1_snapshot_payload_parsing() {
    let snapshot_json = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": 1727400000000,
        "data": {
            "s": "BTCUSDT",
            "b": [
                ["65000.50", "1.500"],
                ["65000.00", "2.000"]
            ],
            "a": [
                ["65001.00", "1.000"],
                ["65001.50", "0.500"]
            ],
            "u": 123456,
            "seq": 789012
        }
    }"#;

    let parsed = parse_bybit_message(snapshot_json).expect("Should parse snapshot");
    match parsed {
        BybitParsedMessage::OrderBookSnapshot {
            symbol,
            bids,
            asks,
            sequence_id,
            seq,
            exchange_ts_ms,
        } => {
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(sequence_id, 123456);
            assert_eq!(seq, Some(789012));
            assert_eq!(exchange_ts_ms, 1727400000000);
            assert_eq!(bids.len(), 2);
            assert_eq!(asks.len(), 2);
            assert_eq!(bids[0], PriceLevel::new(dec!(65000.50), dec!(1.500)));
            assert_eq!(asks[0], PriceLevel::new(dec!(65001.00), dec!(1.000)));
        }
        other => panic!("Expected OrderBookSnapshot, got: {:?}", other),
    }
}

// ============================================================================
// TEST 5: DELTA PAYLOAD PARSING
// ============================================================================

#[test]
fn test_m3_1_delta_payload_parsing() {
    let delta_json = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "delta",
        "ts": 1727400000050,
        "cts": 1727400000045,
        "data": {
            "s": "BTCUSDT",
            "b": [
                ["65000.50", "0.000"],
                ["64999.00", "3.100"]
            ],
            "a": [
                ["65001.00", "1.200"]
            ],
            "u": 123460,
            "seq": 789015
        }
    }"#;

    let parsed = parse_bybit_message(delta_json).expect("Should parse delta");
    match parsed {
        BybitParsedMessage::OrderBookDelta {
            symbol,
            bids,
            asks,
            sequence_id,
            seq,
            transaction_ts_ms,
            exchange_ts_ms,
        } => {
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(sequence_id, 123460);
            assert_eq!(seq, Some(789015));
            assert_eq!(transaction_ts_ms, 1727400000045);
            assert_eq!(exchange_ts_ms, 1727400000050);
            assert_eq!(bids.len(), 2);
            assert_eq!(asks.len(), 1);
            assert_eq!(bids[0].price, dec!(65000.50));
            assert_eq!(bids[0].quantity, dec!(0.000)); // Delete quantity
            assert_eq!(bids[1].price, dec!(64999.00));
            assert_eq!(bids[1].quantity, dec!(3.100));
            assert_eq!(asks[0].price, dec!(65001.00));
            assert_eq!(asks[0].quantity, dec!(1.200));
        }
        other => panic!("Expected OrderBookDelta, got: {:?}", other),
    }
}

// ============================================================================
// TEST 6: STRING-ENCODED PRICE PARSING
// ============================================================================

#[test]
fn test_m3_1_string_encoded_price_parsing() {
    let levels = parse_price_levels(&[["65000.50".to_string(), "1.0".to_string()]]).unwrap();
    assert_eq!(levels[0].price, dec!(65000.50));
}

// ============================================================================
// TEST 7: STRING-ENCODED QUANTITY PARSING
// ============================================================================

#[test]
fn test_m3_1_string_encoded_quantity_parsing() {
    let levels = parse_price_levels(&[["65000.00".to_string(), "2.750".to_string()]]).unwrap();
    assert_eq!(levels[0].quantity, dec!(2.750));
}

// ============================================================================
// TEST 8: ZERO-QUANTITY DELETION PARSING
// ============================================================================

#[test]
fn test_m3_1_zero_quantity_deletion_parsing() {
    let levels = parse_price_levels(&[["65000.50".to_string(), "0".to_string()]]).unwrap();
    assert_eq!(levels[0].price, dec!(65000.50));
    assert_eq!(levels[0].quantity, dec!(0));
    assert!(levels[0].quantity.is_zero());
}

// ============================================================================
// TEST 9: UNKNOWN MESSAGE HANDLING
// ============================================================================

#[test]
fn test_m3_1_unknown_message_handling() {
    let unknown_json = r#"{"topic": "kline.1m.BTCUSDT", "data": []}"#;
    let parsed = parse_bybit_message(unknown_json).expect("Should handle unknown topic safely");
    match parsed {
        BybitParsedMessage::Unknown(msg) => {
            assert!(msg.contains("kline.1m.BTCUSDT"));
        }
        other => panic!("Expected Unknown, got: {:?}", other),
    }
}

// ============================================================================
// TEST 10: MALFORMED PAYLOAD HANDLING
// ============================================================================

#[test]
fn test_m3_1_malformed_payload_handling() {
    // 1. Invalid JSON syntax
    let malformed_syntax = r#"{"topic": "orderbook.50.BTCUSDT", "type": "snapshot""#;
    let res = parse_bybit_message(malformed_syntax);
    assert!(res.is_err());
    match res {
        Err(EngineError::Json(_)) => {}
        other => panic!("Expected EngineError::Json, got: {:?}", other),
    }

    // 2. Invalid price number string
    let bad_price_json = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": 1727400000000,
        "data": {
            "s": "BTCUSDT",
            "b": [["not_a_valid_price", "1.0"]],
            "a": [],
            "u": 100
        }
    }"#;
    let res_price = parse_bybit_message(bad_price_json);
    assert!(res_price.is_err());
    match res_price {
        Err(EngineError::Validation(msg)) => {
            assert!(msg.contains("Invalid price"));
        }
        other => panic!("Expected EngineError::Validation, got: {:?}", other),
    }
}

// ============================================================================
// TEST 11: HEARTBEAT HANDLING
// ============================================================================

#[test]
fn test_m3_1_heartbeat_handling() {
    // Generate ping
    let ping = BybitRequest::ping();
    let ping_json = serde_json::to_string(&ping).unwrap();
    assert_eq!(ping_json, r#"{"op":"ping"}"#);

    // Parse pong (format 1: op == pong)
    let pong_json = r#"{
        "op": "pong",
        "args": ["1727400000000"],
        "conn_id": "2324d924-aa4d-45b0-a858-7b8be29ab52b"
    }"#;
    let parsed = parse_bybit_message(pong_json).expect("Should parse pong");
    match parsed {
        BybitParsedMessage::Pong { args, conn_id } => {
            assert_eq!(args, vec!["1727400000000"]);
            assert_eq!(conn_id, Some("2324d924-aa4d-45b0-a858-7b8be29ab52b".into()));
        }
        other => panic!("Expected Pong, got: {:?}", other),
    }

    // Parse pong (format 2: op == ping with ret_msg == pong as observed live)
    let live_pong_json = r#"{
        "success": true,
        "ret_msg": "pong",
        "conn_id": "d9avu63jf06b2lc5o2ig-eptrh",
        "op": "ping"
    }"#;
    let parsed_live = parse_bybit_message(live_pong_json).expect("Should parse live pong response");
    match parsed_live {
        BybitParsedMessage::Pong { conn_id, .. } => {
            assert_eq!(conn_id, Some("d9avu63jf06b2lc5o2ig-eptrh".into()));
        }
        other => panic!("Expected Pong for live ping/pong ack, got: {:?}", other),
    }
}

// ============================================================================
// TEST 12: BYBIT U EXTRACTION
// ============================================================================

#[test]
fn test_m3_1_bybit_u_extraction() {
    let snap_json = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": 1000,
        "data": { "s": "BTCUSDT", "b": [], "a": [], "u": 99999 }
    }"#;
    let parsed_snap = parse_bybit_message(snap_json).unwrap();
    if let BybitParsedMessage::OrderBookSnapshot { sequence_id, .. } = parsed_snap {
        assert_eq!(sequence_id, 99999);
    } else {
        panic!("Expected snapshot");
    }

    let delta_json = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "delta",
        "ts": 1005,
        "data": { "s": "BTCUSDT", "b": [], "a": [], "u": 100005 }
    }"#;
    let parsed_delta = parse_bybit_message(delta_json).unwrap();
    if let BybitParsedMessage::OrderBookDelta { sequence_id, .. } = parsed_delta {
        assert_eq!(sequence_id, 100005);
    } else {
        panic!("Expected delta");
    }
}

// ============================================================================
// TEST 13: CANONICAL SNAPSHOT EVENT GENERATION
// ============================================================================

#[test]
fn test_m3_1_canonical_snapshot_event_generation() {
    let parsed = BybitParsedMessage::OrderBookSnapshot {
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
        asks: vec![PriceLevel::new(dec!(65010.00), dec!(1.0))],
        sequence_id: 123456,
        seq: Some(789012),
        exchange_ts_ms: 1727400000000,
    };

    let event = into_canonical_event(parsed, MarketType::Spot, 1727400000000000)
        .expect("Should produce canonical event");

    match event {
        MarketEvent::OrderBookSnapshot {
            venue,
            market_type,
            symbol,
            bids,
            asks,
            exchange_ts_ms,
            local_recv_ts_ns,
            sequence_id,
        } => {
            assert_eq!(venue, VenueId::Bybit);
            assert_eq!(market_type, MarketType::Spot);
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(sequence_id, 123456);
            assert_eq!(exchange_ts_ms, 1727400000000);
            assert_eq!(local_recv_ts_ns, 1727400000000000);
            assert_eq!(bids.len(), 1);
            assert_eq!(asks.len(), 1);
        }
        other => panic!("Expected OrderBookSnapshot event, got: {:?}", other),
    }
}

// ============================================================================
// TEST 14: CANONICAL DELTA EVENT GENERATION
// ============================================================================

#[test]
fn test_m3_1_canonical_delta_event_generation() {
    let parsed = BybitParsedMessage::OrderBookDelta {
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(dec!(65000.00), dec!(0.5))],
        asks: vec![],
        sequence_id: 123460,
        seq: Some(789015),
        transaction_ts_ms: 1727400000045,
        exchange_ts_ms: 1727400000050,
    };

    let event = into_canonical_event(parsed, MarketType::LinearPerpetual, 1727400000050000)
        .expect("Should produce canonical event");

    match event {
        MarketEvent::OrderBookDelta {
            venue,
            market_type,
            symbol,
            bids,
            asks,
            first_sequence_id,
            sequence_id,
            prev_sequence_id,
            transaction_ts_ms,
            exchange_ts_ms,
            local_recv_ts_ns,
        } => {
            assert_eq!(venue, VenueId::Bybit);
            assert_eq!(market_type, MarketType::LinearPerpetual);
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(sequence_id, 123460);
            assert_eq!(first_sequence_id, 123460);
            assert_eq!(prev_sequence_id, None); // CRITICAL: Bybit has no pu!
            assert_eq!(transaction_ts_ms, 1727400000045);
            assert_eq!(exchange_ts_ms, 1727400000050);
            assert_eq!(local_recv_ts_ns, 1727400000050000);
            assert_eq!(bids.len(), 1);
            assert!(asks.is_empty());
        }
        other => panic!("Expected OrderBookDelta event, got: {:?}", other),
    }
}

// ============================================================================
// TEST 15: BYBIT EVENT ENTERS MARKETSTATEMANAGER USING MONOTONICSTRICT
// ============================================================================

#[test]
fn test_m3_1_market_state_manager_integration_monotonic_strict() {
    let mut manager = MarketStateManager::new(Duration::from_millis(1000));
    manager.register_instrument(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT");

    // Verify instrument initialized with MonotonicStrict policy
    let state = manager
        .get_state(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT")
        .expect("State should exist");
    assert_eq!(state.sequence_policy, SequencePolicy::MonotonicStrict);
    assert_eq!(state.lifecycle_state, BookLifecycleState::AwaitingSnapshot);

    // 1. Ingest Snapshot at sequence u=100
    let snapshot_event = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Bybit,
        market_type: MarketType::LinearPerpetual,
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(dec!(65000.00), dec!(1.0))],
        asks: vec![PriceLevel::new(dec!(65010.00), dec!(1.0))],
        exchange_ts_ms: 1000,
        local_recv_ts_ns: 1000,
        sequence_id: 100,
    };
    let snap_res = manager.handle_event(&snapshot_event);
    assert!(snap_res.is_ok());
    assert!(manager.is_live(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT"));
    assert!(
        manager
            .get_trusted_book(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT", 1000)
            .is_some()
    );

    // 2. Ingest Delta 1 at u=105 (jumps by 5, prev_sequence_id is None)
    let delta1 = MarketEvent::OrderBookDelta {
        venue: VenueId::Bybit,
        market_type: MarketType::LinearPerpetual,
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(dec!(65002.00), dec!(0.5))],
        asks: vec![],
        first_sequence_id: 105,
        sequence_id: 105,
        prev_sequence_id: None, // No pu!
        transaction_ts_ms: 1010,
        exchange_ts_ms: 1010,
        local_recv_ts_ns: 1010,
    };
    let d1_res = manager.handle_event(&delta1);
    assert!(d1_res.is_ok());
    assert_eq!(
        manager.best_bid(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT"),
        Some(PriceLevel::new(dec!(65002.00), dec!(0.5)))
    );

    // 3. Ingest Delta 2 at u=120 (jumps by 15, prev_sequence_id is None)
    let delta2 = MarketEvent::OrderBookDelta {
        venue: VenueId::Bybit,
        market_type: MarketType::LinearPerpetual,
        symbol: "BTCUSDT".into(),
        bids: vec![],
        asks: vec![PriceLevel::new(dec!(65008.00), dec!(1.2))],
        first_sequence_id: 120,
        sequence_id: 120,
        prev_sequence_id: None,
        transaction_ts_ms: 1020,
        exchange_ts_ms: 1020,
        local_recv_ts_ns: 1020,
    };
    let d2_res = manager.handle_event(&delta2);
    assert!(d2_res.is_ok());
    assert_eq!(
        manager.best_ask(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT"),
        Some(PriceLevel::new(dec!(65008.00), dec!(1.2)))
    );

    // 4. Ingest Duplicate Delta at u=120 -> MUST BE REJECTED & FAIL CLOSED!
    let duplicate_delta = MarketEvent::OrderBookDelta {
        venue: VenueId::Bybit,
        market_type: MarketType::LinearPerpetual,
        symbol: "BTCUSDT".into(),
        bids: vec![],
        asks: vec![],
        first_sequence_id: 120,
        sequence_id: 120,
        prev_sequence_id: None,
        transaction_ts_ms: 1025,
        exchange_ts_ms: 1025,
        local_recv_ts_ns: 1025,
    };
    let dup_res = manager.handle_event(&duplicate_delta);
    assert!(dup_res.is_err());
    assert!(!manager.is_live(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT"));
    assert!(
        manager
            .get_trusted_book(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT", 1025)
            .is_none()
    );
}
