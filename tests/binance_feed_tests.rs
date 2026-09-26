use airbitrage::error::EngineError;
use airbitrage::market::OrderBook;
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use airbitrage::venues::binance::{
    BinanceMetrics, BinanceStreamType, StreamFreshnessTracker, parse_futures_depth_payload,
    parse_futures_mark_price_payload, parse_futures_payload, parse_spot_depth_payload,
};
use rust_decimal_macros::dec;
use std::sync::atomic::Ordering;
use std::time::Duration;

// ============================================================================
// 1. BINANCE SPOT TESTS
// ============================================================================

#[test]
fn test_binance_spot_valid_depth20() {
    let raw_payload = br#"{
        "lastUpdateId": 1027024,
        "bids": [
            ["64500.10", "1.25000000"],
            ["64500.00", "0.50000000"]
        ],
        "asks": [
            ["64501.00", "0.80000000"],
            ["64502.50", "2.10000000"]
        ]
    }"#;

    let event = parse_spot_depth_payload(raw_payload, "BTCUSDT", 1_727_400_000_000)
        .expect("Valid Spot payload should parse")
        .expect("Should produce a MarketEvent");

    match event {
        MarketEvent::OrderBookSnapshot {
            venue,
            market_type,
            symbol,
            bids,
            asks,
            sequence_id,
            exchange_ts_ms,
            local_recv_ts_ns,
        } => {
            assert_eq!(venue, VenueId::Binance);
            assert_eq!(market_type, MarketType::Spot);
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(sequence_id, 1027024);
            assert_eq!(exchange_ts_ms, 0);
            assert_eq!(local_recv_ts_ns, 1_727_400_000_000);
            assert_eq!(bids.len(), 2);
            assert_eq!(asks.len(), 2);
            assert_eq!(bids[0], PriceLevel::new(dec!(64500.10), dec!(1.25)));
            assert_eq!(asks[0], PriceLevel::new(dec!(64501.00), dec!(0.8)));
        }
        _ => panic!("Expected OrderBookSnapshot"),
    }
}

#[test]
fn test_binance_spot_wrapped_stream_payload() {
    let raw_payload = br#"{
        "stream": "btcusdt@depth20@100ms",
        "data": {
            "lastUpdateId": 55555,
            "bids": [["65000.00", "0.1"]],
            "asks": [["65001.00", "0.2"]]
        }
    }"#;

    let event = parse_spot_depth_payload(raw_payload, "BTCUSDT", 1000)
        .expect("Wrapped payload should parse")
        .expect("Should produce MarketEvent");

    if let MarketEvent::OrderBookSnapshot { sequence_id, .. } = event {
        assert_eq!(sequence_id, 55555);
    } else {
        panic!("Expected OrderBookSnapshot");
    }
}

#[test]
fn test_binance_spot_malformed_json() {
    let malformed = br#"{"lastUpdateId": 123, "bids": [broken json"#;
    let result = parse_spot_depth_payload(malformed, "BTCUSDT", 1000);
    assert!(result.is_err());
}

#[test]
fn test_binance_spot_wrong_symbol() {
    let wrong_stream = br#"{
        "stream": "ethusdt@depth20@100ms",
        "data": {
            "lastUpdateId": 1,
            "bids": [["2500.00", "1.0"]],
            "asks": [["2501.00", "1.0"]]
        }
    }"#;

    let result = parse_spot_depth_payload(wrong_stream, "BTCUSDT", 1000);
    assert!(result.is_err());
    match result {
        Err(EngineError::Validation(msg)) => assert!(msg.contains("Mismatched stream symbol")),
        other => panic!("Expected Validation error, got: {:?}", other),
    }
}

#[test]
fn test_binance_spot_empty_book_rejected() {
    let empty_book = br#"{
        "lastUpdateId": 1,
        "bids": [],
        "asks": []
    }"#;
    let result = parse_spot_depth_payload(empty_book, "BTCUSDT", 1000);
    assert!(result.is_err());
    match result {
        Err(EngineError::DataQuality(msg)) => assert!(msg.contains("Empty order book")),
        other => panic!("Expected DataQuality error, got: {:?}", other),
    }
}

#[test]
fn test_binance_spot_crossed_book_rejected() {
    // Bid 65002 >= Ask 65000
    let crossed_payload = br#"{
        "lastUpdateId": 1,
        "bids": [["65002.00", "1.0"]],
        "asks": [["65000.00", "1.0"]]
    }"#;

    let result = parse_spot_depth_payload(crossed_payload, "BTCUSDT", 1000);
    assert!(result.is_err());
    match result {
        Err(EngineError::CrossedBook { bid, ask }) => {
            assert_eq!(bid, dec!(65002.00));
            assert_eq!(ask, dec!(65000.00));
        }
        other => panic!("Expected CrossedBook error, got: {:?}", other),
    }
}

#[test]
fn test_binance_spot_subscription_ack_ignored() {
    let sub_ack = br#"{"result": null, "id": 1}"#;
    let spot_event = parse_spot_depth_payload(sub_ack, "BTCUSDT", 1000).unwrap();
    assert!(spot_event.is_none());
}

// ============================================================================
// 2. BINANCE FUTURES DEPTH TESTS (DELTA SEMANTICS)
// ============================================================================

#[test]
fn test_binance_futures_valid_depth_delta() {
    let raw_payload = br#"{
        "e": "depthUpdate",
        "E": 1727400000500,
        "T": 1727400000490,
        "s": "BTCUSDT",
        "U": 100,
        "u": 120,
        "pu": 99,
        "b": [
            ["64510.00", "2.000"],
            ["64509.00", "1.500"]
        ],
        "a": [
            ["64512.00", "0.500"],
            ["64515.00", "3.000"]
        ]
    }"#;

    let event = parse_futures_depth_payload(raw_payload, "BTCUSDT", 1_727_400_000_100)
        .expect("Valid Futures depth should parse")
        .expect("Should produce MarketEvent");

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
            assert_eq!(venue, VenueId::Binance);
            assert_eq!(market_type, MarketType::LinearPerpetual);
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(first_sequence_id, 100);
            assert_eq!(sequence_id, 120);
            assert_eq!(prev_sequence_id, Some(99));
            assert_eq!(transaction_ts_ms, 1727400000490);
            assert_eq!(exchange_ts_ms, 1727400000500);
            assert_eq!(local_recv_ts_ns, 1_727_400_000_100);
            assert_eq!(bids.len(), 2);
            assert_eq!(asks.len(), 2);
            assert_eq!(bids[0], PriceLevel::new(dec!(64510.00), dec!(2.0)));
            assert_eq!(asks[0], PriceLevel::new(dec!(64512.00), dec!(0.5)));
        }
        _ => panic!("Expected OrderBookDelta"),
    }
}

#[test]
fn test_binance_futures_depth_sequence_inversion_rejected() {
    // U (150) > u (120) is an invalid exchange sequence payload
    let raw_payload = br#"{
        "e": "depthUpdate",
        "E": 1727400000500,
        "s": "BTCUSDT",
        "U": 150,
        "u": 120,
        "pu": 99,
        "b": [["64510.00", "2.000"]],
        "a": [["64512.00", "0.500"]]
    }"#;

    let result = parse_futures_depth_payload(raw_payload, "BTCUSDT", 1000);
    assert!(result.is_err());
    match result {
        Err(EngineError::Validation(msg)) => {
            assert!(msg.contains("Invalid sequence ID ordering"));
        }
        other => panic!("Expected Validation error, got: {:?}", other),
    }
}

#[test]
fn test_binance_futures_depth_symbol_validation() {
    let wrong_symbol_payload = br#"{
        "e": "depthUpdate",
        "E": 1000,
        "s": "ETHUSDT",
        "U": 10,
        "u": 20,
        "b": [["2500.00", "1.0"]],
        "a": [["2501.00", "1.0"]]
    }"#;

    let result = parse_futures_depth_payload(wrong_symbol_payload, "BTCUSDT", 1000);
    assert!(result.is_err());
    match result {
        Err(EngineError::Validation(msg)) => assert!(msg.contains("Symbol mismatch")),
        other => panic!("Expected Validation error, got: {:?}", other),
    }
}

#[test]
fn test_binance_futures_depth_delta_semantics_orderbook_mutation() {
    let mut book = OrderBook::new(VenueId::Binance, MarketType::LinearPerpetual, "BTCUSDT");

    // Initialize with a base snapshot
    let initial_bids = vec![
        PriceLevel::new(dec!(64500.00), dec!(1.0)),
        PriceLevel::new(dec!(64490.00), dec!(2.0)),
    ];
    let initial_asks = vec![
        PriceLevel::new(dec!(64510.00), dec!(1.5)),
        PriceLevel::new(dec!(64520.00), dec!(2.5)),
    ];
    book.set_snapshot(initial_bids, initial_asks, 1000, 1000, 99)
        .expect("Snapshot initialization should succeed");

    // Incoming delta update:
    // Update 64500.00 qty to 3.0
    // Delete 64490.00 (qty 0.0)
    // Insert new top bid 64505.00
    // Delete 64510.00 (qty 0.0), making best ask 64520.00
    let raw_delta = br#"{
        "e": "depthUpdate",
        "E": 1727400000500,
        "T": 1727400000490,
        "s": "BTCUSDT",
        "U": 100,
        "u": 105,
        "pu": 99,
        "b": [
            ["64505.00", "0.500"],
            ["64500.00", "3.000"],
            ["64490.00", "0.000"]
        ],
        "a": [
            ["64510.00", "0.000"]
        ]
    }"#;

    let event = parse_futures_depth_payload(raw_delta, "BTCUSDT", 2000)
        .unwrap()
        .unwrap();

    if let MarketEvent::OrderBookDelta {
        bids,
        asks,
        sequence_id,
        exchange_ts_ms,
        local_recv_ts_ns,
        ..
    } = event
    {
        book.apply_delta(&bids, &asks, exchange_ts_ms, local_recv_ts_ns, sequence_id)
            .expect("Delta apply should succeed");
    } else {
        panic!("Expected OrderBookDelta");
    }

    assert_eq!(
        book.best_bid(),
        Some(PriceLevel::new(dec!(64505.00), dec!(0.5)))
    );
    assert_eq!(
        book.best_ask(),
        Some(PriceLevel::new(dec!(64520.00), dec!(2.5)))
    );
    assert_eq!(book.bids.len(), 2); // 64505 and 64500 (64490 deleted)
    assert_eq!(book.asks.len(), 1); // 64520 (64510 deleted)
    assert_eq!(book.spread(), Some(dec!(15.00)));
}

// ============================================================================
// 3. BINANCE FUTURES MARK PRICE & FUNDING TESTS
// ============================================================================

#[test]
fn test_binance_futures_valid_mark_price_and_funding() {
    let raw_payload = br#"{
        "e": "markPriceUpdate",
        "E": 1727400001000,
        "s": "BTCUSDT",
        "p": "64511.20",
        "P": "64510.50",
        "r": "0.00010000",
        "T": 1727414400000
    }"#;

    let event = parse_futures_mark_price_payload(raw_payload, "BTCUSDT", 1000)
        .expect("Valid Mark Price should parse")
        .expect("Should produce MarketEvent");

    match event {
        MarketEvent::FundingRateUpdate {
            venue,
            symbol,
            mark_price,
            index_price,
            rate,
            next_funding_ts_ms,
            exchange_ts_ms,
            local_recv_ts_ns,
        } => {
            assert_eq!(venue, VenueId::Binance);
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(mark_price, dec!(64511.20));
            assert_eq!(index_price, Some(dec!(64510.50)));
            assert_eq!(rate, dec!(0.00010000));
            assert_eq!(next_funding_ts_ms, 1727414400000);
            assert_eq!(exchange_ts_ms, 1727400001000);
            assert_eq!(local_recv_ts_ns, 1000);
        }
        _ => panic!("Expected FundingRateUpdate"),
    }
}

#[test]
fn test_binance_futures_mark_price_symbol_mismatch() {
    let raw_payload = br#"{
        "e": "markPriceUpdate",
        "E": 1727400001000,
        "s": "SOLUSDT",
        "p": "150.20",
        "r": "0.00010000",
        "T": 1727414400000
    }"#;

    let result = parse_futures_mark_price_payload(raw_payload, "BTCUSDT", 1000);
    assert!(result.is_err());
    match result {
        Err(EngineError::Validation(msg)) => {
            assert!(msg.contains("Mark price symbol mismatch"));
        }
        other => panic!("Expected Validation error, got: {:?}", other),
    }
}

#[test]
fn test_binance_futures_mark_price_invalid_rate() {
    let raw_payload = br#"{
        "e": "markPriceUpdate",
        "E": 1727400001000,
        "s": "BTCUSDT",
        "p": "64511.20",
        "r": "not_a_number",
        "T": 1727414400000
    }"#;

    let result = parse_futures_mark_price_payload(raw_payload, "BTCUSDT", 1000);
    assert!(result.is_err());
    match result {
        Err(EngineError::Validation(msg)) => {
            assert!(msg.contains("Invalid funding rate"));
        }
        other => panic!("Expected Validation error, got: {:?}", other),
    }
}

// ============================================================================
// 4. LIFECYCLE & FRESHNESS TESTS
// ============================================================================

#[test]
fn test_binance_freshness_tracker_and_stale_detection() {
    let tracker = StreamFreshnessTracker::default();
    let threshold = Duration::from_millis(500);

    // Initial state: not yet received -> stale
    assert!(tracker.is_stale(BinanceStreamType::SpotDepth, threshold, 1000));
    assert!(tracker.is_stale(BinanceStreamType::FuturesDepth, threshold, 1000));
    assert!(tracker.is_stale(BinanceStreamType::FuturesMarkPrice, threshold, 1000));

    // Record an event at t = 1000
    tracker.record_event(BinanceStreamType::SpotDepth, 1000);
    assert_eq!(tracker.last_recv_ms(BinanceStreamType::SpotDepth), 1000);

    // At t = 1300 (< 500ms threshold): not stale
    assert!(!tracker.is_stale(BinanceStreamType::SpotDepth, threshold, 1300));

    // At t = 1600 (> 500ms threshold): stale!
    assert!(tracker.is_stale(BinanceStreamType::SpotDepth, threshold, 1600));

    // Reset stream -> back to stale / uninitialized
    tracker.reset(BinanceStreamType::SpotDepth);
    assert_eq!(tracker.last_recv_ms(BinanceStreamType::SpotDepth), 0);
    assert!(tracker.is_stale(BinanceStreamType::SpotDepth, threshold, 1600));
}

#[test]
fn test_binance_unexpected_message_ignored() {
    let kline_payload = br#"{
        "e": "kline",
        "E": 1727400001000,
        "s": "BTCUSDT",
        "k": {}
    }"#;

    let spot_event = parse_spot_depth_payload(kline_payload, "BTCUSDT", 1000);
    assert!(spot_event.is_err()); // Not a spot snapshot

    let futures_event = parse_futures_payload(kline_payload, "BTCUSDT", 1000)
        .expect("Unexpected futures payload should be safely ignored");
    assert!(futures_event.is_none());
}

#[test]
fn test_binance_metrics_granular_tracking() {
    let metrics = BinanceMetrics::default();
    metrics.connection_attempts.fetch_add(2, Ordering::Relaxed);
    metrics
        .successful_connections
        .fetch_add(2, Ordering::Relaxed);
    metrics.spot_depth_messages.fetch_add(50, Ordering::Relaxed);
    metrics
        .futures_depth_messages
        .fetch_add(50, Ordering::Relaxed);
    metrics
        .futures_markprice_messages
        .fetch_add(10, Ordering::Relaxed);
    metrics.crossed_books.fetch_add(1, Ordering::Relaxed);
    metrics.stale_events.fetch_add(2, Ordering::Relaxed);
    metrics.backpressure_drops.fetch_add(3, Ordering::Relaxed);
    metrics.last_spot_sequence.store(1024, Ordering::Relaxed);
    metrics.last_futures_sequence.store(2048, Ordering::Relaxed);
    metrics
        .last_futures_exchange_ts
        .store(1727400000000, Ordering::Relaxed);
    metrics
        .last_markprice_exchange_ts
        .store(1727400001000, Ordering::Relaxed);

    let snap = metrics.snapshot();
    assert_eq!(snap.connection_attempts, 2);
    assert_eq!(snap.successful_connections, 2);
    assert_eq!(snap.spot_depth_messages, 50);
    assert_eq!(snap.futures_depth_messages, 50);
    assert_eq!(snap.futures_markprice_messages, 10);
    assert_eq!(snap.crossed_books, 1);
    assert_eq!(snap.stale_events, 2);
    assert_eq!(snap.backpressure_drops, 3);
    assert_eq!(snap.last_spot_sequence, 1024);
    assert_eq!(snap.last_futures_sequence, 2048);
    assert_eq!(snap.last_futures_exchange_ts, 1727400000000);
    assert_eq!(snap.last_markprice_exchange_ts, 1727400001000);
}
