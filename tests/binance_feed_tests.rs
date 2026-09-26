use airbitrage::error::EngineError;
use airbitrage::market::OrderBook;
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use airbitrage::venues::binance::{
    BinanceMetrics, parse_futures_payload, parse_spot_depth_payload,
};
use rust_decimal_macros::dec;
use std::sync::atomic::Ordering;

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
            ..
        } => {
            assert_eq!(venue, VenueId::Binance);
            assert_eq!(market_type, MarketType::Spot);
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(sequence_id, 1027024);
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
fn test_binance_futures_valid_depth20() {
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

    let event = parse_futures_payload(raw_payload, "BTCUSDT", 1_727_400_000_100)
        .expect("Valid Futures depth should parse")
        .expect("Should produce MarketEvent");

    match event {
        MarketEvent::OrderBookSnapshot {
            venue,
            market_type,
            symbol,
            bids,
            asks,
            exchange_ts_ms,
            sequence_id,
            ..
        } => {
            assert_eq!(venue, VenueId::Binance);
            assert_eq!(market_type, MarketType::LinearPerpetual);
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(exchange_ts_ms, 1727400000500);
            assert_eq!(sequence_id, 120);
            assert_eq!(bids[0], PriceLevel::new(dec!(64510.00), dec!(2.0)));
            assert_eq!(asks[0], PriceLevel::new(dec!(64512.00), dec!(0.5)));
        }
        _ => panic!("Expected OrderBookSnapshot"),
    }
}

#[test]
fn test_binance_futures_valid_mark_price() {
    let raw_payload = br#"{
        "e": "markPriceUpdate",
        "E": 1727400001000,
        "s": "BTCUSDT",
        "p": "64511.20",
        "i": "64510.50",
        "r": "0.00010000",
        "T": 1727414400000
    }"#;

    let event = parse_futures_payload(raw_payload, "BTCUSDT", 1000)
        .expect("Valid Mark Price should parse")
        .expect("Should produce MarketEvent");

    match event {
        MarketEvent::FundingRateUpdate {
            venue,
            symbol,
            rate,
            next_funding_ts_ms,
        } => {
            assert_eq!(venue, VenueId::Binance);
            assert_eq!(symbol, "BTCUSDT");
            assert_eq!(rate, dec!(0.00010000));
            assert_eq!(next_funding_ts_ms, 1727414400000);
        }
        _ => panic!("Expected FundingRateUpdate"),
    }
}

#[test]
fn test_binance_subscription_response_ignored() {
    let sub_ack = br#"{"result": null, "id": 1}"#;
    let spot_event = parse_spot_depth_payload(sub_ack, "BTCUSDT", 1000).unwrap();
    assert!(spot_event.is_none());

    let futures_event = parse_futures_payload(sub_ack, "BTCUSDT", 1000).unwrap();
    assert!(futures_event.is_none());
}

#[test]
fn test_binance_malformed_json() {
    let malformed = br#"{"lastUpdateId": 123, "bids": [broken json"#;
    let result = parse_spot_depth_payload(malformed, "BTCUSDT", 1000);
    assert!(result.is_err());
}

#[test]
fn test_binance_wrong_symbol() {
    let wrong_symbol_payload = br#"{
        "e": "depthUpdate",
        "E": 1000,
        "s": "ETHUSDT",
        "b": [["2500.00", "1.0"]],
        "a": [["2501.00", "1.0"]]
    }"#;

    let result = parse_futures_payload(wrong_symbol_payload, "BTCUSDT", 1000);
    assert!(result.is_err());
    match result {
        Err(EngineError::Validation(msg)) => assert!(msg.contains("Symbol mismatch")),
        other => panic!("Expected Validation error, got: {:?}", other),
    }
}

#[test]
fn test_binance_invalid_price_and_quantity() {
    // Negative price
    let invalid_price = br#"{
        "lastUpdateId": 1,
        "bids": [["-100.00", "1.0"]],
        "asks": [["100.00", "1.0"]]
    }"#;
    assert!(parse_spot_depth_payload(invalid_price, "BTCUSDT", 1000).is_err());

    // Non-numeric quantity
    let invalid_qty = br#"{
        "lastUpdateId": 1,
        "bids": [["100.00", "abc"]],
        "asks": [["101.00", "1.0"]]
    }"#;
    assert!(parse_spot_depth_payload(invalid_qty, "BTCUSDT", 1000).is_err());
}

#[test]
fn test_binance_empty_book_rejected() {
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
fn test_binance_crossed_book_rejected() {
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
fn test_binance_order_book_update_from_parsed_event() {
    let raw_payload = br#"{
        "lastUpdateId": 999,
        "bids": [["64000.00", "1.0"], ["63990.00", "2.0"]],
        "asks": [["64010.00", "1.5"], ["64020.00", "3.0"]]
    }"#;

    let event = parse_spot_depth_payload(raw_payload, "BTCUSDT", 1000)
        .unwrap()
        .unwrap();

    let mut book = OrderBook::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    if let MarketEvent::OrderBookSnapshot {
        bids,
        asks,
        exchange_ts_ms,
        local_recv_ts_ns,
        sequence_id,
        ..
    } = event
    {
        book.set_snapshot(bids, asks, exchange_ts_ms, local_recv_ts_ns, sequence_id)
            .expect("Book update should succeed");
    }

    assert_eq!(
        book.best_bid(),
        Some(PriceLevel::new(dec!(64000.00), dec!(1.0)))
    );
    assert_eq!(
        book.best_ask(),
        Some(PriceLevel::new(dec!(64010.00), dec!(1.5)))
    );
    assert_eq!(book.spread(), Some(dec!(10.00)));
    assert!(!book.is_crossed());
}

#[test]
fn test_binance_metrics_tracking() {
    let metrics = BinanceMetrics::default();
    metrics.connection_attempts.fetch_add(1, Ordering::Relaxed);
    metrics.messages_received.fetch_add(10, Ordering::Relaxed);
    metrics.messages_parsed.fetch_add(9, Ordering::Relaxed);
    metrics.parse_errors.fetch_add(1, Ordering::Relaxed);

    let snap = metrics.snapshot();
    assert_eq!(snap.connection_attempts, 1);
    assert_eq!(snap.messages_received, 10);
    assert_eq!(snap.messages_parsed, 9);
    assert_eq!(snap.parse_errors, 1);
}
