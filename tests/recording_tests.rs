use airbitrage::error::EngineError;
use airbitrage::observatory::types::{
    DislocationObservation, MarketRelationship, ObservationRejectionReason, OpportunityEndReason,
    OpportunityKey, OpportunityRecord, PersistenceTransition,
};
use airbitrage::recording::{
    CURRENT_SCHEMA_VERSION, ResearchEvent, ResearchPayload, ResearchReader, ResearchRecorder,
};
use airbitrage::types::{MarketEvent, MarketType, PriceLevel, VenueId};
use rust_decimal::Decimal;
use std::io::Cursor;

fn make_sample_dislocation(
    now_ns: i64,
    net_edge_bps: Decimal,
    fully_executable: bool,
    is_valid: bool,
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
        buy_book_exchange_ts_ms: Some(1_700_000_000_000),
        sell_book_exchange_ts_ms: Some(1_700_000_000_050),
        buy_book_recv_ts_ns: Some(now_ns),
        sell_book_recv_ts_ns: Some(now_ns),
        timestamp_skew_ms: Some(50),
        buy_book_age_ms: Some(10),
        sell_book_age_ms: Some(12),
        reference_quantity: Decimal::new(15, 1), // 1.5 BTC
        buy_available_quantity: if fully_executable {
            Decimal::new(20, 1)
        } else {
            Decimal::new(10, 1)
        },
        sell_available_quantity: Decimal::new(20, 1),
        common_executable_quantity: if fully_executable {
            Decimal::new(15, 1)
        } else {
            Decimal::new(10, 1)
        },
        fully_executable,
        buy_vwap: Some(Decimal::new(1000025, 4)),
        sell_vwap: Some(Decimal::new(1000250, 4)),
        buy_worst_fill_price: Some(Decimal::new(1000050, 4)),
        sell_worst_fill_price: Some(Decimal::new(1000200, 4)),
        buy_best_ask: Some(Decimal::from(100)),
        sell_best_bid: Some(Decimal::new(10003, 2)),
        gross_spread: Some(Decimal::new(225, 4)),
        gross_spread_bps: Some(Decimal::new(225, 2)),
        gross_edge: Some(Decimal::new(3375, 4)),
        buy_fee: Decimal::new(15, 2),
        sell_fee: Decimal::new(15, 2),
        total_fees: Decimal::new(30, 2),
        net_edge: Some(Decimal::new(375, 4)),
        net_edge_bps: Some(net_edge_bps),
        both_books_trusted: is_valid,
        both_books_fresh: is_valid,
        no_crossed_books: true,
        is_valid,
        rejection_reason: rejection,
    }
}

fn make_sample_opportunity(start_ns: i64, end_ns: i64) -> OpportunityRecord {
    OpportunityRecord {
        key: OpportunityKey::new(
            VenueId::Binance,
            MarketType::Spot,
            VenueId::Bybit,
            MarketType::Spot,
            "BTCUSDT",
            Decimal::ONE,
            MarketRelationship::SpotSpot,
        ),
        start_ts_ns: start_ns,
        end_ts_ns: end_ns,
        duration_ms: ((end_ns - start_ns) / 1_000_000) as u64,
        sample_count: 42,
        first_observed_edge_bps: Decimal::new(125, 1), // 12.5 bps
        last_observed_edge_bps: Decimal::new(21, 1),   // 2.1 bps
        peak_net_edge_bps: Decimal::new(250, 1),       // 25.0 bps
        min_net_edge_bps: Decimal::new(15, 1),         // 1.5 bps
        average_net_edge_bps: Decimal::new(142, 1),    // 14.2 bps
        max_common_executable_quantity: Decimal::new(55, 1),
        termination_reason: OpportunityEndReason::NetEdgeBelowThreshold,
    }
}

#[test]
fn test_01_serialization_round_trip() {
    let now_ns = 1_700_000_000_000_000_000_i64;
    let obs = make_sample_dislocation(now_ns, Decimal::from(10), true, true, None);
    let event = ResearchEvent::new_dislocation(obs);

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).expect("Append must succeed");
    let bytes = recorder.into_inner().expect("Flush must succeed");

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader
        .next_event()
        .expect("Read must succeed")
        .expect("Event must be present");

    assert_eq!(read_event, event);
    assert_eq!(reader.next_event().unwrap(), None); // Clean EOF
}

#[test]
fn test_02_decimal_preservation() {
    // 18-decimal high precision value
    let precise_qty = Decimal::from_str_exact("0.000000000000000001").unwrap();
    let precise_rate = Decimal::from_str_exact("0.000550000000000000").unwrap();

    let mut obs = make_sample_dislocation(1_000_000_000, Decimal::ONE, true, true, None);
    obs.reference_quantity = precise_qty;
    obs.buy_fee = precise_rate;

    let event = ResearchEvent::new_dislocation(obs);

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::DislocationObservation(read_obs) = read_event.payload {
        assert_eq!(read_obs.reference_quantity, precise_qty);
        assert_eq!(read_obs.buy_fee, precise_rate);
    } else {
        panic!("Unexpected payload variant");
    }
}

#[test]
fn test_03_timestamp_preservation() {
    let now_ns = 1_725_000_123_456_789_012_i64;
    let ex_ms = 1_725_000_123_456_i64;

    let mut obs = make_sample_dislocation(now_ns, Decimal::from(10), true, true, None);
    obs.buy_book_exchange_ts_ms = Some(ex_ms);
    obs.buy_book_recv_ts_ns = Some(now_ns);

    let event = ResearchEvent::new_dislocation(obs);

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    assert_eq!(read_event.timestamp_ns, now_ns);
    if let ResearchPayload::DislocationObservation(read_obs) = read_event.payload {
        assert_eq!(read_obs.observation_ts_ns, now_ns);
        assert_eq!(read_obs.buy_book_exchange_ts_ms, Some(ex_ms));
        assert_eq!(read_obs.buy_book_recv_ts_ns, Some(now_ns));
    } else {
        panic!("Unexpected payload variant");
    }
}

#[test]
fn test_04_enum_preservation() {
    let venues = [VenueId::Binance, VenueId::Bybit];
    let markets = [MarketType::Spot, MarketType::LinearPerpetual];
    let relationships = [
        MarketRelationship::SpotSpot,
        MarketRelationship::PerpPerp,
        MarketRelationship::CrossInstrumentBasis,
    ];
    let reasons = [
        OpportunityEndReason::NetEdgeBelowThreshold,
        OpportunityEndReason::UntrustedBook,
        OpportunityEndReason::StaleBook,
        OpportunityEndReason::InvalidBook,
        OpportunityEndReason::TimestampSkewExceeded,
        OpportunityEndReason::InsufficientLiquidity,
        OpportunityEndReason::FeedDisconnected,
        OpportunityEndReason::MissingBook,
    ];

    for venue in venues {
        for market in markets {
            for rel in relationships {
                for reason in reasons {
                    let mut record = make_sample_opportunity(100, 200);
                    record.key.buy_venue = venue;
                    record.key.buy_market = market;
                    record.key.market_relationship = rel;
                    record.termination_reason = reason;

                    let event = ResearchEvent::new_opportunity(record.clone());
                    let mut recorder = ResearchRecorder::new(Vec::new());
                    recorder.append(&event).unwrap();
                    let bytes = recorder.into_inner().unwrap();

                    let mut reader = ResearchReader::new(Cursor::new(bytes));
                    let read_event = reader.next_event().unwrap().unwrap();
                    if let ResearchPayload::OpportunityRecord(read_rec) = read_event.payload {
                        assert_eq!(read_rec.key.buy_venue, venue);
                        assert_eq!(read_rec.key.buy_market, market);
                        assert_eq!(read_rec.key.market_relationship, rel);
                        assert_eq!(read_rec.termination_reason, reason);
                    } else {
                        panic!("Unexpected payload");
                    }
                }
            }
        }
    }
}

#[test]
fn test_05_rejected_observation_serialization() {
    let now_ns = 1_000_000_000_i64;
    let rej = ObservationRejectionReason::StaleBook {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        age_ms: 2500,
        max_age_ms: 1000,
    };
    let obs = make_sample_dislocation(now_ns, Decimal::ZERO, false, false, Some(rej));
    let event = ResearchEvent::new_dislocation(obs);

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::DislocationObservation(read_obs) = read_event.payload {
        assert!(!read_obs.is_valid);
        match read_obs.rejection_reason {
            Some(ObservationRejectionReason::StaleBook {
                age_ms, max_age_ms, ..
            }) => {
                assert_eq!(age_ms, 2500);
                assert_eq!(max_age_ms, 1000);
            }
            other => panic!("Expected StaleBook, got {other:?}"),
        }
    } else {
        panic!("Unexpected payload variant");
    }
}

#[test]
fn test_06_valid_observation_serialization() {
    let now_ns = 1_000_000_000_i64;
    let obs = make_sample_dislocation(now_ns, Decimal::from(15), true, true, None);
    let event = ResearchEvent::new_dislocation(obs);

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::DislocationObservation(read_obs) = read_event.payload {
        assert!(read_obs.is_valid);
        assert!(read_obs.fully_executable);
        assert_eq!(read_obs.net_edge_bps, Some(Decimal::from(15)));
        assert_eq!(read_obs.rejection_reason, None);
    } else {
        panic!("Unexpected payload variant");
    }
}

#[test]
fn test_07_persistence_start_serialization() {
    let now_ns = 1_000_000_000_i64;
    let key = OpportunityKey::new(
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        MarketRelationship::SpotSpot,
    );
    let event = ResearchEvent::new_transition(key.clone(), now_ns, PersistenceTransition::Started);

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::PersistenceTransition(trans_event) = read_event.payload {
        assert_eq!(trans_event.key, key);
        assert_eq!(trans_event.transition, PersistenceTransition::Started);
    } else {
        panic!("Unexpected payload");
    }
}

#[test]
fn test_08_persistence_continue_serialization() {
    let now_ns = 1_500_000_000_i64;
    let key = OpportunityKey::new(
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        MarketRelationship::SpotSpot,
    );
    let event =
        ResearchEvent::new_transition(key.clone(), now_ns, PersistenceTransition::Continued);

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::PersistenceTransition(trans_event) = read_event.payload {
        assert_eq!(trans_event.key, key);
        assert_eq!(trans_event.transition, PersistenceTransition::Continued);
    } else {
        panic!("Unexpected payload");
    }
}

#[test]
fn test_09_persistence_end_serialization() {
    let record = make_sample_opportunity(1_000_000_000, 2_000_000_000);
    let key = record.key.clone();
    let event = ResearchEvent::new_transition(
        key.clone(),
        2_000_000_000,
        PersistenceTransition::Ended(Box::new(record.clone())),
    );

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::PersistenceTransition(trans_event) = read_event.payload {
        assert_eq!(trans_event.key, key);
        match trans_event.transition {
            PersistenceTransition::Ended(rec) => assert_eq!(*rec, record),
            other => panic!("Expected Ended, got {other:?}"),
        }
    } else {
        panic!("Unexpected payload");
    }
}

#[test]
fn test_10_opportunity_record_serialization() {
    let record = make_sample_opportunity(100, 500);
    let event = ResearchEvent::new_opportunity(record.clone());

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::OpportunityRecord(read_rec) = read_event.payload {
        assert_eq!(read_rec, record);
    } else {
        panic!("Unexpected payload");
    }
}

#[test]
fn test_11_malformed_record_rejection() {
    let garbage = b"{ invalid json line }\n";
    let mut reader = ResearchReader::new(Cursor::new(garbage));

    let err = reader.next_event().unwrap_err();
    match err {
        EngineError::CorruptedRecord { line, .. } => {
            assert_eq!(line, 1);
        }
        other => panic!("Expected CorruptedRecord, got {other:?}"),
    }
}

#[test]
fn test_12_unsupported_schema_version_rejection() {
    let raw = r#"{"schema_version":999,"event_type":"opportunity_record","timestamp_ns":100,"payload":{"type":"opportunity_record","data":{"key":{"buy_venue":"binance","buy_market":"spot","sell_venue":"bybit","sell_market":"spot","symbol":"BTCUSDT","reference_quantity":"1","market_relationship":"spot_spot"},"start_ts_ns":10,"end_ts_ns":100,"duration_ms":90,"sample_count":1,"first_observed_edge_bps":"1","last_observed_edge_bps":"1","peak_net_edge_bps":"1","min_net_edge_bps":"1","average_net_edge_bps":"1","max_common_executable_quantity":"1","termination_reason":"net_edge_below_threshold"}}}"#;
    let mut reader = ResearchReader::new(Cursor::new(format!("{raw}\n")));

    let err = reader.next_event().unwrap_err();
    match err {
        EngineError::UnsupportedSchemaVersion { found, supported } => {
            assert_eq!(found, 999);
            assert_eq!(supported, CURRENT_SCHEMA_VERSION);
        }
        other => panic!("Expected UnsupportedSchemaVersion, got {other:?}"),
    }
}

#[test]
fn test_13_truncated_final_record_handling() {
    // Line 1: valid record
    let record = make_sample_opportunity(100, 200);
    let event = ResearchEvent::new_opportunity(record);
    let valid_json = serde_json::to_string(&event).unwrap();

    // Line 2: truncated record
    let truncated = r#"{"schema_version":1,"event_type":"opportunity_record","timestamp_ns":"#;

    let payload = format!("{valid_json}\n{truncated}\n");
    let mut reader = ResearchReader::new(Cursor::new(payload));

    let event1 = reader.next_event().unwrap().unwrap();
    assert_eq!(event1, event);

    let err = reader.next_event().unwrap_err();
    match err {
        EngineError::CorruptedRecord { line, .. } => {
            assert_eq!(line, 2);
        }
        other => panic!("Expected CorruptedRecord for truncated line, got {other:?}"),
    }
}

#[test]
fn test_14_deterministic_serialization_output() {
    let now_ns = 1_700_000_000_000_000_000_i64;
    let obs = make_sample_dislocation(now_ns, Decimal::from(10), true, true, None);
    let event = ResearchEvent::new_dislocation(obs);

    let json1 = serde_json::to_string(&event).unwrap();
    let json2 = serde_json::to_string(&event).unwrap();

    assert_eq!(json1, json2);
}

#[test]
fn test_15_append_multiple_events_and_read_in_order() {
    let now_ns = 1_000_000_000_i64;

    // 1. Raw MarketEvent
    let market_event = MarketEvent::OrderBookSnapshot {
        venue: VenueId::Binance,
        market_type: MarketType::Spot,
        symbol: "BTCUSDT".into(),
        bids: vec![PriceLevel::new(Decimal::from(100), Decimal::ONE)],
        asks: vec![PriceLevel::new(Decimal::from(101), Decimal::ONE)],
        exchange_ts_ms: 1_000,
        local_recv_ts_ns: now_ns,
        sequence_id: 1,
    };
    let event1 = ResearchEvent::new_market_event(now_ns, market_event);

    // 2. DislocationObservation
    let obs = make_sample_dislocation(now_ns + 100, Decimal::from(10), true, true, None);
    let event2 = ResearchEvent::new_dislocation(obs);

    // 3. PersistenceTransition (Started)
    let key = OpportunityKey::new(
        VenueId::Binance,
        MarketType::Spot,
        VenueId::Bybit,
        MarketType::Spot,
        "BTCUSDT",
        Decimal::ONE,
        MarketRelationship::SpotSpot,
    );
    let event3 = ResearchEvent::new_transition(key, now_ns + 200, PersistenceTransition::Started);

    // 4. OpportunityRecord
    let opp = make_sample_opportunity(now_ns + 200, now_ns + 1000);
    let event4 = ResearchEvent::new_opportunity(opp);

    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event1).unwrap();
    recorder.append(&event2).unwrap();
    recorder.append(&event3).unwrap();
    recorder.append(&event4).unwrap();
    assert_eq!(recorder.record_count(), 4);

    let bytes = recorder.into_inner().unwrap();
    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_events = reader.read_all().unwrap();

    assert_eq!(read_events.len(), 4);
    assert_eq!(read_events[0], event1);
    assert_eq!(read_events[1], event2);
    assert_eq!(read_events[2], event3);
    assert_eq!(read_events[3], event4);
}

#[test]
fn test_16_recorder_flush_behavior() {
    let mut recorder = ResearchRecorder::new(Vec::new());
    let obs = make_sample_dislocation(1_000_000_000, Decimal::ONE, true, true, None);
    let event = ResearchEvent::new_dislocation(obs);

    recorder.append(&event).unwrap();
    assert_eq!(recorder.record_count(), 1);

    recorder.flush().expect("Flush must succeed");
    let bytes = recorder.into_inner().unwrap();
    assert!(!bytes.is_empty());
}

#[test]
fn test_17_empty_recorder_behavior() {
    let empty_bytes = Vec::<u8>::new();
    let mut reader = ResearchReader::new(Cursor::new(empty_bytes));
    let events = reader.read_all().unwrap();
    assert!(events.is_empty());
}

#[test]
fn test_18_large_decimal_values() {
    // Very large notional: 100,000,000 BTC @ 100,000 USDT = 10,000,000,000,000
    let big_notional = Decimal::from_str_exact("10000000000000.00000000").unwrap();
    let mut obs = make_sample_dislocation(1_000_000_000, Decimal::ONE, true, true, None);
    obs.gross_edge = Some(big_notional);

    let event = ResearchEvent::new_dislocation(obs);
    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::DislocationObservation(read_obs) = read_event.payload {
        assert_eq!(read_obs.gross_edge, Some(big_notional));
    } else {
        panic!("Unexpected payload");
    }
}

#[test]
fn test_19_negative_net_edges() {
    let neg_edge_bps = Decimal::from_str_exact("-45.6789").unwrap();
    let obs = make_sample_dislocation(1_000_000_000, neg_edge_bps, true, true, None);

    let event = ResearchEvent::new_dislocation(obs);
    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::DislocationObservation(read_obs) = read_event.payload {
        assert_eq!(read_obs.net_edge_bps, Some(neg_edge_bps));
    } else {
        panic!("Unexpected payload");
    }
}

#[test]
fn test_20_partial_fill_observations() {
    let obs = make_sample_dislocation(1_000_000_000, Decimal::ONE, false, true, None);
    assert!(!obs.fully_executable);
    assert!(obs.common_executable_quantity < obs.reference_quantity);

    let event = ResearchEvent::new_dislocation(obs.clone());
    let mut recorder = ResearchRecorder::new(Vec::new());
    recorder.append(&event).unwrap();
    let bytes = recorder.into_inner().unwrap();

    let mut reader = ResearchReader::new(Cursor::new(bytes));
    let read_event = reader.next_event().unwrap().unwrap();

    if let ResearchPayload::DislocationObservation(read_obs) = read_event.payload {
        assert!(!read_obs.fully_executable);
        assert_eq!(read_obs.reference_quantity, obs.reference_quantity);
        assert_eq!(
            read_obs.common_executable_quantity,
            obs.common_executable_quantity
        );
    } else {
        panic!("Unexpected payload");
    }
}
