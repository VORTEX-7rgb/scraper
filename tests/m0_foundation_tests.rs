use airbitrage::config::AppConfig;
use airbitrage::error::EngineError;
use airbitrage::market::OrderBook;
use airbitrage::types::{MarketType, PriceLevel, VenueId};
use rust_decimal_macros::dec;

#[test]
fn test_price_level_construction_and_equality() {
    let p1 = PriceLevel::new(dec!(65000.50), dec!(1.25));
    let p2 = PriceLevel::new(dec!(65000.50), dec!(1.25));
    let p3 = PriceLevel::new(dec!(65001.00), dec!(1.25));

    assert_eq!(p1, p2);
    assert_ne!(p1, p3);
    assert_eq!(p1.price, dec!(65000.50));
    assert_eq!(p1.quantity, dec!(1.25));
}

#[test]
fn test_order_book_sorting_and_best_quotes() {
    let mut book = OrderBook::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    let bids = vec![
        PriceLevel::new(dec!(64998.00), dec!(0.5)),
        PriceLevel::new(dec!(65000.00), dec!(1.0)), // Highest bid
        PriceLevel::new(dec!(64999.00), dec!(2.0)),
    ];

    let asks = vec![
        PriceLevel::new(dec!(65005.00), dec!(1.5)),
        PriceLevel::new(dec!(65001.00), dec!(0.8)), // Lowest ask
        PriceLevel::new(dec!(65003.00), dec!(3.0)),
    ];

    book.set_snapshot(bids, asks, 1727400000000, 1000, 1)
        .expect("Snapshot should succeed");

    assert_eq!(
        book.best_bid(),
        Some(PriceLevel::new(dec!(65000.00), dec!(1.0)))
    );
    assert_eq!(
        book.best_ask(),
        Some(PriceLevel::new(dec!(65001.00), dec!(0.8)))
    );
    assert_eq!(book.spread(), Some(dec!(1.00)));
    assert!(!book.is_crossed());

    // Verify bids are sorted strictly descending
    assert_eq!(book.bids[0].price, dec!(65000.00));
    assert_eq!(book.bids[1].price, dec!(64999.00));
    assert_eq!(book.bids[2].price, dec!(64998.00));

    // Verify asks are sorted strictly ascending
    assert_eq!(book.asks[0].price, dec!(65001.00));
    assert_eq!(book.asks[1].price, dec!(65003.00));
    assert_eq!(book.asks[2].price, dec!(65005.00));
}

#[test]
fn test_order_book_crossed_state_rejection() {
    let mut book = OrderBook::new(VenueId::Binance, MarketType::Spot, "BTCUSDT");

    let invalid_bids = vec![PriceLevel::new(dec!(65010.00), dec!(1.0))];
    let invalid_asks = vec![PriceLevel::new(dec!(65005.00), dec!(1.0))]; // Ask lower than bid!

    let result = book.set_snapshot(invalid_bids, invalid_asks, 1727400000000, 1000, 1);
    assert!(result.is_err());

    match result {
        Err(EngineError::CrossedBook { bid, ask }) => {
            assert_eq!(bid, dec!(65010.00));
            assert_eq!(ask, dec!(65005.00));
        }
        other => panic!("Expected CrossedBook error, got: {:?}", other),
    }
}

#[test]
fn test_order_book_delta_update_and_deletion() {
    let mut book = OrderBook::new(VenueId::Bybit, MarketType::LinearPerpetual, "BTCUSDT");

    let bids = vec![
        PriceLevel::new(dec!(65000.00), dec!(1.0)),
        PriceLevel::new(dec!(64990.00), dec!(2.0)),
    ];
    let asks = vec![
        PriceLevel::new(dec!(65002.00), dec!(1.5)),
        PriceLevel::new(dec!(65010.00), dec!(2.5)),
    ];

    book.set_snapshot(bids, asks, 1727400000000, 1000, 1)
        .expect("Snapshot failed");

    // Apply delta: update 65000.00 qty to 3.0, delete 64990.00 (qty = 0), insert new level 65001.00 (new best bid)
    let delta_bids = vec![
        PriceLevel::new(dec!(65000.00), dec!(3.0)), // Update
        PriceLevel::new(dec!(64990.00), dec!(0.0)), // Deletion
        PriceLevel::new(dec!(65001.00), dec!(0.4)), // Insertion
    ];

    // Delta asks: delete 65002.00, so best ask becomes 65010.00
    let delta_asks = vec![PriceLevel::new(dec!(65002.00), dec!(0.0))];

    book.apply_delta(&delta_bids, &delta_asks, 1727400000100, 1100, 2)
        .expect("Delta apply failed");

    assert_eq!(
        book.best_bid(),
        Some(PriceLevel::new(dec!(65001.00), dec!(0.4)))
    );
    assert_eq!(
        book.best_ask(),
        Some(PriceLevel::new(dec!(65010.00), dec!(2.5)))
    );
    assert_eq!(book.bids.len(), 2); // 65001.00 and 65000.00 (64990 deleted)
    assert_eq!(book.asks.len(), 1); // 65010.00 (65002 deleted)
    assert_eq!(book.spread(), Some(dec!(9.00)));
}

#[test]
fn test_config_loading_and_validation() {
    let valid_toml = r#"
        [app]
        name = "airbitrage-test"
        environment = "test"
        log_level = "debug"

        [storage]
        data_dir = "test_data"
        sqlite_path = "test_data/test.db"
        zstd_raw_dir = "test_data/raw"

        [engine]
        min_spread_bps = 2.5
        test_notional_tiers = [100.0, 500.0]
        max_book_age_ms = 500

        [[venues]]
        id = "binance"
        enabled = true
        symbols = ["BTCUSDT"]
        market_types = ["spot"]
    "#;

    let config: AppConfig = toml::from_str(valid_toml).expect("Valid toml should parse");
    assert!(config.validate().is_ok());
    assert_eq!(config.app.name, "airbitrage-test");
    assert_eq!(config.engine.min_spread_bps, 2.5);
    assert_eq!(config.venues.len(), 1);
    assert_eq!(config.venues[0].id, VenueId::Binance);
}

#[test]
fn test_config_invalid_rejections() {
    // Missing symbols in enabled venue
    let invalid_venue_toml = r#"
        [app]
        name = "test"
        environment = "test"
        log_level = "info"

        [storage]
        data_dir = "d"
        sqlite_path = "s"
        zstd_raw_dir = "z"

        [engine]
        min_spread_bps = 1.0
        test_notional_tiers = [100.0]
        max_book_age_ms = 500

        [[venues]]
        id = "bybit"
        enabled = true
        symbols = []
        market_types = ["spot"]
    "#;

    let config: AppConfig = toml::from_str(invalid_venue_toml).unwrap();
    assert!(config.validate().is_err());

    // Invalid negative spread threshold
    let invalid_spread_toml = r#"
        [app]
        name = "test"
        environment = "test"
        log_level = "info"

        [storage]
        data_dir = "d"
        sqlite_path = "s"
        zstd_raw_dir = "z"

        [engine]
        min_spread_bps = -5.0
        test_notional_tiers = [100.0]
        max_book_age_ms = 500

        [[venues]]
        id = "binance"
        enabled = true
        symbols = ["BTCUSDT"]
        market_types = ["spot"]
    "#;

    let config: AppConfig = toml::from_str(invalid_spread_toml).unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn test_error_formatting() {
    let err = EngineError::CrossedBook {
        bid: dec!(65000.00),
        ask: dec!(64999.00),
    };
    let formatted = format!("{err}");
    assert!(formatted.contains("Crossed order book detected"));
    assert!(formatted.contains("65000.00"));
    assert!(formatted.contains("64999.00"));
}
