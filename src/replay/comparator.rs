use crate::observatory::types::{DislocationObservation, OpportunityRecord, PersistenceTransition};
use crate::recording::types::{PersistenceTransitionEvent, ResearchEventType};
use crate::replay::types::ReplayMismatch;

/// Compare a recorded dislocation observation against a replayed dislocation observation.
///
/// Fast-paths on structural equality (`recorded == replayed`), but on mismatch performs
/// granular field-by-field inspection to construct informative diagnostic reports.
pub fn compare_observations(
    record_index: u64,
    recorded: &DislocationObservation,
    replayed: &DislocationObservation,
) -> Vec<ReplayMismatch> {
    if recorded == replayed {
        return Vec::new();
    }

    let mut mismatches = Vec::new();
    let ts = recorded.observation_ts_ns;
    let b_ven = Some(recorded.buy_venue);
    let b_mkt = Some(recorded.buy_market);
    let s_ven = Some(recorded.sell_venue);
    let s_mkt = Some(recorded.sell_market);
    let sym = recorded.symbol.clone();
    let rel = Some(recorded.market_relationship);
    let qty = Some(recorded.reference_quantity);

    let mut push_diff = |field: &str, rec: String, rep: String| {
        mismatches.push(ReplayMismatch {
            record_index,
            event_timestamp_ns: ts,
            event_type: ResearchEventType::DislocationObservation,
            field_name: field.to_string(),
            recorded_value: rec,
            replayed_value: rep,
            buy_venue: b_ven,
            buy_market: b_mkt,
            sell_venue: s_ven,
            sell_market: s_mkt,
            symbol: sym.clone(),
            market_relationship: rel,
            reference_quantity: qty,
        });
    };

    if recorded.buy_venue != replayed.buy_venue {
        push_diff(
            "buy_venue",
            recorded.buy_venue.to_string(),
            replayed.buy_venue.to_string(),
        );
    }
    if recorded.buy_market != replayed.buy_market {
        push_diff(
            "buy_market",
            recorded.buy_market.to_string(),
            replayed.buy_market.to_string(),
        );
    }
    if recorded.sell_venue != replayed.sell_venue {
        push_diff(
            "sell_venue",
            recorded.sell_venue.to_string(),
            replayed.sell_venue.to_string(),
        );
    }
    if recorded.sell_market != replayed.sell_market {
        push_diff(
            "sell_market",
            recorded.sell_market.to_string(),
            replayed.sell_market.to_string(),
        );
    }
    if recorded.symbol != replayed.symbol {
        push_diff("symbol", recorded.symbol.clone(), replayed.symbol.clone());
    }
    if recorded.market_relationship != replayed.market_relationship {
        push_diff(
            "market_relationship",
            recorded.market_relationship.to_string(),
            replayed.market_relationship.to_string(),
        );
    }
    if recorded.observation_ts_ns != replayed.observation_ts_ns {
        push_diff(
            "observation_ts_ns",
            recorded.observation_ts_ns.to_string(),
            replayed.observation_ts_ns.to_string(),
        );
    }
    if recorded.reference_quantity != replayed.reference_quantity {
        push_diff(
            "reference_quantity",
            recorded.reference_quantity.to_string(),
            replayed.reference_quantity.to_string(),
        );
    }
    if recorded.buy_available_quantity != replayed.buy_available_quantity {
        push_diff(
            "buy_available_quantity",
            recorded.buy_available_quantity.to_string(),
            replayed.buy_available_quantity.to_string(),
        );
    }
    if recorded.sell_available_quantity != replayed.sell_available_quantity {
        push_diff(
            "sell_available_quantity",
            recorded.sell_available_quantity.to_string(),
            replayed.sell_available_quantity.to_string(),
        );
    }
    if recorded.common_executable_quantity != replayed.common_executable_quantity {
        push_diff(
            "common_executable_quantity",
            recorded.common_executable_quantity.to_string(),
            replayed.common_executable_quantity.to_string(),
        );
    }
    if recorded.fully_executable != replayed.fully_executable {
        push_diff(
            "fully_executable",
            recorded.fully_executable.to_string(),
            replayed.fully_executable.to_string(),
        );
    }
    if recorded.buy_vwap != replayed.buy_vwap {
        push_diff(
            "buy_vwap",
            format!("{:?}", recorded.buy_vwap),
            format!("{:?}", replayed.buy_vwap),
        );
    }
    if recorded.sell_vwap != replayed.sell_vwap {
        push_diff(
            "sell_vwap",
            format!("{:?}", recorded.sell_vwap),
            format!("{:?}", replayed.sell_vwap),
        );
    }
    if recorded.buy_worst_fill_price != replayed.buy_worst_fill_price {
        push_diff(
            "buy_worst_fill_price",
            format!("{:?}", recorded.buy_worst_fill_price),
            format!("{:?}", replayed.buy_worst_fill_price),
        );
    }
    if recorded.sell_worst_fill_price != replayed.sell_worst_fill_price {
        push_diff(
            "sell_worst_fill_price",
            format!("{:?}", recorded.sell_worst_fill_price),
            format!("{:?}", replayed.sell_worst_fill_price),
        );
    }
    if recorded.buy_best_ask != replayed.buy_best_ask {
        push_diff(
            "buy_best_ask",
            format!("{:?}", recorded.buy_best_ask),
            format!("{:?}", replayed.buy_best_ask),
        );
    }
    if recorded.sell_best_bid != replayed.sell_best_bid {
        push_diff(
            "sell_best_bid",
            format!("{:?}", recorded.sell_best_bid),
            format!("{:?}", replayed.sell_best_bid),
        );
    }
    if recorded.gross_spread != replayed.gross_spread {
        push_diff(
            "gross_spread",
            format!("{:?}", recorded.gross_spread),
            format!("{:?}", replayed.gross_spread),
        );
    }
    if recorded.gross_spread_bps != replayed.gross_spread_bps {
        push_diff(
            "gross_spread_bps",
            format!("{:?}", recorded.gross_spread_bps),
            format!("{:?}", replayed.gross_spread_bps),
        );
    }
    if recorded.gross_edge != replayed.gross_edge {
        push_diff(
            "gross_edge",
            format!("{:?}", recorded.gross_edge),
            format!("{:?}", replayed.gross_edge),
        );
    }
    if recorded.buy_fee != replayed.buy_fee {
        push_diff(
            "buy_fee",
            recorded.buy_fee.to_string(),
            replayed.buy_fee.to_string(),
        );
    }
    if recorded.sell_fee != replayed.sell_fee {
        push_diff(
            "sell_fee",
            recorded.sell_fee.to_string(),
            replayed.sell_fee.to_string(),
        );
    }
    if recorded.total_fees != replayed.total_fees {
        push_diff(
            "total_fees",
            recorded.total_fees.to_string(),
            replayed.total_fees.to_string(),
        );
    }
    if recorded.net_edge != replayed.net_edge {
        push_diff(
            "net_edge",
            format!("{:?}", recorded.net_edge),
            format!("{:?}", replayed.net_edge),
        );
    }
    if recorded.net_edge_bps != replayed.net_edge_bps {
        push_diff(
            "net_edge_bps",
            format!("{:?}", recorded.net_edge_bps),
            format!("{:?}", replayed.net_edge_bps),
        );
    }
    if recorded.both_books_trusted != replayed.both_books_trusted {
        push_diff(
            "both_books_trusted",
            recorded.both_books_trusted.to_string(),
            replayed.both_books_trusted.to_string(),
        );
    }
    if recorded.both_books_fresh != replayed.both_books_fresh {
        push_diff(
            "both_books_fresh",
            recorded.both_books_fresh.to_string(),
            replayed.both_books_fresh.to_string(),
        );
    }
    if recorded.no_crossed_books != replayed.no_crossed_books {
        push_diff(
            "no_crossed_books",
            recorded.no_crossed_books.to_string(),
            replayed.no_crossed_books.to_string(),
        );
    }
    if recorded.is_valid != replayed.is_valid {
        push_diff(
            "is_valid",
            recorded.is_valid.to_string(),
            replayed.is_valid.to_string(),
        );
    }
    if recorded.rejection_reason != replayed.rejection_reason {
        push_diff(
            "rejection_reason",
            format!("{:?}", recorded.rejection_reason),
            format!("{:?}", replayed.rejection_reason),
        );
    }
    if recorded.buy_book_exchange_ts_ms != replayed.buy_book_exchange_ts_ms {
        push_diff(
            "buy_book_exchange_ts_ms",
            format!("{:?}", recorded.buy_book_exchange_ts_ms),
            format!("{:?}", replayed.buy_book_exchange_ts_ms),
        );
    }
    if recorded.sell_book_exchange_ts_ms != replayed.sell_book_exchange_ts_ms {
        push_diff(
            "sell_book_exchange_ts_ms",
            format!("{:?}", recorded.sell_book_exchange_ts_ms),
            format!("{:?}", replayed.sell_book_exchange_ts_ms),
        );
    }
    if recorded.buy_book_recv_ts_ns != replayed.buy_book_recv_ts_ns {
        push_diff(
            "buy_book_recv_ts_ns",
            format!("{:?}", recorded.buy_book_recv_ts_ns),
            format!("{:?}", replayed.buy_book_recv_ts_ns),
        );
    }
    if recorded.sell_book_recv_ts_ns != replayed.sell_book_recv_ts_ns {
        push_diff(
            "sell_book_recv_ts_ns",
            format!("{:?}", recorded.sell_book_recv_ts_ns),
            format!("{:?}", replayed.sell_book_recv_ts_ns),
        );
    }
    if recorded.timestamp_skew_ms != replayed.timestamp_skew_ms {
        push_diff(
            "timestamp_skew_ms",
            format!("{:?}", recorded.timestamp_skew_ms),
            format!("{:?}", replayed.timestamp_skew_ms),
        );
    }
    if recorded.buy_book_age_ms != replayed.buy_book_age_ms {
        push_diff(
            "buy_book_age_ms",
            format!("{:?}", recorded.buy_book_age_ms),
            format!("{:?}", replayed.buy_book_age_ms),
        );
    }
    if recorded.sell_book_age_ms != replayed.sell_book_age_ms {
        push_diff(
            "sell_book_age_ms",
            format!("{:?}", recorded.sell_book_age_ms),
            format!("{:?}", replayed.sell_book_age_ms),
        );
    }

    mismatches
}

/// Compare a recorded persistence transition against a replayed persistence transition.
pub fn compare_transitions(
    record_index: u64,
    recorded: &PersistenceTransitionEvent,
    replayed: &PersistenceTransition,
) -> Vec<ReplayMismatch> {
    if recorded.transition == *replayed {
        return Vec::new();
    }

    vec![ReplayMismatch {
        record_index,
        event_timestamp_ns: recorded.timestamp_ns,
        event_type: ResearchEventType::PersistenceTransition,
        field_name: "transition".to_string(),
        recorded_value: format!("{:?}", recorded.transition),
        replayed_value: format!("{:?}", replayed),
        buy_venue: Some(recorded.key.buy_venue),
        buy_market: Some(recorded.key.buy_market),
        sell_venue: Some(recorded.key.sell_venue),
        sell_market: Some(recorded.key.sell_market),
        symbol: recorded.key.symbol.clone(),
        market_relationship: Some(recorded.key.market_relationship),
        reference_quantity: Some(recorded.key.reference_quantity),
    }]
}

/// Compare a recorded opportunity record against a replayed opportunity record.
pub fn compare_opportunity_records(
    record_index: u64,
    recorded: &OpportunityRecord,
    replayed: &OpportunityRecord,
) -> Vec<ReplayMismatch> {
    if recorded == replayed {
        return Vec::new();
    }

    let mut mismatches = Vec::new();
    let ts = recorded.end_ts_ns;
    let b_ven = Some(recorded.key.buy_venue);
    let b_mkt = Some(recorded.key.buy_market);
    let s_ven = Some(recorded.key.sell_venue);
    let s_mkt = Some(recorded.key.sell_market);
    let sym = recorded.key.symbol.clone();
    let rel = Some(recorded.key.market_relationship);
    let qty = Some(recorded.key.reference_quantity);

    let mut push_diff = |field: &str, rec: String, rep: String| {
        mismatches.push(ReplayMismatch {
            record_index,
            event_timestamp_ns: ts,
            event_type: ResearchEventType::OpportunityRecord,
            field_name: field.to_string(),
            recorded_value: rec,
            replayed_value: rep,
            buy_venue: b_ven,
            buy_market: b_mkt,
            sell_venue: s_ven,
            sell_market: s_mkt,
            symbol: sym.clone(),
            market_relationship: rel,
            reference_quantity: qty,
        });
    };

    if recorded.key != replayed.key {
        push_diff(
            "key",
            format!("{:?}", recorded.key),
            format!("{:?}", replayed.key),
        );
    }
    if recorded.start_ts_ns != replayed.start_ts_ns {
        push_diff(
            "start_ts_ns",
            recorded.start_ts_ns.to_string(),
            replayed.start_ts_ns.to_string(),
        );
    }
    if recorded.end_ts_ns != replayed.end_ts_ns {
        push_diff(
            "end_ts_ns",
            recorded.end_ts_ns.to_string(),
            replayed.end_ts_ns.to_string(),
        );
    }
    if recorded.duration_ms != replayed.duration_ms {
        push_diff(
            "duration_ms",
            recorded.duration_ms.to_string(),
            replayed.duration_ms.to_string(),
        );
    }
    if recorded.sample_count != replayed.sample_count {
        push_diff(
            "sample_count",
            recorded.sample_count.to_string(),
            replayed.sample_count.to_string(),
        );
    }
    if recorded.first_observed_edge_bps != replayed.first_observed_edge_bps {
        push_diff(
            "first_observed_edge_bps",
            recorded.first_observed_edge_bps.to_string(),
            replayed.first_observed_edge_bps.to_string(),
        );
    }
    if recorded.last_observed_edge_bps != replayed.last_observed_edge_bps {
        push_diff(
            "last_observed_edge_bps",
            recorded.last_observed_edge_bps.to_string(),
            replayed.last_observed_edge_bps.to_string(),
        );
    }
    if recorded.peak_net_edge_bps != replayed.peak_net_edge_bps {
        push_diff(
            "peak_net_edge_bps",
            recorded.peak_net_edge_bps.to_string(),
            replayed.peak_net_edge_bps.to_string(),
        );
    }
    if recorded.min_net_edge_bps != replayed.min_net_edge_bps {
        push_diff(
            "min_net_edge_bps",
            recorded.min_net_edge_bps.to_string(),
            replayed.min_net_edge_bps.to_string(),
        );
    }
    if recorded.average_net_edge_bps != replayed.average_net_edge_bps {
        push_diff(
            "average_net_edge_bps",
            recorded.average_net_edge_bps.to_string(),
            replayed.average_net_edge_bps.to_string(),
        );
    }
    if recorded.max_common_executable_quantity != replayed.max_common_executable_quantity {
        push_diff(
            "max_common_executable_quantity",
            recorded.max_common_executable_quantity.to_string(),
            replayed.max_common_executable_quantity.to_string(),
        );
    }
    if recorded.termination_reason != replayed.termination_reason {
        push_diff(
            "termination_reason",
            format!("{:?}", recorded.termination_reason),
            format!("{:?}", replayed.termination_reason),
        );
    }

    mismatches
}
