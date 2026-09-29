# M5.5 — Deterministic Historical Replay Engine

## 1. Motivation & Purpose

Airbitrage is designed to address a central quantitative question:
> *Can observed cross-book market dislocations survive realistic execution frictions, fees, order book depth consumption, and latency decay?*

Milestone **M5.4** implemented high-fidelity serialization and recording of market and dislocation event streams.
Milestone **M5.5** implements the **Deterministic Historical Replay Engine**.

The purpose of M5.5 is NOT to simulate live trade execution or claim trading profitability. Rather, it is a **verification and research system** that proves Airbitrage can take recorded raw market events and deterministically reconstruct the exact same order book states, executable VWAPs, and cross-book dislocation observations produced during original execution.

```
                    RECORDED RESEARCH DATASET (NDJSON)
                                    │
                                    ▼
                             ResearchReader
                                    │
                     ┌──────────────┴──────────────┐
                     ▼                             ▼
               MarketEvent               DislocationObservation /
             (OBSERVED INPUT)             PersistenceTransition
                     │                    (REFERENCE EXPECTATION)
                     ▼                             │
             MarketStateManager                    │
                     │                             │
                     ▼                             │
            Reconstructed Books                    │
                     │                             │
                     ▼                             │
            M4 Pricing Engine                      │
                     │                             │
                     ▼                             │
             M5 Observatory                        │
                     │                             │
                     ▼                             │
            Replayed Observation                   │
                     │                             │
                     └──────────────┬──────────────┘
                                    ▼
                         Exact Decimal Comparison
                                    │
                     ┌──────────────┴──────────────┐
                     ▼                             ▼
              Match Confirmed              Diagnostic Mismatch
```

---

## 2. Core Replay Principle: Observed Inputs vs. Derived Reference

The replay engine treats recorded **observed exchange events** as primary ground truth:
- **`MarketEvent`** payloads (`OrderBookSnapshot`, `OrderBookDelta`, `FundingRateUpdate`) are the **INPUTS** driving state reconstruction.
- Recorded **`DislocationObservation`**, **`PersistenceTransition`**, and **`OpportunityRecord`** payloads are **NEVER** fed into the state engine to recreate state. They serve solely as **REFERENCE TARGETS** against which recomputed analytical outputs are evaluated.

This guarantees that historical replay exercises the actual state machine, sequence validators, depth walking, VWAP arithmetic, fee schedules, and persistence trackers rather than merely replaying pre-calculated answers.

---

## 3. Architecture & Data Flow

The replay subsystem is organized into modular components:

```
src/replay/
├── types.rs         # ReplayConfig, ReplayMismatch, ReplayStepOutcome, ReplayResult
├── comparator.rs    # Exact field-by-field diagnostic comparison logic
├── engine.rs        # Streaming ReplayEngine driving state reconstruction and M4/M5
└── mod.rs           # Clean public API re-exports
```

### Component Responsibilities

1. **`ResearchReader<R>`**:
   Streams self-describing newline-delimited JSON (`ResearchEvent`) line-by-line without loading entire datasets into memory.
2. **`ReplayEngine`**:
   - Owns `MarketStateManager` initialized with configured staleness limits.
   - Owns `ObservationEngine` configured with taker fee schedules and observation thresholds.
   - Owns `OpportunityTracker` tracking temporal persistence lifecycles.
   - Dispatches incoming events sequentially to `replay_step`.
3. **`comparator`**:
   - Compares replayed `DislocationObservation` against recorded reference using exact `rust_decimal::Decimal` arithmetic.
   - On mismatch, emits a structured `ReplayMismatch` recording line index, field name, expected value, replayed value, pair, and symbol.

---

## 4. Event Ordering & Timestamp Semantics

### Deterministic Sequence Preservation
- Replay processes records in the exact order they were serialized.
- Records are NOT reordered based on timestamps.
- Monotonically increasing sequence IDs are preserved and verified per venue protocol rules.

### Timestamp Semantics: Internal vs. Host Time
- **`ResearchEvent.timestamp_ns`**: Envelope metadata indicating when the record was written to disk. The replay engine strictly ignores host recording timestamps during state transitions.
- **`MarketEvent.local_recv_ts_ns` / `exchange_ts_ms`**: Exchange and transport timestamps carried inside the raw event. These internal timestamps drive all order book freshness and inter-book skew calculations.
- **`DislocationObservation.observation_ts_ns`**: The exact observation timestamp recorded with the reference observation is passed as `now_ns` when replaying that observation, ensuring deterministic evaluation of book ages and freshness gates.

---

## 5. MarketState Reconstruction & Invariants

During replay of `OrderBookSnapshot` and `OrderBookDelta`:
1. **Protocol-Specific Sequencing**:
   - Binance Spot: Snapshot-only cadence (`@depth20@100ms`).
   - Binance USD-M Futures: Initial snapshot alignment ($U \le S \le u$) followed by continuous update ID continuity ($pu == \text{previous } u$).
   - Bybit Spot & Linear: Strict monotonic update ID advancement ($u > \text{last } u$).
2. **Book Validity & Trust Gate**:
   - Order books transition through lifecycle states: `AwaitingSnapshot` $\to$ `Synchronizing` $\to$ `Live`.
   - Downstream M4 and M5 access books via `manager.get_trusted_book(venue, market_type, symbol, now_ns)`.
   - An order book is untrusted if it is not in state `Live`, is crossed (`best_bid >= best_ask`), has empty depth, or exceeds `max_staleness`.
3. **Sequence Gap / Invariant Failure**:
   - If an event causes a sequence break or crossed book, `ReplayEngine` fails loudly when `fail_on_market_error = true`, halting with an actionable error.

---

## 6. M4 & M5 Observation Comparison

For every recorded observation encountered in the dataset:
1. `ReplayEngine` queries current reconstructed order books for the specified pair and requested reference quantity.
2. It walks visible book depth, computes VWAP, determines executable quantities, calculates taker fees from the `VenueFeeRegistry`, and determines net executable spread.
3. It evaluates freshness gates (`max_book_age_ms`, `max_timestamp_skew_ms`) and trust status.
4. The newly calculated observation is compared against the recorded reference across all fields:
   - `buy_venue`, `buy_market`, `sell_venue`, `sell_market`, `symbol`
   - `market_relationship` (Spot-Spot, Perp-Perp, Spot-Perp Basis)
   - `reference_quantity`, `buy_available_quantity`, `sell_available_quantity`, `common_executable_quantity`, `fully_executable`
   - `buy_vwap`, `sell_vwap`, `buy_worst_fill_price`, `sell_worst_fill_price`
   - `buy_best_ask`, `sell_best_bid`
   - `gross_spread`, `gross_spread_bps`, `gross_edge`
   - `buy_fee`, `sell_fee`, `total_fees`
   - `net_edge`, `net_edge_bps`
   - `both_books_trusted`, `both_books_fresh`, `no_crossed_books`, `is_valid`
   - `rejection_reason`
   - `buy_book_exchange_ts_ms`, `sell_book_exchange_ts_ms`
   - `buy_book_recv_ts_ns`, `sell_book_recv_ts_ns`
   - `timestamp_skew_ms`, `buy_book_age_ms`, `sell_book_age_ms`

### Zero Floating-Point Tolerance
All financial values are represented as `rust_decimal::Decimal`. Comparisons use exact equality (`==`). Floating-point tolerances (`epsilon`) are prohibited.

---

## 7. Mismatch Reporting & Failure Handling

When a replayed observation or transition differs from reference data:
- `ReplayMismatch` captures the discrepancy with complete context:
  ```
  Mismatch at record #18342 (ts: 1727600000000ns) [DislocationObservation]: field 'net_edge' expected 'Some(15.25)', got 'Some(10.50)' for Binance Spot -> Bybit Spot BTCUSDT (qty 0.1)
  ```
- If `stop_on_first_mismatch = true`, replay halts immediately.
- If `stop_on_first_mismatch = false`, replay continues, collecting all discrepancies for a final audit report.

---

## 8. Deterministic Guarantees

Given the same historical research NDJSON dataset:
- Run 1 and Run 2 produce **identical** `ReplayResult` structures.
- All reconstructed book levels, lifecycle states, observations, and persistence records match bit-for-bit.
- Zero reliance on host wall-clock time, system randomness, or nondeterministic hash map iteration.

---

## 9. Usage Examples

### Replaying from Code
```rust
use airbitrage::recording::ResearchReader;
use airbitrage::replay::{ReplayConfig, ReplayEngine};

fn replay_file(path: &str) -> airbitrage::error::Result<()> {
    let mut reader = ResearchReader::from_path(path)?;
    let mut engine = ReplayEngine::with_defaults();
    let result = engine.replay(&mut reader)?;

    println!("{result}");
    if !result.is_clean() {
        panic!("Replay detected {} mismatches", result.mismatches.len());
    }
    Ok(())
}
```

### Running from CLI
```powershell
cargo run -- --replay data/research/BTCUSDT_2026-09-29.ndjson
```

---

## 10. Explicit Limitations & Non-Claims

1. **Research Reconstruction Only**: M5.5 verifies that historical state and observations can be deterministically reproduced. It does NOT simulate fill probability, queuing delays, network latency, or market impact of live orders.
2. **Replay Does NOT Prove Profitability**:
   - Apparent historical dislocations may have been unfillable in real time.
   - Market orders placed by other participants may have consumed liquidity before local packets arrived.
   - Exchange rate limits, API latency, and toxic flow are not modeled in pure book replay.
3. **Single-Threaded Sequential Execution**: Replay operates sequentially to maintain strict sequence determinism. Parallel or distributed replay is deferred to future optimization milestones.
