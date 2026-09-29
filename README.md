# Airbitrage

**Airbitrage** is a market dislocation research observatory and analysis engine written in Rust.

The project is designed to answer a single quantitative question:
> *Can we observe, reconstruct, model, replay, and realistically simulate market dislocations across fragmented venues well enough to determine whether an economically viable edge exists?*

Airbitrage does not assume arbitrage is profitable or risk-free. It treats every observed price discrepancy as a hypothesis subject to fee hurdles, book depth consumption, order arrival latency, and execution uncertainty.

---

## Current Status: Milestone M5.5 (Deterministic Replay Engine Complete)
* **Active Status:** M0 COMPLETE | M1 COMPLETE | M2 COMPLETE | M3 COMPLETE | M4 COMPLETE | M5.1–M5.5 COMPLETE | M5.6 NEXT
* **Toolchain:** Rust (Edition 2024, `rustc 1.85+` / `1.98.1+`)
* **Verified Test Suite:** **167 deterministic tests passing** (`cargo test`)
* **Market-Data Pipeline:**
  * **Binance Spot:** Continuous snapshot ingestion (`@depth20@100ms`), invariant checking, and microsecond freshness tracking.
  * **Binance USD-M Futures:** REST depth snapshot synchronization, buffered delta drainage, $U \le S \le u$ initial alignment, continuous $pu == \text{previous } u$ sequence validation, and funding rate stream tracking.
  * **Bybit Spot & Linear:** Continuous WebSocket delta ingestion with monotonic update ID verification and snapshot re-synchronization.
  * **Four-Book State Engine:** Deterministic multi-venue state machine (`Empty`, `AwaitingSnapshot`, `Synchronizing`, `Live`, `Invalidated`, `Resyncing`) isolating Binance Spot, Binance Linear, Bybit Spot, and Bybit Linear.
  * **Trust Gate:** Downstream components access order books only when state is strictly `Live`, `Valid`, uncrossed, and non-stale.
* **Executable Pricing & Cost Engine (M4):**
  * Exact Decimal L2 order book depth walking (zero floating-point arithmetic).
  * True Volume-Weighted Average Price (VWAP) calculation.
  * Configurable fee modeling (maker/taker bps schedules per venue and market type).
  * Net executable edge calculation accounting for price impact and double-sided transaction fees.
* **Cross-Book Dislocation Observatory & Persistence Engine (M5.1–M5.3):**
  * Live cross-venue comparison across all 4 book relationships (Spot-Spot, Perp-Perp, Spot-Perp basis).
  * Reference-quantity sweep evaluations (e.g. 0.01, 0.1, 0.5, 1.0 BTC).
  * Real-time opportunity persistence tracking (`Start`, `Continue`, `End`) measuring dislocation duration, tick count, peak edge, and decay half-life.
* **Research Serialization & High-Fidelity Recording (M5.4):**
  * Canonical schema-versioned research events (`schema_version: 1`).
  * Newline-Delimited JSON (NDJSON) append-only recorder with explicit flush behavior.
  * Strict separation between raw observed data (`MarketEvent`), configured assumptions, and derived analytical observations (`DislocationObservation`, `OpportunityRecord`).
  * Loud failure on corrupted lines, truncated records, or unsupported schema versions.
* **Deterministic Historical Replay Engine (M5.5):**
  * Step-by-step market state reconstruction driven strictly by historical `MarketEvent`s.
  * Pipeline re-evaluation through the active M4 pricing engine and M5 dislocation observatory.
  * Exact Decimal field-by-field verification against recorded reference observations and persistence transitions.
  * High-fidelity diagnostic mismatch reporting identifying discrepancy line numbers, field names, and values.
* **Live Trading:** Strictly disabled. No API keys, no private endpoints, no order routing.

---

## Scope of Version 1 (V1)

### What V1 Does
1. Ingests public, unauthenticated L2 market data and funding streams from **Binance** (Spot & USD-M Futures) and **Bybit** (Spot & Linear Futures).
2. Reconstructs normalized, synchronized local limit order books using true protocol semantics (snapshots for full book state, deltas with strict sequence continuity for incremental updates).
3. Computes executable Volume-Weighted Average Price (VWAP) across discrete sizing tiers.
4. Models all explicit frictions: exchange-specific taker fees, book slippage, and market impact.
5. Measures dislocation persistence decay and tracks complete opportunity lifecycles.
6. Serializes raw and derived events to deterministic, schema-versioned NDJSON datasets for auditable offline research.
7. Replays historical market datasets deterministically, verifying bit-for-bit consistency between observed market reconstruction and recorded analytical models.
8. Simulates non-atomic execution via a realistic paper execution state machine (in subsequent milestones).

### What V1 Explicitly Does NOT Do
* No live order placement, order modification, or cancellation.
* No private exchange API integration, key management, or credential handling.
* No account balance tracking, deposit, withdrawal, or wallet transfers.
* No leverage, margin borrowing, or automated capital allocation.
* No Traditional Finance (NSE, BSE, CME) or Decentralized Exchange (DEX) integrations.

---

## Building and Running

### Prerequisites
* Rust toolchain (`rustc` and `cargo` version 1.85+ / Edition 2024 support).

### Build
```powershell
cargo build
```

### Run Tests (167 Passing)
```powershell
cargo test
```

### Static Analysis & Lints
```powershell
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

### Historical Replay Execution
```powershell
# Replay a recorded research dataset through MarketState -> M4 -> M5
cargo run -- --replay data/research/BTCUSDT_2026-09-29.ndjson
```

### Live Validation Modes
```powershell
# Live Binance smoke test (public depth feeds)
cargo run -- --binance-smoke

# Binance soak test (continuous stream validation)
cargo run -- --binance-soak 60

# Live multi-venue market state engine validation
cargo run -- --market-state-live 20

# Live 4-book dislocation observatory
cargo run -- --observatory-live 30
```

---

## Documentation
* [Master Phased Roadmap](docs/MASTER_PLAN.md)
* [System Architecture](docs/ARCHITECTURE.md)
* [Assumptions Register](docs/ASSUMPTIONS.md)
* [M4 Executable Pricing Engine](docs/execution/m4_pricing_engine.md)
* [M5 Dislocation Observatory](docs/observatory/m5_cross_book_dislocations.md)
* [M5.4 Research Recording Specification](docs/recording/m5_4_research_recording.md)
* [M5.5 Historical Replay Specification](docs/replay/m5_5_historical_replay.md)
* [Binance Integration Specification](docs/venues/binance.md)
* [Bybit Integration Specification](docs/venues/bybit.md)
* [ADR 0001: V1 Research Scope](docs/adr/0001-v1-scope.md)
