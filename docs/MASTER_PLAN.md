# Airbitrage Master Plan — Executable Roadmap

This document defines the complete phased engineering roadmap for Airbitrage. Each milestone has strictly defined inputs, outputs, acceptance criteria, and failure triggers.

---

## Current Status Overview
* **Active Milestone:** Phase 2 (M2 Bybit Market Data)
* **M0 STATUS: COMPLETE** (Foundation & Project Scaffolding)
* **M1 STATUS: COMPLETE** (Binance Public Market-Data Ingestion)
* **Rust Version:** `rustc 1.98.1 (48a229cea 2026-09-01)` / Edition `2024`
* **Cargo Check:** PASSED (0 errors, 0 warnings)
* **Cargo Test:** PASSED (19 passed; 0 failed; 0 ignored across M0 and M1 test suites)
* **Cargo Clippy:** PASSED (`--all-targets --all-features -- -D warnings`, 0 warnings)
* **Cargo Fmt:** PASSED (`cargo fmt --check`, 0 diffs)
* **Live Smoke Test:** PASSED (Binance Spot & USD-M Futures streams connected and parsed live quotes)
* **Live Soak Test:** PASSED (15s continuous run: 269 messages received, 267 parsed, 0 rejected, 0 errors, 0 crossed books)

---

## Phase 0 — Foundation (M0)
* **Goal:** Establish a clean, reproducible Rust project foundation with canonical domain types, structured error taxonomy, minimal configuration parsing, structured tracing, and comprehensive test/clippy verification.
* **Inputs:** Local toolchain, configuration template (`config.toml`).
* **Outputs:** Compiling binary and library, clean test suite, zero clippy warnings, complete documentation, Git repository baseline.
* **Files/Modules:**
  * `Cargo.toml`, `README.md`, `.gitignore`
  * `src/main.rs`, `src/config.rs`, `src/error.rs`, `src/types.rs`
  * `src/market/mod.rs`, `src/market/orderbook.rs`
  * `src/venues/mod.rs`, `src/venues/binance.rs`, `src/venues/bybit.rs`
  * `tests/m0_foundation_tests.rs`
* **Dependencies:** `serde`, `serde_json`, `toml`, `rust_decimal`, `thiserror`, `tracing`, `tracing-subscriber`.
* **Tests:** Canonical type creation, order book invariant sorting, configuration validation, error conversion.
* **Benchmarks:** None (M0 establishes correctness).
* **Acceptance Criteria:** `cargo check`, `cargo test`, `cargo clippy -- -D warnings`, and `cargo fmt --check` all exit 0.
* **Failure Criteria:** Compilation failure, clippy warnings, floating-point types in canonical price/quantity models.
* **Explicitly NOT Included:** Network connections, WebSockets, real exchange streaming, database engines, order execution.

---

## Phase 1 — Binance Market Data (M1)
* **Goal:** Implement an unauthenticated public WebSocket client for Binance Spot and USD-M Futures, subscribing to partial depth (`@depth20@100ms`) and mark price streams, emitting normalized `MarketEvent` instances over a bounded channel.
* **Inputs:** Public Binance WebSocket streams (`wss://stream.binance.com:9443/ws`, `wss://fstream.binance.com/ws`).
* **Outputs:** Normalized stream of `MarketEvent::OrderBookSnapshot` and `MarketEvent::FundingRateUpdate`.
* **Files/Modules:** `src/venues/binance.rs`, `tests/binance_feed_tests.rs`.
* **Dependencies:** `tokio` (rt, macros), `tokio-tungstenite`, `rustls-tls`.
* **Tests:** Mock WebSocket payload deserialization, bad frame rejection, reconnect backoff timing test.
* **Benchmarks:** JSON parsing benchmark ($< 25\,\mu\text{s}$ target).
* **Acceptance Criteria:** Stream connects, parses live BTCUSDT Spot and Perp frames continuously for 1 hour without panic, emits monotonically timed events.
* **Failure Criteria:** Unhandled disconnect, dropped packets without error flag, memory leak under steady stream.
* **Explicitly NOT Included:** Bybit connector, local book mutation, spread calculation, API keys.

---

## Phase 2 — Bybit Market Data (M2)
* **Goal:** Implement an unauthenticated public WebSocket client for Bybit V5 Spot and Linear Futures, handling connection management, 20s ping/pong heartbeats, and subscribing to `orderbook.50` and `tickers` topics.
* **Inputs:** Public Bybit WebSocket streams (`wss://stream.bybit.com/v5/public/spot`, `/linear`).
* **Outputs:** Normalized stream of `MarketEvent::OrderBookSnapshot`, `MarketEvent::OrderBookDelta`, and `MarketEvent::FundingRateUpdate`.
* **Files/Modules:** `src/venues/bybit.rs`, `tests/bybit_feed_tests.rs`.
* **Dependencies:** Reuses network stack from M1.
* **Tests:** Snapshot initialisation verification, delta application parsing, heartbeat ping/pong timer verification.
* **Benchmarks:** Frame normalization throughput benchmark.
* **Acceptance Criteria:** Sustained 1-hour live stream of BTCUSDT Spot and Linear with zero unhandled frame drops.
* **Failure Criteria:** Heartbeat timeout disconnects, sequence gaps unflagged.
* **Explicitly NOT Included:** Cross-venue comparison, order execution, private endpoints.

---

## Phase 3 — Local Order Books (M3)
* **Goal:** Reconstruct and maintain live, in-memory limit order books for all active streams inside the single-threaded engine core.
* **Inputs:** Channel stream of `MarketEvent` from M1 and M2.
* **Outputs:** Validated `OrderBook` state maintaining top 20–50 bids and asks sorted strictly by price.
* **Files/Modules:** `src/market/orderbook.rs`, `src/market/manager.rs`.
* **Dependencies:** Core crates only.
* **Tests:** Monotonic sorting property tests, level insert/update/delete tests, crossed-book detection invariants.
* **Benchmarks:** Book mutation benchmark ($< 5\,\mu\text{s}$ target).
* **Acceptance Criteria:** Local book matches live exchange top-of-book quotes without state drift over 60-minute runs.
* **Failure Criteria:** `Best_Bid >= Best_Ask` without immediate `CrossedBook` error flag; out-of-order sequence applied.
* **Explicitly NOT Included:** Dislocation detection, VWAP calculation.

---

## Phase 4 — Executable VWAP (M4)
* **Goal:** Implement a deterministic depth-walking VWAP algorithm to calculate the true executable buy cost and sell proceeds for discrete notional sizing tiers ($100, $500, $1,000).
* **Inputs:** Validated `OrderBook` state from M3 and target notional amount.
* **Outputs:** `ExecutableQuote { vwap_price: Decimal, total_notional: Decimal, depth_levels_consumed: usize }`.
* **Files/Modules:** `src/market/vwap.rs`.
* **Dependencies:** `rust_decimal`.
* **Tests:** Synthetic book depth walking, edge cases (insufficient depth, exact level matches).
* **Benchmarks:** VWAP calculation benchmark across 20 levels ($< 5\,\mu\text{s}$ target).
* **Acceptance Criteria:** Matches mathematical manual walk; rejects trade with `InsufficientLiquidity` if depth $< N$.
* **Failure Criteria:** Floating-point rounding errors, inaccurate partial-level consumption.
* **Explicitly NOT Included:** Fee deductions, cross-venue spread comparison.

---

## Phase 5 — Cost Engine (M5)
* **Goal:** Build an isolated, configurable cost model that computes exact exchange trading fees, estimated slippage, market impact, and latency risk penalties.
* **Inputs:** Executable VWAP quotes, exchange fee configurations, local latency measurements.
* **Outputs:** `CostBreakdown { fees: Decimal, slippage: Decimal, latency_risk: Decimal, total_deduction: Decimal }`.
* **Files/Modules:** `src/engine/costs.rs`.
* **Dependencies:** `config.rs`.
* **Tests:** Tier-0 fee calculation, BNB discount fee calculation, extreme volatility latency penalty test.
* **Benchmarks:** Cost evaluation execution latency ($< 2\,\mu\text{s}$).
* **Acceptance Criteria:** Exactly matches contractual exchange taker fee schedules across Binance and Bybit.
* **Failure Criteria:** Hardcoded fee assumptions, combined unexplained "fudge-factor" slippage variables.
* **Explicitly NOT Included:** Signal detection, order routing.

---

## Phase 6 — Dislocation Detection (M6)
* **Goal:** Continuously compare normalized books across venues (Binance vs Bybit) for Spot-Spot, Perp-Perp, and Spot-Perp pairs to detect positive net expected edge after all M5 costs.
* **Inputs:** Live books from M3, VWAP from M4, cost model from M5.
* **Outputs:** `DislocationEvent` records emitted when $\text{Net Edge} > \text{Min Threshold}$.
* **Files/Modules:** `src/engine/detector.rs`.
* **Dependencies:** Core engine modules.
* **Tests:** Synthetic spread detection, crossed book rejection, negative edge filtering.
* **Benchmarks:** Tick-to-detection latency ($< 50\,\mu\text{s}$ target).
* **Acceptance Criteria:** Evaluates all pairwise directions continuously; false signals caused by stale feeds are rejected.
* **Failure Criteria:** Emitting a dislocation when one feed is stale ($> 500\text{ms}$).
* **Explicitly NOT Included:** Persistence decay, automated trading.

---

## Phase 7 — Persistence & Decay Tracker (M7)
* **Goal:** Monitor detected dislocations over fixed time horizons ($0\text{ms}$ to $60\text{s}$) to measure empirical half-life and decay curves.
* **Inputs:** `DislocationEvent` triggers and ongoing `MarketEvent` stream.
* **Outputs:** `PersistenceRecord { initial_edge: Decimal, edge_at_100ms: Decimal, half_life_ms: u64 }`.
* **Files/Modules:** `src/engine/decay.rs`.
* **Dependencies:** `tokio` time utilities.
* **Tests:** Decay curve calculation on synthetic delayed books.
* **Benchmarks:** Minimal tracking memory footprint ($< 100\text{ KB}$ active tracking table).
* **Acceptance Criteria:** Accurately logs the exact moment an observed edge vanishes or reverses.
* **Failure Criteria:** Assuming persistence implies executable liquidity without re-walking depth.
* **Explicitly NOT Included:** Order execution.

---

## Phase 8 — Raw Recording (M8)
* **Goal:** Implement an append-only, high-performance tick recorder writing raw market data frames into compressed Zstandard (`.zst`) files.
* **Inputs:** Raw byte payloads received by venue adapters.
* **Outputs:** Sequenced, timestamped `.zst` archive files partitioned by date and venue.
* **Files/Modules:** `src/storage/recorder.rs`.
* **Dependencies:** `zstd`.
* **Tests:** Compression ratio verification, file integrity on abrupt termination.
* **Benchmarks:** Disk write throughput ($> 50\text{ MB/s}$ write capability).
* **Acceptance Criteria:** Uninterrupted recording over 24 hours; files cleanly decompress with 100% byte fidelity.
* **Failure Criteria:** Disk I/O blocking the hot-path network reader.
* **Explicitly NOT Included:** Database inserts of every tick.

---

## Phase 9 — Replay Engine (M9)
* **Goal:** Build a deterministic historical replay engine that feeds recorded `.zst` tick streams back into the exact same canonical engine pipeline.
* **Inputs:** Compressed historical market data files from M8.
* **Outputs:** Identical sequence of `MarketEvent`, order book states, and detected dislocations as live mode.
* **Files/Modules:** `src/replay/mod.rs`, `src/replay/feeder.rs`.
* **Dependencies:** Core engine and M8 storage.
* **Tests:** Replay determinism test (feed live, record, replay, assert state equality).
* **Benchmarks:** Replay processing speed ($> 50,000\text{ msgs/sec}$ offline throughput).
* **Acceptance Criteria:** Replay produces bit-for-bit identical dislocation events from recorded data.
* **Failure Criteria:** Code divergence between live ingestion and historical replay paths.
* **Explicitly NOT Included:** Live trading.

---

## Phase 10 — Paper Execution Simulator (M10)
* **Goal:** Simulate two-legged order execution against live or replayed market data, modeling order transit delay, queue priority, partial fills, and leg failure.
* **Inputs:** `DislocationEvent` signals from M6 and ongoing order book depth.
* **Outputs:** `SimulatedExecutionReport { leg_a_fill: Fill, leg_b_fill: Fill, realized_pnl: Decimal, unhedged_duration_ms: u64 }`.
* **Files/Modules:** `src/execution/paper.rs`, `src/execution/fsm.rs`.
* **Dependencies:** Core engine.
* **Tests:** State machine transition tests (Reject, Full Fill, Partial Fill, Emergency Abort).
* **Benchmarks:** Execution simulation latency ($< 10\,\mu\text{s}$).
* **Acceptance Criteria:** Realistic slippage and partial fill models produce realistic P&L distributions.
* **Failure Criteria:** Assuming instantaneous fills at observed top-of-book prices.
* **Explicitly NOT Included:** Live order routing, private exchange keys.

---

## Phase 11 — Risk Engine & Kill Switches (M11)
* **Goal:** Implement non-negotiable risk gates and automated circuit breakers that override strategy signals during market anomalies.
* **Inputs:** System health metrics, feed freshness, cumulative simulated daily loss, unhedged exposure.
* **Outputs:** `RiskDecision::Allow` or `RiskDecision::Halt(Reason)`.
* **Files/Modules:** `src/risk/engine.rs`, `src/risk/breakers.rs`.
* **Dependencies:** Core engine.
* **Tests:** Stale data trip test, maximum drawdown trip test, sequence gap trip test.
* **Benchmarks:** Risk check evaluation ($< 1\,\mu\text{s}$).
* **Acceptance Criteria:** Risk engine terminates execution intent within $100\,\mu\text{s}$ of anomalous state detection.
* **Failure Criteria:** Any code path allowing an execution intent to bypass risk gates.
* **Explicitly NOT Included:** Live trading.

---

## Phase 12 — Statistical Strategy Evaluation (M12)
* **Goal:** Analyze captured dislocation datasets and paper trading runs to determine whether any observed strategy satisfies statistical viability criteria.
* **Inputs:** SQLite dislocation database, paper trading execution logs.
* **Outputs:** Formal statistical report: expected net value, Sharpe ratio, half-life distributions, failure rates.
* **Files/Modules:** `research/analysis.py` (DuckDB / Polars pipeline).
* **Dependencies:** Python 3.11, DuckDB, Polars.
* **Tests:** Data consistency verification between SQLite and analytical aggregates.
* **Benchmarks:** Offline report generation ($< 60\text{s}$ over 10M records).
* **Acceptance Criteria:** Objective pass/kill determination on examined strategy families.
* **Failure Criteria:** Confirming viability without statistical significance ($p < 0.01$).
* **Explicitly NOT Included:** Live trading.

---

## Phase 13 — Additional Venues (M13)
* **Goal:** Plug in additional candidate venues (e.g., OKX, Kraken, or specialized DEX pools) using the established venue adapter pattern without altering core engine logic.
* **Inputs:** Target exchange public API documentation and WebSockets.
* **Outputs:** New venue connector module emitting standard `MarketEvent` instances.
* **Files/Modules:** `src/venues/<venue>.rs`.
* **Acceptance Criteria:** Core engine compiles and operates without modifying `src/market/` or `src/engine/`.

---

## Phase 14 — Controlled Live Execution (M14 — Future)
* **Goal:** Deploy micro-capital live execution only if Phase 12 statistically proves a durable edge.
* **Prerequisites:** Complete compliance, tax, and legal clearance; strict multi-factor authentication and hard stop-loss limits.
* **Explicitly Excluded from Present Scope.**
