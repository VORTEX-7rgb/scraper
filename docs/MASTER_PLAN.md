# Airbitrage Master Plan — Executable Roadmap

This document defines the complete phased engineering roadmap for Airbitrage. Each milestone has strictly defined inputs, outputs, acceptance criteria, and failure triggers.

---

## Current Status Overview
* **Active Status:**
  * **M0 STATUS: COMPLETE** (Foundation & Project Scaffolding — Commit `52ccb6f`)
  * **M1 STATUS: COMPLETE WITH M1.1 HARDENING** (Binance Protocol & Stream Hardening — Commit `12c42e8`)
  * **M2 STATUS: COMPLETE** (Local Market-State Engine — Deterministic Multi-Book Synchronization & Validation)
  * **M2.1 STATUS: COMPLETE** (Local Market-State Engine Hardening — Sequence Policies & Epochs)
  * **M3 STATUS: NOT STARTED** (Bybit Public Market-Data Ingestion)
* **Rust Toolchain:** `rustc 1.98.1 (48a229cea 2026-09-01)` / Edition `2024`
* **Cargo Check:** PASSED (0 errors, 0 warnings)
* **Cargo Test:** PASSED (55 passed; 0 failed across foundation, binance feed, and market state suites)
* **Cargo Clippy:** PASSED (`--all-targets --all-features -- -D warnings`, 0 warnings)
* **Cargo Fmt:** PASSED (`cargo fmt --check`, 0 diffs)
* **Live Smoke Test:** PASSED (Binance Spot, Futures Depth `/public`, and Futures Mark Price `/market` streams verified live)
* **Live Market-State Engine Test:** PASSED (Spot depth snapshots applied continuously, Futures REST snapshot fetched and aligned with buffered deltas, $U \le S \le u$ initial covering alignment verified, continuous $pu == \text{previous } u$ stream continuity enforced without sequence errors, crossed books rejected, trusted books verified)
* **Extended Soak Test:** PASSED (60.00s continuous run: 1,126 stream messages received, 1,126 processed, 0 parse errors, 0 validation errors, 0 backpressure drops, 0 crossed books, p50: 8 µs, p99: 46 µs, peak working set: 15.12 MB)

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

## Phase 1 — Binance Market Data & Protocol Hardening (M1 & M1.1)
* **Goal:** Implement an unauthenticated public WebSocket client for Binance Spot and USD-M Futures complying with the 2026 routed architecture, distinguishing snapshot vs delta semantics, preserving update sequence IDs (`U`, `u`, `pu`), ingesting mark price & funding rate, and tracking per-stream freshness.
* **Inputs:**
  * Binance Spot WebSocket: `wss://stream.binance.com/ws` (`<symbol>@depth20@100ms`)
  * Binance USD-M Futures Public Route: `wss://fstream.binance.com/public/ws` (`<symbol>@depth20@100ms`)
  * Binance USD-M Futures Market Route: `wss://fstream.binance.com/market/ws` (`<symbol>@markPrice@1s`)
* **Outputs:** Normalized stream of:
  * `MarketEvent::OrderBookSnapshot` (Spot full snapshots)
  * `MarketEvent::OrderBookDelta` (Futures incremental updates with `U`, `u`, `pu`, `E`, `T`)
  * `MarketEvent::FundingRateUpdate` (Perpetual mark price, index price, funding rate, settlement ts)
  * `MarketEvent::ConnectionState` (Lifecycle transitions)
* **Files/Modules:** `src/venues/binance.rs`, `src/types.rs`, `src/main.rs`, `tests/binance_feed_tests.rs`.
* **Dependencies:** `tokio` (rt, macros, sync), `tokio-tungstenite`, `native-tls`, `futures-util`, `rust_decimal`.
* **Hardening Improvements (M1.1):**
  1. *Futures Routed Endpoints:* Migrated from legacy `fstream.binance.com/ws` to dedicated `/public/ws` (depth) and `/market/ws` (mark price) base URLs.
  2. *True Event Semantics:* Futures `depthUpdate` emitted as `OrderBookDelta`, retaining `U`, `u`, `pu`, matching engine transaction timestamp `T`, and gateway event timestamp `E`.
  3. *Direct Strongly-Typed Deserialization:* Removed intermediate `serde_json::Value` parsing and cloning; payloads deserialize directly into typed structs.
  4. *Mark Price & Funding Ingestion:* Emits mark price, index price, funding rate, and next funding timestamp with microsecond-level local arrival tracking.
  5. *Per-Stream Freshness & Stale Detection:* Individual streams (`SpotDepth`, `FuturesDepth`, `FuturesMarkPrice`) tracked with cadence-aware staleness thresholds.
  6. *Granular Metrics:* Specific counters for stream message counts, ACKs, parse errors, validation errors, crossed books, stale events, and backpressure drops.
* **Acceptance Criteria:**
  * `cargo fmt --check`, `cargo check`, `cargo test`, `cargo clippy -- -D warnings` all exit 0.
  * Live smoke test verifies all 3 streams with sequence identifiers and funding fields observed.
  * Continuous soak test demonstrates 0 parse errors, 0 validation errors, 0 backpressure drops, and deterministic memory usage.
* **Explicitly NOT Included:** Bybit connector (M3), cross-venue comparison, VWAP calculation, fee deductions, arbitrage detection, execution.

---

## Phase 2 — Local Market-State Engine (M2) — *COMPLETE*
* **Goal:** Build the trustworthy, deterministic local market-state engine that maintains mathematically validated local limit order books for Binance Spot and USD-M Futures, implementing an explicit 6-state lifecycle machine, strict sequence continuity enforcement, duplicate/old update suppression, crossed-book rejection, and official Binance Futures snapshot + buffered delta alignment.
* **Inputs:** Canonical `MarketEvent` stream from M1/M1.1 (`OrderBookSnapshot`, `OrderBookDelta`, `FundingRateUpdate`, `ConnectionState`).
* **Outputs:**
  * Validated, uncrossed, monotonic `OrderBook` states.
  * Safe public query interface (`MarketStateManager::get_trusted_book`) returning books strictly when `Live`, `Valid`, uncrossed, and non-stale.
  * Rich diagnostic counters (`snapshots_applied`, `deltas_received`, `deltas_applied`, `duplicate_deltas`, `old_deltas`, `sequence_failures`, `invalidations`, `resync_attempts`, `resync_successes`, `crossed_books`, `stale_events`).
* **Files/Modules:**
  * `src/market/state.rs` (`BookLifecycleState`, `InvalidationReason`, `BookValidity`, `DeltaUpdate`, `MarketState`, `MarketStateMetrics`)
  * `src/market/manager.rs` (`MarketStateManager`, `InstrumentKey`, `LatestFundingState`)
  * `src/market/mod.rs` (re-exports)
  * `src/error.rs` (`SequenceGap`, `OutOfOrderUpdate`, `InvalidStateTransition`, `InvalidBookState`, `SnapshotSyncFailure`, `StaleMarketState`)
  * `tests/market_state_tests.rs` (20 deterministic unit and integration tests)
* **Lifecycle State Machine:**
  * `Empty` → `AwaitingSnapshot` → `Synchronizing` → `Live`
  * `Live` → (sequence gap / crossed book / disconnect) → `Invalidated`
  * `Invalidated` → `Resyncing` → `AwaitingSnapshot` → `Synchronizing` → `Live`
* **Binance USD-M Futures Protocol Implementation:**
  * Buffers incoming WebSocket deltas during `AwaitingSnapshot`.
  * Obtains REST depth snapshot with `lastUpdateId` $S$.
  * Discards obsolete buffered deltas where $u < S$.
  * Aligns first covering delta where $U \le S \le u$ (or $pu = S$ / $U = S+1$).
  * Applies subsequent buffered deltas enforcing $pu = \text{previous } u$.
  * Live updates enforce continuous $pu == \text{previous } u$, silently dropping duplicates ($u == \text{last } u$) and old updates ($u < \text{last } u$).
  * Invalidates immediately upon any sequence continuity breach.
* **Acceptance Criteria:**
  * 20 deterministic tests passing covering all 15+ edge scenarios.
  * Live 20-second test verifying real-time Spot snapshots and synchronized Futures deltas with 0 sequence errors and 0 crossed books.
  * Zero compiler warnings, 0 clippy warnings, clean formatting.
* **Explicitly NOT Included:** Bybit connector (M3), executable VWAP (M4), cost modeling (M5), dislocation signals (M6), live trading.

---

## Phase 2.1 — Local Market-State Engine Hardening (M2.1) — *COMPLETE*
* **Goal:** Harden the local market-state engine with explicit venue sequence policies, eliminate the covering update vulnerability in live state, support Bybit-compatible monotonic sequences without $+1$ restriction, implement synchronization epochs for clean restart snapshots ($u=1$), and enforce strict fail-closed trust gates.
* **Inputs:** Sequence policy specifications, empirical Bybit V5 matching engine sequence characteristics, Binance USD-M Futures continuity requirements.
* **Outputs:**
  * `SequencePolicy` enum (`SnapshotOnly`, `ContiguousPrevious`, `MonotonicStrict`) with venue/market defaults.
  * One-time covering update guard in `MarketState` (`awaiting_initial_covering`).
  * Synchronization epoch counter (`epoch: u64`) ensuring restart snapshots re-anchor cleanly.
  * 11 new deterministic tests (31 tests total in `market_state_tests.rs`).
* **Files/Modules:**
  * `src/market/state.rs` (`SequencePolicy`, `MarketState::with_policy`, hardened `apply_snapshot` and `apply_delta`, `epoch` tracking)
  * `src/market/manager.rs` (`register_instrument_with_policy`)
  * `src/market/mod.rs` (re-export `SequencePolicy`)
  * `tests/market_state_tests.rs` (11 new deterministic regression & unit tests)
  * `docs/ARCHITECTURE.md` (Section 5.4)
* **Acceptance Criteria:**
  * All 11 new tests passing, all 44 existing tests passing (55 total).
  * `cargo check`, `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` all exit 0.
* **Explicitly NOT Included:** Bybit network connector (M3), Bybit WebSockets, authentication, private APIs, trading.

---

## Phase 3 — Bybit Market Data (M3) — *NOT STARTED*
* **Goal:** Implement an unauthenticated public WebSocket client for Bybit V5 Spot and Linear Futures, handling connection management, 20s ping/pong heartbeats, and subscribing to `orderbook.50` and `tickers` topics.
* **Inputs:** Public Bybit WebSocket streams (`wss://stream.bybit.com/v5/public/spot`, `/linear`).
* **Outputs:** Normalized stream of `MarketEvent::OrderBookSnapshot`, `MarketEvent::OrderBookDelta`, and `MarketEvent::FundingRateUpdate`.
* **Files/Modules:** `src/venues/bybit.rs`, `tests/bybit_feed_tests.rs`.
* **Dependencies:** Reuses network stack from M1.
* **Tests:** Snapshot initialisation verification, delta application parsing, heartbeat ping/pong timer verification.
* **Benchmarks:** Frame normalization throughput benchmark.
* **Acceptance Criteria:** Sustained live stream of BTCUSDT Spot and Linear with zero unhandled frame drops.
* **Failure Criteria:** Heartbeat timeout disconnects, sequence gaps unflagged.
* **Explicitly NOT Included:** Cross-venue comparison, order execution, private endpoints.

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
