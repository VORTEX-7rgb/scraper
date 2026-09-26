# Airbitrage — Assumptions & Verification Register

This register documents every core assumption, design decision, hypothesis, and benchmark target underpinning the Airbitrage architecture. No hypothesis is treated as an architectural fact until verified through empirical measurement.

---

## 1. Verified Facts (FACT)

* **FACT-01: Public WebSocket Endpoints & Routing Architecture.**
  * Binance Spot: `wss://stream.binance.com/ws` (TLS port 443). Port 9443 is blocked on certain residential ISPs/firewalls.
  * Binance USD-M Futures (2026 Routed Architecture):
    * Public Depth Route: `wss://fstream.binance.com/public/ws` (`<symbol>@depth20@100ms`).
    * Market Data Route: `wss://fstream.binance.com/market/ws` (`<symbol>@markPrice@1s`).
    * Legacy unrouted endpoint `wss://fstream.binance.com/ws` is retired. Subscribing to `@markPrice` on `/public` or unrouted URLs fails silently.
  * Bybit Spot: `wss://stream.bybit.com/v5/public/spot`.
  * Bybit Linear Futures: `wss://stream.bybit.com/v5/public/linear`.
  * All listed streams allow unauthenticated, read-only WebSocket connections without API credentials or KYC.
  * *Source:* Official Binance Spot & USD-M Futures Documentation (Verified September 2026); Bybit V5 Public WebSocket Overview.
* **FACT-02: Binance Spot REST Weight Scaling.** Calling `GET /api/v3/depth` consumes IP request weight scaling non-linearly with limit: limit 1–100 consumes 5 weight points; limit 501–1000 consumes 50 weight points. Rapid REST snapshot requests during reconnect loops can trigger HTTP 429 rate limit bans.
  * *Source:* Binance Official Spot API Documentation.
* **FACT-03: Order Book Event Semantics & Sequencing.**
  * Binance Spot partial depth (`@depth20@100ms`) provides an explicit snapshot of top 20 levels (`lastUpdateId`, `bids`, `asks`). Mapped to `MarketEvent::OrderBookSnapshot`.
  * Binance USD-M Futures depth (`@depth20@100ms`) provides an incremental delta update (`depthUpdate`) with sequence IDs `U` (first update ID), `u` (final update ID), and `pu` (previous update ID), alongside transaction timestamp `T` and gateway event timestamp `E`. Mapped to `MarketEvent::OrderBookDelta`. Continuity requires `current.pu == previous.u` and `U <= u`.
  * *Source:* Binance Developers Derivatives WebSocket Documentation.
* **FACT-04: Bybit V5 Initial Snapshot.** Subscribing to Bybit V5 orderbook topics (`orderbook.50.<symbol>`) automatically sends an initial message with `"type": "snapshot"`. A separate REST orderbook request is not required for WebSocket initialization.
  * *Source:* Bybit V5 Public WebSocket Orderbook Specification.
* **FACT-05: Baseline Retail Fee Schedules.** Non-VIP retail taker fees are:
  * Binance Spot: 0.1000% (10.0 bps), reduced to 0.0750% with BNB deduction.
  * Binance USD-M Futures: 0.0500% (5.0 bps), reduced to 0.0450% with BNB deduction.
  * Bybit Spot: 0.1000% (10.0 bps).
  * Bybit Linear Futures: 0.0550% (5.5 bps).
  * *Source:* Official fee schedules of Binance and Bybit (verified September 2026).
* **FACT-06: Local Environment Toolchain.** The host machine runs Windows with `rustc 1.98.1` and `cargo 1.98.1` under toolchain `stable-x86_64-pc-windows-msvc`.
  * *Source:* Local host command verification (`rustc --version`, `cargo --version`).
* **FACT-07: Indian Taxation Asymmetry.** Under Income Tax Act Section 115BBH, Virtual Digital Assets (VDAs) are taxed at a flat 30% without loss setoff across transactions or legs. Section 194S requires 1% TDS on turnover.
  * *Source:* Government of India Finance Act; Income Tax Department Circulars.
* **FACT-08: Binance WebSocket Transport Port.** Connecting to Binance Spot via port 9443 (`stream.binance.com:9443`) fails on certain residential ISPs/firewalls. Standard TLS port 443 (`wss://stream.binance.com/ws`) is fully accessible and verified.
  * *Source:* Direct network socket probe and successful live WebSocket handshake.
* **FACT-09: Empirical Stream Throughput & Ingestion Latencies (Measured M1.1 60s Soak).**
  * Subscribing to BTCUSDT Spot `depth20@100ms`, USD-M Futures depth `depth20@100ms` (`/public`), and Mark Price `markPrice@1s` (`/market`):
    * Duration: 60.00 seconds.
    * Total stream messages: 1,126 (590 Spot depth, 477 Futures depth, 59 Mark Price).
    * Total throughput: ~18.8 msgs/sec (Spot: 9.8 msgs/sec, Futures depth: 7.9 msgs/sec, Mark Price: 1.0 msgs/sec).
    * Errors: 0 parse errors, 0 validation errors, 0 backpressure drops, 0 disconnects, 0 crossed books.
    * Processing latency: p50: 8 µs, p95: 39 µs, p99: 46 µs, average: 17 µs.
    * Memory working set: 7.27 MB initial, 14.93 MB ending, 15.12 MB peak working set.
  * *Source:* Live 60-second continuous soak test execution (`--binance-soak 60`).

---

## 2. Explicit Design Decisions (DESIGN DECISION)

* **DEC-01: Research-Only Scope for V1.** Live execution, order routing, private API keys, and balance tracking are explicitly excluded until data quality and statistical profitability are proven.
* **DEC-02: Numeric Representation.** `rust_decimal::Decimal` is adopted for all price levels, quantities, VWAP values, and fee models in V1 to prevent floating-point rounding errors and non-deterministic crossed-book states.
* **DEC-03: Threading Model & Hot-Path Isolation.** Network I/O is managed by Tokio asynchronous worker tasks. Incoming frames are parsed and dispatched via bounded cross-thread channels to a single-threaded order book core with exclusive mutable ownership, eliminating lock contention.
* **DEC-04: Contiguous Vector Order Book.** For top-N depth ($N \le 50$), order books are backed by pre-allocated contiguous vectors (`Vec<PriceLevel>`) rather than `BTreeMap`, leveraging cache locality for VWAP depth iteration.
* **DEC-05: Multi-Tiered Storage Separation.** Raw tick streams are logged to append-only Zstandard-compressed files for deterministic replay. Detected opportunities ($\text{Net Edge} > 0$) are recorded in a local SQLite database. High-throughput time-series databases (ClickHouse) are deferred.
* **DEC-06: Message-Driven Enum Venue Abstraction.** Venue adapters emit standardized `MarketEvent` enum variants over channels rather than dynamic trait objects (`Box<dyn VenueAdapter>`), avoiding heap allocation and runtime vtable indirection.
* **DEC-07: Monotonic Clock for Latency Tracking.** Monotonic `std::time::Instant` is used exclusively for internal latency delta calculations ($\Delta t = t_1 - t_0$). Wall-clock timestamps (Unix epoch nanoseconds) are recorded separately for logging and correlation.
* **DEC-08: Cadence-Aware Stream Freshness.** Stream staleness thresholds are calibrated to nominal publication intervals: 500ms for 100ms depth streams, 3,000ms for 1,000ms mark price streams.

---

## 3. Hypotheses to be Empirically Tested (HYPOTHESIS)

* **HYP-01: Slower Dislocation Existence.** Persistent market dislocations with half-lives $\ge 1.0\text{ second}$ occur between Binance and Bybit on perpetual contracts or mid-cap instruments during volatility regimes.
* **HYP-02: Partial Depth Adequacy.** Binance partial depth (`@depth20@100ms`) provides sufficient cumulative liquidity to satisfy $\$100$ to $\$1,000$ notional VWAP test sizes without requiring full diff depth synchronization.
* **HYP-03: Perp-Perp Funding Rate Spread.** Divergence in perpetual funding rates across Binance and Bybit persists across 8-hour settlement intervals with sufficient margin to absorb 4-leg taker fee hurdles.
* **HYP-04: Execution Feasibility.** Market dislocations that survive $> 2.0\text{ seconds}$ have a higher simulated fill probability and lower adverse selection risk than sub-100ms dislocations.

---

## 4. Benchmark Targets (BENCHMARK TARGET)

*These are engineering targets to be verified through criterion benchmarks in later milestones; they are NOT claims of current performance.*

* **TGT-01:** WebSocket frame JSON parsing and normalization into `MarketEvent`: $< 25\,\mu\text{s}$ per message (Measured: p50 8 µs, p99 46 µs).
* **TGT-02:** Local order book mutation (in-place top-20 update): $< 5\,\mu\text{s}$.
* **TGT-03:** Executable VWAP calculation across 20 depth levels: $< 5\,\mu\text{s}$.
* **TGT-04:** Tick-to-detection latency (socket receipt to dislocation evaluation): $< 50\,\mu\text{s}$.
* **TGT-05:** Zero heap allocations inside the hot-path order book update loop once initialized.

---

## 5. Working Estimates (ESTIMATE)

* **EST-01: Message Volume.** 4 market data streams (BTC Spot and Perp on Binance + Bybit) will generate approximately 40 to 60 messages per second during normal trading, peaking at 150 messages/sec during high volatility.
* **EST-02: Raw Storage Footprint.** Average uncompressed JSON payload is estimated at 1,000 to 1,200 bytes per update, resulting in roughly $3.5\text{ to }5.2\text{ GB/day}$ of uncompressed raw text for 4 streams.
* **EST-03: Compression Ratio.** Zstandard (level 3) compression on structured, repetitive JSON order book updates is estimated to achieve a $6\times\text{ to }8\times$ reduction, yielding approximately $500\text{ to }700\text{ MB/day}$ per 4 active streams.

---

## 6. Unverified Areas (UNVERIFIED)

* **UNV-01: Exchange Matching Engine Queue Priority.** The true queue position of limit orders placed during a dislocation cannot be verified from public market data; paper trading fill probability must rely on conservative assumptions.
* **UNV-02: ISP Cross-Border Jitter.** Latency consistency from Indian broadband networks to Tokyo AWS (`ap-northeast-1`) under peak domestic network congestion requires continuous measurement over a minimum 14-day observation window.
