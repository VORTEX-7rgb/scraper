# Airbitrage Architecture Specification

## 1. System Overview

Airbitrage is structured as a pipeline that ingests heterogeneous, public, real-time market data, reconstructs normalized local limit order books, computes executable Volume-Weighted Average Prices (VWAP) for discrete capital sizes, and evaluates whether net-positive market dislocations exist after deducting all realistic market frictions.

```text
                 AIRBITRAGE CONCEPTUAL FLOW

        ┌──────────────────────────────────────────────┐
        │     Public Market Data Ingestion             │
        │   • Binance Spot WS (stream.binance.com)     │
        │   • Binance Futures /public (fstream depth)  │
        │   • Binance Futures /market (mark & funding) │
        │   • Bybit Spot & Linear WS (M2)              │
        └──────────────────────┬───────────────────────┘
                               ↓
                        Venue Adapters
                     (Strongly-Typed)
                               ↓
                        Normalization
                (Canonical MarketEvent Stream)
              [Snapshot / Delta / Funding / State]
                               ↓
                        Bounded Channel
                               ↓
                      Local Order Books
                    (Single-Threaded Core)
                               ↓
                        VWAP / Pricing
                    (Top 20/50 Depth Walk)
                               ↓
                        Cost Modeling
                  (Fees, Slippage, Latency)
                               ↓
                    Dislocation Detection
                 (Net Edge > Threshold Signal)
                               ↓
                    Opportunity Recording
                      (SQLite Database)
                               ↓
                       Replay / Research
                     (Zstd Raw Tick Files)
                               ↓
                       [Paper Execution]
                               ↓
                             [Risk]
                               ↓
                  [LIVE EXECUTION — FUTURE]
```

---

## 2. Latency & Execution Paths

The architecture strictly segregates computational tasks into three distinct performance domains to protect execution predictability:

### 2.1 Hot Path (Single-Threaded Engine Core)
* **Scope:** 
  1. Socket frame byte-slice parsing into canonical `MarketEvent`.
  2. In-place mutation of pre-allocated local order books (`Vec<PriceLevel>`).
  3. Depth-walking VWAP calculation for $\$100, \$500, \$1,000$ notional tiers.
  4. Execution of the Net Expected Edge equation.
  5. Generating a `DislocationEvent` when net edge exceeds the minimum configurable threshold.
* **Constraints:** 
  * Zero heap allocations inside the steady-state loop.
  * Zero mutex acquisitions or lock contention (single thread owns the local books).
  * No file I/O, no network calls, no formatting strings, and no dynamic heap resizing.

### 2.2 Warm Path (Asynchronous Auxiliary Tasks)
* **Scope:**
  * Bounded MPSC channel event forwarding to persistence buffers.
  * Periodic Zstandard chunk compression and disk flushing (every 1–5 seconds).
  * 8-hour funding rate schedule tracking and basis divergence calculations.
  * Metrics aggregation (throughput, queue depth, half-life persistence checks, cadence-aware freshness tracking).
* **Constraints:** Runs on non-engine Tokio worker threads. Does not block order book mutation.

### 2.3 Cold Path (Setup, Failure Recovery, Diagnostic)
* **Scope:**
  * Application startup, configuration loading (`config.toml`), and self-checks.
  * TCP connection establishment and WebSocket handshakes across dedicated routes (`/public` for depth, `/market` for mark price).
  * Reconnection with exponential backoff and jitter upon network drop.
  * SQLite schema migration and persistence for detected dislocation records.
  * Terminal UI / structured log formatting (`tracing-subscriber`).

---

## 3. Module Boundaries & Isolation

```text
src/
├── main.rs         # Application entrypoint, CLI, smoke & soak verification
├── config.rs       # Strongly-typed configuration schema (config.toml)
├── error.rs        # Explicit, actionable error taxonomy
├── types.rs        # Canonical domain models (VenueId, PriceLevel, OrderBook, MarketEvent)
├── market/         # Domain engine
│   ├── mod.rs      # Market module interface
│   └── orderbook.rs# Order book representation, updates, invariants, and VWAP logic
└── venues/         # External exchange adapters
    ├── mod.rs      # Adapter traits and shared connection management
    ├── binance.rs  # Binance 2026 routed schemas, WebSocket client, metrics, and freshness
    └── bybit.rs    # Bybit-specific payload schemas and WebSocket client (M2)
```

### Module Boundary Invariants
1. **Exchange-Specific Leakage:** Neither `src/market/` nor `src/types.rs` may import or reference Binance-specific or Bybit-specific payload structs. All exchange payloads are parsed and normalized inside `src/venues/`.
2. **Channel Decoupling:** Ingestion tasks run asynchronously inside Tokio tasks and communicate with the engine exclusively by sending `MarketEvent` enum variants over bounded channels.
3. **Event Semantics Integrity:** Adapters must emit `OrderBookSnapshot` only when receiving full book states (e.g. Spot `depth20`), and must emit `OrderBookDelta` when receiving incremental updates (e.g. Futures `depthUpdate` with `U`, `u`, `pu`), preserving all sequence and timestamp fields.
4. **Storage Boundary:** Replay streams write raw bytes to compressed files (`.zst`). Analytical queries run offline via DuckDB or Polars. The runtime engine does not execute ad-hoc SQL queries during market processing.

---

## 4. Replay & Future Execution Boundaries

```text
[ Live Market Ingestion ] ──────┐
                                │
                                ▼
                       [ MarketEvent Channel ]
                                ▲
                                │
[ Replay Stream (Zstd Files) ] ─┘
                                │
                                ▼
                    [ Canonical Order Book Core ]
                                │
                                ▼
                    [ Dislocation & VWAP Engine ]
                                │
                                ▼
                    [ Paper Execution Simulator ]
                                │
                                ▼
                    [ Execution State Machine ]
                                │
                                ▼
                 [ Live Venue Router (FUTURE ONLY) ]
```

* **Deterministic Replay Guarantee:** Because the order book core consumes `MarketEvent` streams without querying system clocks for internal logic, a historical recording played back through the channel produces the exact same sequence of order book states and dislocation calculations as live streaming.
* **Execution Boundary:** The execution engine (simulated in V1, live in future phases) sits downstream of the Dislocation Detector and Risk Engine. It receives `ExecutionIntent` and cannot bypass risk checks or order book validation rules.
