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
├── main.rs         # Application entrypoint, CLI, smoke, soak, and live state verification
├── config.rs       # Strongly-typed configuration schema (config.toml)
├── error.rs        # Explicit, actionable error taxonomy
├── types.rs        # Canonical domain models (VenueId, PriceLevel, OrderBook, MarketEvent)
├── market/         # Local Market-State Engine (M2)
│   ├── mod.rs      # Market module interface and re-exports
│   ├── orderbook.rs# Order book representation, updates, invariants, and sorting logic
│   ├── state.rs    # Book lifecycle state machine, sequence continuity, snapshot/delta alignment
│   └── manager.rs  # Multi-book state manager, event router, and safe query interface
└── venues/         # External exchange adapters
    ├── mod.rs      # Adapter traits and shared connection management
    ├── binance.rs  # Binance 2026 routed schemas, WebSocket client, metrics, and freshness
    └── bybit.rs    # Bybit-specific payload schemas and WebSocket client (M3)
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

---

## 5. Local Market-State Engine Architecture (M2)

The Local Market-State Engine (`src/market/state.rs`, `src/market/manager.rs`) is the deterministic, venue-agnostic foundation that consumes canonical `MarketEvent`s, reconstructs order books, enforces sequence continuity, suppresses duplicates and stale updates, detects crossed books, and exposes trusted market states for downstream pricing and dislocation engines.

### 5.1 Architecture & Flow

```text
                  MarketEvent
                      │
                      ▼
             MarketStateManager
                      │
          ┌───────────┴───────────┐
          │                       │
          ▼                       ▼
   Binance Spot Book       Binance Futures Book
   (Snapshot-Driven)       (Delta-Driven)
          │                       │
          ▼                       ▼
    BookValidity            SequenceValidity
   (Crossed/Stale)         (pu == previous.u)
          │                       │
          └───────────┬───────────┘
                      │
                      ▼
             Trusted Market State
         (Only if Live, Valid, Non-Stale)
```

### 5.2 Explicit 6-State Lifecycle Machine

```text
      ┌──────────┐
      │  Empty   │
      └────┬─────┘
           │ register
           ▼
┌──────────────────────┐
│   AwaitingSnapshot   │◄─────────────────────────┐
└──────────┬───────────┘                          │
           │ snapshot arrives                     │
           ▼                                      │
┌──────────────────────┐                          │
│    Synchronizing     │                          │
└──────────┬───────────┘                          │
           │ deltas aligned                       │
           ▼                                      │ resync cycle
┌──────────────────────┐                          │
│         Live         │                          │
└──────────┬───────────┘                          │
           │ sequence gap / crossed book / drop   │
           ▼                                      │
┌──────────────────────┐                          │
│     Invalidated      │                          │
└──────────┬───────────┘                          │
           │ request_resync                       │
           ▼                                      │
┌──────────────────────┐                          │
│      Resyncing       ├──────────────────────────┘
└──────────────────────┘
```

* **Trust Invariant:** Downstream consumers calling `MarketStateManager::get_trusted_book` only receive `Some(&OrderBook)` when:
  1. Lifecycle state is strictly `BookLifecycleState::Live`.
  2. Validity is strictly `BookValidity::Valid`.
  3. Internal order book is uncrossed (`best_bid < best_ask`).
  4. Local receive age is within the configured freshness threshold ($\Delta t \le \tau_{\text{staleness}}$).

### 5.3 Binance USD-M Futures Synchronization Protocol

1. **Delta Buffering:** During `AwaitingSnapshot` and `Synchronizing`, incoming WebSocket depth updates (`depthUpdate`) are queued in a FIFO buffer (`delta_buffer`).
2. **Snapshot Acquisition:** REST depth snapshot is fetched from `https://fapi.binance.com/fapi/v1/depth?symbol=<symbol>&limit=50`, providing baseline levels and snapshot sequence ID $S = \text{lastUpdateId}$.
3. **Obsolete Drop:** All buffered events with final update ID $u < S$ are discarded.
4. **Initial Covering Alignment:** The first valid event is identified where $(U \le S \le u)$ or $(pu = S \lor U = S + 1)$.
5. **Sequential Drainage:** Subsequent buffered deltas are applied in order, validating continuity: $pu == \text{current } u$.
6. **Live Continuity:** For subsequent live updates:
   - Duplicates ($u == \text{last } u$) and stale updates ($u < \text{last } u$) are discarded without mutating the book.
   - Stream continuity ($pu == \text{last } u$) is strictly checked. Any gap transitions the book to `Invalidated` with `InvalidationReason::SequenceGap`.

### 5.4 Sequence Policy Model & Protocol Hardening (M2.1)

To support heterogeneous exchange architectures (Binance, Bybit) without fragile ad-hoc heuristics, the market-state engine delegates sequence validation to a typed `SequencePolicy`:

```rust
pub enum SequencePolicy {
    SnapshotOnly,       // Binance Spot depth20
    ContiguousPrevious, // Binance USD-M Futures (pu == last_u)
    MonotonicStrict,    // Bybit Spot & Linear (new_u > last_u)
}
```

1. **Why Binance Futures Uses `pu` Continuity:**
   Binance Futures orderbook updates represent match engine sequence IDs which advance by transaction ranges ($u - U + 1 \ge 1$). Because depth updates are throttled (e.g. 100ms or 250ms), $u$ naturally jumps ($u > \text{previous\_u} + 1$). Requiring $+1$ would falsely flag healthy stream updates as sequence gaps. Strict continuity is uniquely and deterministically guaranteed by validating that incoming `pu == previous_accepted_u`.

2. **Why Bybit-Compatible Semantics Use Strict Monotonicity (`new_u > previous_u`):**
   Bybit V5 orderbook feeds (e.g. `orderbook.50`) emit sequence numbers generated by the 1000-level core matching engine. Updates to price levels outside the top 50 increment the engine's update ID $u$ without emitting a message on the `orderbook.50` topic. Consequently, consecutive updates on the top 50 stream have gaps in $u$ (e.g. $100 \to 105 \to 120$). A strict $+1$ continuity check would immediately and falsely break the feed. However, duplicate updates ($u == \text{previous\_u}$) and decreasing updates ($u < \text{previous\_u}$) indicate lost ordering or unaligned retransmissions and must fail closed immediately.

3. **Why the Initial Covering Exception is Strictly One-Time:**
   During the transition from `AwaitingSnapshot` $\to$ `Synchronizing` $\to$ `Live`, the first delta update must bridge the snapshot baseline ($U \le S \le u$). Once the state engine reaches `Live`, this covering rule is permanently deactivated via `awaiting_initial_covering = false`. If evaluated repeatedly during `Live`, any subsequent delta whose range happens to span `last_u` would bypass the mandatory `pu == last_u` check, opening a severe correctness vulnerability where out-of-order deltas corrupt book state.

4. **Why a Resnapshot Resets the Synchronization Epoch:**
   Following invalidation or feed recovery, exchanges (such as Bybit upon reconnect or engine restart) may reset sequence IDs to a known baseline (e.g. $u = 1$). If sequence comparisons were global, the engine would reject authoritative restart snapshots as "old" ($1 < 105$). By tracking `epoch: u64` and clearing `last_update_sequence` upon entering `Resyncing`/`AwaitingSnapshot`, the engine cleanly anchors to the new epoch without weakening live-state monotonicity.

5. **Fail-Closed Guarantee:**
   If sequence continuity cannot be proven beyond doubt, the engine transitions immediately to `Invalidated`. `MarketStateManager::get_trusted_book` immediately returns `None`, shielding downstream pricing, spread detectors, and trading models from corrupt book state.
