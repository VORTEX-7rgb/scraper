# Milestone M5: Live Cross-Book Dislocation Observatory & Persistence Tracking

## 1. Overview & Objective

Milestone M5 builds an auditable, deterministic market dislocation research observatory directly on top of the validated M4 execution pricing engine and M3.6 multi-venue market data foundation.

It answers two empirical research questions:
1. **At size:** *"Given two currently trusted and sufficiently fresh order books, at a specified reference quantity, what executable cross-book price dislocation exists after subtracting configured trading fees and friction costs?"*
2. **Over time:** *"How long did that executable net edge continuously persist before market depth shifted, book trust broke, or the edge decayed below threshold?"*

M5 strictly operates as an **observation and measurement layer**. It contains **no live trading, no order routing, no private exchange API keys, and makes no claims of realized execution profitability**.

---

## 2. Architecture & Data Flow

M5 is structured as a dedicated, modular domain inside `src/observatory/`:

```text
                               Raw WebSocket Feeds
                                       │
                                       ▼
                             [MarketStateManager]
                                       │
                         Is get_trusted_book() Some?
                                      / \
                                    YES  NO ──► Reject (UntrustedBook / Stale / Crossed)
                                    /
                        [Trusted OrderBook (Depth)]
                                    │
                         [M4 Depth Walker & VWAP]
                                    │
                        [M4 CrossBookComparison]
                                    │
                        [M5 ObservationEngine]
                                    │
    ┌───────────────────────────────┴──────────────────────────────┐
    ▼                               ▼                              ▼
Freshness Gate              Reference Quantity Sweep      Instrument Classification
(Age & Timestamp Skew)     (e.g. 0.001 to 0.10 BTC)       (SpotSpot / PerpPerp / Basis)
    │                               │                              │
    └───────────────────────────────┬──────────────────────────────┘
                                    │
                        [DislocationObservation]
                                    │
                                    ▼
                         [OpportunityTracker]
                                    │
                       Temporal State Transition
                       (Started / Continued / Ended)
                                    │
                         [OpportunityRecord]
                   (Duration, Peak BPS, Samples, Reason)
```

---

## 3. Core Modules

### 3.1 `src/observatory/types.rs`
* [`MarketRelationship`](file:///c:/projects/airbitrage/src/observatory/types.rs): Distinguishes direct executable dislocations (`SpotSpot`, `PerpPerp`) from cross-instrument basis (`CrossInstrumentBasis`, e.g. Spot vs Perpetual). Spot-perp relationships are explicitly tagged and never conflated with direct same-instrument arbitrage.
* [`ObservationRejectionReason`](file:///c:/projects/airbitrage/src/observatory/types.rs): Strongly typed enum tracking why an observation was rejected (e.g. `SelfComparison`, `UntrustedBook`, `StaleBook`, `CrossedBook`, `TimestampSkewExceeded`, `EmptyBook`, `MissingBook`).
* [`DislocationObservation`](file:///c:/projects/airbitrage/src/observatory/types.rs): Canonical immutable snapshot capturing identity, timing/skew, requested quantity, consumed visible depth, VWAPs, worst fills, fees, gross/net edge in USD and basis points, and quality assessments.
* [`OpportunityKey`](file:///c:/projects/airbitrage/src/observatory/types.rs): Unique identifier for a continuous opportunity stream: `(buy_venue, buy_market, sell_venue, sell_market, symbol, reference_quantity, market_relationship)`.
* [`OpportunityEndReason`](file:///c:/projects/airbitrage/src/observatory/types.rs): Explicit taxonomy of why an opportunity ended (`NetEdgeBelowThreshold`, `UntrustedBook`, `StaleBook`, `InvalidBook`, `TimestampSkewExceeded`, `InsufficientLiquidity`, `FeedDisconnected`, `MissingBook`).
* [`OpportunityRecord`](file:///c:/projects/airbitrage/src/observatory/types.rs): Historical record of a terminated opportunity, capturing duration in milliseconds, sample count, first/last/peak/min/average net edge in basis points, and maximum executable depth observed.

### 3.2 `src/observatory/observer.rs`
* [`ObservationConfig`](file:///c:/projects/airbitrage/src/observatory/observer.rs): Configuration specifying reference quantities (e.g. 0.001, 0.005, 0.01, 0.025, 0.05, 0.10 BTC), `max_book_age_ms` (default 1,000 ms), `max_timestamp_skew_ms` (default 2,000 ms), and `other_cost_rate`.
* [`ObservationEngine`](file:///c:/projects/airbitrage/src/observatory/observer.rs): Evaluates pairs of instruments from [`MarketStateManager`](file:///c:/projects/airbitrage/src/market/manager.rs). Enforces the trust gate, timestamp skew checks, staleness bounds, and executes M4 pricing without duplicating calculation logic. Supports sweeping single pairs or all permutations of active venue endpoints.

### 3.3 `src/observatory/persistence.rs`
* [`OpportunityTracker`](file:///c:/projects/airbitrage/src/observatory/persistence.rs): In-memory temporal opportunity tracker.
  - **Activation (`INACTIVE -> ACTIVE`):** Triggers when an observation is trusted, fresh, fully executable, and `net_edge_bps >= min_net_edge_bps`.
  - **Continuation (`ACTIVE -> ACTIVE`):** Maintains in-flight metrics across sequential observations, accumulating sample count, duration, running average, and peak/min edge bounds.
  - **Termination (`ACTIVE -> ENDED`):** Triggers immediately when any precondition fails (liquidity drops, book becomes untrusted or stale, feed drops, or edge decays below threshold). Emits a finalized [`OpportunityRecord`](file:///c:/projects/airbitrage/src/observatory/types.rs).

---

## 4. Key Microstructure Invariants

1. **Exact Visible Depth Only:** Fictitious liquidity is never invented. If a 1.0 BTC request is submitted to a book with only 0.4 BTC visible depth, `fully_executable` is strictly `false`, `common_executable_quantity = 0.4`, and the opportunity cannot trigger an active persistence state.
2. **Negative Edges Are Preserved:** Negative net edge observations are not discarded or filtered out. They represent critical empirical baseline data proving that apparent gross spreads are consumed by fees and market impact.
3. **No Cross-Instrument Conflation:** Spot ↔ Perpetual comparisons are tagged `CrossInstrumentBasis`. Perpetual funding obligations, basis convergence risk, and margin costs differ fundamentally from spot arbitrage and are kept economically isolated.
4. **Authoritative Trust Gating:** If [`MarketStateManager::get_trusted_book`](file:///c:/projects/airbitrage/src/market/manager.rs#L240-L249) returns `None` for either side (due to sequence gaps, disconnects, resyncing, or crossed books), no executable pricing is performed.

---

## 5. Non-Goals & Limitations

M5 does NOT model:
* **Execution Latency:** Order transit and matching engine latency are not simulated.
* **Queue Priority:** Limit orders resting at levels are assumed immediately available to market takers.
* **Adverse Selection:** Market impact after taking visible depth is not simulated.
* **Capital & Funding:** Perpetual funding rate transfer costs and borrow interest are not included in instantaneous spread calculations.
* **Trading / Execution:** There are no order execution routes, no API keys, and no automated actions.
