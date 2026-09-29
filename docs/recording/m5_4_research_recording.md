# M5.4 — Dislocation Event Serialization & High-Fidelity Research Recording

## 1. Motivation & Context

Airbitrage is designed to answer an empirical question:
> *Can observed cross-book market dislocations survive real-world execution frictions, fees, order book depth consumption, and latency decay?*

In M5.1–M5.3, the system introduced an in-memory dislocation observatory, reference-quantity sweeps, and an opportunity persistence tracker. However, in-memory observations alone cannot be audited, verified after the fact, or re-evaluated under alternative parameter assumptions.

**Milestone M5.4** establishes the canonical research recording infrastructure. It converts live in-memory states and observations into append-only, deterministic, schema-versioned research datasets on disk. This foundation enables:
1. Historical dislocation analysis without relying on live market access.
2. Independent offline auditability of every detected edge.
3. The future **M5.5 Deterministic Replay Engine**, which can replay identical raw market inputs through the pricing engine to re-verify or re-tune observations under varying fee schedules or thresholds.

---

## 2. Core Architectural Separation: Observed vs. Derived vs. Configured

To prevent research artifacts from conflating primary reality with analytical models, M5.4 strictly maintains three categories of data:

```
+-------------------------------------------------------------------------+
|                              OBSERVED DATA                              |
|  Direct feed payloads: exchange timestamps, order book snapshots/deltas |
|  (e.g., MarketEvent::OrderBookSnapshot, MarketEvent::OrderBookDelta)    |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                         CONFIGURED ASSUMPTIONS                          |
|  Fee schedules (taker bps), max staleness, max timestamp skew,          |
|  reference quantity sweep tiers (e.g. 0.01, 0.1, 0.5, 1.0 BTC)          |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                          DERIVED OBSERVATIONS                           |
|  Calculated metrics: VWAP, depth consumption, fees, net edge,           |
|  persistence transitions (START, CONTINUE, END), opportunity records   |
|  (e.g., DislocationObservation, OpportunityRecord)                     |
+-------------------------------------------------------------------------+
```

### The Dual Stream Design
- **Raw Events (`MarketEvent`):** Recorded so that an offline engine (M5.5) can reconstruct order books from scratch and test alternative analytical hypotheses.
- **Derived Events (`DislocationObservation`, `PersistenceTransition`, `OpportunityRecord`):** Recorded so that researchers can immediately evaluate observed dislocation frequency, edge size distributions, and duration without executing full book reconstruction.

---

## 3. Canonical Event Model & Schema Versioning

### Schema Version
All serialized records include an explicit top-level `schema_version` (integer).
- The current canonical schema version is **`1`**.
- Any parser encountering a schema version other than `1` rejects the record with `AirbitrageError::UnsupportedSchemaVersion`. It **never** silently attempts partial deserialization or best-effort field guessing.

### ResearchEvent Envelope
Every serialized line is an independent `ResearchEvent` envelope:
```json
{
  "schema_version": 1,
  "event_type": "dislocation_observation",
  "sequence_number": 42,
  "recorded_at_epoch_ms": 1727600000000,
  "payload": { ... }
}
```

#### Envelope Fields
- `schema_version`: `u32` (Must equal 1).
- `event_type`: `ResearchEventType` enum:
  - `"market_event"`
  - `"dislocation_observation"`
  - `"persistence_transition"`
  - `"opportunity_record"`
- `sequence_number`: Monotonically increasing `u64` per recording stream, providing deterministic sequence verification and gap detection.
- `recorded_at_epoch_ms`: Local machine wall-clock epoch timestamp at serialization time.
- `payload`: Tagged polymorphic enum `ResearchPayload` containing the specific domain event.

### Payloads

1. **`market_event`**:
   Wraps `MarketEvent`, preserving exchange sequence numbers (`first_update_id`, `last_update_id`, `prev_update_id`), timestamps (`event_time`, `transaction_time`), and price/size delta arrays.

2. **`dislocation_observation`**:
   Wraps `DislocationObservation`, preserving:
   - Observation identity: `observation_time`, `pair` (`buy_venue`, `buy_market`, `sell_venue`, `sell_market`, `symbol`).
   - Quantities: `reference_quantity`, `buy_executable_qty`, `sell_executable_qty`, `executable_quantity`, `is_fully_executable`.
   - Prices & Metrics: `buy_vwap`, `sell_vwap`, `buy_worst_price`, `sell_worst_price`, `gross_spread`, `gross_spread_bps`, `gross_edge`, `net_edge`, `net_edge_bps`.
   - Fees: `buy_fees`, `sell_fees`, `total_fees`.
   - Microstructure Quality: `buy_book_age_ms`, `sell_book_age_ms`, `timestamp_skew_ms`, `trust_status`, `rejection_reason`.

3. **`persistence_transition`**:
   Wraps `PersistenceTransitionEvent`, containing the canonical `OpportunityKey`, the `PersistenceTransition` (`Start`, `Continue`, or `End`), and the associated `DislocationObservation` or termination context.

4. **`opportunity_record`**:
   Wraps `OpportunityRecord`, preserving the complete opportunity lifecycle summary: start/end timestamps, duration ms, tick count, peak net edge, mean net edge, and exact `OpportunityEndReason`.

---

## 4. Storage Format & Serialization Rules

- **Format:** Newline-Delimited JSON (NDJSON / JSON Lines).
- **Exact Numeric Representation:** All monetary quantities, prices, fees, and edges are encoded via `rust_decimal::Decimal` as exact decimal strings or canonical JSON numbers via serde. **Zero floating-point numbers** are permitted in the financial pathway.
- **Record Independence:** Every line is a self-contained, valid JSON document. No cross-line multi-token structures are used.
- **Append-Only:** Files are opened with standard append flags (`write(true).create(true).append(true)`).

---

## 5. Failure & Corruption Semantics

Research data integrity is non-negotiable. Silent data correction is prohibited.

1. **Malformed JSON:** Encountering an invalid JSON string returns `AirbitrageError::CorruptedRecord` with exact line number context.
2. **Unsupported Schema Version:** Encountering `schema_version != 1` immediately aborts with `AirbitrageError::UnsupportedSchemaVersion { found, expected: 1 }`.
3. **Mismatched Event Type & Payload:** If `event_type` does not match the variant in `payload`, parsing fails validation with `AirbitrageError::CorruptedRecord`.
4. **Truncated Final Line:** If an NDJSON file ends with an incomplete line (e.g. process terminated mid-write), the reader distinguishes clean EOF from truncated lines. An unparseable trailing line triggers an explicit corruption error so incomplete writes are surfaced.
5. **Invalid Decimals / Timestamps:** Missing or invalid mandatory fields fail deserialization loudly.

---

## 6. Determinism & Zero-Loss Guarantees

1. **No Wall-Clock Logic in Processing:** Local recording timestamps (`recorded_at_epoch_ms`) are strictly metadata. Downstream analysis and replay rely entirely on internal sequence numbers and market feed timestamps.
2. **Deterministic Sequence Ordering:** Every event written to the recorder receives a strictly monotonic `sequence_number`.
3. **Flushing & Reliability:** `ResearchRecorder` provides an explicit `flush()` method as well as an `auto_flush` configuration setting to guarantee sync to the OS buffer cache without silent data drop.

---

## 7. Replay Implications for M5.5

A core design requirement of M5.4 is preparing for **Milestone M5.5 (Deterministic Historical Replay)**:
- Replay does not simply play back pre-calculated `DislocationObservation` records.
- Instead, M5.5 feeds recorded `MarketEvent` streams back into `MarketStateManager` $\to$ `pricing engine` $\to$ `observatory`.
- Researchers can compare the newly computed observations against the historical recorded `DislocationObservation` events to verify bit-level determinism of the entire pipeline.
- Furthermore, researchers can alter fee structures (e.g. testing VIP fee tiers or zero-fee maker promotions) and replay the exact historical book deltas to re-evaluate profitability hypotheses.

---

## 8. Explicit Limitations

- **Local Research Prototype:** M5.4 records to local filesystem NDJSON files. It does not implement distributed object storage (S3/GCS) or relational/time-series databases.
- **Uncompressed Plaintext in V1:** NDJSON is selected for maximum transparency, readability, and compatibility with standard command-line tools (`jq`, `ripgrep`, Python pandas). Compression (e.g., Zstandard streaming) is deferred to future optimization milestones.
- **Synchronous Recording:** Intended for deterministic research recording. In high-frequency production setups, buffered asynchronous logging channels would be evaluated if disk I/O introduces backpressure.
- **No Trading Claims:** Recording market dislocations does not prove arbitrage profitability. Apparent edges may be unfillable due to network latency, queuing delays, exchange API throttling, or toxic flow.
