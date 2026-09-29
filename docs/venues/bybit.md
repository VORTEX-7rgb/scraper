# Bybit Market Data Specification & Integration Guide (Verified 2026)

This document records the first-party verified protocols, stream schemas, public WebSocket endpoints, and sequencing semantics for Bybit V5 Spot and Linear public market data.

---

## 1. Endpoints & Connectivity Architecture

### 1.1 Public WebSocket Endpoints (Bybit V5)
Bybit separates public market data streams across explicit market categories:

* **Spot Market Data:**
  * **URL:** `wss://stream.bybit.com/v5/public/spot`
  * **Markets:** Spot trading pairs (e.g. `BTCUSDT`).
* **Linear Perpetuals & Futures:**
  * **URL:** `wss://stream.bybit.com/v5/public/linear`
  * **Markets:** USDT & USDC settled Linear Perpetuals and Futures (e.g. `BTCUSDT`).
* **Inverse Contracts:**
  * **URL:** `wss://stream.bybit.com/v5/public/inverse`
  * *Excluded from initial V1 scope.*

### 1.2 Connection Lifecycle & Limits
* **Maximum Connections:** Up to 500 connections per IP.
* **Stream Limit:** Maximum 100 subscriptions per connection.
* **Keep-Alive Protocol:** Client-driven ping/pong.
  * The client must send a JSON ping frame periodically (every 20 seconds):
    ```json
    {"op": "ping"}
    ```
  * The Bybit server responds with:
    ```json
    {"op": "pong", "args": ["1672304484978"], "conn_id": "2324d924-aa4d-45b0-a858-7b8be29ab52b"}
    ```
  * If the connection is idle with no ping received for over 20 seconds, the server will disconnect.

---

## 2. Subscription Schema & Handshake

### 2.1 Subscription Request
Clients send a JSON subscription message specifying operation `"op": "subscribe"` and an array of topic strings in `"args"`, with an optional `"req_id"` for correlation:
```json
{
  "op": "subscribe",
  "args": ["orderbook.50.BTCUSDT"],
  "req_id": "sub_orderbook_spot_1"
}
```

### 2.2 Subscription Acknowledgement
The Bybit server returns an explicit confirmation response:
```json
{
  "success": true,
  "ret_msg": "subscribe",
  "conn_id": "2324d924-aa4d-45b0-a858-7b8be29ab52b",
  "req_id": "sub_orderbook_spot_1",
  "op": "subscribe"
}
```
* If `success == true`: Subscription was registered successfully.
* If `success == false`: Subscription failed; `ret_msg` contains the error reason.

---

## 3. Order Book Stream Semantics (`orderbook.{depth}.{symbol}`)

### 3.1 Topics & Depth Levels
* **Topic Pattern:** `orderbook.{depth}.{symbol}`
* **Supported Depths:**
  * `orderbook.1.{symbol}`: Top level (Best Bid and Offer).
  * `orderbook.50.{symbol}`: Top 50 levels (20ms push frequency for Linear; 50ms push frequency for Spot).
  * `orderbook.200.{symbol}`: Top 200 levels (100ms push frequency).
  * `orderbook.1000.{symbol}`: Full 1000 levels (100ms push frequency).
* **Initial Snapshot Delivery:** Upon subscribing, the first message delivered on the topic is always a full `"type": "snapshot"` containing baseline levels up to the subscribed depth.
* **Subsequent Incremental Updates:** Following the snapshot, all incremental changes are pushed as `"type": "delta"`.

### 3.2 Snapshot Payload Schema
```json
{
  "topic": "orderbook.50.BTCUSDT",
  "type": "snapshot",
  "ts": 1672304484978,
  "data": {
    "s": "BTCUSDT",
    "b": [
      ["65000.50", "1.500"],
      ["65000.00", "2.000"]
    ],
    "a": [
      ["65001.00", "1.000"],
      ["65001.50", "0.500"]
    ],
    "u": 123456,
    "seq": 789012
  }
}
```

### 3.3 Delta Payload Schema
```json
{
  "topic": "orderbook.50.BTCUSDT",
  "type": "delta",
  "ts": 1672304485000,
  "cts": 1672304484998,
  "data": {
    "s": "BTCUSDT",
    "b": [
      ["65000.50", "0.000"],
      ["64999.00", "3.100"]
    ],
    "a": [
      ["65001.00", "1.200"]
    ],
    "u": 123460,
    "seq": 789015
  }
}
```

### 3.4 Price Level & Mutation Semantics
* `b`: Bids array of `[price: String, size: String]`.
* `a`: Asks array of `[price: String, size: String]`.
* In `delta` updates:
  * Size `"0"` (or `"0.000"`): **Delete** the price level from the local book.
  * Size `> 0`: **Insert** (if missing) or **Update** (if existing) the price level quantity.

---

## 4. Sequence & Continuity Semantics

### 4.1 Update ID (`u`) vs Cross Sequence (`seq`)
* **`u` (Order Book Update ID):**
  * Monotonically increasing sequence number unique to the matching engine state.
  * In filtered streams such as `orderbook.50`, updates to price levels outside the top 50 increment the core engine's `u` without producing a public message.
  * Consequently, $u$ advances monotonically ($u_k > u_{k-1}$), but **does NOT advance by strictly $+1$**.
  * Duplicate update IDs ($u_k == u_{k-1}$) and decreasing IDs ($u_k < u_{k-1}$) indicate ordering loss, buffer corruption, or out-of-order delivery.
* **`seq` (Cross-Stream Sequence Number):**
  * Global matching engine sequence used for cross-stream time alignment.
* **Absence of `pu`:**
  * Bybit does **NOT** provide a `pu` (previous update ID) field.
  * Unlike Binance Futures, continuity cannot and must not be verified by comparing `pu == last_u`.
  * The appropriate sequence policy is strictly `SequencePolicy::MonotonicStrict`.

### 4.2 Snapshot Resets & Epoch Handling
* When Bybit internal services restart or network connections recover, Bybit may send a fresh `snapshot` where $u$ resets to a baseline (such as $u = 1$).
* The market-state engine must not reject a restart snapshot merely because $u_{\text{snap}} < u_{\text{previous}}$.
* Entering resynchronization starts a new synchronization epoch (`epoch += 1`), clearing the previous update sequence and anchoring cleanly to the new snapshot.

---

## 5. Canonical Event Mapping

| Bybit V5 Field / Message | Canonical `MarketEvent` Representation | Notes |
|:---|:---|:---|
| `"type": "snapshot"` | `MarketEvent::OrderBookSnapshot` | Baseline order book levels |
| `"type": "delta"` | `MarketEvent::OrderBookDelta` | Incremental book mutations |
| `data.s` | `symbol: String` | Upper-case symbol string |
| `data.b` | `bids: Vec<PriceLevel>` | Price and quantity parsed as `Decimal` |
| `data.a` | `asks: Vec<PriceLevel>` | Price and quantity parsed as `Decimal` |
| `data.u` | `sequence_id: u64` | Target update ID for monotonic ordering |
| `data.u` | `first_sequence_id: u64` | In deltas, set to `u` |
| *(None)* | `prev_sequence_id: Option<u64>` | Set strictly to `None` (Bybit does not emit `pu`) |
| `cts` (if present) | `transaction_ts_ms: i64` | Matching engine transaction timestamp |
| `ts` | `exchange_ts_ms: i64` | System publication timestamp |
| Local arrival clock | `local_recv_ts_ns: i64` | Wall-clock nanoseconds at socket reception |
| Socket connect/drop | `MarketEvent::ConnectionState` | Lifecycle tracking |

---

## 6. Known Edge Cases & Fail-Closed Rules

1. **Duplicate or Decreasing `u` in `Live` State:**
   * Handled by `SequencePolicy::MonotonicStrict`.
   * Triggers `EngineError::OutOfOrderUpdate`, invalidating the book and failing closed.
2. **In-Band Resnapshot:**
   * If an unsolicited snapshot arrives while the book is live with a reset sequence ID (e.g. $u=1$), it initiates a new synchronization epoch.
3. **Empty Bids/Asks:**
   * Empty books trigger `InvalidationReason::EmptyBook` and invalidate state.
4. **Crossed Book:**
   * Any delta or snapshot where $\text{best\_bid} \ge \text{best\_ask}$ triggers `InvalidationReason::CrossedBook`, invalidating state.
