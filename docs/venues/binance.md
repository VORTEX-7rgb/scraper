# Binance Market Data Specification & Integration Guide (Verified 2026)

This document records first-party verified protocols, stream schemas, endpoints, and sequencing rules for Binance Spot and USD-M Futures public market data.

---

## 1. Endpoints & Connectivity

### 1.1 Binance Spot
* **Raw WebSocket Stream:** `wss://stream.binance.com:9443/ws`
* **Combined WebSocket Stream:** `wss://stream.binance.com:9443/stream?streams=<streamName1>/<streamName2>`
* **Connection Lifecycle:** 
  * A single connection is valid for up to 24 hours. The server will disconnect connections reaching 24h.
  * Maximum 1,024 streams per single WebSocket connection.
  * Incoming message limit: Maximum 5 messages per second; exceeding this triggers connection termination and potential IP ban.
  * WebSocket Ping/Pong: The Binance gateway periodically sends standard WebSocket ping frames (RFC 6455). Clients must respond with pong frames (handled automatically by `tokio-tungstenite`).

### 1.2 Binance USD-M Futures
* **Raw WebSocket Stream:** `wss://fstream.binance.com/ws`
* **Combined WebSocket Stream:** `wss://fstream.binance.com/stream?streams=<streamName1>/<streamName2>`
* **Connection Lifecycle:**
  * Same 24-hour connection lifetime.
  * Rate limits: Maximum 5 messages/sec sent to the server.
  * Keep-alive ping frames sent every 3 minutes; client must respond with pong.

---

## 2. Stream Subscriptions & Payloads

### 2.1 Spot Partial Book Depth (`<symbol>@depth<levels>@100ms`)
* **Topic Name:** `btcusdt@depth20@100ms`
* **Push Frequency:** Pushed every 100 milliseconds.
* **Content:** Snapshot of the top 20 bids and asks sorted respectively.
* **Subscription Request:**
  ```json
  {
    "method": "SUBSCRIBE",
    "params": [
      "btcusdt@depth20@100ms"
    ],
    "id": 1
  }
  ```
* **Payload Structure:**
  ```json
  {
    "lastUpdateId": 47291849102,
    "bids": [
      ["64210.50", "0.45000000"],
      ["64210.00", "1.25000000"]
    ],
    "asks": [
      ["64211.00", "0.82000000"],
      ["64211.50", "2.10000000"]
    ]
  }
  ```
  *(Note: If consumed through `/stream?streams=`, the payload is wrapped in `{"stream":"btcusdt@depth20@100ms", "data":{...}}`)*

### 2.2 USD-M Futures Partial Book Depth (`<symbol>@depth<levels>@100ms`)
* **Topic Name:** `btcusdt@depth20@100ms`
* **Push Frequency:** Pushed every 100 milliseconds.
* **Payload Structure (Raw / Stream):**
  ```json
  {
    "e": "depthUpdate",
    "E": 1727401234567,
    "T": 1727401234560,
    "s": "BTCUSDT",
    "U": 482019201,
    "u": 482019220,
    "pu": 482019200,
    "b": [
      ["64212.00", "0.500"],
      ["64211.00", "1.200"]
    ],
    "a": [
      ["64213.00", "0.400"],
      ["64214.00", "2.000"]
    ]
  }
  ```
  *Key Fields:*
  * `E`: Event time (Unix epoch ms)
  * `T`: Matching engine transaction time (Unix epoch ms)
  * `s`: Symbol
  * `U`: First update ID in event
  * `u`: Final update ID in event
  * `pu`: Final update ID in previous event (critical for Futures sequence checking)
  * `b`: Bids (`[price, quantity]`)
  * `a`: Asks (`[price, quantity]`)

### 2.3 USD-M Futures Mark Price & Funding Rate (`<symbol>@markPrice@1s`)
* **Topic Name:** `btcusdt@markPrice@1s`
* **Push Frequency:** Pushed every 1,000 milliseconds (1 second).
* **Payload Structure:**
  ```json
  {
    "e": "markPriceUpdate",
    "E": 1727401235000,
    "s": "BTCUSDT",
    "p": "64212.50",
    "i": "64210.00",
    "P": "64215.00",
    "r": "0.00010000",
    "T": 1727414400000
  }
  ```
  *Key Fields:*
  * `p`: Mark price
  * `i`: Index price
  * `r`: Latest funding rate (e.g. `0.00010000` = 0.01% = 1 bp)
  * `T`: Next funding settlement time (Unix epoch ms)

---

## 3. Order Book Synchronization & Recovery Strategy

1. **Spot Synchronization (`depth20@100ms`):**
   * Each payload represents a clean snapshot of top 20 levels.
   * Overwrite local bids and asks in-place.
   * If `best_bid >= best_ask`, flag `CrossedBook`, invalidate local book, and wait for the next 100ms message.
2. **Futures Synchronization:**
   * Validate contiguous sequence: `event.pu == previous_event.u`.
   * If a gap is detected (`event.pu != previous_event.u`), set `sequence_gap` warning flag and invalidate book until a fresh snapshot or contiguous sequence arrives.
3. **Heartbeat & Reconnect Strategy:**
   * Connection supervisor maintains an active WebSocket reader loop.
   * If no message is received for 10 seconds (heartbeat timeout), close the socket and trigger exponential backoff reconnect:
     $$T_{\text{backoff}} = \min(30\text{s}, 1\text{s} \times 2^{\text{retries}}) + \text{rand}(0, 500\text{ms})$$
