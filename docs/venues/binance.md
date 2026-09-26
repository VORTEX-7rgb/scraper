# Binance Market Data Specification & Integration Guide (Verified 2026)

This document records first-party verified protocols, stream schemas, endpoints, and sequencing rules for Binance Spot and USD-M Futures public market data.

---

## 1. Endpoints & Connectivity Architecture

### 1.1 Binance Spot
* **Base WebSocket Endpoint:** `wss://stream.binance.com/ws` (using standard TLS port 443; port 9443 is blocked on certain residential ISPs/firewalls).
* **Combined WebSocket Stream:** `wss://stream.binance.com/stream?streams=<streamName1>/<streamName2>`
* **Connection Lifecycle:** 
  * Maximum single connection lifetime: 24 hours. The gateway will disconnect connections reaching 24h.
  * Maximum 1,024 streams per single WebSocket connection.
  * Incoming rate limit: Maximum 5 messages per second to the server.
  * Keep-alive ping frames: Server sends RFC 6455 Ping frames periodically; client responds with standard Pong frames.

### 1.2 Binance USD-M Futures (2026 Routed Architecture)
Binance USD-M Futures separates traffic across three distinct, dedicated route endpoints. The legacy unrouted endpoint (`wss://fstream.binance.com/ws`) is deprecated and retired.

1. **Public Market Data Route (`/public`):**
   * **URL:** `wss://fstream.binance.com/public/ws`
   * **Streams:** High-frequency market data such as order book depth (`<symbol>@depth20@100ms`, `<symbol>@depth@100ms`) and aggregated trades (`<symbol>@aggTrade`).
2. **General Market Data Route (`/market`):**
   * **URL:** `wss://fstream.binance.com/market/ws`
   * **Streams:** Regular cadence market data such as mark price (`<symbol>@markPrice@1s`), tickers, and Kline updates.
   * *Critical Note:* Subscribing to `@markPrice` on `/public` or unrouted base URLs fails silently or disconnects. The `/market` route must be strictly used.
3. **Private User Data Route (`/private`):**
   * **URL:** `wss://fstream.binance.com/private/ws?listenKey=<listenKey>`
   * *Excluded from V1 Scope.*

---

## 2. Stream Subscriptions & Canonical Event Mapping

### 2.1 Spot Partial Book Depth (`<symbol>@depth20@100ms`)
* **Endpoint:** `wss://stream.binance.com/ws`
* **Topic Name:** `btcusdt@depth20@100ms`
* **Cadence:** Pushed every 100 milliseconds.
* **Semantic Contract:** Full snapshot of the top 20 bid and ask price levels.
* **Canonical Mapping:** `MarketEvent::OrderBookSnapshot`
* **Payload Structure:**
  ```json
  {
    "lastUpdateId": 100706410507,
    "bids": [
      ["84343.08000000", "0.45000000"],
      ["84343.00000000", "1.25000000"]
    ],
    "asks": [
      ["84343.09000000", "0.82000000"],
      ["84343.50000000", "2.10000000"]
    ]
  }
  ```

### 2.2 USD-M Futures Depth Updates (`<symbol>@depth20@100ms`)
* **Endpoint:** `wss://fstream.binance.com/public/ws`
* **Topic Name:** `btcusdt@depth20@100ms`
* **Cadence:** Pushed every 100 milliseconds.
* **Semantic Contract:** Incremental order book delta. Levels with quantity `"0.000"` denote deletion.
* **Canonical Mapping:** `MarketEvent::OrderBookDelta`
* **Payload Structure:**
  ```json
  {
    "e": "depthUpdate",
    "E": 1790461851965,
    "T": 1790461851964,
    "s": "BTCUSDT",
    "U": 11666557692821,
    "u": 11666557704077,
    "pu": 11666557692741,
    "b": [
      ["84310.40", "0.500"],
      ["84309.00", "0.000"]
    ],
    "a": [
      ["84310.50", "0.400"]
    ]
  }
  ```
* **Key Fields & Invariants:**
  * `E`: Gateway event publication timestamp (ms since epoch).
  * `T`: Matching engine transaction timestamp (ms since epoch).
  * `s`: Symbol verification.
  * `U`: First update ID in event; must satisfy `U <= u`.
  * `u`: Final update ID in event.
  * `pu`: Final update ID in previous event; must satisfy `current.pu == previous.u` for continuity.

### 2.3 USD-M Futures Mark Price & Funding (`<symbol>@markPrice@1s`)
* **Endpoint:** `wss://fstream.binance.com/market/ws`
* **Topic Name:** `btcusdt@markPrice@1s`
* **Cadence:** Pushed every 1,000 milliseconds (1 second).
* **Canonical Mapping:** `MarketEvent::FundingRateUpdate`
* **Payload Structure:**
  ```json
  {
    "e": "markPriceUpdate",
    "E": 1790461852000,
    "s": "BTCUSDT",
    "p": "84310.40000000",
    "P": "84300.93456691",
    "r": "0.00002921",
    "T": 1790467200000
  }
  ```
* **Key Fields:**
  * `p`: Mark price (used for liquidation and funding settlement).
  * `P`: Spot index price.
  * `r`: Latest estimated funding rate (e.g. `0.00002921` = 0.002921% = ~0.29 bps).
  * `T`: Next funding settlement timestamp (ms since epoch).
  * `E`: Exchange publication timestamp (ms since epoch).

---

## 3. Freshness Tracking & Stale Detection
Airbitrage tracks stream health independently based on publication cadence:
* **Spot Depth (`SpotDepth`):** Cadence 100ms → Freshness threshold: 500ms (5 missing frames).
* **Futures Depth (`FuturesDepth`):** Cadence 100ms → Freshness threshold: 500ms (5 missing frames).
* **Futures Mark Price (`FuturesMarkPrice`):** Cadence 1,000ms → Freshness threshold: 3,000ms (3 missing frames).

If any stream fails to receive updates within its cadence threshold, `is_stream_stale` evaluates to `true`, preventing stale data from being treated as live in downstream analytics.
