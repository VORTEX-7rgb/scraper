# ADR 0001: V1 Research Scope — Public Market Data Only

## Status
Accepted

## Date
2026-09-27

## Context
Airbitrage is designed to evaluate whether executable market dislocations exist across fragmented cryptocurrency markets (starting with Binance and Bybit). Prior attempts at building retail arbitrage engines frequently fail due to:
1. Immediate reliance on unverified profitability assumptions.
2.Premature implementation of execution infrastructure (private APIs, order placement, wallet transfers) before proving that a statistically significant net edge survives realistic market friction.
3. Complex legal, taxation, and counterparty risks (specifically in India under Income Tax Act Sections 115BBH and 194S, which levy a 30% tax on gross crypto gains without loss setoff, alongside 1% TDS per transaction).

Before risking financial capital, committing to execution infrastructure, or incurring regulatory liabilities, the system must first operate as an empirical **Market Dislocation Observatory**.

## Decision
Version 1 (V1) of Airbitrage is strictly restricted to **research, observation, data reconstruction, cost modeling, and paper simulation** using **public, unauthenticated market data streams**.

### In-Scope for V1
* Public WebSocket and REST market data ingestion from Binance and Bybit.
* L2 order book reconstruction, sequence validation, and crossed-book detection.
* Volume-Weighted Average Price (VWAP) calculation for discrete sizing tiers ($100, $500, $1,000).
* Comprehensive friction modeling (tier-specific taker fees, book slippage, funding rates, latency decay).
* Latency persistence decay measurement (half-life across 0ms to 60s windows).
* Append-only raw tick recording and deterministic historical replay.
* Simulated paper execution modeling order queues, arrival delays, and partial fills.

### Explicitly Excluded from V1
* Private exchange APIs and account authentication.
* API key and secret storage or management.
* Live order placement, order modification, and cancellations.
* Fund deposits, withdrawals, or on-chain transfers.
* Automated inventory rebalancing across venues.
* Traditional Finance (TradFi) venues (NSE, BSE, CME, MCX).
* Decentralized Exchanges (DEXs) and on-chain MEV searchers.
* Margin borrowing or leveraged live trading.

## Consequences
* **Positive:** Zero financial capital risk; zero tax liabilities incurred; zero exposure to exchange credential leaks; complete focus on data quality, latency measurement, and statistical validation.
* **Negative:** Live execution latency and real exchange matching engine queue dynamics cannot be empirically verified with 100% certainty until paper trading passes statistical rigor and controlled micro-execution is approved in a future ADR.
