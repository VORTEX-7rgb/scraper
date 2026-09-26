# Airbitrage

**Airbitrage** is a market dislocation research observatory and analysis engine written in Rust.

The project is designed to answer a single quantitative question:
> *Can we observe, reconstruct, model, replay, and realistically simulate market dislocations across fragmented venues well enough to determine whether an economically viable edge exists?*

Airbitrage does not assume arbitrage is profitable or risk-free. It treats every observed price discrepancy as a hypothesis subject to fee hurdles, book depth consumption, order arrival latency, and execution uncertainty.

---

## Current Status: Milestone M0 (Foundation)
* **Active Phase:** Phase 0 — Core Architecture & Project Scaffolding
* **Toolchain:** Rust (Edition 2024, `rustc 1.98.1+`)
* **Live Trading:** Strictly disabled. No API keys, no private endpoints, no order routing.

---

## Scope of Version 1 (V1)

### What V1 Does
1. Ingests public, unauthenticated L2 market data and funding streams from **Binance** (Spot & USD-M Futures) and **Bybit** (Spot & Linear Futures).
2. Reconstructs normalized, synchronized local limit order books.
3. Computes executable Volume-Weighted Average Price (VWAP) across discrete sizing tiers ($100, $500, $1,000).
4. Models all explicit frictions: exchange-specific taker fees, book slippage, market impact, and latency decay risk.
5. Measures dislocation persistence decay (half-life from 0ms to 60s).
6. Records raw tick streams to compressed Zstandard files for deterministic historical replay.
7. Simulates non-atomic execution via a realistic paper execution state machine.

### What V1 Explicitly Does NOT Do
* No live order placement, order modification, or cancellation.
* No private exchange API integration, key management, or credential handling.
* No account balance tracking, deposit, withdrawal, or wallet transfers.
* No leverage, margin borrowing, or automated capital allocation.
* No Traditional Finance (NSE, BSE, CME) or Decentralized Exchange (DEX) integrations.

---

## Building and Running

### Prerequisites
* Rust toolchain (`rustc` and `cargo` version 1.85+ / Edition 2024 support).

### Build
```powershell
cargo build
```

### Run Tests
```powershell
cargo test
```

### Static Analysis & Lints
```powershell
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

### Run M0 Binary
```powershell
cargo run -- --config config/config.toml
```

---

## Documentation
* [Master Phased Roadmap](docs/MASTER_PLAN.md)
* [System Architecture](docs/ARCHITECTURE.md)
* [Assumptions Register](docs/ASSUMPTIONS.md)
* [ADR 0001: V1 Research Scope](docs/adr/0001-v1-scope.md)
