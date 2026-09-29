# M4 — Executable VWAP & Cost-Aware Pricing Engine

## 1. Overview & Objective

The **M4 Executable Pricing Engine** extends Airbitrage's trusted market-data layer into an executable depth, VWAP, and cost-aware pricing observatory.

Rather than relying on non-executable abstractions (such as mid-price, last traded price, or top-of-book quotes alone), M4 answers the core question:

> *"Given a currently trusted order book and a requested quantity, what price and notional could theoretically be obtained by consuming the currently available visible depth?"*

```text
Trusted Order Book
        ↓
  Depth Walker
        ↓
Execution Estimate (Notional, Worst Fill, Fill Ratio)
        ↓
       VWAP
        ↓
   Price Impact (vs Top of Book)
        ↓
Configurable Cost Model (Taker/Maker Fees + Operational Rates)
        ↓
Cross-Book Executable Pricing Primitive
```

---

## 2. Core Concepts & Definitions

### 2.1 Observed Facts vs. Configured Assumptions

Airbitrage strictly separates **market observations** from **operational assumptions**:

| Domain | Elements | Source |
| :--- | :--- | :--- |
| **Observed Market Data** | Best bid, best ask, resting price levels, resting quantities, cumulative visible depth | Exchange WebSocket / REST feeds |
| **Derived Execution Metrics** | Filled quantity, remaining quantity, levels consumed, notional, VWAP, worst fill price, price impact | Exact depth-walking algorithms |
| **Configured Assumptions** | Taker fee rates, maker fee rates, fixed order fees, additional operational cost buffers | User / deployment configuration |
| **Derived Net Metrics** | Gross spread, fee deductions, effective unit price, net executable difference | Combined analytical projection |

### 2.2 What an "Executable Estimate" Means

An `ExecutionEstimate` represents the simulated consumption of visible resting limit orders in the local order book at an exact point in time:
* **Buy orders** walk the ask book upward starting from the lowest available price.
* **Sell orders** walk the bid book downward starting from the highest available price.
* Arithmetic is carried out with exact `rust_decimal::Decimal` precision (28 decimal places), eliminating floating-point rounding errors.

---

## 3. Mathematical Specifications

### 3.1 Volume-Weighted Average Price (VWAP)

For a sequence of $n$ consumed price levels where level $i$ provides quantity $q_i$ at price $p_i$:

$$\text{Total Filled Quantity } Q = \sum_{i=1}^n q_i$$

$$\text{Gross Notional } N = \sum_{i=1}^n (p_i \times q_i)$$

$$\text{VWAP} = \frac{N}{Q} = \frac{\sum_{i=1}^n (p_i \times q_i)}{\sum_{i=1}^n q_i}$$

* **Precondition:** $Q > 0$.
* If $Q = 0$ (e.g. empty order book or 0 available depth), $\text{VWAP} = \text{None}$.

### 3.2 Price Impact (Slippage vs. Top of Book)

Price impact measures the divergence between the actual average fill price (VWAP) and the initial top-of-book quote:

* **For BUY executions:**
  $$\text{Impact Ratio} = \frac{\text{VWAP} - \text{Best Ask}}{\text{Best Ask}}$$
  $$\text{Worst Impact Ratio} = \frac{\text{Worst Fill Price} - \text{Best Ask}}{\text{Best Ask}}$$

* **For SELL executions:**
  $$\text{Impact Ratio} = \frac{\text{Best Bid} - \text{VWAP}}{\text{Best Bid}}$$
  $$\text{Worst Impact Ratio} = \frac{\text{Best Bid} - \text{Worst Fill Price}}{\text{Best Bid}}$$

* **Basis Points Conversion:**
  $$\text{Impact (bps)} = \text{Impact Ratio} \times 10{,}000$$

### 3.3 Partial-Fill Semantics

When requested quantity $Q_{\text{req}}$ exceeds available visible depth $Q_{\text{avail}}$:
* $\text{filled\_quantity} = Q_{\text{avail}}$
* $\text{remaining\_quantity} = Q_{\text{req}} - Q_{\text{avail}}$
* $\text{fully\_filled} = \text{false}$
* $\text{VWAP}$ is calculated **strictly over the filled quantity $Q_{\text{avail}}$**.
* The caller can unambiguously evaluate liquidity status via:
  * `is_fully_executable() -> bool`
  * `is_partially_executable() -> bool`
  * `is_not_executable() -> bool`

The engine **never** invents phantom liquidity or extrapolates prices beyond visible order book depth.

---

## 4. Cost Model & Fee Schedules

Exchange trading fees are represented by `FeeSchedule`:
* `taker_rate`: Proportional fee rate as a decimal fraction (e.g. `0.0010` = 10 bps, `0.0005` = 5 bps).
* `maker_rate`: Proportional maker rebate/fee rate.
* `fixed_fee`: Fixed per-transaction charge in quote currency.

### Fee Calculations

$$\text{Taker Fee} = (N \times \text{taker\_rate}) + \text{fixed\_fee}$$

$$\text{Maker Fee} = (N \times \text{maker\_rate}) + \text{fixed\_fee}$$

### Effective Execution Price

Operational costs shift the effective price paid or received per unit:

* **For BUY:**
  $$\text{Effective Price} = \frac{N + \text{Total Costs}}{Q}$$
  *(Costs increase the effective purchase price)*

* **For SELL:**
  $$\text{Effective Price} = \frac{N - \text{Total Costs}}{Q}$$
  *(Costs decrease effective net proceeds)*

---

## 5. Cross-Book Pricing Primitive

The `CrossBookComparison` primitive evaluates the theoretical simultaneous execution of buying on Book A and selling on Book B for the **exact same requested quantity**:

* **Precondition:** Both Book A and Book B must be validated as **Trusted** by `MarketStateManager`.
* If either book is untrusted (invalidated, resyncing, crossed, stale, disconnected), the query returns `None`.
* **Full Execution Requirement:**
  $$\text{fully\_executable} \iff \text{buy\_estimate.fully\_filled} \land \text{sell\_estimate.fully\_filled}$$
* For partial fills, metrics are calculated on the matched common quantity $Q_{\text{matched}} = \min(Q_{\text{buy}}, Q_{\text{sell}})$.

### Spread & PnL Formulas

$$\text{Gross Spread} = \text{VWAP}_{\text{sell}} - \text{VWAP}_{\text{buy}}$$

$$\text{Gross PnL} = (\text{VWAP}_{\text{sell}} \times Q_{\text{matched}}) - (\text{VWAP}_{\text{buy}} \times Q_{\text{matched}})$$

$$\text{Net PnL} = \text{Gross PnL} - \text{Total Costs}$$

$$\text{Net Spread (bps)} = \frac{\text{Net PnL}}{\text{Buy Notional Matched}} \times 10{,}000$$

---

## 6. Numerical Examples (Hand-Verified)

### Example 1: Multi-Level Buy Depth Walk

* **Book Asks:**
  * Level 1: `100.00` × `1.0` BTC
  * Level 2: `101.00` × `2.0` BTC
  * Level 3: `102.00` × `5.0` BTC
* **Requested:** `2.0` BTC
* **Walk:**
  * Take `1.0` @ `100.00` $\rightarrow$ notional = $100.00$, remaining = $1.0$
  * Take `1.0` @ `101.00` $\rightarrow$ notional = $101.00$, remaining = $0.0$
* **Results:**
  * $\text{Filled} = 2.0$ BTC
  * $\text{Notional} = 100.00 + 101.00 = 201.00$ USDT
  * $\text{VWAP} = 201.00 / 2.0 = 100.50$ USDT/BTC
  * $\text{Worst Fill Price} = 101.00$ USDT/BTC
  * $\text{Levels Consumed} = 2$
  * $\text{Price Impact} = (100.50 - 100.00) / 100.00 = 0.0050$ (50 bps)

### Example 2: Cross-Book Execution Comparison (Section 27 Acceptance Test)

* **Book A (Buy Side):**
  * Asks: `100.00` × `1.0`, `101.00` × `2.0`, `102.00` × `5.0`
* **Book B (Sell Side):**
  * Bids: `103.00` × `1.0`, `102.00` × `2.0`, `101.00` × `5.0`
* **Requested Quantity:** `2.0` BTC
* **Fee Schedules:** `0.0010` (10 bps) taker fee on both venues.
* **Calculations:**
  * Buy Side on A: `1.0 @ 100.00 + 1.0 @ 101.00` $\rightarrow$ Notional = `201.00`, VWAP = `100.50`
  * Sell Side on B: `1.0 @ 103.00 + 1.0 @ 102.00` $\rightarrow$ Notional = `205.00`, VWAP = `102.50`
  * $\text{Gross Spread} = 102.50 - 100.50 = 2.00$ USDT/BTC
  * $\text{Gross PnL} = 205.00 - 201.00 = 4.00$ USDT
  * Buy Taker Fee: $201.00 \times 0.0010 = 0.201$ USDT
  * Sell Taker Fee: $205.00 \times 0.0010 = 0.205$ USDT
  * Total Fees: $0.201 + 0.205 = 0.406$ USDT
  * $\text{Net PnL} = 4.00 - 0.406 = 3.594$ USDT
  * $\text{Net Spread (bps)} = (3.594 / 201.00) \times 10{,}000 = 178.806$ bps

---

## 7. Explicit Non-Goals & Limitations

M4 is strictly a deterministic analytical pricing model. It **does NOT**:
1. Place live orders or connect to private/authenticated exchange endpoints.
2. Guarantee fill certainty in production (quotes may be executed or canceled before an order arrives).
3. Model matching engine queue position or latency arbitrage race dynamics.
4. Model adverse selection or toxic order flow.
5. Model cross-venue collateral transfer times, margin maintenance, or funding rate drag.
6. Make claims of trading profitability.
