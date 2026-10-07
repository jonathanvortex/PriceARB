# Spec — CEX ↔ Perp DEX Price Arbitrage

## 1. Strategy summary

The same perp trades on Binance (deep, price-discovery venue) and on perp DEXs (thinner, slower).
When their prices disagree by more than all-in costs, there is an arbitrage. Two variants:

**A. Taker–taker.** When DEX bid > Binance ask (or DEX ask < Binance bid) by more than costs,
hit both books simultaneously. Delta-neutral; profit realised when prices converge and both legs close.
Main risks: leg risk (one side fails to fill), latency (gap already gone), funding while holding.

**B. Maker–hedge (expected primary).** Rest quotes on the DEX at Binance mid ± offset.
When filled, immediately hedge as taker on Binance. Earns offset + DEX maker rebate/incentives − Binance taker fee.
Main risk: adverse selection (getting filled when our quote is stale). Measured via markouts.

Binance is always the **hedge venue (venue_b)**. The DEX is always **venue_a**.

## 2. Phases and acceptance criteria

### Phase 1 — Census (current)

**Build:**
1. `VenueAdapter` for Binance USDⓈ-M futures and Hyperliquid (public market data only).
2. Recorder: maintains local L2 books per (venue, instrument), writes `BookSnapshot`, `TradePrint`, `FundingObs` to Parquet.
3. Spread calculator: on every book update, computes `SpreadObs` for every configured venue pair × direction × size tier.
4. Episode detector: groups `SpreadObs` into `Opportunity` episodes for a given threshold. Must run **offline** on stored `SpreadObs` so thresholds can be swept.
5. Census report notebook (see below).

**Universe:** ETH, BTC, SOL to start (configurable).

**Acceptance criteria:**
- Runs unattended for 24h with automatic reconnect; data gaps < 1% of wall time per stream, and gaps are logged.
- Book integrity: sequence/gap checks per venue; on a gap, resync from snapshot and log it.
- Latency stats: distribution of `ts_local − ts_exchange` per venue (p50/p95/p99).
- Census report answers, per pair and size tier:
  - Distribution of `net_bps`
  - Episodes per hour at thresholds 0 / 5 / 10 / 20 bps
  - Episode duration distribution vs our measured latency (how many last longer than our round-trip?)
  - Max size available at threshold
  - Time-of-day pattern (UTC hour)

**Phase 1 exit decision (human):** is there enough net edge, lasting long enough, at useful size, to justify Phase 2?

### Phase 2 — Simulation (not started)

- Taker–taker sim: on each episode, simulate entry with `leg_risk_buffer_bps` and a configurable execution delay; exit when spread reverts or after `max_hold_s`; include funding accrued.
- Maker–hedge sim: quotes priced from Binance mid per params; DEX fill when a `TradePrint` trades *through* our price (queue position assumed back); Binance hedge executed against the recorded Binance book at `fill_ts + hedge_delay_ms`.
- `Markout` at 1s / 5s / 30s for every DEX fill.
- `PnLAttribution` per round trip.
- Parameter sweeps with walk-forward splits (fit on days 1..k, evaluate on day k+1). Report out-of-sample only.

### Phase 3 — Live tiny (not started, human-gated)

Spec to be written after Phase 2 review. Will include: trade-only keys, per-venue margin accounts, kill switches from `risk` params, live-vs-sim reconciliation.

## 3. Adapter interface

```python
class VenueAdapter(Protocol):
    venue: Venue

    async def load_instruments(self) -> list[Instrument]: ...
    async def stream_books(self, canonicals: list[str]) -> AsyncIterator[BookSnapshot]: ...
    async def stream_trades(self, canonicals: list[str]) -> AsyncIterator[TradePrint]: ...
    async def poll_funding(self, canonicals: list[str]) -> list[FundingObs]: ...

    # Phase 3 only — raise NotImplementedError until then
    async def place_order(self, *args, **kwargs): raise NotImplementedError
    async def cancel_order(self, *args, **kwargs): raise NotImplementedError
```

Adapters map venue symbols to a `canonical` asset name (e.g. Binance `ETHUSDT` and Hyperliquid `ETH` → `ETH`).
Note quote currency differences (USDT vs USDC) on `Instrument.quote_ccy`; do not silently treat them as equal.

## 4. Calculations

**Hedge mid:** `mid_b = (best_bid_b + best_ask_b) / 2`

**Executable VWAP for a USD size:** walk the relevant side of the book level by level until cumulative notional ≥ `size_usd`.
If depth is insufficient, the observation is **not executable** at that tier (store it with `vwap = None`, do not drop it).

**Gross spread (bps):**
- `sell_a_buy_b`: `(vwap_bid_a − vwap_ask_b) / mid_b × 10_000`
- `buy_a_sell_b`: `(vwap_bid_b − vwap_ask_a) / mid_b × 10_000`

**Fees (bps, round trip):** `fee_bps = 2 × (taker_fee_a + taker_fee_b)`
(assumes taker entry and exit on both venues, exit spread ≈ 0 at convergence; conservative baseline).

**Net spread:** `net_bps = gross_bps − fee_bps`. **No buffer applied here** — buffers are strategy params applied later.

**Staleness:** `staleness_ms = (now_local − min(ts_local_a, ts_local_b)) / 1e6`. Observations with staleness above `max_staleness_ms` are flagged, not dropped.

**Episodes:** consecutive `SpreadObs` (same pair, direction, size tier) with `net_bps ≥ threshold`.
An episode ends when `net_bps < threshold` for longer than `episode_gap_tolerance_ms`.

## 5. Recording and storage

- Books: maintain full local book; write top `book_depth_levels` levels on top-of-book change, throttled to at most once per `snapshot_throttle_ms` per stream.
- Trades: write every trade.
- Funding: poll every `funding_poll_s`.
- Spread obs: computed live and written, so the census report doesn't need to rebuild books.
- Clock: server must run NTP/chrony. Log clock offset at startup.
- Hosting: run the census from the machine we intend to trade from (target: AWS Tokyo), since latency figures depend on it.

## 6. Open questions (resolve with a human, then log in docs/decisions.md)

- Fee tiers to assume for Binance and Hyperliquid (VIP level / staking discounts).
- Which DEX after Hyperliquid: Papertrade or Variational (Variational is RFQ-based — may need a different adapter shape).
- USDT vs USDC quote basis: track it as a separate series or ignore at this stage?
