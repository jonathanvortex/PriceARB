# Decisions log

Append-only. Format: `YYYY-MM-DD — decision — why`

- 2026-10-07 — Start with CEX↔perp DEX price arb, Binance as hedge venue, Hyperliquid as first DEX — deepest DEX book with public websocket data; validates the pipeline before thinner venues.
- 2026-10-07 — Phase 1 is read-only census before any execution code — confirm edge exists at our latency before spending effort on execution.
- 2026-10-07 — Store SpreadObs pre-threshold — every parameter question becomes a cheap offline replay.
- 2026-10-07 — Binance WS uses the split routes `wss://fstream.binance.com/public/stream` (depth) and `/market/stream` (aggTrade) — Binance's March 2026 URL change; the legacy `/stream?streams=` form no longer accepts new subscriptions.
- 2026-10-07 — Binance adapter yields a BookSnapshot (top `book_depth_levels`) on every applied depth diff; throttling to `snapshot_throttle_ms` and "only on top-of-book change" are the recorder's job — keeps the adapter stateless w.r.t. recording policy and lets the spread calc see every update.
- 2026-10-07 — Binance `ts_exchange` = matching-engine time `T` (depth, aggTrade) and `time` (premiumIndex), not event-emit time `E` — `T` is when the state was true, so `ts_local − ts_exchange` includes the 100ms diff batching we actually suffer.
- 2026-10-07 — StreamGap is reported through an `on_gap` callback passed to the adapter, not yielded from the stream iterators — keeps the spec §3 return types intact. A book gap runs from loss of sync (pu gap / disconnect / stale snapshot) until the book is valid again; the initial sync is not a gap.
- 2026-10-07 — Binance trades come from `@aggTrade` and record `q` (includes fills against RPI orders) rather than `nq` — `q` is the full traded quantity; depth feeds exclude RPI orders, so book depth is RPI-free. Provisional: FundingObs.current_rate = premiumIndex `lastFundingRate`, predicted_rate = None (see PR open questions).
- 2026-10-07 — Funding: FundingObs.current_rate = Binance premiumIndex `lastFundingRate`, the live rate for the current interval (confirmed by Jonathan; replaces the provisional note above). predicted_rate stays None for Binance.
- 2026-10-07 — FundingObs carries `funding_interval_h`, refreshed on every poll (Binance: `GET /fapi/v1/fundingInfo`, venue default for unlisted symbols) — venues change intervals per symbol, so a static per-venue value goes stale and mis-scales the rate.
- 2026-10-07 — Venue symbol → canonical mapping lives in `params/instruments.yaml` (`symbol_overrides.<venue>`), loaded by the venue-agnostic `vortex/adapters/symbols.py`; unmapped symbols default to the venue's base asset — one table for every venue, including future ones without trading APIs (Arcus, RiseX, Extended…). Two symbols on one requested canonical is an error (clashes outside the configured universe are ignored).
- 2026-10-07 — Adapters emit market data in canonical units (px / contract_mult, qty × contract_mult); Instrument tick/lot stay in venue units — records sharing a `canonical` must be directly comparable, so scaled contracts (1000PEPE, kPEPE) are normalised at the boundary, not in every consumer.
