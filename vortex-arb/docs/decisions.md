# Decisions log

Append-only. Format: `YYYY-MM-DD — decision — why`

- 2026-10-07 — Start with CEX↔perp DEX price arb, Binance as hedge venue, Hyperliquid as first DEX — deepest DEX book with public websocket data; validates the pipeline before thinner venues.
- 2026-10-07 — Phase 1 is read-only census before any execution code — confirm edge exists at our latency before spending effort on execution.
- 2026-10-07 — Store SpreadObs pre-threshold — every parameter question becomes a cheap offline replay.
