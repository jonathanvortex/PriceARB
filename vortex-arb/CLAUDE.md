# Vortex Arb — Claude Code Brief

Research and (later) execution codebase for Vortex, a 3-person, ~$5M AUM multi-strategy crypto fund.
The first strategy is **CEX ↔ perp DEX price arbitrage**: Binance USDⓈ-M perps vs perp DEXs (Hyperliquid first; Papertrade and Variational later).

Full spec: `docs/spec-price-arb.md`. Read it before starting any task.

## Current phase: PHASE 1 — Census (read-only)

We are only **observing** markets. No keys, no wallets, no orders.
Do not build anything from a later phase unless a human explicitly says the phase has changed.

| Phase | Goal | Touches money? |
|---|---|---|
| 1. Census | Stream books, measure executable spreads and how often/long/large they are | No |
| 2. Sim | Paper-trade taker-taker and maker-hedge variants on recorded data, sweep params | No |
| 3. Live (tiny) | One pair, small size, compare live vs sim | Yes — human-gated |

## Hard rules (never break these)

1. **Never write code that withdraws, transfers, or bridges funds.** Not even behind a flag.
2. **No order placement code outside `src/vortex/exec/`.** That folder stays empty until Phase 3.
3. **Never hardcode or log secrets.** Keys come from environment variables only (see `.env.example`). Never print them, never write them to Parquet/logs.
4. **Money math uses `Decimal`**, never `float`. Floats are fine only in `notebooks/` and plotting.
5. **Timestamps are integer nanoseconds (UTC).** Record both `ts_exchange` and `ts_local` on every market data record.
6. **Spreads are in bps relative to the hedge venue (Binance) mid.** Sizes are in USD notional.
7. **Parameters live in `params/*.yaml`, never hardcoded.** Raw data is stored pre-threshold so any parameter can be replayed offline.
8. **Verify venue API details against official docs** before implementing an adapter. Do not guess endpoints, message formats, or fee tiers.

## Stack and conventions

- Python 3.11+, `asyncio` + `websockets` for streams, `httpx` for REST
- `pyarrow` for Parquet, `duckdb` for queries, `pydantic` or dataclasses for config parsing
- `ruff` for lint/format, `pytest` for tests
- Schema lives in `src/vortex/schema/models.py`. Extend it; do not create parallel record types.
- Each venue is one adapter in `src/vortex/adapters/<venue>.py` implementing the `VenueAdapter` interface (see spec §3).
- Data goes to `data/` (gitignored), partitioned `data/<record_type>/venue=<v>/date=<YYYY-MM-DD>/*.parquet`.

## Repo layout

```
CLAUDE.md
docs/spec-price-arb.md    # what to build and acceptance criteria
docs/decisions.md         # append-only log: date, decision, why
params/                   # strategy and run configs (YAML)
src/vortex/
  schema/models.py        # all record types
  adapters/               # one file per venue
  census/                 # recorder, spread calculator, episode detector
  sim/                    # Phase 2
  exec/                   # Phase 3 — EMPTY until approved
notebooks/                # analysis of census output
tests/
```

## Workflow

- Work on a branch per task; open a PR. Keep PRs small (one adapter, one component).
- Every component gets tests. Adapters get tests against recorded sample messages in `tests/fixtures/`.
- When you make a non-obvious design choice, add a line to `docs/decisions.md`.
- When the spec is ambiguous, stop and ask rather than inventing behaviour.
