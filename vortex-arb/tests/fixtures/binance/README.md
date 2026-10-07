# Binance USDⓈ-M fixtures

**Provenance: these are hand-built, not captured from production.** The machine
that wrote the adapter could not reach `fapi.binance.com` / `fstream.binance.com`.
Field names, types and shapes follow Binance's official docs and the sample payloads
in the official `binance-connector-python` (derivatives_trading_usds_futures 17.6.0).
Values are realistic but invented; sequence ids are chosen to exercise the sync rules.

Replace them with real captures by running `python scripts/record_binance_fixtures.py`
from a host that can reach Binance, then re-run the tests.

| File | Source | Exercises |
|---|---|---|
| `exchange_info.json` | `GET /fapi/v1/exchangeInfo` | filters to TRADING USDT perpetuals (drops USDC, quarterly, SETTLING) |
| `depth_snapshot_ethusdt.json` | `GET /fapi/v1/depth?symbol=ETHUSDT&limit=1000` | initial sync, `lastUpdateId = L` |
| `depth_updates_ethusdt.jsonl` | `/public/stream?streams=ethusdt@depth@100ms` | 1: `u < L` dropped · 2: `U <= L <= u` first applied · 3: removes an unknown level · 4: removes best ask · 5: `pu` gap · 6: continues after resync |
| `depth_snapshot_ethusdt_resync.json` | `GET /fapi/v1/depth` | resync after the gap (`lastUpdateId = L + 50`) |
| `agg_trades.jsonl` | `/market/stream?streams=ethusdt@aggTrade/btcusdt@aggTrade` | aggressor from `m`, `q` vs `nq` |
| `premium_index_ethusdt.json` | `GET /fapi/v1/premiumIndex?symbol=ETHUSDT` | funding / mark / index |
