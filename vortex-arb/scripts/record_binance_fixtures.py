"""Capture real Binance USDⓈ-M public payloads into tests/fixtures/binance/recorded/.

Read-only: public endpoints only, no keys. Usage:

    python scripts/record_binance_fixtures.py [--symbol ETHUSDT] [--seconds 5]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

import httpx
from websockets.asyncio.client import connect

from vortex.adapters.binance import REST_BASE, SNAPSHOT_LIMIT, WS_BASE

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "binance" / "recorded"


async def record_stream(url: str, path: Path, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    async with connect(url) as ws:
        with path.open("w") as f:
            while (left := deadline - time.monotonic()) > 0:
                try:
                    f.write(await asyncio.wait_for(ws.recv(), left) + "\n")
                except TimeoutError:
                    break


async def main(symbol: str, seconds: float) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    s = symbol.lower()
    depth = asyncio.create_task(
        record_stream(
            f"{WS_BASE}/public/stream?streams={s}@depth@100ms",
            OUT / f"depth_updates_{s}.jsonl",
            seconds,
        )
    )
    trades = asyncio.create_task(
        record_stream(
            f"{WS_BASE}/market/stream?streams={s}@aggTrade", OUT / "agg_trades.jsonl", seconds
        )
    )
    await asyncio.sleep(1)  # snapshot must land inside the recorded diff window
    async with httpx.AsyncClient(base_url=REST_BASE, timeout=10) as http:
        for name, path, params in [
            ("exchange_info.json", "/fapi/v1/exchangeInfo", None),
            (
                f"depth_snapshot_{s}.json",
                "/fapi/v1/depth",
                {"symbol": symbol, "limit": SNAPSHOT_LIMIT},
            ),
            (f"premium_index_{s}.json", "/fapi/v1/premiumIndex", {"symbol": symbol}),
        ]:
            resp = await http.get(path, params=params)
            resp.raise_for_status()
            (OUT / name).write_text(json.dumps(resp.json(), indent=1))
    await asyncio.gather(depth, trades)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="ETHUSDT")
    ap.add_argument("--seconds", type=float, default=5.0)
    args = ap.parse_args()
    asyncio.run(main(args.symbol, args.seconds))
