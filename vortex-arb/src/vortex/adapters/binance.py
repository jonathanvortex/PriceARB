"""Binance USDⓈ-M futures adapter — public market data only (Phase 1).

API details verified against Binance's official docs and the official
`binance-connector-python` (derivatives_trading_usds_futures, v17.6.0):

- REST base `https://fapi.binance.com`
  - `GET /fapi/v1/exchangeInfo`            instruments (weight 1)
  - `GET /fapi/v1/depth?limit=1000`        book snapshot (weight 20)
  - `GET /fapi/v1/premiumIndex?symbol=`    mark/index price + funding (weight 1)
- WS base `wss://fstream.binance.com`, routed by traffic class since 2026-03:
  - `/public/stream?streams=<sym>@depth@100ms`  diff book depth
  - `/market/stream?streams=<sym>@aggTrade`     aggregate trades
- Local book sync ("How to manage a local order book correctly", USDⓈ-M):
  buffer diffs, fetch snapshot, drop events with `u < lastUpdateId`, first applied
  event must have `U <= lastUpdateId <= u`, then every event's `pu` must equal the
  previous event's `u`, else resync from a new snapshot. Quantities are absolute;
  qty 0 removes the level (removing an unknown level is normal).
- RPI (Retail Price Improvement) orders are excluded from depth streams/snapshots.
"""

from __future__ import annotations

import asyncio
import heapq
import json
import logging
import time
from collections.abc import AsyncIterator, Callable, Sequence
from decimal import Decimal
from typing import Any

import httpx
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import WebSocketException

from vortex.adapters.base import GapCallback
from vortex.schema.models import (
    BookSnapshot,
    FundingObs,
    Instrument,
    PriceLevel,
    Side,
    StreamGap,
    TradePrint,
    Venue,
    VenueKind,
)

log = logging.getLogger(__name__)

VENUE_ID = "binance"
REST_BASE = "https://fapi.binance.com"
WS_BASE = "wss://fstream.binance.com"
SNAPSHOT_LIMIT = 1000  # deepest documented depth snapshot

_MS_TO_NS = 1_000_000


def ms_to_ns(ms: int) -> int:
    return int(ms) * _MS_TO_NS


def loads(raw: str | bytes) -> Any:
    """JSON decode with any bare floats as Decimal (Binance sends prices as strings)."""
    return json.loads(raw, parse_float=Decimal)


# ---------------------------------------------------------------------------
# Pure parsers (tested against tests/fixtures/binance)
# ---------------------------------------------------------------------------


def parse_exchange_info(payload: dict, quote_ccy: str) -> list[Instrument]:
    """Tradeable linear perpetuals quoted in `quote_ccy`. canonical = baseAsset."""
    out = []
    for s in payload["symbols"]:
        if (
            s.get("contractType") != "PERPETUAL"
            or s.get("status") != "TRADING"
            or s.get("quoteAsset") != quote_ccy
        ):
            continue
        filters = {f["filterType"]: f for f in s.get("filters", [])}
        out.append(
            Instrument(
                venue_id=VENUE_ID,
                venue_symbol=s["symbol"],
                canonical=s["baseAsset"],
                quote_ccy=s["quoteAsset"],
                tick_size=Decimal(filters["PRICE_FILTER"]["tickSize"]),
                lot_size=Decimal(filters["LOT_SIZE"]["stepSize"]),
            )
        )
    return out


def _levels(raw: Sequence[Sequence[str]]) -> list[PriceLevel]:
    return [(Decimal(px), Decimal(qty)) for px, qty in raw]


def parse_agg_trade(data: dict, canonical: str, ts_local: int) -> TradePrint:
    """`m` = buyer is maker, so the aggressor was the seller.

    `q` includes fills against RPI orders (`nq` excludes them); we record `q`,
    the total traded quantity.
    """
    return TradePrint(
        venue_id=VENUE_ID,
        canonical=canonical,
        ts_exchange=ms_to_ns(data["T"]),
        ts_local=ts_local,
        px=Decimal(data["p"]),
        qty=Decimal(data["q"]),
        aggressor=Side.SELL if data["m"] else Side.BUY,
        trade_id=str(data["a"]),
    )


def parse_premium_index(data: dict, canonical: str, ts_local: int) -> FundingObs:
    """`lastFundingRate` ("latest funding rate" per docs) -> current_rate.

    Binance publishes no separate predicted rate here, so predicted_rate is None.
    indexPrice is Binance's oracle-equivalent and goes to oracle_px.
    """
    return FundingObs(
        venue_id=VENUE_ID,
        canonical=canonical,
        ts_exchange=ms_to_ns(data["time"]),
        ts_local=ts_local,
        current_rate=Decimal(data["lastFundingRate"]),
        predicted_rate=None,
        mark_px=Decimal(data["markPrice"]),
        oracle_px=Decimal(data["indexPrice"]),
        next_funding_ts=ms_to_ns(data["nextFundingTime"]) or None,
    )


# ---------------------------------------------------------------------------
# Local book + sequence sync
# ---------------------------------------------------------------------------


class LocalBook:
    """Full-depth L2 book. Prices and quantities are Decimal."""

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def load(self, bids: list[PriceLevel], asks: list[PriceLevel]) -> None:
        self.clear()
        self.apply(bids, asks)

    def apply(self, bids: list[PriceLevel], asks: list[PriceLevel]) -> None:
        for side, levels in ((self.bids, bids), (self.asks, asks)):
            for px, qty in levels:
                if qty == 0:
                    side.pop(px, None)
                else:
                    side[px] = qty

    def top(self, n: int) -> tuple[list[PriceLevel], list[PriceLevel]]:
        bids = [(p, self.bids[p]) for p in heapq.nlargest(n, self.bids)]
        asks = [(p, self.asks[p]) for p in heapq.nsmallest(n, self.asks)]
        return bids, asks


class BookSync:
    """Per-symbol implementation of Binance's USDⓈ-M local order book procedure.

    Feed every diff event to `on_event`; while `wants_snapshot` is True, fetch a
    REST snapshot and pass it to `apply_snapshot`. Both return a BookSnapshot when
    the book is valid after the call, else None. Any loss of sync after the initial
    one (sequence gap, disconnect, stale snapshot) is reported as a StreamGap via
    `on_gap` once the book is valid again.
    """

    def __init__(
        self,
        canonical: str,
        depth_levels: int,
        on_gap: GapCallback | None = None,
    ) -> None:
        self.canonical = canonical
        self.depth_levels = depth_levels
        self.on_gap = on_gap
        self.book = LocalBook()
        self.synced = False
        self._buffer: list[tuple[dict, int]] = []
        self._snapshot_id: int | None = None
        self._last_u: int | None = None  # None = next event is the first after snapshot
        self._gap_start: int | None = None
        self._gap_reason: str | None = None

    @property
    def wants_snapshot(self) -> bool:
        # Only fetch once the stream is delivering, so the snapshot overlaps the buffer.
        return not self.synced and bool(self._buffer)

    def invalidate(self, reason: str, ts_local: int) -> None:
        if self._gap_start is None:
            self._gap_start = ts_local
            self._gap_reason = reason
        if self.synced:
            log.warning("binance %s book resync: %s", self.canonical, reason)
        self.synced = False
        self.book.clear()
        self._buffer.clear()
        self._snapshot_id = None
        self._last_u = None

    def on_event(self, data: dict, ts_local: int) -> BookSnapshot | None:
        if not self.synced:
            self._buffer.append((data, ts_local))
            return None
        if self._process(data, ts_local):
            return self._emit(ms_to_ns(data["T"]), ts_local)
        return None

    def apply_snapshot(self, snap: dict, ts_local: int) -> BookSnapshot | None:
        buffered = self._buffer
        self._buffer = []
        self.book.load(_levels(snap["bids"]), _levels(snap["asks"]))
        self._snapshot_id = snap["lastUpdateId"]
        self._last_u = None
        self.synced = True
        ts_exchange = ms_to_ns(snap["T"])
        for data, ts in buffered:
            if not self._process(data, ts):
                if not self.synced:
                    return None
                continue
            ts_exchange = ms_to_ns(data["T"])
        if self._gap_start is not None and self.on_gap is not None:
            self.on_gap(
                StreamGap(
                    venue_id=VENUE_ID,
                    canonical=self.canonical,
                    stream="book",
                    ts_start=self._gap_start,
                    ts_end=ts_local,
                    reason=self._gap_reason or "",
                )
            )
        self._gap_start = self._gap_reason = None
        return self._emit(ts_exchange, ts_local)

    def _process(self, data: dict, ts_local: int) -> bool:
        """Apply one diff if it is in sequence. False = dropped or sync lost."""
        U, u, pu = data["U"], data["u"], data["pu"]
        if self._last_u is None:
            if u < self._snapshot_id:
                return False
            if U > self._snapshot_id:
                self.invalidate(f"snapshot {self._snapshot_id} older than event U={U}", ts_local)
                self._buffer.append((data, ts_local))
                return False
        elif pu != self._last_u:
            self.invalidate(f"sequence gap: pu={pu} expected {self._last_u}", ts_local)
            self._buffer.append((data, ts_local))
            return False
        self.book.apply(_levels(data["b"]), _levels(data["a"]))
        self._last_u = u
        return True

    def _emit(self, ts_exchange: int, ts_local: int) -> BookSnapshot:
        bids, asks = self.book.top(self.depth_levels)
        return BookSnapshot(
            venue_id=VENUE_ID,
            canonical=self.canonical,
            ts_exchange=ts_exchange,
            ts_local=ts_local,
            bids=bids,
            asks=asks,
            seq=self._last_u if self._last_u is not None else self._snapshot_id,
        )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class BinanceUsdmAdapter:
    """Implements VenueAdapter for Binance USDⓈ-M perpetuals (read-only)."""

    def __init__(
        self,
        venue: Venue,
        *,
        quote_ccy: str,
        depth_levels: int,
        depth_update_ms: int,
        reconnect_backoff_s: Sequence[float],
        on_gap: GapCallback | None = None,
        http: httpx.AsyncClient | None = None,
        connect: Callable[[str], Any] = ws_connect,
        clock: Callable[[], int] = time.time_ns,
        rest_base: str = REST_BASE,
        ws_base: str = WS_BASE,
    ) -> None:
        self.venue = venue
        self.quote_ccy = quote_ccy
        self.depth_levels = depth_levels
        self.depth_update_ms = depth_update_ms
        self.reconnect_backoff_s = list(reconnect_backoff_s)
        self.on_gap = on_gap
        self._http = http or httpx.AsyncClient(timeout=10.0)
        self._connect = connect
        self._clock = clock
        self._rest_base = rest_base
        self._ws_base = ws_base
        self._instruments: dict[str, Instrument] = {}

    @classmethod
    def from_config(cls, cfg: dict, **kwargs: Any) -> BinanceUsdmAdapter:
        """Build from a loaded params/census.yaml."""
        v = cfg["venues"][VENUE_ID]
        venue = Venue(
            venue_id=VENUE_ID,
            kind=VenueKind(v["kind"]),
            maker_fee_bps=Decimal(str(v["maker_fee_bps"])),
            taker_fee_bps=Decimal(str(v["taker_fee_bps"])),
            funding_interval_h=int(v["funding_interval_h"]),
        )
        return cls(
            venue,
            quote_ccy=v["quote_ccy"],
            depth_levels=cfg["recording"]["book_depth_levels"],
            depth_update_ms=v["depth_update_ms"],
            reconnect_backoff_s=cfg["recording"]["reconnect_backoff_s"],
            **kwargs,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    # -- REST ---------------------------------------------------------------

    async def _get(self, path: str, **params: Any) -> Any:
        resp = await self._http.get(self._rest_base + path, params=params or None)
        resp.raise_for_status()
        return loads(resp.content)

    async def load_instruments(self) -> list[Instrument]:
        instruments = parse_exchange_info(await self._get("/fapi/v1/exchangeInfo"), self.quote_ccy)
        self._instruments = {i.canonical: i for i in instruments}
        return instruments

    async def _resolve(self, canonicals: list[str]) -> dict[str, str]:
        """venue_symbol -> canonical."""
        if not self._instruments:
            await self.load_instruments()
        missing = [c for c in canonicals if c not in self._instruments]
        if missing:
            raise KeyError(f"no tradeable {self.quote_ccy} perpetual on binance for {missing}")
        return {self._instruments[c].venue_symbol: c for c in canonicals}

    async def poll_funding(self, canonicals: list[str]) -> list[FundingObs]:
        symbols = await self._resolve(canonicals)

        async def one(sym: str, canonical: str) -> FundingObs:
            data = await self._get("/fapi/v1/premiumIndex", symbol=sym)
            return parse_premium_index(data, canonical, self._clock())

        return list(await asyncio.gather(*(one(s, c) for s, c in symbols.items())))

    # -- Streams ------------------------------------------------------------

    async def _messages(
        self,
        route: str,
        streams: list[str],
        on_disconnect: Callable[[str, int], None],
        on_connect: Callable[[int], None],
    ) -> AsyncIterator[tuple[dict, int]]:
        """Combined-stream payloads with receipt time, reconnecting forever with backoff."""
        url = f"{self._ws_base}/{route}/stream?streams={'/'.join(streams)}"
        attempt = 0
        while True:
            try:
                async with self._connect(url) as ws:
                    on_connect(self._clock())
                    async for raw in ws:
                        ts_local = self._clock()
                        attempt = 0
                        msg = loads(raw)
                        if "data" in msg:
                            yield msg["data"], ts_local
                reason = "connection closed by server"
            except (WebSocketException, OSError, httpx.HTTPError) as exc:
                reason = f"{type(exc).__name__}: {exc}"
            on_disconnect(reason, self._clock())
            delay = self.reconnect_backoff_s[min(attempt, len(self.reconnect_backoff_s) - 1)]
            attempt += 1
            log.warning("binance %s stream down (%s); reconnecting in %ss", route, reason, delay)
            await asyncio.sleep(delay)

    async def stream_books(self, canonicals: list[str]) -> AsyncIterator[BookSnapshot]:
        symbols = await self._resolve(canonicals)
        syncs = {sym: BookSync(c, self.depth_levels, self.on_gap) for sym, c in symbols.items()}
        streams = [f"{s.lower()}@depth@{self.depth_update_ms}ms" for s in symbols]

        def on_disconnect(reason: str, ts: int) -> None:
            for sync in syncs.values():
                sync.invalidate(f"disconnect: {reason}", ts)

        async for data, ts_local in self._messages(
            "public", streams, on_disconnect, on_connect=lambda ts: None
        ):
            sync = syncs.get(data.get("s"))
            if sync is None or data.get("e") != "depthUpdate":
                continue
            snap = sync.on_event(data, ts_local)
            if snap is not None:
                yield snap
            elif sync.wants_snapshot:
                try:
                    raw = await self._get("/fapi/v1/depth", symbol=data["s"], limit=SNAPSHOT_LIMIT)
                except httpx.HTTPError as exc:
                    log.warning("binance %s depth snapshot failed: %s", data["s"], exc)
                    continue  # retried on the next event
                snap = sync.apply_snapshot(raw, self._clock())
                if snap is not None:
                    yield snap

    async def stream_trades(self, canonicals: list[str]) -> AsyncIterator[TradePrint]:
        symbols = await self._resolve(canonicals)
        streams = [f"{s.lower()}@aggTrade" for s in symbols]
        down: dict[str, Any] = {}

        def on_disconnect(reason: str, ts: int) -> None:
            down.setdefault("ts", ts)
            down.setdefault("reason", reason)

        def on_connect(ts: int) -> None:
            if down and self.on_gap is not None:
                for c in symbols.values():
                    self.on_gap(StreamGap(VENUE_ID, c, "trades", down["ts"], ts, down["reason"]))
            down.clear()

        async for data, ts_local in self._messages("market", streams, on_disconnect, on_connect):
            canonical = symbols.get(data.get("s"))
            if canonical is not None and data.get("e") == "aggTrade":
                yield parse_agg_trade(data, canonical, ts_local)

    # -- Phase 3 only -------------------------------------------------------

    async def place_order(self, *args, **kwargs):
        raise NotImplementedError("Phase 1 is read-only")

    async def cancel_order(self, *args, **kwargs):
        raise NotImplementedError("Phase 1 is read-only")
