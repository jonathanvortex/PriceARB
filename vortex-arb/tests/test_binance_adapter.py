"""Binance USDⓈ-M adapter tests against tests/fixtures/binance."""

from __future__ import annotations

import asyncio
import itertools
import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import yaml
from websockets.exceptions import ConnectionClosedError

from vortex.adapters.binance import (
    BinanceUsdmAdapter,
    BookSync,
    loads,
    parse_agg_trade,
    parse_exchange_info,
    parse_funding_info,
    parse_premium_index,
)
from vortex.adapters.symbols import SymbolOverride, load_symbol_map
from vortex.schema.models import Instrument, Side, VenueKind

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "binance"
L = 7500000000100  # lastUpdateId of depth_snapshot_ethusdt.json
D = Decimal


def inst(canonical="ETH", venue_symbol="ETHUSDT", mult="1") -> Instrument:
    return Instrument("binance", venue_symbol, canonical, "USDT", D("0.01"), D("0.001"), D(mult))


ETH = inst()
PEPE = inst("PEPE", "1000PEPEUSDT", "1000")
PEPE_MAP = {"1000PEPEUSDT": SymbolOverride("PEPE", D(1000))}


def load(name: str):
    return loads((FIX / name).read_bytes())


def load_lines(name: str) -> list[str]:
    return (FIX / name).read_text().splitlines()


def depth_events() -> list[dict]:
    return [loads(line)["data"] for line in load_lines("depth_updates_ethusdt.jsonl")]


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def test_exchange_info_keeps_only_trading_usdt_perps():
    instruments = parse_exchange_info(load("exchange_info.json"), "USDT")
    by_canonical = {i.canonical: i for i in instruments}
    assert sorted(by_canonical) == ["1000PEPE", "BTC", "ETH", "SOL"]  # unmapped: baseAsset
    eth = by_canonical["ETH"]
    assert eth.venue_symbol == "ETHUSDT"
    assert eth.quote_ccy == "USDT"
    assert eth.tick_size == D("0.01")
    assert eth.lot_size == D("0.001")
    assert eth.contract_mult == 1
    assert by_canonical["BTC"].tick_size == D("0.10")


def test_exchange_info_applies_symbol_overrides():
    instruments = parse_exchange_info(load("exchange_info.json"), "USDT", PEPE_MAP)
    by_canonical = {i.canonical: i for i in instruments}
    pepe = by_canonical["PEPE"]
    assert "1000PEPE" not in by_canonical
    assert (pepe.venue_symbol, pepe.contract_mult) == ("1000PEPEUSDT", D(1000))
    assert pepe.tick_size == D("0.0000001") and pepe.lot_size == D("1")  # venue units
    assert by_canonical["ETH"].contract_mult == 1


async def test_two_symbols_on_one_requested_canonical_is_an_error():
    clash = {"1000PEPEUSDT": {"canonical": "ETH", "contract_mult": 1000}}
    adapter = make_adapter(instruments_cfg={"symbol_overrides": {"binance": clash}})
    with pytest.raises(ValueError, match="both map to canonical ETH"):
        await adapter.poll_funding(["ETH"])
    [btc] = await adapter.poll_funding(["BTC"])  # clash outside the requested universe: fine
    assert btc.canonical == "BTC"


def test_symbol_map_loading():
    cfg = yaml.safe_load((ROOT / "params" / "instruments.yaml").read_text())
    assert load_symbol_map(cfg, "binance") == {}  # only commented examples so far
    assert load_symbol_map(None, "binance") == {}
    pepe = {"1000PEPEUSDT": {"canonical": "PEPE", "contract_mult": 1000}}
    cfg = {"symbol_overrides": {"binance": pepe}}
    assert load_symbol_map(cfg, "binance") == PEPE_MAP
    assert load_symbol_map(cfg, "hyperliquid") == {}
    bad = {"symbol_overrides": {"binance": {"X": {"canonical": "X", "contract_mult": 0}}}}
    with pytest.raises(ValueError):
        load_symbol_map(bad, "binance")


def test_exchange_info_usdc_is_a_separate_universe():
    instruments = parse_exchange_info(load("exchange_info.json"), "USDC")
    assert [(i.venue_symbol, i.quote_ccy) for i in instruments] == [("ETHUSDC", "USDC")]


def test_agg_trade_aggressor_and_units():
    msgs = [loads(line)["data"] for line in load_lines("agg_trades.jsonl")]
    buy = parse_agg_trade(msgs[0], ETH, ts_local=5)
    assert buy.aggressor is Side.BUY  # m=false: buyer was taker
    assert buy.px == D("2450.11") and buy.qty == D("1.250")
    assert buy.ts_exchange == 1791360000148 * 1_000_000
    assert buy.ts_local == 5
    assert buy.trade_id == "2834791021"
    sell = parse_agg_trade(msgs[1], ETH, ts_local=6)
    assert sell.aggressor is Side.SELL  # m=true: buyer was maker
    assert sell.qty == D("0.400")  # q (all fills), not nq (excludes RPI)


def test_scaled_contract_trade_is_in_canonical_units():
    data = {
        "e": "aggTrade",
        "s": "1000PEPEUSDT",
        "a": 1,
        "p": "0.0123450",
        "q": "250000",
        "T": 1791360000000,
        "m": False,
    }
    t = parse_agg_trade(data, PEPE, ts_local=1)
    assert t.canonical == "PEPE"
    assert t.px == D("0.0000123450")  # per PEPE
    assert t.qty == D("250000000")  # PEPE
    assert t.px * t.qty == D("0.0123450") * D("250000")  # notional unchanged


def test_premium_index_to_funding_obs():
    obs = parse_premium_index(load("premium_index_ethusdt.json"), ETH, 4, ts_local=7)
    assert obs.venue_id == "binance" and obs.canonical == "ETH"
    assert obs.current_rate == D("0.00007531")
    assert obs.predicted_rate is None
    assert obs.mark_px == D("2450.11250000")
    assert obs.oracle_px == D("2449.87412345")
    assert obs.ts_exchange == 1791360000008 * 1_000_000
    assert obs.next_funding_ts == 1791388800000 * 1_000_000
    assert obs.funding_interval_h == 4
    assert all(isinstance(v, Decimal) for v in (obs.current_rate, obs.mark_px, obs.oracle_px))


def test_funding_info_intervals():
    assert parse_funding_info(load("funding_info.json")) == {"ETHUSDT": 4, "BLZUSDT": 8}


# ---------------------------------------------------------------------------
# Book sync
# ---------------------------------------------------------------------------


def test_book_sync_happy_path_then_gap_then_resync():
    gaps = []
    sync = BookSync(ETH, depth_levels=3, on_gap=gaps.append)
    ev = depth_events()

    assert not sync.wants_snapshot  # never fetch before the stream delivers
    assert sync.on_event(ev[0], ts_local=100) is None
    assert sync.wants_snapshot

    snap = sync.apply_snapshot(load("depth_snapshot_ethusdt.json"), ts_local=110)
    assert snap is not None and snap.seq == L  # ev[0] dropped: u < lastUpdateId
    assert snap.bids[0] == (D("2450.10"), D("3.500"))
    assert snap.ts_exchange == 1791360000110 * 1_000_000

    s1 = sync.on_event(ev[1], ts_local=200)  # U <= L <= u
    assert s1.seq == L + 5 and s1.ts_local == 200
    assert s1.bids[0] == (D("2450.10"), D("4.000"))
    assert s1.asks[0] == (D("2450.11"), D("1.500"))
    assert s1.ts_exchange == (1791360000100 - 3) * 1_000_000  # T, not E

    s2 = sync.on_event(ev[2], ts_local=300)  # removes unknown ask 2450.13: fine
    assert s2.bids == [
        (D("2450.10"), D("4.000")),
        (D("2450.08"), D("2.000")),
        (D("2450.05"), D("8.000")),
    ]

    s3 = sync.on_event(ev[3], ts_local=400)
    assert s3.asks == [
        (D("2450.12"), D("0.800")),
        (D("2450.15"), D("1.000")),
        (D("2450.20"), D("5.000")),
    ]
    assert gaps == []

    assert sync.on_event(ev[4], ts_local=500) is None  # pu gap
    assert not sync.synced and sync.wants_snapshot
    assert sync.book.bids == {}  # stale book discarded, never served

    resync = sync.apply_snapshot(load("depth_snapshot_ethusdt_resync.json"), ts_local=550)
    assert resync.seq == L + 55  # gap event replayed on top of the new snapshot
    assert len(gaps) == 1
    g = gaps[0]
    assert (g.venue_id, g.canonical, g.stream, g.ts_start, g.ts_end) == (
        "binance",
        "ETH",
        "book",
        500,
        550,
    )
    assert "pu=" in g.reason

    s5 = sync.on_event(ev[5], ts_local=600)
    assert s5.seq == L + 60
    assert s5.bids[:2] == [(D("2450.12"), D("1.000")), (D("2450.10"), D("4.000"))]


def test_book_sync_snapshot_older_than_buffer_refetches():
    gaps = []
    sync = BookSync(ETH, depth_levels=5, on_gap=gaps.append)
    ev = depth_events()
    sync.on_event(ev[2], ts_local=100)  # U = L+6 > snapshot lastUpdateId
    assert sync.apply_snapshot(load("depth_snapshot_ethusdt.json"), ts_local=110) is None
    assert not sync.synced and sync.wants_snapshot
    assert sync.book.asks == {}


def test_book_sync_scaled_contract_emits_canonical_units():
    sync = BookSync(PEPE, depth_levels=1)
    sync.on_event({"U": 9, "u": 11, "pu": 8, "T": 1, "b": [], "a": []}, ts_local=1)
    snap = sync.apply_snapshot(
        {"lastUpdateId": 10, "T": 1, "bids": [["0.0123450", "1000"]], "asks": [["0.0123460", "2"]]},
        ts_local=2,
    )
    assert snap.canonical == "PEPE"
    assert snap.bids == [(D("0.0000123450"), D("1000000"))]
    assert snap.asks == [(D("0.0000123460"), D("2000"))]
    assert sync.book.bids == {D("0.0123450"): D("1000")}  # local book stays in venue units


def test_book_sync_initial_sync_is_not_a_gap():
    gaps = []
    sync = BookSync(ETH, depth_levels=5, on_gap=gaps.append)
    sync.on_event(depth_events()[0], ts_local=1)
    sync.apply_snapshot(load("depth_snapshot_ethusdt.json"), ts_local=2)
    assert sync.synced and gaps == []


# ---------------------------------------------------------------------------
# Adapter end to end (fake websocket + mocked REST)
# ---------------------------------------------------------------------------


class FakeWS:
    def __init__(self, lines: list[str], error: bool = False):
        self.lines, self.error = lines, error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def __aiter__(self):
        for line in self.lines:
            yield line
        if self.error:
            raise ConnectionClosedError(None, None)


class FakeConnect:
    def __init__(self, *sessions: FakeWS):
        self.sessions = list(sessions)
        self.urls: list[str] = []

    def __call__(self, url: str):
        self.urls.append(url)
        if self.sessions:
            return self.sessions.pop(0)
        return _Hang()


class _Hang(FakeWS):
    def __init__(self):
        super().__init__([])

    async def __aiter__(self):
        await asyncio.Event().wait()
        yield ""  # pragma: no cover


def mock_http(snapshots: list[str]) -> httpx.AsyncClient:
    queue = list(snapshots)

    def handler(req: httpx.Request) -> httpx.Response:
        path = req.url.path
        if path == "/fapi/v1/exchangeInfo":
            body = (FIX / "exchange_info.json").read_bytes()
        elif path == "/fapi/v1/depth":
            assert req.url.params["limit"] == "1000"
            body = (FIX / queue.pop(0)).read_bytes()
        elif path == "/fapi/v1/fundingInfo":
            body = (FIX / "funding_info.json").read_bytes()
        elif path == "/fapi/v1/premiumIndex":
            data = json.loads((FIX / "premium_index_ethusdt.json").read_text())
            data["symbol"] = req.url.params["symbol"]
            body = json.dumps(data).encode()
        else:
            return httpx.Response(404)
        return httpx.Response(200, content=body)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def make_adapter(connect=None, snapshots=(), gaps=None, instruments_cfg=None):
    cfg = yaml.safe_load((ROOT / "params" / "census.yaml").read_text())
    if instruments_cfg is None:
        instruments_cfg = yaml.safe_load((ROOT / "params" / "instruments.yaml").read_text())
    cfg["recording"]["reconnect_backoff_s"] = [0]
    clock = itertools.count(1_000)
    return BinanceUsdmAdapter.from_config(
        cfg,
        instruments_cfg,
        on_gap=gaps.append if gaps is not None else None,
        http=mock_http(list(snapshots)),
        connect=connect or FakeConnect(),
        clock=lambda: next(clock),
    )


async def take(agen, n: int) -> list:
    out = []
    async for item in agen:
        out.append(item)
        if len(out) == n:
            break
    await agen.aclose()
    return out


def test_from_config_reads_params():
    a = make_adapter()
    assert a.venue.venue_id == "binance" and a.venue.kind is VenueKind.CEX
    assert a.venue.taker_fee_bps == D("5.0")
    assert (a.quote_ccy, a.depth_levels, a.depth_update_ms) == ("USDT", 20, 100)


async def test_stream_books_end_to_end_with_resync():
    gaps = []
    conn = FakeConnect(FakeWS(load_lines("depth_updates_ethusdt.jsonl")))
    adapter = make_adapter(
        conn, ["depth_snapshot_ethusdt.json", "depth_snapshot_ethusdt_resync.json"], gaps
    )
    books = await take(adapter.stream_books(["ETH"]), 6)
    assert conn.urls[0] == "wss://fstream.binance.com/public/stream?streams=ethusdt@depth@100ms"
    assert [b.seq for b in books] == [L, L + 5, L + 20, L + 30, L + 55, L + 60]
    assert all(b.canonical == "ETH" and b.venue_id == "binance" for b in books)
    assert all(b.ts_local > 0 and b.ts_exchange > 0 for b in books)
    assert len(gaps) == 1 and gaps[0].stream == "book"


async def test_stream_books_reconnect_logs_gap_and_resyncs():
    gaps = []
    lines = load_lines("depth_updates_ethusdt.jsonl")
    conn = FakeConnect(FakeWS(lines[:2], error=True), FakeWS(lines[:2]))
    adapter = make_adapter(
        conn, ["depth_snapshot_ethusdt.json", "depth_snapshot_ethusdt.json"], gaps
    )
    books = await take(adapter.stream_books(["ETH"]), 4)
    assert [b.seq for b in books] == [L, L + 5, L, L + 5]  # fresh snapshot after reconnect
    assert len(conn.urls) == 2
    assert len(gaps) == 1 and gaps[0].reason.startswith("disconnect: ConnectionClosedError")


async def test_stream_trades_and_reconnect_gap():
    gaps = []
    lines = load_lines("agg_trades.jsonl")
    conn = FakeConnect(FakeWS(lines, error=True), FakeWS(lines[:1]))
    adapter = make_adapter(conn, gaps=gaps)
    trades = await take(adapter.stream_trades(["ETH", "BTC"]), 4)
    assert conn.urls[0] == (
        "wss://fstream.binance.com/market/stream?streams=ethusdt@aggTrade/btcusdt@aggTrade"
    )
    assert [(t.canonical, t.aggressor) for t in trades] == [
        ("ETH", Side.BUY),
        ("ETH", Side.SELL),
        ("BTC", Side.SELL),
        ("ETH", Side.BUY),
    ]
    assert sorted(g.canonical for g in gaps) == ["BTC", "ETH"]
    assert all(g.stream == "trades" and g.ts_end > g.ts_start for g in gaps)


async def test_poll_funding_uses_live_per_symbol_interval():
    adapter = make_adapter()
    eth, btc = await adapter.poll_funding(["ETH", "BTC"])
    assert eth.canonical == "ETH" and eth.current_rate == D("0.00007531")
    assert eth.funding_interval_h == 4  # adjusted, from fundingInfo
    assert btc.canonical == "BTC" and btc.funding_interval_h == 8  # not listed: venue default
    assert eth.ts_local > 0


async def test_adapter_uses_symbol_map_from_params():
    pepe = {"1000PEPEUSDT": {"canonical": "PEPE", "contract_mult": 1000}}
    adapter = make_adapter(instruments_cfg={"symbol_overrides": {"binance": pepe}})
    [obs] = await adapter.poll_funding(["PEPE"])
    assert obs.canonical == "PEPE"
    assert obs.mark_px == D("2450.11250000") / 1000  # fixture prices, rescaled to per-PEPE


async def test_unknown_canonical_raises():
    with pytest.raises(KeyError):
        await make_adapter().poll_funding(["DOGE"])


async def test_order_methods_are_disabled():
    adapter = make_adapter()
    with pytest.raises(NotImplementedError):
        await adapter.place_order()
    with pytest.raises(NotImplementedError):
        await adapter.cancel_order()


# ---------------------------------------------------------------------------
# Real captures (from scripts/record_binance_fixtures.py), when present
# ---------------------------------------------------------------------------

RECORDED = FIX / "recorded"


@pytest.mark.skipif(not RECORDED.exists(), reason="no real captures recorded yet")
def test_recorded_captures_parse_and_sync():
    instruments = parse_exchange_info(loads((RECORDED / "exchange_info.json").read_bytes()), "USDT")
    assert {"BTC", "ETH", "SOL"} <= {i.canonical for i in instruments}
    for line in (RECORDED / "agg_trades.jsonl").read_text().splitlines():
        parse_agg_trade(json.loads(line)["data"], ETH, 0)
    parse_funding_info(loads((RECORDED / "funding_info.json").read_bytes()))
    parse_premium_index(loads((RECORDED / "premium_index_ethusdt.json").read_bytes()), ETH, 8, 0)

    gaps = []
    sync = BookSync(ETH, 20, gaps.append)
    lines = (RECORDED / "depth_updates_ethusdt.jsonl").read_text().splitlines()
    events = [loads(x)["data"] for x in lines]
    snap = loads((RECORDED / "depth_snapshot_ethusdt.json").read_bytes())
    for e in events:  # recorder fetched the snapshot mid-capture: buffer all, then replay
        sync.on_event(e, 0)
    book = sync.apply_snapshot(snap, 0)
    assert book is not None and sync.synced and gaps == []
    assert book.bids[0][0] < book.asks[0][0]
