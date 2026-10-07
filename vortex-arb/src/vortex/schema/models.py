"""Vortex record types. Single source of truth for all data structures.

Conventions (see CLAUDE.md):
- Money and prices: Decimal, never float.
- Timestamps: int nanoseconds UTC. Market data records carry ts_exchange AND ts_local.
- Spreads: bps relative to hedge venue (Binance) mid. Sizes: USD notional.
- venue_a = DEX (quoting/arb venue), venue_b = Binance (hedge venue).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class VenueKind(str, Enum):
    CEX = "cex"
    PERP_DEX = "perp_dex"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class Direction(str, Enum):
    SELL_A_BUY_B = "sell_a_buy_b"  # DEX rich vs Binance
    BUY_A_SELL_B = "buy_a_sell_b"  # DEX cheap vs Binance


class Strategy(str, Enum):
    TAKER_TAKER = "taker_taker"
    MAKER_HEDGE = "maker_hedge"


PriceLevel = tuple[Decimal, Decimal]  # (price, quantity in base units)


# ---------------------------------------------------------------------------
# 1. Reference data (static; loaded from config / venue REST)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Venue:
    venue_id: str                  # "binance", "hyperliquid", "papertrade", "variational"
    kind: VenueKind
    maker_fee_bps: Decimal         # negative = rebate
    taker_fee_bps: Decimal
    funding_interval_h: int        # 8, 4, 1
    est_latency_ms: int | None = None  # measured during census, not assumed


@dataclass(frozen=True)
class Instrument:
    venue_id: str
    venue_symbol: str              # "ETHUSDT", "ETH", ...
    canonical: str                 # cross-venue join key: "ETH"
    quote_ccy: str                 # "USDT" | "USDC" — do not treat as equal silently
    tick_size: Decimal
    lot_size: Decimal
    contract_mult: Decimal = Decimal(1)  # canonical units per venue qty unit (adapters/symbols.py)


# ---------------------------------------------------------------------------
# 2. Market data (raw, append-only)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BookSnapshot:
    venue_id: str
    canonical: str
    ts_exchange: int
    ts_local: int
    bids: list[PriceLevel]         # best first, top N levels
    asks: list[PriceLevel]
    seq: int | None = None         # venue sequence/update id, for gap checks


@dataclass(frozen=True)
class TradePrint:
    venue_id: str
    canonical: str
    ts_exchange: int
    ts_local: int
    px: Decimal
    qty: Decimal
    aggressor: Side
    trade_id: str | None = None


@dataclass(frozen=True)
class FundingObs:
    venue_id: str
    canonical: str
    ts_exchange: int
    ts_local: int
    current_rate: Decimal          # live rate for the current interval (paid at next_funding_ts)
    predicted_rate: Decimal | None
    mark_px: Decimal
    oracle_px: Decimal | None
    funding_interval_h: int        # interval current_rate applies to, as of this observation
    next_funding_ts: int | None = None


@dataclass(frozen=True)
class StreamGap:
    """Logged whenever a stream disconnects or a sequence gap forces a resync."""
    venue_id: str
    canonical: str | None
    stream: str                    # "book" | "trades" | "funding"
    ts_start: int
    ts_end: int
    reason: str


# ---------------------------------------------------------------------------
# 3. Derived: spread observations and episodes (Phase 1 output)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SpreadObs:
    ts: int                        # ts_local of the update that triggered this calc
    canonical: str
    venue_a: str                   # DEX
    venue_b: str                   # Binance (hedge)
    direction: Direction
    size_usd: Decimal              # size tier
    mid_b: Decimal
    vwap_a: Decimal | None         # None = insufficient depth at this tier
    vwap_b: Decimal | None
    gross_bps: Decimal | None
    fee_bps: Decimal
    net_bps: Decimal | None        # gross - fees; NO buffer applied
    staleness_ms: int
    stale: bool                    # staleness_ms > max_staleness_ms


@dataclass(frozen=True)
class Opportunity:
    """Episode of consecutive SpreadObs with net_bps >= threshold. Built offline."""
    opp_id: str
    canonical: str
    venue_a: str
    venue_b: str
    direction: Direction
    size_usd: Decimal
    threshold_bps: Decimal
    t_start: int
    t_end: int
    duration_ms: int
    peak_net_bps: Decimal
    mean_net_bps: Decimal
    n_obs: int


# ---------------------------------------------------------------------------
# 4. Simulation (Phase 2)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SimQuote:
    ts: int
    venue_id: str
    canonical: str
    side: Side
    px: Decimal
    size_usd: Decimal
    ref_mid: Decimal               # Binance mid it was priced off
    run_id: str


@dataclass(frozen=True)
class SimFill:
    fill_id: str
    run_id: str
    ts: int
    strategy: Strategy
    venue_id: str
    canonical: str
    side: Side
    px: Decimal
    size_usd: Decimal
    fee_usd: Decimal
    is_hedge: bool
    parent_fill_id: str | None = None   # hedge leg -> its DEX fill
    hedge_delay_ms: int | None = None


@dataclass(frozen=True)
class Markout:
    fill_id: str
    ref_mid_at_fill: Decimal
    markout_bps_1s: Decimal
    markout_bps_5s: Decimal
    markout_bps_30s: Decimal


@dataclass(frozen=True)
class PnLAttribution:
    trade_id: str
    run_id: str
    strategy: Strategy
    canonical: str
    t_open: int
    t_close: int
    spread_captured_usd: Decimal
    fees_usd: Decimal
    slippage_usd: Decimal
    funding_usd: Decimal
    adverse_selection_usd: Decimal
    net_usd: Decimal


@dataclass(frozen=True)
class SimRun:
    """One parameter set evaluated over one date range."""
    run_id: str
    params_hash: str
    params_path: str
    data_start: int
    data_end: int
    split: str                     # "train" | "test"
    created_ts: int


# ---------------------------------------------------------------------------
# 5. Execution state (Phase 3 — defined now so sim and live share shapes)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MarginAccount:
    venue_id: str
    ts: int
    equity_usd: Decimal
    used_margin_usd: Decimal
    free_margin_usd: Decimal


@dataclass(frozen=True)
class Position:
    venue_id: str
    canonical: str
    ts: int
    qty: Decimal                   # signed, base units
    avg_px: Decimal
    unrealised_pnl_usd: Decimal
    funding_accrued_usd: Decimal = field(default=Decimal(0))
