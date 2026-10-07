"""Venue symbol -> canonical asset mapping, shared by every adapter.

The mapping table lives in `params/instruments.yaml` (`symbol_overrides.<venue>`).
A symbol that is not listed maps to the venue's own base asset with
`contract_mult = 1`.

`contract_mult` is the number of canonical units in one unit of venue quantity
(Binance `1000PEPEUSDT` quotes the price of 1000 PEPE, so its contract_mult is 1000).
Adapters emit market data in canonical units, so records with the same `canonical`
are directly comparable across venues: price / contract_mult, quantity × contract_mult.
`Instrument.tick_size` and `lot_size` stay in venue units.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal

from vortex.schema.models import Instrument, PriceLevel


@dataclass(frozen=True)
class SymbolOverride:
    canonical: str
    contract_mult: Decimal = Decimal(1)


SymbolMap = Mapping[str, SymbolOverride]  # venue_symbol -> override


def load_symbol_map(instruments_cfg: Mapping | None, venue_id: str) -> dict[str, SymbolOverride]:
    """Overrides for one venue from a loaded params/instruments.yaml (None = no overrides)."""
    raw = ((instruments_cfg or {}).get("symbol_overrides") or {}).get(venue_id) or {}
    out = {}
    for venue_symbol, entry in raw.items():
        mult = Decimal(str(entry.get("contract_mult", 1)))
        if mult <= 0:
            raise ValueError(f"{venue_id} {venue_symbol}: contract_mult must be > 0")
        out[str(venue_symbol)] = SymbolOverride(str(entry["canonical"]), mult)
    return out


def resolve(symbol_map: SymbolMap, venue_symbol: str, base_asset: str) -> SymbolOverride:
    return symbol_map.get(venue_symbol) or SymbolOverride(base_asset)


def check_unique(instruments: Iterable[Instrument]) -> None:
    """Two venue symbols claiming one canonical would silently overwrite each other.

    Adapters call this on the instruments they were asked to stream, not the whole venue.
    """
    seen: dict[str, str] = {}
    for i in instruments:
        if i.canonical in seen:
            raise ValueError(
                f"{i.venue_id}: {seen[i.canonical]} and {i.venue_symbol} both map to "
                f"canonical {i.canonical}; fix params/instruments.yaml"
            )
        seen[i.canonical] = i.venue_symbol


def canonical_px(px: Decimal, inst: Instrument) -> Decimal:
    return px if inst.contract_mult == 1 else px / inst.contract_mult


def canonical_qty(qty: Decimal, inst: Instrument) -> Decimal:
    return qty if inst.contract_mult == 1 else qty * inst.contract_mult


def canonical_levels(levels: list[PriceLevel], inst: Instrument) -> list[PriceLevel]:
    if inst.contract_mult == 1:
        return levels
    return [(canonical_px(p, inst), canonical_qty(q, inst)) for p, q in levels]
