"""Venue adapter interface (spec §3)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Protocol

from vortex.schema.models import BookSnapshot, FundingObs, Instrument, StreamGap, TradePrint, Venue

GapCallback = Callable[[StreamGap], None]


class VenueAdapter(Protocol):
    venue: Venue

    async def load_instruments(self) -> list[Instrument]: ...
    def stream_books(self, canonicals: list[str]) -> AsyncIterator[BookSnapshot]: ...
    def stream_trades(self, canonicals: list[str]) -> AsyncIterator[TradePrint]: ...
    async def poll_funding(self, canonicals: list[str]) -> list[FundingObs]: ...

    # Phase 3 only — raise NotImplementedError until then
    async def place_order(self, *args, **kwargs): ...
    async def cancel_order(self, *args, **kwargs): ...
