from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping


@dataclass(frozen=True)
class MatchedOption:
    underlying: str
    expiry_date: str
    strike_price: str
    option_type: str
    deribit_instrument_name: str
    binance_symbol: str


@dataclass(frozen=True)
class CollectorEvent:
    exchange: str
    data_type: str
    instrument_name: str
    row: Mapping[str, str]

    @property
    def utc_date(self) -> str:
        received_time = self.row.get("received_time", "")
        try:
            return datetime.fromtimestamp(
                int(received_time) / 1000,
                tz=timezone.utc,
            ).date().isoformat()
        except (TypeError, ValueError):
            return datetime.now(timezone.utc).date().isoformat()


@dataclass(frozen=True)
class StreamGroup:
    exchange: str
    data_type: str
    name: str
    streams: tuple[str, ...]


@dataclass(frozen=True)
class RuntimePlan:
    matches: tuple[MatchedOption, ...]
    binance_groups: tuple[StreamGroup, ...]
    deribit_groups: tuple[StreamGroup, ...]
