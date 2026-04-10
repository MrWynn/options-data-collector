from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from options_data import TARGET_EXPIRIES


@dataclass(frozen=True)
class CollectorConfig:
    underlyings: tuple[str, ...] = ("BTC",)
    target_expiries: frozenset[str] = frozenset(TARGET_EXPIRIES)
    output_dir: Path = Path("data")
    binance_orderbook_depth: int = 20
    deribit_orderbook_depth: int = 20
    binance_max_streams_per_connection: int = 150
    deribit_max_channels_per_connection: int = 100
    sink_flush_rows: int = 500
    sink_flush_interval_seconds: float = 0.25
    sink_queue_maxsize: int = 50_000
    sink_open_file_limit: int = 512
    reconnect_min_seconds: float = 1.0
    reconnect_max_seconds: float = 60.0
    deribit_heartbeat_interval: int = 30
