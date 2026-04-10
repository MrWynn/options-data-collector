from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Sequence

from collector_config import CollectorConfig
from collector_models import MatchedOption, RuntimePlan, StreamGroup


def chunked(values: Sequence[str], size: int) -> list[tuple[str, ...]]:
    if size <= 0:
        raise ValueError("size must be positive")
    return [tuple(values[index : index + size]) for index in range(0, len(values), size)]


def build_runtime_plan(
    matches: Iterable[MatchedOption],
    config: CollectorConfig,
) -> RuntimePlan:
    ordered_matches = tuple(matches)
    by_underlying: dict[str, list[MatchedOption]] = defaultdict(list)
    for match in ordered_matches:
        by_underlying[match.underlying].append(match)

    binance_groups: list[StreamGroup] = []
    deribit_groups: list[StreamGroup] = []

    orderbook_streams = sorted(
        {
            f"{item.binance_symbol.lower()}@depth{config.binance_orderbook_depth}@100ms"
            for item in ordered_matches
        }
    )
    for index, streams in enumerate(
        chunked(orderbook_streams, config.binance_max_streams_per_connection),
        start=1,
    ):
        binance_groups.append(
            StreamGroup(
                exchange="binance",
                data_type="orderbook",
                name=f"binance-orderbook-{index}",
                streams=streams,
            )
        )

    trade_streams = sorted({f"{underlying.lower()}usdt@optionTrade" for underlying in by_underlying})
    for index, streams in enumerate(
        chunked(trade_streams, config.binance_max_streams_per_connection),
        start=1,
    ):
        binance_groups.append(
            StreamGroup(
                exchange="binance",
                data_type="trade",
                name=f"binance-trade-{index}",
                streams=streams,
            )
        )

    deribit_books = sorted(
        {
            (
                f"book.{item.deribit_instrument_name}."
                f"none.{config.deribit_orderbook_depth}.100ms"
            )
            for item in ordered_matches
        }
    )
    for index, streams in enumerate(
        chunked(deribit_books, config.deribit_max_channels_per_connection),
        start=1,
    ):
        deribit_groups.append(
            StreamGroup(
                exchange="deribit",
                data_type="orderbook",
                name=f"deribit-orderbook-{index}",
                streams=streams,
            )
        )

    deribit_trades = sorted({f"trades.option.{underlying}.100ms" for underlying in by_underlying})
    for index, streams in enumerate(
        chunked(deribit_trades, config.deribit_max_channels_per_connection),
        start=1,
    ):
        deribit_groups.append(
            StreamGroup(
                exchange="deribit",
                data_type="trade",
                name=f"deribit-trade-{index}",
                streams=streams,
            )
        )

    return RuntimePlan(
        matches=ordered_matches,
        binance_groups=tuple(binance_groups),
        deribit_groups=tuple(deribit_groups),
    )
