from __future__ import annotations

import json
import time
from typing import Iterable

from collector_models import CollectorEvent


def utc_now_millis() -> str:
    return str(time.time_ns() // 1_000_000)


def raw_millis(timestamp_ms: int | float | str | None) -> str:
    if timestamp_ms is None:
        return ""
    if isinstance(timestamp_ms, str):
        return timestamp_ms.strip()
    return str(int(timestamp_ms))


def _stringify_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return json.dumps(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def _copy_scalar_fields(
    target: dict[str, str],
    payload: dict,
    excluded_keys: set[str] | None = None,
) -> dict[str, str]:
    skip = excluded_keys or set()
    for key, value in payload.items():
        if key in skip:
            continue
        target[str(key)] = _stringify_value(value)
    return target


def _flatten_orderbook(
    row: dict[str, str],
    bids: Iterable[Iterable[object]],
    asks: Iterable[Iterable[object]],
    depth: int,
    bid_prefix: str,
    ask_prefix: str,
) -> dict[str, str]:
    bid_list = list(bids)
    ask_list = list(asks)

    for index in range(depth):
        if index < len(bid_list):
            bid = list(bid_list[index])
            row[f"{bid_prefix}_price_{index}"] = str(bid[0]) if len(bid) > 0 else ""
            row[f"{bid_prefix}_size_{index}"] = str(bid[1]) if len(bid) > 1 else ""
        else:
            row[f"{bid_prefix}_price_{index}"] = ""
            row[f"{bid_prefix}_size_{index}"] = ""

        if index < len(ask_list):
            ask = list(ask_list[index])
            row[f"{ask_prefix}_price_{index}"] = str(ask[0]) if len(ask) > 0 else ""
            row[f"{ask_prefix}_size_{index}"] = str(ask[1]) if len(ask) > 1 else ""
        else:
            row[f"{ask_prefix}_price_{index}"] = ""
            row[f"{ask_prefix}_size_{index}"] = ""

    return row


def normalize_binance_message(
    message: dict,
    matched_symbols: set[str] | None,
    depth: int = 20,
) -> list[CollectorEvent]:
    payload = message.get("data", message)
    stream_name = str(message.get("stream", ""))
    received_time = utc_now_millis()
    event_type = payload.get("e")
    symbol = payload.get("s")
    if matched_symbols is not None and symbol not in matched_symbols:
        return []

    if event_type == "depthUpdate":
        row: dict[str, str] = {}
        _copy_scalar_fields(row, payload, excluded_keys={"b", "a"})
        if stream_name:
            row["stream"] = stream_name
        row["received_time"] = received_time
        return [
            CollectorEvent(
                exchange="binance",
                data_type="orderbook",
                instrument_name=symbol,
                row=_flatten_orderbook(
                    row,
                    bids=payload.get("b", []),
                    asks=payload.get("a", []),
                    depth=depth,
                    bid_prefix="b",
                    ask_prefix="a",
                ),
            )
        ]

    if event_type == "trade":
        row = _copy_scalar_fields({}, payload)
        if stream_name:
            row["stream"] = stream_name
        row["received_time"] = received_time
        return [
            CollectorEvent(
                exchange="binance",
                data_type="trade",
                instrument_name=symbol,
                row=row,
            )
        ]

    return []


def _normalize_deribit_trade(trade: dict, received_time: str) -> CollectorEvent:
    instrument_name = str(trade.get("instrument_name", ""))
    row = _copy_scalar_fields({}, trade)
    row["received_time"] = received_time
    return CollectorEvent(
        exchange="deribit",
        data_type="trade",
        instrument_name=instrument_name,
        row=row,
    )


def normalize_deribit_message(
    message: dict,
    matched_instruments: set[str] | None,
    depth: int = 20,
) -> list[CollectorEvent]:
    if message.get("method") != "subscription":
        return []

    params = message.get("params", {})
    channel = str(params.get("channel", ""))
    data = params.get("data")
    received_time = utc_now_millis()
    if not isinstance(data, (dict, list)):
        return []

    if channel.startswith("book.") and isinstance(data, dict):
        instrument_name = str(data.get("instrument_name", ""))
        if matched_instruments is not None and instrument_name not in matched_instruments:
            return []

        row = {
            "jsonrpc": _stringify_value(message.get("jsonrpc")),
            "method": _stringify_value(message.get("method")),
            "channel": channel,
        }
        _copy_scalar_fields(row, data, excluded_keys={"bids", "asks"})
        row["received_time"] = received_time
        return [
            CollectorEvent(
                exchange="deribit",
                data_type="orderbook",
                instrument_name=instrument_name,
                row=_flatten_orderbook(
                    row,
                    bids=data.get("bids", []),
                    asks=data.get("asks", []),
                    depth=depth,
                    bid_prefix="bids",
                    ask_prefix="asks",
                ),
            )
        ]

    if channel.startswith("trades.") and isinstance(data, list):
        records: list[CollectorEvent] = []
        for trade in data:
            instrument_name = str(trade.get("instrument_name", ""))
            if matched_instruments is None or instrument_name in matched_instruments:
                event = _normalize_deribit_trade(trade, received_time)
                row = dict(event.row)
                row["jsonrpc"] = _stringify_value(message.get("jsonrpc"))
                row["method"] = _stringify_value(message.get("method"))
                row["channel"] = channel
                records.append(
                    CollectorEvent(
                        exchange=event.exchange,
                        data_type=event.data_type,
                        instrument_name=event.instrument_name,
                        row=row,
                    )
                )
        return records

    return []
