from __future__ import annotations

import asyncio
import csv
import itertools
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, TextIO

from collector_config import CollectorConfig
from collector_deribit import (
    DERIBIT_WS_URL,
    _is_test_request,
    _prepare_subscription,
    _require_websockets,
    _send_request,
)
from collector_planner import chunked
from options_data import fetch_json, normalize_decimal


DERIBIT_INSTRUMENTS_URL = "https://www.deribit.com/api/v2/public/get_instruments"
EXPIRY_REFRESH_SECONDS = 60 * 60
SNAPSHOT_INTERVAL_SECONDS = 1.0
TICKER_INTERVAL = "100ms"
BOOK_SUBSCRIPTION_DEPTH = 10
CSV_BOOK_DEPTH = 5


@dataclass(frozen=True)
class DeribitOptionInstrument:
    instrument_name: str
    expiration_timestamp: int
    strike: str
    option_type: str


@dataclass(frozen=True)
class BookSnapshot:
    bids: tuple[tuple[object, object], ...]
    asks: tuple[tuple[object, object], ...]


def build_deribit_instruments_url(currency: str) -> str:
    return (
        f"{DERIBIT_INSTRUMENTS_URL}?currency={currency}"
        "&kind=option&expired=false"
    )


def parse_deribit_instruments(
    payload: Mapping[str, object],
    currency: str,
) -> list[DeribitOptionInstrument]:
    result = payload.get("result", [])
    if not isinstance(result, list):
        return []

    instruments: list[DeribitOptionInstrument] = []
    for item in result:
        if (
            not isinstance(item, dict)
            or item.get("is_active") is False
            or item.get("instrument_type") != "reversed"
            or item.get("base_currency") != currency
            or item.get("quote_currency") != currency
            or item.get("settlement_currency") != currency
        ):
            continue
        instrument_name = item.get("instrument_name")
        expiration_timestamp = item.get("expiration_timestamp")
        option_type = item.get("option_type")
        strike = item.get("strike")
        if (
            not isinstance(instrument_name, str)
            or not isinstance(expiration_timestamp, int)
            or option_type not in {"call", "put"}
            or strike is None
        ):
            continue
        instruments.append(
            DeribitOptionInstrument(
                instrument_name=instrument_name,
                expiration_timestamp=expiration_timestamp,
                strike=normalize_decimal(strike),
                option_type=str(option_type),
            )
        )

    return sorted(
        instruments,
        key=lambda item: (item.expiration_timestamp, item.instrument_name),
    )


def select_expiry_candidates(
    instruments: Iterable[DeribitOptionInstrument],
) -> list[DeribitOptionInstrument]:
    ordered = list(instruments)
    expirations = sorted({item.expiration_timestamp for item in ordered})
    selected_expirations = set(expirations[2:7])
    return [item for item in ordered if item.expiration_timestamp in selected_expirations]


def _book_side(values: object) -> tuple[tuple[object, object], ...]:
    if not isinstance(values, list):
        return ()
    levels: list[tuple[object, object]] = []
    for value in values:
        if isinstance(value, (list, tuple)) and len(value) >= 2:
            levels.append((value[0], value[1]))
    return tuple(levels)


class DeribitOptionsBookState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._candidates: dict[str, DeribitOptionInstrument] = {}
        self._groups: dict[tuple[int, str], tuple[str, ...]] = {}
        self._deltas: dict[str, float] = {}
        self._targets: dict[tuple[int, str], str] = {}
        self._books: dict[str, BookSnapshot] = {}
        self.target_updates: asyncio.Queue[frozenset[str]] = asyncio.Queue(maxsize=1)

    async def replace_candidates(self, instruments: Iterable[DeribitOptionInstrument]) -> None:
        candidates = {item.instrument_name: item for item in instruments}
        groups: dict[tuple[int, str], list[str]] = {}
        for item in candidates.values():
            groups.setdefault((item.expiration_timestamp, item.option_type), []).append(
                item.instrument_name
            )

        async with self._lock:
            previous_targets = frozenset(self._targets.values())
            self._candidates = candidates
            self._groups = {
                key: tuple(sorted(names))
                for key, names in groups.items()
            }
            self._deltas = {
                name: delta for name, delta in self._deltas.items() if name in candidates
            }
            self._recompute_targets()
            current_targets = frozenset(self._targets.values())
            self._books = {
                name: book for name, book in self._books.items() if name in current_targets
            }
            self._notify_target_change(previous_targets, current_targets)

    async def update_delta(self, instrument_name: str, delta: float) -> None:
        async with self._lock:
            instrument = self._candidates.get(instrument_name)
            if instrument is None:
                return
            previous_targets = frozenset(self._targets.values())
            self._deltas[instrument_name] = delta
            self._recompute_expiry(instrument.expiration_timestamp)
            current_targets = frozenset(self._targets.values())
            self._books = {
                name: book for name, book in self._books.items() if name in current_targets
            }
            self._notify_target_change(previous_targets, current_targets)

    async def update_book(self, instrument_name: str, bids: object, asks: object) -> None:
        async with self._lock:
            if instrument_name not in self._targets.values():
                return
            self._books[instrument_name] = BookSnapshot(
                bids=_book_side(bids),
                asks=_book_side(asks),
            )

    async def clear_books(self) -> None:
        async with self._lock:
            self._books.clear()

    async def current_targets(self) -> frozenset[str]:
        async with self._lock:
            return frozenset(self._targets.values())

    async def snapshot_rows(self, timestamp: int) -> list[dict[str, str]]:
        async with self._lock:
            rows: list[dict[str, str]] = []
            for key in sorted(self._targets):
                instrument_name = self._targets[key]
                instrument = self._candidates[instrument_name]
                book = self._books.get(instrument_name)
                delta = self._deltas.get(instrument_name)
                if book is None or delta is None:
                    continue
                row = {
                    "timestamp": str(timestamp),
                    "instrument_name": instrument.instrument_name,
                    "expiration_timestamp": str(instrument.expiration_timestamp),
                    "strike": instrument.strike,
                    "option_type": instrument.option_type,
                    "delta": str(delta),
                }
                for index in range(CSV_BOOK_DEPTH):
                    level = index + 1
                    bid = book.bids[index] if index < len(book.bids) else ("", "")
                    ask = book.asks[index] if index < len(book.asks) else ("", "")
                    row[f"bid_price_{level}"] = str(bid[0])
                    row[f"bid_amount_{level}"] = str(bid[1])
                    row[f"ask_price_{level}"] = str(ask[0])
                    row[f"ask_amount_{level}"] = str(ask[1])
                rows.append(row)
            return rows

    def _recompute_targets(self) -> None:
        self._targets.clear()
        for expiration_timestamp in {key[0] for key in self._groups}:
            self._recompute_expiry(expiration_timestamp)

    def _recompute_expiry(self, expiration_timestamp: int) -> None:
        selected: dict[tuple[int, str], str] = {}
        for option_type, target_delta in (("call", 0.3), ("put", -0.3)):
            key = (expiration_timestamp, option_type)
            eligible = [
                name for name in self._groups.get(key, ()) if name in self._deltas
            ]
            if not eligible:
                self._targets.pop((expiration_timestamp, "call"), None)
                self._targets.pop((expiration_timestamp, "put"), None)
                return
            selected[key] = min(
                eligible,
                key=lambda name: (abs(self._deltas[name] - target_delta), name),
            )
        self._targets.update(selected)

    def _notify_target_change(
        self,
        previous_targets: frozenset[str],
        current_targets: frozenset[str],
    ) -> None:
        if previous_targets == current_targets:
            return
        if self.target_updates.full():
            self.target_updates.get_nowait()
        self.target_updates.put_nowait(current_targets)


CSV_FIELDS = [
    "timestamp",
    "instrument_name",
    "expiration_timestamp",
    "strike",
    "option_type",
    "delta",
    *[
        field
        for level in range(1, CSV_BOOK_DEPTH + 1)
        for field in (
            f"bid_price_{level}",
            f"bid_amount_{level}",
            f"ask_price_{level}",
            f"ask_amount_{level}",
        )
    ],
]


class DailyOptionsBookCsvSink:
    def __init__(self, output_dir: Path, currency: str) -> None:
        self.output_dir = Path(output_dir)
        self.currency = currency.lower()
        self._date = ""
        self._handle: TextIO | None = None
        self._writer: csv.DictWriter | None = None

    def write_rows(self, timestamp: int, rows: list[dict[str, str]]) -> None:
        if not rows:
            return
        utc_date = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).date().isoformat()
        self._ensure_file(utc_date)
        assert self._writer is not None
        assert self._handle is not None
        self._writer.writerows(rows)
        self._handle.flush()

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
        self._handle = None
        self._writer = None
        self._date = ""

    def _ensure_file(self, utc_date: str) -> None:
        if self._date == utc_date and self._handle is not None:
            return
        self.close()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.output_dir / f"deribit_{self.currency}_options_books_{utc_date}.csv"
        should_write_header = not path.exists() or path.stat().st_size == 0
        if not should_write_header:
            with path.open("r", newline="", encoding="utf-8-sig") as handle:
                existing_header = next(csv.reader(handle), [])
            if existing_header != CSV_FIELDS:
                raise ValueError(f"unexpected CSV header in {path}")
        self._handle = path.open("a", newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._handle, fieldnames=CSV_FIELDS)
        self._date = utc_date
        if should_write_header:
            self._writer.writeheader()
            self._handle.flush()


async def handle_ticker_message(message: dict, state: DeribitOptionsBookState) -> None:
    if message.get("method") != "subscription":
        return
    params = message.get("params", {})
    channel = str(params.get("channel", ""))
    data = params.get("data")
    if not channel.startswith("ticker.") or not isinstance(data, dict):
        return
    instrument_name = data.get("instrument_name")
    greeks = data.get("greeks")
    if not isinstance(instrument_name, str) or not isinstance(greeks, dict):
        return
    delta = greeks.get("delta")
    if not isinstance(delta, (int, float)):
        return
    await state.update_delta(instrument_name, float(delta))


async def handle_book_message(message: dict, state: DeribitOptionsBookState) -> None:
    if message.get("method") != "subscription":
        return
    params = message.get("params", {})
    channel = str(params.get("channel", ""))
    data = params.get("data")
    if not channel.startswith("book.") or not isinstance(data, dict):
        return
    instrument_name = data.get("instrument_name")
    if not isinstance(instrument_name, str):
        return
    await state.update_book(
        instrument_name,
        bids=data.get("bids", []),
        asks=data.get("asks", []),
    )


async def _stream_ticker_group(
    instruments: tuple[str, ...],
    state: DeribitOptionsBookState,
    config: CollectorConfig,
    logger: logging.Logger,
) -> None:
    ws_lib = _require_websockets()
    channels = tuple(f"ticker.{name}.{TICKER_INTERVAL}" for name in instruments)
    backoff = config.reconnect_min_seconds
    while True:
        try:
            async with ws_lib.connect(
                DERIBIT_WS_URL,
                ping_interval=60,
                ping_timeout=60,
                open_timeout=15,
                close_timeout=5,
                max_queue=None,
            ) as websocket:
                await _prepare_subscription(
                    websocket,
                    channels=channels,
                    heartbeat_interval=config.deribit_heartbeat_interval,
                )
                logger.info("Deribit ticker connected: %s instruments", len(instruments))
                backoff = config.reconnect_min_seconds
                while True:
                    message = json.loads(await websocket.recv())
                    if _is_test_request(message):
                        await _send_request(websocket, 9_999, "public/test", {})
                        continue
                    await handle_ticker_message(message, state)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover
            logger.warning("Deribit ticker group failed: %s", exc)
            await asyncio.sleep(backoff + random.uniform(0.0, 0.5))
            backoff = min(backoff * 2, config.reconnect_max_seconds)


async def _run_candidate_supervisor(
    state: DeribitOptionsBookState,
    config: CollectorConfig,
    currency: str,
    logger: logging.Logger,
) -> None:
    workers: list[asyncio.Task] = []
    current_names: tuple[str, ...] = ()
    backoff = config.reconnect_min_seconds
    try:
        while True:
            try:
                payload = await asyncio.to_thread(
                    fetch_json,
                    build_deribit_instruments_url(currency),
                )
                instruments = parse_deribit_instruments(payload, currency)
                candidates = select_expiry_candidates(instruments)
                selected_expirations = {item.expiration_timestamp for item in candidates}
                if len(selected_expirations) != 5:
                    raise RuntimeError(
                        f"Deribit returned fewer than seven distinct active {currency} "
                        "reversed option expirations"
                    )
                candidate_names = tuple(item.instrument_name for item in candidates)
                if candidate_names != current_names:
                    await state.replace_candidates(candidates)
                    for worker in workers:
                        worker.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
                    workers = []
                    for index, names in enumerate(
                        chunked(candidate_names, config.deribit_max_channels_per_connection),
                        start=1,
                    ):
                        workers.append(
                            asyncio.create_task(
                                _stream_ticker_group(
                                    names,
                                    state=state,
                                    config=config,
                                    logger=logging.getLogger(
                                        f"deribit-{currency.lower()}-delta-{index}"
                                    ),
                                ),
                                name=f"deribit-{currency.lower()}-delta-{index}",
                            )
                        )
                    current_names = candidate_names
                    logger.info(
                        "Monitoring %s %s options across expirations %s",
                        len(candidates),
                        currency,
                        ",".join(str(value) for value in sorted(selected_expirations)),
                    )
                backoff = config.reconnect_min_seconds
                await asyncio.sleep(EXPIRY_REFRESH_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "Deribit %s option discovery failed; keeping current targets: %s",
                    currency,
                    exc,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, config.reconnect_max_seconds)
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


async def _run_book_manager(
    state: DeribitOptionsBookState,
    config: CollectorConfig,
    logger: logging.Logger,
) -> None:
    ws_lib = _require_websockets()
    request_ids = itertools.count(10_000)
    backoff = config.reconnect_min_seconds
    while True:
        try:
            async with ws_lib.connect(
                DERIBIT_WS_URL,
                ping_interval=60,
                ping_timeout=60,
                open_timeout=15,
                close_timeout=5,
                max_queue=None,
            ) as websocket:
                await state.clear_books()
                await _send_request(
                    websocket,
                    next(request_ids),
                    "public/set_heartbeat",
                    {"interval": config.deribit_heartbeat_interval},
                )
                subscribed = await state.current_targets()
                if subscribed:
                    await _send_request(
                        websocket,
                        next(request_ids),
                        "public/subscribe",
                        {
                            "channels": [
                                f"book.{name}.none.{BOOK_SUBSCRIPTION_DEPTH}.100ms"
                                for name in sorted(subscribed)
                            ]
                        },
                    )
                logger.info("Deribit book connected: %s targets", len(subscribed))
                backoff = config.reconnect_min_seconds

                receive_task = asyncio.create_task(websocket.recv())
                update_task = asyncio.create_task(state.target_updates.get())
                try:
                    while True:
                        done, _ = await asyncio.wait(
                            (receive_task, update_task),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if receive_task in done:
                            message = json.loads(receive_task.result())
                            if _is_test_request(message):
                                await _send_request(websocket, 9_999, "public/test", {})
                            else:
                                await handle_book_message(message, state)
                            receive_task = asyncio.create_task(websocket.recv())

                        if update_task in done:
                            targets = update_task.result()
                            while not state.target_updates.empty():
                                targets = state.target_updates.get_nowait()
                            removed = subscribed - targets
                            added = targets - subscribed
                            if removed:
                                await _send_request(
                                    websocket,
                                    next(request_ids),
                                    "public/unsubscribe",
                                    {
                                        "channels": [
                                            f"book.{name}.none.{BOOK_SUBSCRIPTION_DEPTH}.100ms"
                                            for name in sorted(removed)
                                        ]
                                    },
                                )
                            if added:
                                await _send_request(
                                    websocket,
                                    next(request_ids),
                                    "public/subscribe",
                                    {
                                        "channels": [
                                            f"book.{name}.none.{BOOK_SUBSCRIPTION_DEPTH}.100ms"
                                            for name in sorted(added)
                                        ]
                                    },
                                )
                            if added or removed:
                                logger.info(
                                    "Deribit book targets updated: +%s -%s total=%s",
                                    len(added),
                                    len(removed),
                                    len(targets),
                                )
                            subscribed = targets
                            update_task = asyncio.create_task(state.target_updates.get())
                finally:
                    receive_task.cancel()
                    update_task.cancel()
                    await asyncio.gather(receive_task, update_task, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover
            await state.clear_books()
            logger.warning("Deribit book connection failed: %s", exc)
            await asyncio.sleep(backoff + random.uniform(0.0, 0.5))
            backoff = min(backoff * 2, config.reconnect_max_seconds)


async def _run_snapshot_loop(
    state: DeribitOptionsBookState,
    sink: DailyOptionsBookCsvSink,
) -> None:
    loop = asyncio.get_running_loop()
    while True:
        started_at = loop.time()
        timestamp = time.time_ns() // 1_000_000
        rows = await state.snapshot_rows(timestamp)
        sink.write_rows(timestamp, rows)
        await asyncio.sleep(
            max(0.0, SNAPSHOT_INTERVAL_SECONDS - (loop.time() - started_at))
        )


async def _run_tasks(tasks: list[asyncio.Task]) -> None:
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def _run_currency_options_books(
    config: CollectorConfig,
    currency: str,
) -> None:
    currency_name = currency.lower()
    state = DeribitOptionsBookState()
    sink = DailyOptionsBookCsvSink(config.output_dir, currency)
    tasks = [
        asyncio.create_task(
            _run_candidate_supervisor(
                state,
                config,
                currency,
                logging.getLogger(f"deribit-{currency_name}-discovery"),
            ),
            name=f"deribit-{currency_name}-discovery",
        ),
        asyncio.create_task(
            _run_book_manager(
                state,
                config,
                logging.getLogger(f"deribit-{currency_name}-books"),
            ),
            name=f"deribit-{currency_name}-books",
        ),
        asyncio.create_task(
            _run_snapshot_loop(state, sink),
            name=f"deribit-{currency_name}-book-csv",
        ),
    ]
    try:
        await _run_tasks(tasks)
    finally:
        sink.close()


async def run_deribit_options_books(config: CollectorConfig) -> None:
    unsupported = sorted(set(config.underlyings) - {"BTC", "ETH"})
    if unsupported:
        raise ValueError(
            "deribit-books supports only BTC and ETH; unsupported: "
            + ",".join(unsupported)
        )
    if not config.underlyings:
        raise ValueError("deribit-books requires at least one underlying")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s [%(filename)s:%(lineno)d] %(message)s",
    )
    tasks = [
        asyncio.create_task(
            _run_currency_options_books(config, currency),
            name=f"deribit-{currency.lower()}",
        )
        for currency in dict.fromkeys(config.underlyings)
    ]
    await _run_tasks(tasks)
