from __future__ import annotations

import asyncio
import csv
import logging
import os
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from collector_models import CollectorEvent


_SENTINEL = object()


@dataclass
class _OpenFile:
    handle: TextIO
    writer: csv.writer


class CsvSink:
    def __init__(
        self,
        output_dir: Path,
        flush_rows: int = 500,
        flush_interval_seconds: float = 0.25,
        queue_maxsize: int = 50_000,
        open_file_limit: int = 128,
        orderbook_depth: int = 20,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.flush_rows = flush_rows
        self.flush_interval_seconds = flush_interval_seconds
        self.orderbook_depth = orderbook_depth
        self.queue: asyncio.Queue[CollectorEvent | object] = asyncio.Queue(maxsize=queue_maxsize)
        self._buffers: dict[Path, list[CollectorEvent]] = defaultdict(list)
        self._headers: dict[Path, list[str]] = {}
        self._last_flush: dict[Path, float] = {}
        self._open_files: OrderedDict[Path, _OpenFile] = OrderedDict()
        self._open_file_limit = open_file_limit
        self._stats_logger = logging.getLogger("csv-sink")
        self._last_stats_log_at = 0.0
        self._stats_log_interval_seconds = 60.0

    async def publish(self, event: CollectorEvent) -> None:
        await self.queue.put(event)

    async def close(self) -> None:
        await self.queue.put(_SENTINEL)

    async def run(self) -> None:
        try:
            while True:
                try:
                    item = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=self.flush_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    self._flush_due(force=False)
                    continue

                if item is _SENTINEL:
                    self.queue.task_done()
                    break

                assert isinstance(item, CollectorEvent)
                self._buffer_event(item)
                self.queue.task_done()
                self._flush_due(force=False)
                self._log_periodic_stats()
        except Exception:  # pragma: no cover
            logging.getLogger("csv-sink").exception("CSV sink failed")
            raise
        finally:
            self._flush_due(force=True)
            self._close_all()

    def _buffer_event(self, event: CollectorEvent) -> None:
        path = self.output_dir / event.exchange / event.utc_date / event.data_type / f"{event.instrument_name}.csv"
        if path not in self._headers:
            self._headers[path] = list(event.row.keys())
        else:
            self._merge_headers(path, list(event.row.keys()))
        self._buffers[path].append(event)
        self._last_flush.setdefault(path, time.monotonic())
        if len(self._buffers[path]) >= self.flush_rows:
            self._flush_path(path)

    def _merge_headers(self, path: Path, columns: list[str]) -> None:
        header = self._headers[path]
        additions = [column for column in columns if column not in header]
        if not additions:
            return
        header.extend(additions)
        if path.exists() and path.stat().st_size > 0:
            self._rewrite_existing_file(path, header)

    def _flush_due(self, force: bool) -> None:
        now = time.monotonic()
        for path in list(self._buffers):
            if not self._buffers[path]:
                continue
            if force or now - self._last_flush.get(path, now) >= self.flush_interval_seconds:
                self._flush_path(path)
        self._log_periodic_stats(now=now)

    def _log_periodic_stats(self, now: float | None = None) -> None:
        current = now if now is not None else time.monotonic()
        if current - self._last_stats_log_at < self._stats_log_interval_seconds:
            return
        self._last_stats_log_at = current
        buffered_files = sum(1 for rows in self._buffers.values() if rows)
        buffered_rows = sum(len(rows) for rows in self._buffers.values())
        self._stats_logger.info(
            "CSV sink stats: queue_size=%s buffered_files=%s buffered_rows=%s open_files=%s",
            self.queue.qsize(),
            buffered_files,
            buffered_rows,
            len(self._open_files),
        )

    def _flush_path(self, path: Path) -> None:
        rows = self._buffers[path]
        if not rows:
            return

        header = self._headers[path]
        file_state = self._ensure_file(path, header)
        for event in rows:
            file_state.writer.writerow([event.row.get(column, "") for column in header])
        file_state.handle.flush()
        self._buffers[path] = []
        self._last_flush[path] = time.monotonic()

    def _rewrite_existing_file(self, path: Path, header: list[str]) -> None:
        open_file = self._open_files.pop(path, None)
        if open_file is not None:
            open_file.handle.close()

        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            rows = list(csv.reader(handle))

        if not rows:
            return

        old_header = rows[0]
        temp_path = path.with_suffix(path.suffix + ".tmp")
        with temp_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            for row in rows[1:]:
                row_map = {
                    column: row[index] if index < len(row) else ""
                    for index, column in enumerate(old_header)
                }
                writer.writerow([row_map.get(column, "") for column in header])
        os.replace(temp_path, path)

    def _ensure_file(self, path: Path, header: list[str]) -> _OpenFile:
        file_state = self._open_files.get(path)
        if file_state is not None:
            self._open_files.move_to_end(path)
            return file_state

        path.parent.mkdir(parents=True, exist_ok=True)
        should_write_header = not path.exists() or path.stat().st_size == 0
        handle = path.open("a", newline="", encoding="utf-8-sig")
        writer = csv.writer(handle)
        if should_write_header:
            writer.writerow(header)
            handle.flush()

        file_state = _OpenFile(handle=handle, writer=writer)
        self._open_files[path] = file_state
        self._open_files.move_to_end(path)
        while len(self._open_files) > self._open_file_limit:
            _, oldest = self._open_files.popitem(last=False)
            oldest.handle.close()
        return file_state

    def _close_all(self) -> None:
        for file_state in self._open_files.values():
            file_state.handle.close()
        self._open_files.clear()
