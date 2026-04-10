from __future__ import annotations

import asyncio
import csv
import tempfile
import unittest
from pathlib import Path

from collector_binance import build_subscribe_request
from collector_models import CollectorEvent
from collector_normalizer import normalize_binance_message
from collector_sink_csv import CsvSink


class CollectorBinanceTests(unittest.IsolatedAsyncioTestCase):
    def test_build_subscribe_request_preserves_trade_stream_casing(self) -> None:
        request = build_subscribe_request(
            ["btcusdt@optionTrade", "btc-260417-63000-p@depth20@100ms"],
            request_id=7,
        )

        self.assertEqual(
            request,
            {
                "method": "SUBSCRIBE",
                "params": ["btcusdt@optionTrade", "btc-260417-63000-p@depth20@100ms"],
                "id": 7,
            },
        )

    def test_normalize_binance_trade_keeps_raw_unix_timestamps(self) -> None:
        events = normalize_binance_message(
            {
                "e": "trade",
                "E": 1775799368754,
                "T": 1775799368754,
                "s": "BTC-260411-72000-C",
                "t": 93,
                "p": "740.000",
                "q": "0.01",
                "X": "MARKET",
                "S": "SELL",
                "m": True,
            },
            matched_symbols=None,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].row["E"], "1775799368754")
        self.assertEqual(events[0].row["T"], "1775799368754")
        self.assertEqual(events[0].row["s"], "BTC-260411-72000-C")
        self.assertTrue(events[0].row["received_time"].isdigit())

    async def test_csv_sink_persists_raw_unix_timestamps(self) -> None:
        event = CollectorEvent(
            exchange="binance",
            data_type="trade",
            instrument_name="BTC-260417-63000-P",
            row={
                "e": "trade",
                "E": "1767225600123",
                "T": "1767225600122",
                "s": "BTC-260417-63000-P",
                "t": "123",
                "p": "1.23",
                "q": "0.5",
                "X": "MARKET",
                "S": "BUY",
                "m": "false",
                "received_time": "1767225600456",
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            sink = CsvSink(
                output_dir=Path(temp_dir),
                flush_rows=1,
                flush_interval_seconds=0.01,
                queue_maxsize=10,
                open_file_limit=4,
            )
            sink_task = asyncio.create_task(sink.run())
            await sink.publish(event)
            await sink.close()
            await sink_task

            output_path = (
                Path(temp_dir)
                / "binance"
                / "2026-01-01"
                / "trade"
                / "BTC-260417-63000-P.csv"
            )
            self.assertTrue(output_path.exists())

            with output_path.open("r", newline="", encoding="utf-8-sig") as csvfile:
                rows = list(csv.reader(csvfile))

        header = rows[0]
        values = dict(zip(header, rows[1], strict=False))
        self.assertEqual(values["E"], "1767225600123")
        self.assertEqual(values["T"], "1767225600122")
        self.assertEqual(values["received_time"], "1767225600456")

    async def test_csv_sink_expands_header_when_new_fields_appear(self) -> None:
        first = CollectorEvent(
            exchange="binance",
            data_type="trade",
            instrument_name="BTC-260417-63000-P",
            row={
                "e": "trade",
                "E": "1",
                "s": "BTC-260417-63000-P",
                "received_time": "1767225600456",
            },
        )
        second = CollectorEvent(
            exchange="binance",
            data_type="trade",
            instrument_name="BTC-260417-63000-P",
            row={
                "e": "trade",
                "E": "2",
                "s": "BTC-260417-63000-P",
                "new_field": "extra",
                "received_time": "1767225600457",
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            sink = CsvSink(
                output_dir=Path(temp_dir),
                flush_rows=1,
                flush_interval_seconds=0.01,
                queue_maxsize=10,
                open_file_limit=4,
            )
            sink_task = asyncio.create_task(sink.run())
            await sink.publish(first)
            await sink.publish(second)
            await sink.close()
            await sink_task

            output_path = (
                Path(temp_dir)
                / "binance"
                / "2026-01-01"
                / "trade"
                / "BTC-260417-63000-P.csv"
            )
            with output_path.open("r", newline="", encoding="utf-8-sig") as csvfile:
                rows = list(csv.reader(csvfile))

        header = rows[0]
        first_values = dict(zip(header, rows[1], strict=False))
        second_values = dict(zip(header, rows[2], strict=False))
        self.assertIn("new_field", header)
        self.assertEqual(first_values["new_field"], "")
        self.assertEqual(second_values["new_field"], "extra")


if __name__ == "__main__":
    unittest.main()
