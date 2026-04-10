from __future__ import annotations

import asyncio
import csv
import tempfile
import unittest
from pathlib import Path

from collector_binance import collect_binance_records
from collector_config import CollectorConfig
from collector_deribit import collect_deribit_records
from collector_discovery import discover_matched_options
from collector_planner import build_runtime_plan
from collector_sink_csv import CsvSink


class CollectorLiveIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_discovery_and_orderbook_collection(self) -> None:
        config = CollectorConfig()
        matches = discover_matched_options(config)
        self.assertGreater(len(matches), 0)

        plan = build_runtime_plan(matches, config)
        self.assertGreater(len(plan.binance_groups), 0)
        self.assertGreater(len(plan.deribit_groups), 0)

        first_match = matches[0]
        orderbook_symbols = [match.binance_symbol for match in matches[:10]]
        orderbook_instruments = [match.deribit_instrument_name for match in matches[:10]]

        binance_orderbook_records, deribit_orderbook_records = await asyncio.gather(
            collect_binance_records(
                streams=[f"{symbol.lower()}@depth20@100ms" for symbol in orderbook_symbols],
                matched_symbols=set(orderbook_symbols),
                limit=1,
                timeout=20,
            ),
            collect_deribit_records(
                channels=[
                    f"book.{instrument}.none.20.100ms"
                    for instrument in orderbook_instruments
                ],
                matched_instruments=set(orderbook_instruments),
                limit=1,
                timeout=20,
            ),
        )

        self.assertEqual(binance_orderbook_records[0].data_type, "orderbook")
        self.assertEqual(deribit_orderbook_records[0].data_type, "orderbook")

        self.assertIn(binance_orderbook_records[0].instrument_name, set(orderbook_symbols))
        self.assertIn(deribit_orderbook_records[0].instrument_name, set(orderbook_instruments))

        self.assertIn("b_price_0", binance_orderbook_records[0].row)
        self.assertIn("asks_size_0", deribit_orderbook_records[0].row)
        self.assertEqual(first_match.underlying, "BTC")

    async def test_live_deribit_trade_collection(self) -> None:
        matches = discover_matched_options(CollectorConfig())
        first_match = matches[0]
        records = await collect_deribit_records(
            channels=[f"trades.option.{first_match.underlying}.100ms"],
            matched_instruments=None,
            limit=1,
            timeout=90,
        )

        self.assertEqual(records[0].data_type, "trade")
        self.assertEqual(records[0].exchange, "deribit")
        self.assertTrue(records[0].instrument_name)
        self.assertTrue(records[0].row["price"])

    async def test_live_binance_trade_stream_when_active(self) -> None:
        matches = discover_matched_options(CollectorConfig())
        first_match = matches[0]
        try:
            records = await collect_binance_records(
                streams=[f"{first_match.underlying.lower()}usdt@optionTrade"],
                matched_symbols=None,
                limit=1,
                timeout=30,
            )
        except TimeoutError:
            self.skipTest("Binance option trade stream produced no live trades within 30 seconds.")

        self.assertEqual(records[0].data_type, "trade")
        self.assertEqual(records[0].exchange, "binance")
        self.assertTrue(records[0].instrument_name)
        self.assertTrue(records[0].row["price"])

    async def test_live_csv_sink_persists_real_orderbook_rows(self) -> None:
        matches = discover_matched_options(CollectorConfig())
        orderbook_symbols = [match.binance_symbol for match in matches[:10]]
        records = await collect_binance_records(
            streams=[f"{symbol.lower()}@depth20@100ms" for symbol in orderbook_symbols],
            matched_symbols=set(orderbook_symbols),
            limit=1,
            timeout=20,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            sink = CsvSink(
                output_dir=Path(temp_dir),
                flush_rows=1,
                flush_interval_seconds=0.01,
                queue_maxsize=10,
                open_file_limit=8,
            )
            sink_task = asyncio.create_task(sink.run())
            await sink.publish(records[0])
            await sink.close()
            await sink_task

            output_path = (
                Path(temp_dir)
                / "binance"
                / records[0].utc_date
                / "orderbook"
                / f"{records[0].instrument_name}.csv"
            )
            self.assertTrue(output_path.exists())

            with output_path.open("r", newline="", encoding="utf-8-sig") as csvfile:
                rows = list(csv.reader(csvfile))

        header = rows[0]
        values = dict(zip(header, rows[1], strict=False))
        self.assertIn("E", header)
        self.assertIn(values["s"], orderbook_symbols)
        self.assertTrue(values["b_price_0"])
