from __future__ import annotations

import argparse
import asyncio
import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import collector_main
from collector_config import CollectorConfig
from collector_deribit_options_books import (
    CSV_FIELDS,
    DailyOptionsBookCsvSink,
    DeribitOptionInstrument,
    DeribitOptionsBookState,
    handle_book_message,
    handle_ticker_message,
    parse_deribit_instruments,
    select_expiry_candidates,
)


def option(name: str, expiry: int, strike: str, option_type: str) -> DeribitOptionInstrument:
    return DeribitOptionInstrument(
        instrument_name=name,
        expiration_timestamp=expiry,
        strike=strike,
        option_type=option_type,
    )


class DeribitInstrumentSelectionTests(unittest.TestCase):
    def test_parse_and_select_third_through_seventh_expirations(self) -> None:
        payload = {
            "result": [
                {
                    "instrument_name": f"BTC-{expiry}-100-C",
                    "expiration_timestamp": expiry,
                    "strike": 100.0,
                    "option_type": "call",
                    "is_active": True,
                }
                for expiry in range(1, 9)
            ]
            + [
                {
                    "instrument_name": "BTC-INACTIVE-100-P",
                    "expiration_timestamp": 3,
                    "strike": 100,
                    "option_type": "put",
                    "is_active": False,
                }
            ]
        }

        instruments = parse_deribit_instruments(payload)
        selected = select_expiry_candidates(instruments)

        self.assertEqual({item.expiration_timestamp for item in selected}, {3, 4, 5, 6, 7})
        self.assertEqual(selected[0].strike, "100")
        self.assertNotIn("BTC-INACTIVE-100-P", {item.instrument_name for item in selected})


class DeribitOptionsBookStateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.call_low = option("BTC-CALL-LOW", 3000, "90", "call")
        self.call_high = option("BTC-CALL-HIGH", 3000, "110", "call")
        self.put_low = option("BTC-PUT-LOW", 3000, "90", "put")
        self.put_high = option("BTC-PUT-HIGH", 3000, "110", "put")
        self.state = DeribitOptionsBookState()
        await self.state.replace_candidates(
            [self.call_low, self.call_high, self.put_low, self.put_high]
        )

    async def test_delta_updates_switch_targets_immediately(self) -> None:
        await self.state.update_delta(self.call_low.instrument_name, 0.25)
        await self.state.update_delta(self.call_high.instrument_name, 0.38)
        await self.state.update_delta(self.put_low.instrument_name, -0.22)
        await self.state.update_delta(self.put_high.instrument_name, -0.31)
        self.assertEqual(
            await self.state.current_targets(),
            {self.call_low.instrument_name, self.put_high.instrument_name},
        )

        await self.state.update_delta(self.call_high.instrument_name, 0.29)
        self.assertEqual(
            await self.state.current_targets(),
            {self.call_high.instrument_name, self.put_high.instrument_name},
        )

    async def test_equal_distance_uses_instrument_name(self) -> None:
        await self.state.update_delta(self.call_low.instrument_name, 0.29)
        self.assertEqual(await self.state.current_targets(), set())
        await self.state.update_delta(self.put_low.instrument_name, -0.3)
        await self.state.update_delta(self.call_high.instrument_name, 0.31)
        expected = min(self.call_low.instrument_name, self.call_high.instrument_name)
        self.assertEqual(
            await self.state.current_targets(),
            {expected, self.put_low.instrument_name},
        )

    async def test_handlers_and_snapshot_keep_latest_delta_and_five_levels(self) -> None:
        await self.state.update_delta(self.put_low.instrument_name, -0.3)
        await handle_ticker_message(
            {
                "method": "subscription",
                "params": {
                    "channel": f"ticker.{self.call_low.instrument_name}.100ms",
                    "data": {
                        "instrument_name": self.call_low.instrument_name,
                        "greeks": {"delta": 0.28},
                    },
                },
            },
            self.state,
        )
        bids = [[index, index * 10] for index in range(1, 8)]
        asks = [[index + 10, index * 20] for index in range(1, 8)]
        await handle_book_message(
            {
                "method": "subscription",
                "params": {
                    "channel": f"book.{self.call_low.instrument_name}.none.10.100ms",
                    "data": {
                        "instrument_name": self.call_low.instrument_name,
                        "bids": bids,
                        "asks": asks,
                    },
                },
            },
            self.state,
        )

        rows = await self.state.snapshot_rows(123456)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["delta"], "0.28")
        self.assertEqual(rows[0]["bid_price_5"], "5")
        self.assertNotIn("bid_price_6", rows[0])

        await self.state.clear_books()
        self.assertEqual(await self.state.snapshot_rows(123457), [])

    async def test_candidate_refresh_replaces_targets_and_clears_old_books(self) -> None:
        await self.state.update_delta(self.call_low.instrument_name, 0.3)
        await self.state.update_delta(self.put_low.instrument_name, -0.3)
        await self.state.update_book(self.call_low.instrument_name, [[1, 2]], [[3, 4]])
        await self.state.update_book(self.put_low.instrument_name, [[1, 2]], [[3, 4]])
        self.assertEqual(len(await self.state.snapshot_rows(1)), 2)

        new_call = option("BTC-NEW-CALL", 4000, "100", "call")
        new_put = option("BTC-NEW-PUT", 4000, "100", "put")
        await self.state.replace_candidates([new_call, new_put])

        self.assertEqual(await self.state.current_targets(), set())
        self.assertEqual(await self.state.snapshot_rows(2), [])
        await self.state.update_delta(new_call.instrument_name, 0.3)
        await self.state.update_delta(new_put.instrument_name, -0.3)
        self.assertEqual(
            await self.state.current_targets(),
            {new_call.instrument_name, new_put.instrument_name},
        )

    async def test_missing_levels_are_empty(self) -> None:
        await self.state.update_delta(self.put_low.instrument_name, -0.3)
        await self.state.update_delta(self.call_low.instrument_name, 0.3)
        await self.state.update_book(self.call_low.instrument_name, [[1, 2]], [])
        rows = await self.state.snapshot_rows(1)
        self.assertEqual(rows[0]["bid_price_1"], "1")
        self.assertEqual(rows[0]["bid_price_2"], "")
        self.assertEqual(rows[0]["ask_amount_5"], "")


class DailyOptionsBookCsvSinkTests(unittest.TestCase):
    def test_daily_file_header_append_and_rollover(self) -> None:
        row = {field: "" for field in CSV_FIELDS}
        row.update({"timestamp": "1", "instrument_name": "BTC-OPTION", "delta": "0.3"})
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            sink = DailyOptionsBookCsvSink(output_dir)
            sink.write_rows(1767225600000, [row])
            sink.write_rows(1767225601000, [row])
            sink.write_rows(1767312000000, [row])
            sink.close()

            first_path = output_dir / "deribit_options_books_2026-01-01.csv"
            second_path = output_dir / "deribit_options_books_2026-01-02.csv"
            with first_path.open("r", newline="", encoding="utf-8-sig") as handle:
                first_rows = list(csv.reader(handle))
            with second_path.open("r", newline="", encoding="utf-8-sig") as handle:
                second_rows = list(csv.reader(handle))

        self.assertEqual(first_rows[0], CSV_FIELDS)
        self.assertEqual(len(first_rows), 3)
        self.assertEqual(len(second_rows), 2)

    def test_rejects_existing_file_with_wrong_header(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            path = output_dir / "deribit_options_books_2026-01-01.csv"
            path.write_text("wrong,header\n", encoding="utf-8")
            sink = DailyOptionsBookCsvSink(output_dir)
            with self.assertRaises(ValueError):
                sink.write_rows(1767225600000, [{field: "" for field in CSV_FIELDS}])


class DeribitBooksCliTests(unittest.TestCase):
    @patch("collector_main.notify_exit")
    @patch("collector_main.parse_args")
    @patch("collector_main.build_config")
    @patch("collector_main.run_deribit_options_books")
    def test_main_dispatches_deribit_books_command(
        self,
        mock_run,
        mock_build_config,
        mock_parse_args,
        mock_notify_exit,
    ) -> None:
        mock_parse_args.return_value = argparse.Namespace(
            command="deribit-books", underlyings="BTC", output_dir="data"
        )
        mock_build_config.return_value = CollectorConfig(
            underlyings=("BTC",), output_dir=Path("data")
        )

        exit_code = collector_main.main()

        self.assertEqual(exit_code, 0)
        mock_run.assert_awaited_once()
        mock_notify_exit.assert_called_once_with(
            command="deribit-books",
            underlyings=("BTC",),
            exit_code=0,
            reason="completed normally",
        )


if __name__ == "__main__":
    unittest.main()
