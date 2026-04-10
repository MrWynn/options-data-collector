import csv
import tempfile
import unittest
from pathlib import Path

from options_data import (
    find_matches,
    parse_binance_options,
    parse_deribit_expiry,
    parse_deribit_reverse_options,
    save_matches_to_csv,
)


class OptionsDataTests(unittest.TestCase):
    def test_parse_deribit_expiry(self) -> None:
        self.assertEqual(parse_deribit_expiry("26JUN26"), "2026-06-26")
        self.assertEqual(parse_deribit_expiry("1MAY26"), "2026-05-01")

    def test_parse_deribit_reverse_options_filters_target_expiry(self) -> None:
        payload = {
            "result": [
                {
                    "instrument_name": "BTC-26JUN26-135000-C",
                    "quote_currency": "BTC",
                },
                {
                    "instrument_name": "BTC-25DEC26-135000-C",
                    "quote_currency": "BTC",
                },
                {
                    "instrument_name": "ETH-26JUN26-3000-C",
                    "quote_currency": "ETH",
                },
            ]
        }

        options = parse_deribit_reverse_options(payload)

        self.assertEqual(len(options), 1)
        self.assertEqual(options[0].expiry_date, "2026-06-26")
        self.assertEqual(options[0].strike_price, "135000")
        self.assertEqual(options[0].option_type, "call")

    def test_parse_deribit_reverse_options_supports_eth(self) -> None:
        payload = {
            "result": [
                {
                    "instrument_name": "ETH-26JUN26-3000-C",
                    "quote_currency": "ETH",
                },
                {
                    "instrument_name": "BTC-26JUN26-135000-C",
                    "quote_currency": "BTC",
                },
            ]
        }

        options = parse_deribit_reverse_options(payload, underlying="ETH")

        self.assertEqual(len(options), 1)
        self.assertEqual(options[0].raw_symbol, "ETH-26JUN26-3000-C")

    def test_find_matches_uses_expiry_strike_and_type(self) -> None:
        deribit_payload = {
            "result": [
                {
                    "instrument_name": "BTC-26JUN26-135000-C",
                    "quote_currency": "BTC",
                },
                {
                    "instrument_name": "BTC-26JUN26-135000-P",
                    "quote_currency": "BTC",
                },
            ]
        }
        binance_payload = {
            "optionSymbols": [
                {
                    "symbol": "BTC-260626-135000-C",
                    "underlying": "BTCUSDT",
                    "expiryDate": 1782460800000,
                    "side": "CALL",
                    "strikePrice": "135000.000",
                },
                {
                    "symbol": "BTC-260626-135000-P",
                    "underlying": "BTCUSDT",
                    "expiryDate": 1782460800000,
                    "side": "PUT",
                    "strikePrice": "135000.000",
                },
                {
                    "symbol": "BTC-260626-140000-C",
                    "underlying": "BTCUSDT",
                    "expiryDate": 1782460800000,
                    "side": "CALL",
                    "strikePrice": "140000.000",
                },
            ]
        }

        matches = find_matches(
            parse_deribit_reverse_options(deribit_payload),
            parse_binance_options(binance_payload),
        )

        self.assertEqual(len(matches), 2)
        self.assertEqual(matches[0]["deribit_instrument_name"], "BTC-26JUN26-135000-C")
        self.assertEqual(matches[0]["binance_symbol"], "BTC-260626-135000-C")
        self.assertEqual(matches[1]["option_type"], "put")

    def test_find_matches_supports_eth(self) -> None:
        deribit_payload = {
            "result": [
                {
                    "instrument_name": "ETH-26JUN26-3000-C",
                    "quote_currency": "ETH",
                }
            ]
        }
        binance_payload = {
            "optionSymbols": [
                {
                    "symbol": "ETH-260626-3000-C",
                    "underlying": "ETHUSDT",
                    "expiryDate": 1782460800000,
                    "side": "CALL",
                    "strikePrice": "3000.0000",
                },
                {
                    "symbol": "BTC-260626-3000-C",
                    "underlying": "BTCUSDT",
                    "expiryDate": 1782460800000,
                    "side": "CALL",
                    "strikePrice": "3000.0000",
                },
            ]
        }

        matches = find_matches(
            parse_deribit_reverse_options(deribit_payload, underlying="ETH"),
            parse_binance_options(binance_payload, underlying="ETH"),
        )

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["deribit_instrument_name"], "ETH-26JUN26-3000-C")
        self.assertEqual(matches[0]["binance_symbol"], "ETH-260626-3000-C")

    def test_save_matches_to_csv_keeps_raw_symbols(self) -> None:
        rows = [
            {
                "expiry_date": "2026-06-26",
                "strike_price": "135000",
                "option_type": "call",
                "deribit_instrument_name": "BTC-26JUN26-135000-C",
                "binance_symbol": "BTC-260626-135000-C",
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "matches.csv"
            save_matches_to_csv(rows, output_path)

            with output_path.open("r", newline="", encoding="utf-8-sig") as csvfile:
                reader = list(csv.reader(csvfile))

        self.assertEqual(
            reader,
            [
                ["deribit_instrument_name", "binance_symbol"],
                ["BTC-26JUN26-135000-C", "BTC-260626-135000-C"],
            ],
        )


if __name__ == "__main__":
    unittest.main()
