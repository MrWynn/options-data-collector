from __future__ import annotations

import argparse
import signal
import unittest
from pathlib import Path
from unittest.mock import patch

import collector_main
from collector_config import CollectorConfig
from collector_lark import build_exit_message


class CollectorLarkTests(unittest.TestCase):
    def test_build_exit_message_includes_exit_reason(self) -> None:
        message = build_exit_message(
            command="run",
            underlyings=("BTC", "ETH"),
            exit_code=1,
            reason="RuntimeError: boom",
        )

        self.assertIn("options collector exit: FAILED", message)
        self.assertIn("command: run", message)
        self.assertIn("underlyings: BTC,ETH", message)
        self.assertIn("exit_code: 1", message)
        self.assertIn("reason: RuntimeError: boom", message)

    @patch("collector_main.notify_exit")
    @patch("collector_main.parse_args")
    @patch("collector_main.build_config")
    @patch("collector_main.discover_matched_options")
    @patch("collector_main.build_runtime_plan")
    def test_main_notifies_lark_on_normal_exit(
        self,
        mock_build_runtime_plan,
        mock_discover,
        mock_build_config,
        mock_parse_args,
        mock_notify_exit,
    ) -> None:
        mock_parse_args.return_value = argparse.Namespace(command="discover", underlyings="BTC", output_dir="data")
        mock_build_config.return_value = CollectorConfig(underlyings=("BTC",), output_dir=Path("data"))
        mock_discover.return_value = []
        mock_build_runtime_plan.return_value = type(
            "Plan",
            (),
            {"binance_groups": (), "deribit_groups": ()},
        )()

        exit_code = collector_main.main()

        self.assertEqual(exit_code, 0)
        mock_notify_exit.assert_called_once_with(
            command="discover",
            underlyings=("BTC",),
            exit_code=0,
            reason="completed normally",
        )

    @patch("collector_main.notify_exit")
    @patch("collector_main.parse_args")
    @patch("collector_main.build_config")
    @patch("collector_main.run_collector")
    def test_main_notifies_lark_on_exception_exit(
        self,
        mock_run_collector,
        mock_build_config,
        mock_parse_args,
        mock_notify_exit,
    ) -> None:
        mock_parse_args.return_value = argparse.Namespace(command="run", underlyings="BTC", output_dir="data")
        mock_build_config.return_value = CollectorConfig(underlyings=("BTC",), output_dir=Path("data"))
        mock_run_collector.side_effect = RuntimeError("boom")

        exit_code = collector_main.main()

        self.assertEqual(exit_code, 1)
        mock_notify_exit.assert_called_once_with(
            command="run",
            underlyings=("BTC",),
            exit_code=1,
            reason="RuntimeError: boom",
        )

    @patch("collector_main.notify_exit")
    @patch("collector_main.parse_args")
    @patch("collector_main.build_config")
    @patch("collector_main.run_collector")
    def test_main_notifies_lark_on_sigterm_exit(
        self,
        mock_run_collector,
        mock_build_config,
        mock_parse_args,
        mock_notify_exit,
    ) -> None:
        mock_parse_args.return_value = argparse.Namespace(command="run", underlyings="BTC", output_dir="data")
        mock_build_config.return_value = CollectorConfig(underlyings=("BTC",), output_dir=Path("data"))
        mock_run_collector.side_effect = collector_main.TerminationRequested("received SIGTERM")

        exit_code = collector_main.main()

        self.assertEqual(exit_code, 130)
        mock_notify_exit.assert_called_once_with(
            command="run",
            underlyings=("BTC",),
            exit_code=130,
            reason="received SIGTERM",
        )


if __name__ == "__main__":
    unittest.main()
