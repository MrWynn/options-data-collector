from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BINANCE_URL = "https://eapi.binance.com/eapi/v1/exchangeInfo"
DEFAULT_UNDERLYING = "BTC"
TARGET_EXPIRIES = {
    "2026-04-17",
    "2026-04-24",
    "2026-05-29",
    "2026-06-26",
}
MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


@dataclass(frozen=True)
class NormalizedOption:
    exchange: str
    raw_symbol: str
    expiry_date: str
    strike_price: str
    option_type: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.expiry_date, self.strike_price, self.option_type)


def normalize_underlying(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("underlying must not be empty")
    return normalized


def normalize_target_expiries(
    target_expiries: Iterable[str] | None = None,
) -> frozenset[str]:
    if target_expiries is None:
        return frozenset(TARGET_EXPIRIES)

    normalized = frozenset(str(item).strip() for item in target_expiries if str(item).strip())
    if not normalized:
        raise ValueError("target_expiries must not be empty")
    return normalized


def build_deribit_url(underlying: str) -> str:
    return (
        "https://www.deribit.com/api/v2/public/"
        f"get_book_summary_by_currency?currency={underlying}&kind=option"
    )


def fetch_json(url: str, timeout: int = 15) -> dict:
    request = Request(
        url,
        headers={
            "User-Agent": "options-data-script/1.0",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def normalize_decimal(value: object) -> str:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc

    normalized = format(decimal_value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def parse_deribit_expiry(token: str) -> str:
    match = re.fullmatch(r"(\d{1,2})([A-Z]{3})(\d{2})", token.upper())
    if match is None:
        raise ValueError(f"invalid Deribit expiry token: {token}")

    day_text, month_text, year_text = match.groups()
    day = int(day_text)
    year = 2000 + int(year_text)
    month = MONTHS.get(month_text)
    if month is None:
        raise ValueError(f"invalid Deribit month token: {token}")

    return datetime(year, month, day).date().isoformat()


def parse_deribit_reverse_options(
    payload: dict,
    underlying: str = DEFAULT_UNDERLYING,
    target_expiries: Iterable[str] | None = None,
) -> list[NormalizedOption]:
    options: list[NormalizedOption] = []
    normalized_underlying = normalize_underlying(underlying)
    normalized_expiries = normalize_target_expiries(target_expiries)

    for item in payload.get("result", []):
        instrument_name = item.get("instrument_name")
        if not isinstance(instrument_name, str):
            continue

        parts = instrument_name.split("-")
        if len(parts) != 4:
            continue

        base_currency, expiry_token, strike_token, side_token = parts
        if base_currency != normalized_underlying:
            continue

        if item.get("quote_currency") != normalized_underlying:
            continue

        try:
            expiry_date = parse_deribit_expiry(expiry_token)
        except ValueError:
            continue
        if expiry_date not in normalized_expiries:
            continue

        side_map = {"C": "call", "P": "put"}
        option_type = side_map.get(side_token.upper())
        if option_type is None:
            continue

        options.append(
            NormalizedOption(
                exchange="deribit",
                raw_symbol=instrument_name,
                expiry_date=expiry_date,
                strike_price=normalize_decimal(strike_token),
                option_type=option_type,
            )
        )

    return options


def parse_binance_options(
    payload: dict,
    underlying: str = DEFAULT_UNDERLYING,
    target_expiries: Iterable[str] | None = None,
) -> list[NormalizedOption]:
    options: list[NormalizedOption] = []
    normalized_underlying = normalize_underlying(underlying)
    target_underlying = f"{normalized_underlying}USDT"
    normalized_expiries = normalize_target_expiries(target_expiries)

    for item in payload.get("optionSymbols", []):
        if item.get("underlying") != target_underlying:
            continue

        expiry_millis = item.get("expiryDate")
        if not isinstance(expiry_millis, int):
            continue

        expiry_date = datetime.fromtimestamp(
            expiry_millis / 1000, tz=timezone.utc
        ).date().isoformat()
        if expiry_date not in normalized_expiries:
            continue

        side = item.get("side")
        if side not in {"CALL", "PUT"}:
            continue

        symbol = item.get("symbol")
        strike_price = item.get("strikePrice")
        if not isinstance(symbol, str) or strike_price is None:
            continue

        options.append(
            NormalizedOption(
                exchange="binance",
                raw_symbol=symbol,
                expiry_date=expiry_date,
                strike_price=normalize_decimal(strike_price),
                option_type=side.lower(),
            )
        )

    return options


def build_lookup(options: Iterable[NormalizedOption]) -> dict[tuple[str, str, str], NormalizedOption]:
    return {option.key: option for option in options}


def find_matches(
    deribit_options: Iterable[NormalizedOption],
    binance_options: Iterable[NormalizedOption],
) -> list[dict[str, str]]:
    deribit_lookup = build_lookup(deribit_options)
    binance_lookup = build_lookup(binance_options)

    matches: list[dict[str, str]] = []
    for key in sorted(
        deribit_lookup.keys() & binance_lookup.keys(),
        key=lambda item: (item[0], Decimal(item[1]), item[2]),
    ):
        deribit_option = deribit_lookup[key]
        binance_option = binance_lookup[key]
        matches.append(
            {
                "expiry_date": deribit_option.expiry_date,
                "strike_price": deribit_option.strike_price,
                "option_type": deribit_option.option_type,
                "deribit_instrument_name": deribit_option.raw_symbol,
                "binance_symbol": binance_option.raw_symbol,
            }
        )

    return matches


def find_matches_from_payloads(
    deribit_payload: dict,
    binance_payload: dict,
    underlying: str = DEFAULT_UNDERLYING,
    target_expiries: Iterable[str] | None = None,
) -> list[dict[str, str]]:
    return find_matches(
        parse_deribit_reverse_options(
            deribit_payload,
            underlying=underlying,
            target_expiries=target_expiries,
        ),
        parse_binance_options(
            binance_payload,
            underlying=underlying,
            target_expiries=target_expiries,
        ),
    )


def fetch_remote_matches(
    underlying: str = DEFAULT_UNDERLYING,
    target_expiries: Iterable[str] | None = None,
    timeout: int = 15,
) -> list[dict[str, str]]:
    normalized_underlying = normalize_underlying(underlying)
    deribit_payload = fetch_json(build_deribit_url(normalized_underlying), timeout=timeout)
    binance_payload = fetch_json(BINANCE_URL, timeout=timeout)
    return find_matches_from_payloads(
        deribit_payload,
        binance_payload,
        underlying=normalized_underlying,
        target_expiries=target_expiries,
    )


def save_matches_to_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["deribit_instrument_name", "binance_symbol"])
        for row in rows:
            writer.writerow(
                [row["deribit_instrument_name"], row["binance_symbol"]]
            )


def render_table(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "No matching options found."

    columns = [
        ("expiry_date", "Expiry"),
        ("strike_price", "Strike"),
        ("option_type", "Type"),
        ("deribit_instrument_name", "Deribit"),
        ("binance_symbol", "Binance"),
    ]
    widths = []
    for key, title in columns:
        widths.append(max(len(title), *(len(str(row[key])) for row in rows)))

    header = " | ".join(
        title.ljust(width) for (_, title), width in zip(columns, widths)
    )
    separator = "-+-".join("-" * width for width in widths)
    body = [
        " | ".join(str(row[key]).ljust(width) for (key, _), width in zip(columns, widths))
        for row in rows
    ]
    return "\n".join([header, separator, *body])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare options between Deribit reverse options and Binance options."
    )
    parser.add_argument(
        "--underlying",
        default=DEFAULT_UNDERLYING,
        help="underlying asset symbol, for example BTC or ETH",
    )
    parser.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help="output format for matched options",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        underlying = normalize_underlying(args.underlying)
    except ValueError as exc:
        print(f"Invalid underlying: {exc}", file=sys.stderr)
        return 1

    try:
        deribit_payload = fetch_json(build_deribit_url(underlying))
        binance_payload = fetch_json(BINANCE_URL)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Failed to fetch remote data: {exc}", file=sys.stderr)
        return 1

    deribit_options = parse_deribit_reverse_options(deribit_payload, underlying=underlying)
    binance_options = parse_binance_options(binance_payload, underlying=underlying)
    matches = find_matches(deribit_options, binance_options)
    csv_output_path = Path(f"matched_options_{underlying.lower()}.csv")
    save_matches_to_csv(matches, csv_output_path)

    summary = {
        "underlying": underlying,
        "target_expiries": sorted(TARGET_EXPIRIES),
        "deribit_reverse_count": len(deribit_options),
        "binance_count": len(binance_options),
        "matched_count": len(matches),
        "csv_output_path": str(csv_output_path),
        "matches": matches,
    }

    if args.format == "json":
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    print("Underlying:", summary["underlying"])
    print("Target expiries:", ", ".join(summary["target_expiries"]))
    print("Deribit reverse options:", summary["deribit_reverse_count"])
    print("Binance options:", summary["binance_count"])
    print("Matched options:", summary["matched_count"])
    print("CSV output:", summary["csv_output_path"])
    print()
    print(render_table(matches))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
