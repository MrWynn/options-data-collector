from __future__ import annotations

from options_data import (
    BINANCE_URL,
    build_deribit_url,
    fetch_json,
    find_matches_from_payloads,
    normalize_underlying,
)

from collector_config import CollectorConfig
from collector_models import MatchedOption


def discover_matched_options(config: CollectorConfig) -> list[MatchedOption]:
    binance_payload = fetch_json(BINANCE_URL)
    matches: list[MatchedOption] = []

    for underlying in config.underlyings:
        normalized_underlying = normalize_underlying(underlying)
        deribit_payload = fetch_json(build_deribit_url(normalized_underlying))
        rows = find_matches_from_payloads(
            deribit_payload,
            binance_payload,
            underlying=normalized_underlying,
            target_expiries=config.target_expiries,
        )
        for row in rows:
            matches.append(
                MatchedOption(
                    underlying=normalized_underlying,
                    expiry_date=row["expiry_date"],
                    strike_price=row["strike_price"],
                    option_type=row["option_type"],
                    deribit_instrument_name=row["deribit_instrument_name"],
                    binance_symbol=row["binance_symbol"],
                )
            )

    return sorted(
        matches,
        key=lambda item: (
            item.underlying,
            item.expiry_date,
            item.strike_price,
            item.option_type,
        ),
    )
