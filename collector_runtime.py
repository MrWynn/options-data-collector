from __future__ import annotations

import asyncio
import logging

from collector_binance import stream_binance_group
from collector_config import CollectorConfig
from collector_deribit import stream_deribit_group
from collector_discovery import discover_matched_options
from collector_planner import build_runtime_plan
from collector_sink_csv import CsvSink


async def run_collector(config: CollectorConfig) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s [%(filename)s:%(lineno)d] %(message)s",
    )
    logger = logging.getLogger("collector")

    matches = discover_matched_options(config)
    if not matches:
        raise RuntimeError("no matched options found for the configured underlyings")

    plan = build_runtime_plan(matches, config)
    logger.info("Discovered %s matched options", len(plan.matches))
    logger.info("Binance groups: %s", len(plan.binance_groups))
    logger.info("Deribit groups: %s", len(plan.deribit_groups))

    matched_binance_symbols = {match.binance_symbol for match in plan.matches}
    matched_deribit_instruments = {match.deribit_instrument_name for match in plan.matches}
    sink = CsvSink(
        output_dir=config.output_dir,
        flush_rows=config.sink_flush_rows,
        flush_interval_seconds=config.sink_flush_interval_seconds,
        queue_maxsize=config.sink_queue_maxsize,
        open_file_limit=config.sink_open_file_limit,
        orderbook_depth=max(config.binance_orderbook_depth, config.deribit_orderbook_depth),
    )

    sink_task = asyncio.create_task(sink.run(), name="csv-sink")
    worker_tasks = [
        asyncio.create_task(
            stream_binance_group(
                group.streams,
                matched_symbols=matched_binance_symbols,
                sink=sink,
                config=config,
                logger=logging.getLogger(group.name),
            ),
            name=group.name,
        )
        for group in plan.binance_groups
    ]
    worker_tasks.extend(
        asyncio.create_task(
            stream_deribit_group(
                group.streams,
                matched_instruments=matched_deribit_instruments,
                sink=sink,
                config=config,
                logger=logging.getLogger(group.name),
            ),
            name=group.name,
        )
        for group in plan.deribit_groups
    )
    all_tasks = [sink_task, *worker_tasks]

    try:
        await asyncio.gather(*all_tasks)
    finally:
        for task in worker_tasks:
            task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        if not sink_task.done():
            await sink.close()
        await asyncio.gather(sink_task, return_exceptions=True)
