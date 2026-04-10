from __future__ import annotations

import asyncio
import json
import logging
import random

from collector_config import CollectorConfig
from collector_models import CollectorEvent
from collector_normalizer import normalize_binance_message

try:
    import websockets
except ImportError:  # pragma: no cover
    websockets = None


BINANCE_WS_URL = "wss://fstream.binance.com/public/ws"


def _require_websockets():
    if websockets is None:
        raise RuntimeError("websockets is required; install dependencies from requirements.txt")
    return websockets


def build_subscribe_request(
    streams: list[str] | tuple[str, ...],
    request_id: int = 1,
) -> dict[str, object]:
    return {
        "method": "SUBSCRIBE",
        "params": list(streams),
        "id": request_id,
    }


def _raise_if_control_message(message: dict) -> bool:
    if "code" in message and "msg" in message:
        raise RuntimeError(f"Binance subscription failed: {message}")
    return "result" in message and "id" in message


async def _prepare_subscription(
    websocket,
    streams: list[str] | tuple[str, ...],
    request_id: int = 1,
) -> None:
    await websocket.send(json.dumps(build_subscribe_request(streams, request_id=request_id)))


async def collect_binance_records(
    streams: list[str] | tuple[str, ...],
    matched_symbols: set[str] | None,
    limit: int,
    timeout: float,
    depth: int = 20,
) -> list[CollectorEvent]:
    ws_lib = _require_websockets()
    records: list[CollectorEvent] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    async with ws_lib.connect(
        BINANCE_WS_URL,
        ping_interval=60,
        ping_timeout=60,
        open_timeout=15,
        close_timeout=5,
        max_queue=None,
    ) as websocket:
        await _prepare_subscription(websocket, streams)
        while len(records) < limit:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for Binance records")
            payload = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            message = json.loads(payload)
            if _raise_if_control_message(message):
                continue
            records.extend(
                normalize_binance_message(
                    message,
                    matched_symbols=matched_symbols,
                    depth=depth,
                )
            )

    return records[:limit]


async def stream_binance_group(
    streams: tuple[str, ...],
    matched_symbols: set[str] | None,
    sink,
    config: CollectorConfig,
    logger: logging.Logger,
) -> None:
    ws_lib = _require_websockets()
    backoff = config.reconnect_min_seconds

    while True:
        try:
            async with ws_lib.connect(
                BINANCE_WS_URL,
                ping_interval=60,
                ping_timeout=60,
                open_timeout=15,
                close_timeout=5,
                max_queue=None,
            ) as websocket:
                await _prepare_subscription(websocket, streams)
                logger.info("Binance connected: %s", ",".join(streams))
                backoff = config.reconnect_min_seconds
                while True:
                    payload = await websocket.recv()
                    message = json.loads(payload)
                    if _raise_if_control_message(message):
                        continue
                    for event in normalize_binance_message(
                        message,
                        matched_symbols=matched_symbols,
                        depth=config.binance_orderbook_depth,
                    ):
                        await sink.publish(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover
            logger.warning("Binance stream %s failed: %s", ",".join(streams), exc)
            await asyncio.sleep(backoff + random.uniform(0.0, 0.5))
            backoff = min(backoff * 2, config.reconnect_max_seconds)
