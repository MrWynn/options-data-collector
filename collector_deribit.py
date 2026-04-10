from __future__ import annotations

import asyncio
import itertools
import json
import logging
import random

from collector_config import CollectorConfig
from collector_models import CollectorEvent
from collector_normalizer import normalize_deribit_message

try:
    import websockets
except ImportError:  # pragma: no cover
    websockets = None


DERIBIT_WS_URL = "wss://www.deribit.com/ws/api/v2"


def _require_websockets():
    if websockets is None:
        raise RuntimeError("websockets is required; install dependencies from requirements.txt")
    return websockets


def _is_test_request(message: dict) -> bool:
    if message.get("method") == "test_request":
        return True

    if message.get("method") == "heartbeat":
        params = message.get("params", {})
        return params.get("type") == "test_request"

    return False


async def _send_request(websocket, request_id: int, method: str, params: dict) -> None:
    await websocket.send(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
    )


async def _prepare_subscription(websocket, channels: tuple[str, ...], heartbeat_interval: int) -> None:
    request_ids = itertools.count(1)
    await _send_request(
        websocket,
        next(request_ids),
        "public/set_heartbeat",
        {"interval": heartbeat_interval},
    )
    await _send_request(
        websocket,
        next(request_ids),
        "public/subscribe",
        {"channels": list(channels)},
    )


async def collect_deribit_records(
    channels: list[str] | tuple[str, ...],
    matched_instruments: set[str] | None,
    limit: int,
    timeout: float,
    heartbeat_interval: int = 30,
    depth: int = 20,
) -> list[CollectorEvent]:
    ws_lib = _require_websockets()
    records: list[CollectorEvent] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    async with ws_lib.connect(
        DERIBIT_WS_URL,
        ping_interval=60,
        ping_timeout=60,
        open_timeout=15,
        close_timeout=5,
        max_queue=None,
    ) as websocket:
        await _prepare_subscription(websocket, tuple(channels), heartbeat_interval)
        while len(records) < limit:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for Deribit records")
            payload = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            message = json.loads(payload)
            if _is_test_request(message):
                await _send_request(websocket, 9_999, "public/test", {})
                continue
            records.extend(
                normalize_deribit_message(
                    message,
                    matched_instruments=matched_instruments,
                    depth=depth,
                )
            )

    return records[:limit]


async def stream_deribit_group(
    channels: tuple[str, ...],
    matched_instruments: set[str] | None,
    sink,
    config: CollectorConfig,
    logger: logging.Logger,
) -> None:
    ws_lib = _require_websockets()
    backoff = config.reconnect_min_seconds

    while True:
        try:
            async with ws_lib.connect(
                DERIBIT_WS_URL,
                ping_interval=60,
                ping_timeout=60,
                open_timeout=15,
                close_timeout=5,
                max_queue=None,
            ) as websocket:
                await _prepare_subscription(
                    websocket,
                    channels=channels,
                    heartbeat_interval=config.deribit_heartbeat_interval,
                )
                logger.info("Deribit connected: %s", ",".join(channels))
                backoff = config.reconnect_min_seconds
                while True:
                    payload = await websocket.recv()
                    message = json.loads(payload)
                    if _is_test_request(message):
                        await _send_request(websocket, 9_999, "public/test", {})
                        continue
                    for event in normalize_deribit_message(
                        message,
                        matched_instruments=matched_instruments,
                        depth=config.deribit_orderbook_depth,
                    ):
                        await sink.publish(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover
            logger.warning("Deribit channels %s failed: %s", ",".join(channels), exc)
            await asyncio.sleep(backoff + random.uniform(0.0, 0.5))
            backoff = min(backoff * 2, config.reconnect_max_seconds)
