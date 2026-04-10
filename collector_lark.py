from __future__ import annotations

import json
import socket
from datetime import datetime, timezone
from urllib.request import Request, urlopen


LARK_WEBHOOK_URL = (
    "https://open.larksuite.com/open-apis/bot/v2/hook/"
    "73be631b-fcb6-4973-9135-4c12452d81e5"
)


def build_exit_message(
    *,
    command: str,
    underlyings: tuple[str, ...],
    exit_code: int,
    reason: str,
) -> str:
    status = "SUCCESS" if exit_code == 0 else "FAILED"
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    hostname = socket.gethostname()
    return "\n".join(
        [
            f"options collector exit: {status}",
            f"time: {timestamp}",
            f"host: {hostname}",
            f"command: {command}",
            f"underlyings: {','.join(underlyings)}",
            f"exit_code: {exit_code}",
            f"reason: {reason}",
        ]
    )


def send_lark_text_message(text: str, timeout: float = 10.0) -> None:
    payload = json.dumps(
        {
            "msg_type": "text",
            "content": {
                "text": text,
            },
        }
    ).encode("utf-8")
    request = Request(
        LARK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        response.read()


def notify_exit(
    *,
    command: str,
    underlyings: tuple[str, ...],
    exit_code: int,
    reason: str,
) -> None:
    send_lark_text_message(
        build_exit_message(
            command=command,
            underlyings=underlyings,
            exit_code=exit_code,
            reason=reason,
        )
    )
