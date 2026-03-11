"""Telegram connector readiness and controlled probe utilities."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib import request

from ..config import CommonConfig


@dataclass(frozen=True)
class TelegramConnectorStatus:
    enabled: bool
    bot_token_set: bool
    chat_id_set: bool
    ready: bool


def get_telegram_connector_status(cfg: CommonConfig) -> TelegramConnectorStatus:
    ready = cfg.telegram_enabled and cfg.telegram_bot_token_set and cfg.telegram_chat_id_set
    return TelegramConnectorStatus(
        enabled=cfg.telegram_enabled,
        bot_token_set=cfg.telegram_bot_token_set,
        chat_id_set=cfg.telegram_chat_id_set,
        ready=ready,
    )


def send_controlled_test_message(message: str) -> tuple[bool, str]:
    """Send one explicit Telegram test message using local env values.

    This method performs outbound call only when directly invoked.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return False, "telegram_not_configured"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": message}).encode("utf-8")
    req = request.Request(url=url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with request.urlopen(req, timeout=8) as resp:
            ok = 200 <= int(getattr(resp, "status", 0)) < 300
            return (ok, "telegram_sent" if ok else "telegram_http_error")
    except Exception as exc:
        return False, f"telegram_error:{exc}"
