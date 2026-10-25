"""Telegram notices for headless auto-restore: sent into the topic as new messages."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from telegram.error import RetryAfter

from ...telegram_client import PTBTelegramClient
from ...telegram_rate_limiter import retry_after_seconds
from ..messaging_pipeline.message_sender import safe_send

if TYPE_CHECKING:
    from telegram import Bot

logger = structlog.get_logger()

CREATION_FAILED = "❌ Not restored: could not create the new session. Use /restore."
_SKIP_TEXTS = {
    "session_live": "⚠ Not restored: this session is already running elsewhere.",
    "cwd_missing": "⚠ Not restored: the folder no longer exists.\n(`{cwd}`)",
    "session_not_found": "⚠ Not restored: session not found. Use /resume to pick one.",
    "liveness_unconfirmed": "⚠ Not restored: could not confirm the session is idle. Use /restore.",
    "provider_unsupported": "⚠ Not restored: unsupported provider. Use /restore.",
    "id_reservation_unavailable": "⚠ Not restored: window ids not reserved. Use /restore.",
}


def skip_text(reason: str, cwd: str = "") -> str | None:
    """The user-facing text for a skip ``reason``; None for internal reasons."""
    template = _SKIP_TEXTS.get(reason)
    return None if template is None else template.format(cwd=cwd)


async def send_notice(
    bot: Bot, chat_id: int, thread_id: int, text: str
) -> float | None:
    """Send ``text`` into a topic. Returns retry-after if asked; other failures are logged."""
    log = logger.bind(chat_id=chat_id, thread_id=thread_id)
    try:
        sent = await safe_send(
            PTBTelegramClient(bot), chat_id, text, message_thread_id=thread_id
        )
    except RetryAfter as exc:
        return retry_after_seconds(exc)
    except Exception:
        log.exception("auto_restore_notice_failed")
    else:
        if sent is None:  # safe_send logs and swallows Telegram errors
            log.warning("auto_restore_notice_failed")
    return None
