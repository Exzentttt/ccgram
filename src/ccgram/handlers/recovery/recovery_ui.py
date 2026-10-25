"""Presentation seam for the recovery window-replacement flow.

``recovery_banner.create_and_bind_window`` replaces a topic's dead window
with a new one. Every Fresh / Continue / Resume button reaches it through a
Telegram callback, but the replacement itself needs no callback: it only
reports progress and reads or clears the pending-recovery state. This module
names that surface so a non-interactive caller can run the same flow.

Public surface:
  - :class:`ReplacementRequest` — what to create and for which topic
  - :class:`RecoveryUi` — the progress/state surface the flow talks to
  - :class:`HeadlessUi` — a :class:`RecoveryUi` with no Telegram message to edit

The callback-backed implementation lives in :mod:`recovery_banner`, next to
the ``safe_edit`` it wraps.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

import structlog

logger = structlog.get_logger()


@dataclass(frozen=True)
class ReplacementRequest:
    """One topic whose dead window is to be replaced.

    ``chat_id`` is the chat the request came from, when known; the flow falls
    back to the topic's routed chat otherwise. ``provider_name`` overrides the
    provider inherited from ``old_window_id``.

    ``offer_verified`` means the caller already proved ``old_window_id`` dead,
    so the flow skips its own liveness check. ``approval_mode`` overrides the
    mode read from ``old_window_id``; both exist for callers whose old id may
    since name another topic's window.
    """

    user_id: int
    thread_id: int
    cwd: str
    chat_id: int | None = None
    resume_id: str | None = None
    continue_session: bool = False
    success_label: str = "Session started."
    old_window_id: str = ""
    provider_name: str = ""
    offer_verified: bool = False
    approval_mode: Literal["normal", "yolo", ""] = ""


class RecoveryUi(Protocol):
    """Where the replacement flow reports progress and keeps recovery state."""

    @property
    def user_data(self) -> dict[str, Any] | None:
        """Per-user state holding the pending recovery keys, if any."""

    async def edit(self, text: str, **kwargs: Any) -> None:
        """Show ``text`` as the current progress message."""

    async def answer(self, text: str) -> None:
        """Acknowledge the flow's outcome with a short ``text``."""

    def pending_text(self) -> str | None:
        """Return the message that arrived while the window was dead."""

    def clear_state(self) -> None:
        """Drop every pending-recovery key."""

    def note_rate_limit(self, retry_after: float) -> None:
        """Record that Telegram asked the flow to wait ``retry_after`` seconds."""


class HeadlessUi:
    """A :class:`RecoveryUi` for runs with no Telegram message to edit."""

    user_data: dict[str, Any] | None = None

    def __init__(self) -> None:
        self.retry_after: float | None = None

    async def edit(self, text: str, **_kwargs: Any) -> None:
        logger.info("recovery_progress", text=text)

    async def answer(self, text: str) -> None:
        logger.debug("recovery_outcome", text=text)

    def pending_text(self) -> str | None:
        return None

    def clear_state(self) -> None:
        return None

    def note_rate_limit(self, retry_after: float) -> None:
        self.retry_after = retry_after
