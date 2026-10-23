"""Session monitoring service — thin coordinator and poll loop.

Orchestrates the session-monitoring subsystem:
  1. Reads hook events via event_reader and dispatches them.
  2. Reconciles session_map changes via SessionLifecycle.
  3. Reads transcript updates via TranscriptReader.
  4. Emits NewMessage / NewWindowEvent to registered callbacks.

All heavy logic lives in the extracted modules:
  - event_reader.py   — reads events.jsonl incrementally
  - idle_tracker.py   — per-session idle timers
  - session_lifecycle.py — session-map diff, claude_task_state authority
  - transcript_reader.py — transcript I/O and parsing

Key classes: SessionMonitor, NewMessage, NewWindowEvent, SessionInfo.
Re-exported from transcript_reader for backward-compatible imports.
"""

import asyncio
import contextlib
import hashlib
import os
import structlog
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from telegram.error import TelegramError

from .config import _replay_cap_bytes, config
from .delivery_contract import (
    DeliveryReceipt,
    activate_delivery_receipt,
    deactivate_delivery_receipt,
    new_delivery_receipt,
    settled_prefix,
    settled_run_offset,
)
from .event_reader import read_new_events
from .idle_tracker import IdleTracker
from .monitor_state import BacklogSkipIntent, MonitorState, TrackedSession
from .providers import (
    detect_provider_from_transcript_path,
    get_provider_for_window,
    registry,  # noqa: F401 (used by test patches)
)
from .session_map import (
    acknowledge_replay_from_start,
    observed_provider,
    parse_session_map,
    read_session_map_raw,
    session_map_prefix,
    strip_session_map_prefix,
)
from .window_state_ports import identity_state
from .session_lifecycle import session_lifecycle
from .multiplexer import multiplexer as tmux_manager
from .multiplexer.base import canonical_window_id
from .multiplexer.reconciliation import list_windows_for_reconciliation
from .multiplexer.window_liveness import note_live_windows
from .multiplexer.topic_mapping import is_agent_topic_window
from .monitor_events import NewMessage, NewWindowEvent, SessionInfo
from .transcript_reader import (
    TranscriptReader,
    _resolve_provider_for_file,
    render_transcript_batch,
)
from .session_protocol.events import EventHub
from .session_protocol.messages import TranscriptBatch
from .utils import task_done_callback

import json

# Re-export for backward-compatible imports from other modules
__all__ = [
    "NewMessage",
    "NewWindowEvent",
    "SessionInfo",
    "SessionMonitor",
    "get_active_monitor",
    "set_active_monitor",
]

_CallbackError = Exception
_LoopError = (OSError, RuntimeError, json.JSONDecodeError, ValueError, TelegramError)

_BACKOFF_MIN = 2.0
_BACKOFF_MAX = 30.0
_SKIP_RETRY_BASE_SECONDS = 2.0
_SKIP_RETRY_MAX_SECONDS = 60.0
logger = structlog.get_logger()


def _source_stat_signature(source_stat: Any) -> tuple[int, int, int, int, int]:
    """Identify one stable transcript EOF observation."""
    return (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_size,
        source_stat.st_ctime_ns,
        source_stat.st_mtime_ns,
    )


def _stable_source_stat(file_path: Path) -> Any | None:
    """Return a stat result only when two adjacent EOF observations match."""
    try:
        before = file_path.stat()
        after = file_path.stat()
    except OSError:
        return None
    return (
        after
        if _source_stat_signature(before) == _source_stat_signature(after)
        else None
    )


def _source_range_sha256(
    file_path: Path,
    start: int,
    end: int,
    *,
    device: int,
    inode: int,
) -> str | None:
    """Hash a frozen byte range only when its open file generation stays current."""
    if start < 0 or end < start:
        return None
    expected_identity = (device, inode)
    digest = hashlib.sha256()
    try:
        with file_path.open("rb") as source:
            before = os.fstat(source.fileno())
            if (
                before.st_dev,
                before.st_ino,
            ) != expected_identity or before.st_size < end:
                return None
            source.seek(start)
            remaining = end - start
            while remaining:
                chunk = source.read(min(remaining, 1_048_576))
                if not chunk:
                    return None
                digest.update(chunk)
                remaining -= len(chunk)
            after = os.fstat(source.fileno())
        current_path = file_path.stat()
    except OSError, ValueError:
        return None
    if _source_stat_signature(before) != _source_stat_signature(
        after
    ) or _source_stat_signature(after) != _source_stat_signature(current_path):
        return None
    if any(
        (observed.st_dev, observed.st_ino) != expected_identity
        or observed.st_size < end
        for observed in (before, after, current_path)
    ):
        return None
    return digest.hexdigest()


def _source_has_complete_incremental_tail(
    file_path: Path,
    expected_signature: tuple[int, int, int, int, int] | None,
) -> bool | None:
    """Read EOF once, requiring a stable source and newline-terminated JSONL."""
    if expected_signature is None:
        return None
    try:
        with file_path.open("rb") as source:
            before = os.fstat(source.fileno())
            if _source_stat_signature(before) != expected_signature:
                return None
            has_boundary = before.st_size == 0
            if before.st_size:
                source.seek(-1, 2)
                has_boundary = source.read(1) == b"\n"
            after = os.fstat(source.fileno())
        current_path = file_path.stat()
    except OSError, ValueError:
        return None
    if any(
        _source_stat_signature(observed) != expected_signature
        for observed in (before, after, current_path)
    ):
        return None
    return has_boundary


def _same_resolved_path(first: Path, second: Path) -> bool:
    try:
        return first.resolve() == second.resolve()
    except OSError, ValueError:
        return False


def _adoption_lookup(adoptable_window_ids: set[str]) -> set[str]:
    """The comparison form of the adoption set, built once per pass.

    Folded, because these keys were written by the hook while the set comes
    from the backend's listing, and agterm reports UUIDs uppercase while a
    caller may round-trip one lowercased. The set itself keeps the backend's
    spelling, since the audit reports orphan ids straight out of it.
    """
    return {canonical_window_id(wid) for wid in adoptable_window_ids}


class SessionMonitor:
    """Monitors Claude Code sessions for new assistant messages.

    Thin coordinator: delegates I/O to TranscriptReader, event reading to
    event_reader, session-map diffing to SessionLifecycle, and idle tracking
    to IdleTracker.
    """

    def __init__(
        self,
        projects_path: Path | None = None,
        poll_interval: float | None = None,
        state_file: Path | None = None,
        event_hub: EventHub | None = None,
    ):
        self.projects_path = (
            projects_path if projects_path is not None else config.claude_projects_path
        )
        self.poll_interval = (
            poll_interval if poll_interval is not None else config.monitor_poll_interval
        )

        self.state = MonitorState(state_file=state_file or config.monitor_state_file)
        self.state.load()

        self._running = False
        self._task: asyncio.Task | None = None
        self._event_hub = event_hub or EventHub()
        self._new_window_callback: (
            Callable[[NewWindowEvent], Awaitable[None]] | None
        ) = None
        # Lazy: providers.base imports HookEvent and gets imported back
        # through tmux_manager → providers; keep at call site.
        # Lazy: HookEvent pulled by hook dispatch path; defer until that path runs
        from .providers.base import HookEvent

        self._hook_event_callback: Callable[[HookEvent], Awaitable[None]] | None = None

        self._idle_tracker = IdleTracker()
        self._transcript_reader = TranscriptReader(
            self.state,
            self._idle_tracker,
            on_session_retired=self._discard_session_delivery_state,
        )
        # Receipts are grouped by transcript session so one failed send only
        # freezes its own watermark.
        self._delivery_receipts: dict[str, list[DeliveryReceipt]] = {}
        # Backlog skips cross the monitor/queue boundary through injected
        # adapters, preserving this module's handler independence.
        self._skip_purge_callback: (
            Callable[[BacklogSkipIntent], Awaitable[int | None]] | None
        ) = None
        self._skip_validate_callback: Callable[[BacklogSkipIntent], bool] | None = None
        self._skip_notice_callback: (
            Callable[[BacklogSkipIntent], Awaitable[None]] | None
        ) = None
        self._skip_notice_receipts: dict[str, DeliveryReceipt] = {}
        self._skip_retry_attempts: dict[str, int] = {}
        self._skip_retry_at: dict[str, float] = {}

    # Delegation properties for backward-compatible test access
    @property
    def _last_session_map(self) -> dict:
        return session_lifecycle.last_session_map

    @_last_session_map.setter
    def _last_session_map(self, value: dict) -> None:
        session_lifecycle.initialize(value)

    @property
    def _last_activity(self) -> dict:
        return self._idle_tracker._last_activity

    @property
    def _file_mtimes(self) -> dict:
        return self._transcript_reader._file_mtimes

    @property
    def _pending_tools(self) -> dict:
        return self._transcript_reader._pending_tools

    def get_last_activity(self, session_id: str) -> float | None:
        """Get monotonic timestamp of last transcript activity for a session."""
        return self._idle_tracker.get_last_activity(session_id)

    def _discard_session_delivery_state(self, session_id: str) -> None:
        """Drop receipts tied to a session identity that no longer exists."""
        self._delivery_receipts.pop(session_id, None)
        self._skip_notice_receipts.pop(session_id, None)
        self._clear_skip_retry(session_id)

    def _cancel_pending_skip(self, session_id: str) -> None:
        """Cancel a barrier and restore source work to its durable watermark."""
        self.state.cancel_skip(session_id)
        self._transcript_reader.replay_from_watermark(session_id)
        self.state.save_if_dirty()
        self._discard_session_delivery_state(session_id)

    def set_message_callback(
        self, callback: Callable[[NewMessage], Awaitable[None]]
    ) -> None:
        """Register the legacy Telegram message adapter for monitor tests."""

        async def render_and_deliver(batch: TranscriptBatch) -> None:
            for message in render_transcript_batch(batch):
                await callback(message)

        self._event_hub.unregister_durable_consumer("telegram")
        self._event_hub.register_durable_consumer("telegram", render_and_deliver)

    def register_delivery_consumer(
        self, name: str, handler: Callable[[TranscriptBatch], Awaitable[None]]
    ) -> None:
        self._event_hub.register_durable_consumer(name, handler)

    def unregister_delivery_consumer(self, name: str) -> None:
        self._event_hub.unregister_durable_consumer(name)

    def set_new_window_callback(
        self, callback: Callable[[NewWindowEvent], Awaitable[None]]
    ) -> None:
        self._new_window_callback = callback

    def set_hook_event_callback(self, callback: Callable[..., Awaitable[None]]) -> None:
        self._hook_event_callback = callback

    def set_skip_callbacks(
        self,
        *,
        purge: Callable[[BacklogSkipIntent], Awaitable[int | None]],
        notice: Callable[[BacklogSkipIntent], Awaitable[None]],
        validate: Callable[[BacklogSkipIntent], bool],
    ) -> None:
        """Install the queue adapters used by confirmed backlog skips."""
        self._skip_purge_callback = purge
        self._skip_validate_callback = validate
        self._skip_notice_callback = notice

    async def request_backlog_skip(
        self,
        user_id: int,
        window_id: str,
        thread_id: int | None,
        chat_id: int,
        *,
        expected_source: tuple[int, int, int, int, int] | None = None,
        automatic: bool = False,
    ) -> BacklogSkipIntent | None:
        """Freeze one source at EOF, persist its barrier, then retire its queue work."""
        if (
            self._skip_purge_callback is None
            or self._skip_notice_callback is None
            or self._skip_validate_callback is None
        ):
            raise RuntimeError("backlog skip callbacks are not wired")
        session_id = session_lifecycle.resolve_session_id(window_id)
        session = self.state.get_session(session_id) if session_id else None
        if not session_id or session is None or session_id in self.state.pending_skips:
            return None
        recipients = self._skip_recipients(session_id)
        requested_recipient = (user_id, thread_id, chat_id, window_id)
        if len(recipients) > 1 or (
            recipients
            and not any(
                recipient[0] == requested_recipient[0]
                and recipient[2] == requested_recipient[1]
                and recipient[3] == requested_recipient[2]
                and canonical_window_id(recipient[1])
                == canonical_window_id(requested_recipient[3])
                for recipient in recipients
            )
        ):
            logger.warning(
                "Refusing backlog skip for a shared or mismatched recipient set",
                session_id=session_id,
                recipient_count=len(recipients),
            )
            return None
        source_path = Path(session.file_path)
        source_stat = _stable_source_stat(source_path)
        source_signature = (
            _source_stat_signature(source_stat) if source_stat is not None else None
        )
        if (
            source_stat is None
            or (expected_source is not None and source_signature != expected_source)
            or source_stat.st_size < session.last_byte_offset
        ):
            logger.debug(
                "Deferring backlog skip because the transcript source is unstable",
                session_id=session_id,
            )
            return None
        snapshot_offset = source_stat.st_size
        provider = _resolve_provider_for_file(window_id, source_path)
        if provider.capabilities.supports_incremental_read:
            tail_is_complete = _source_has_complete_incremental_tail(
                source_path, source_signature
            )
            if tail_is_complete is not True:
                logger.debug(
                    "Deferring backlog skip because the incremental transcript has no stable complete-record boundary",
                    session_id=session_id,
                )
                return None
        source_range_sha256 = _source_range_sha256(
            source_path,
            session.last_byte_offset,
            snapshot_offset,
            device=source_stat.st_dev,
            inode=source_stat.st_ino,
        )
        source_stat_after_hash = _stable_source_stat(source_path)
        if (
            source_range_sha256 is None
            or source_stat_after_hash is None
            or _source_stat_signature(source_stat_after_hash) != source_signature
        ):
            logger.debug(
                "Deferring backlog skip because the transcript changed while freezing its range",
                session_id=session_id,
            )
            return None
        intent = BacklogSkipIntent(
            session_id=session_id,
            window_id=window_id,
            user_id=user_id,
            thread_id=thread_id,
            chat_id=chat_id,
            snapshot_offset=snapshot_offset,
            range_start=session.last_byte_offset,
            created_at=time.time(),
            source_path=str(source_path),
            source_device=source_stat.st_dev,
            source_inode=source_stat.st_ino,
            source_range_sha256=source_range_sha256,
            automatic=automatic,
        )
        # The durable barrier is written before destructive queue retirement.
        self.state.begin_skip(intent)
        if not self.state.save_if_dirty():
            # Queue work has not been purged yet. Rewinding here would make
            # the monitor enqueue a second copy while the original still runs.
            self.state.cancel_skip(session_id)
            self.state.save_if_dirty()
            return None
        prepared = await self._prepare_pending_skip(intent)
        if prepared is True and await self._enqueue_pending_skip_notice(intent):
            return intent
        return None

    def _skip_recipients(
        self, session_id: str
    ) -> list[tuple[int, str, int, int | None]]:
        """Return every topic currently routed to a source session."""
        # Lazy: ThreadRouter is wired through SessionManager during bootstrap.
        from .thread_router import thread_router

        return [
            (user_id, window_id, thread_id, chat_id)
            for user_id, chat_id, thread_id, window_id in thread_router.iter_thread_bindings_with_chat()
            if session_lifecycle.resolve_session_id(window_id) == session_id
        ]

    def _skip_source_is_current(
        self, intent: BacklogSkipIntent, *, verify_content: bool = False
    ) -> bool:
        """Keep barriers tied to the frozen path, generation, and optional content."""
        if not intent.source_path:
            # Legacy barriers cannot be committed against an unknown source.
            return False
        if (
            intent.source_device is None
            or intent.source_inode is None
            or not intent.source_range_sha256
        ):
            # Legacy barriers without a frozen-range digest must replay.
            return False
        tracked = self.state.get_session(intent.session_id)
        if (
            tracked is None
            or session_lifecycle.resolve_session_id(intent.window_id)
            != intent.session_id
        ):
            return False
        source_path = Path(intent.source_path)
        try:
            resolved_source = source_path.resolve()
            source_matches = resolved_source == Path(tracked.file_path).resolve()
            for window_id, details in session_lifecycle.last_session_map.items():
                if canonical_window_id(window_id) != canonical_window_id(
                    intent.window_id
                ):
                    continue
                current_path = details.get("transcript_path", "")
                if current_path:
                    source_matches = source_matches and (
                        Path(current_path).resolve() == resolved_source
                    )
                break
            current = source_path.stat()
        except OSError, ValueError:
            return False
        source_is_current = (
            source_matches
            and current.st_dev == intent.source_device
            and current.st_ino == intent.source_inode
            and current.st_size >= intent.snapshot_offset
        )
        if not source_is_current or not verify_content:
            return source_is_current
        return bool(intent.source_range_sha256) and (
            _source_range_sha256(
                source_path,
                intent.range_start,
                intent.snapshot_offset,
                device=intent.source_device,
                inode=intent.source_inode,
            )
            == intent.source_range_sha256
        )

    def _skip_is_current(
        self,
        intent: BacklogSkipIntent,
        *,
        strict: bool = False,
        verify_content: bool = False,
    ) -> bool | None:
        """True when the barrier source and registered recipient are current.

        ``strict=False`` (default) collapses a missing or erroring validator
        to False, for callers that only need a "still valid" gate.
        ``strict=True`` surfaces that failure as None instead, so a caller
        that must not act on an unknown verdict can defer to a later pass.
        ``verify_content=True`` checks the frozen-range digest at skip commit.
        """
        if not self._skip_source_is_current(intent, verify_content=verify_content):
            return False
        recipients = self._skip_recipients(intent.session_id)
        if len(recipients) != 1:
            return False
        user_id, window_id, thread_id, chat_id = recipients[0]
        if (
            user_id != intent.user_id
            or canonical_window_id(window_id) != canonical_window_id(intent.window_id)
            or thread_id != intent.thread_id
            or chat_id != intent.chat_id
        ):
            return False
        callback = self._skip_validate_callback
        if callback is None:
            return None if strict else False
        try:
            return callback(intent)
        except Exception:
            logger.exception(
                "Failed to validate backlog skip for %s", intent.session_id
            )
            return None if strict else False

    async def _maybe_skip_replay_cap(
        self,
        session_id: str,
        file_path: Path,
        window_id: str,
        details: dict[str, Any],
        cap_bytes: int,
    ) -> None:
        """Auto-skip one over-cap source only after identity and EOF stabilize."""
        if (
            cap_bytes <= 0
            or session_id in self.state.pending_skips
            or self._skip_purge_callback is None
            or self._skip_notice_callback is None
            or self._skip_validate_callback is None
        ):
            return
        tracked = self.state.get_session(session_id)
        recipients = self._skip_recipients(session_id)
        if tracked is None or len(recipients) != 1:
            return
        user_id, recipient_window, thread_id, chat_id = recipients[0]
        if chat_id is None or not _same_resolved_path(
            Path(tracked.file_path), file_path
        ):
            return
        before = _stable_source_stat(file_path)
        if before is None:
            return
        if before.st_size < tracked.replay_cap_retry_after:
            tracked.replay_cap_retry_after = 0
            self.state.update_session(tracked)
            return
        if (
            before.st_size
            - max(tracked.last_byte_offset, tracked.replay_cap_retry_after)
            <= cap_bytes
        ):
            return
        provider = get_provider_for_window(window_id, details.get("provider_name"))
        provider_name = (
            detect_provider_from_transcript_path(str(file_path))
            or provider.capabilities.name
        )
        if not identity_state.accepts_provider_observation(window_id, provider_name):
            return
        (
            generation_changed,
            _consumed_intact,
        ) = await self._transcript_reader._prepare_observed_generation(
            session_id,
            tracked,
            file_path,
            before,
            check_marker=provider.capabilities.supports_incremental_read,
        )
        if generation_changed:
            tracked.replay_cap_retry_after = 0
            self.state.update_session(tracked)
        after = _stable_source_stat(file_path)
        signature = _source_stat_signature(after) if after is not None else None
        if (
            generation_changed
            or signature is None
            or _source_stat_signature(before) != signature
            or not identity_state.accepts_provider_observation(window_id, provider_name)
            or session_lifecycle.resolve_session_id(window_id) != session_id
            or set(self._skip_recipients(session_id)) != set(recipients)
        ):
            return
        await self.request_backlog_skip(
            user_id,
            recipient_window,
            thread_id,
            chat_id,
            expected_source=signature,
            automatic=True,
        )

    def _skip_retry_due(self, session_id: str) -> bool:
        return time.monotonic() >= self._skip_retry_at.get(session_id, 0.0)

    def _schedule_skip_retry(self, session_id: str) -> None:
        attempt = self._skip_retry_attempts.get(session_id, 0) + 1
        delay = min(
            _SKIP_RETRY_MAX_SECONDS,
            _SKIP_RETRY_BASE_SECONDS * (2 ** min(attempt - 1, 5)),
        )
        self._skip_retry_attempts[session_id] = attempt
        self._skip_retry_at[session_id] = time.monotonic() + delay
        logger.warning(
            "Backlog skip step failed; retrying later",
            session_id=session_id,
            retry=attempt,
            retry_in_seconds=delay,
        )

    def _clear_skip_retry(self, session_id: str) -> None:
        self._skip_retry_attempts.pop(session_id, None)
        self._skip_retry_at.pop(session_id, None)

    async def _prepare_pending_skip(self, intent: BacklogSkipIntent) -> bool | None:
        """Retire frozen source work before a skip notice may be sent."""
        if not self._skip_is_current(intent):
            logger.warning("Cancelling stale backlog skip for %s", intent.session_id)
            self._cancel_pending_skip(intent.session_id)
            return None
        if intent.purge_complete:
            return True
        callback = self._skip_purge_callback
        if callback is None or not self._skip_retry_due(intent.session_id):
            return False
        try:
            skipped = await callback(intent)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to purge backlog for %s", intent.session_id)
            self._schedule_skip_retry(intent.session_id)
            return False
        if skipped is None:
            logger.warning("Cancelling stale backlog skip for %s", intent.session_id)
            self._cancel_pending_skip(intent.session_id)
            return None
        if not self._skip_is_current(intent):
            logger.warning("Cancelling stale backlog skip for %s", intent.session_id)
            self._cancel_pending_skip(intent.session_id)
            return None
        self.state.update_skip_count(intent.session_id, skipped)
        if not self.state.save_if_dirty():
            intent.purge_complete = False
            self._schedule_skip_retry(intent.session_id)
            return False
        self._clear_skip_retry(intent.session_id)
        return True

    async def _enqueue_pending_skip_notice(self, intent: BacklogSkipIntent) -> bool:
        """Create one receipt-tracked visible notice for a persisted barrier."""
        if not self._skip_is_current(intent):
            self._cancel_pending_skip(intent.session_id)
            return False
        if intent.session_id in self._skip_notice_receipts or not self._skip_retry_due(
            intent.session_id
        ):
            return False
        callback = self._skip_notice_callback
        if callback is None:
            return False
        receipt = new_delivery_receipt(checkpoint=intent.snapshot_offset)
        token = activate_delivery_receipt(receipt)
        try:
            await callback(intent)
        except asyncio.CancelledError:
            receipt.fail()
            raise
        except Exception:
            receipt.fail()
            logger.exception(
                "Failed to enqueue backlog skip notice for %s", intent.session_id
            )
            self._schedule_skip_retry(intent.session_id)
        finally:
            deactivate_delivery_receipt(token)
            receipt.close()
        if receipt.failed:
            return False
        self._clear_skip_retry(intent.session_id)
        self._skip_notice_receipts[intent.session_id] = receipt
        return True

    def _expire_aged_skip_barriers(self) -> None:
        """Cancel aged barriers so an unacknowledged range replays.

        A deadline never acknowledges a skip. If the visible notice has not
        reached Telegram, cancel the persisted barrier and resume from the
        delivered watermark; this favors replay over permanent source silence.
        """
        if not self.state.pending_skips:
            return
        now = time.time()
        for session_id, intent in tuple(self.state.pending_skips.items()):
            if not intent.created_at:
                # Legacy record predating the stamp: start its clock now,
                # through the mutator so the stamp actually persists.
                self.state.stamp_skip_clock(session_id, now)
                continue
            if now - intent.created_at <= config.skip_barrier_deadline_s:
                continue
            logger.warning(
                "Backlog skip barrier expired without an acknowledged notice; "
                "cancelling so the range replays",
                session_id=session_id,
                barrier_age_seconds=round(now - intent.created_at, 1),
            )
            if intent.automatic:
                tracked = self.state.get_session(session_id)
                if tracked is not None:
                    tracked.replay_cap_retry_after = max(
                        tracked.replay_cap_retry_after, intent.snapshot_offset
                    )
                    self.state.update_session(tracked)
            self._cancel_pending_skip(session_id)
        self.state.save_if_dirty()

    async def _advance_skip_barriers(self) -> None:
        """Resume attempts, commit acknowledged skips, then expire unacknowledged ones.

        Resume runs before expiry so a process that slept past the deadline
        still gets one notice delivery attempt. A notice already acknowledged
        at the deadline commits before expiry; unacknowledged barriers replay.
        """
        await self._resume_pending_skip_notices()
        self._commit_pending_skips()
        self._expire_aged_skip_barriers()

    async def _resume_pending_skip_notices(self) -> None:
        """Resume persisted skip barriers before reading any skipped bytes."""
        for intent in tuple(self.state.pending_skips.values()):
            if not self._skip_retry_due(intent.session_id):
                continue
            prepared = await self._prepare_pending_skip(intent)
            if prepared:
                await self._enqueue_pending_skip_notice(intent)

    def _commit_pending_skips(self) -> None:
        """Advance only barriers whose visible notices reached Telegram."""
        for session_id, receipt in tuple(self._skip_notice_receipts.items()):
            if receipt.failed:
                # A failed notice has no acknowledgement boundary. Remove the
                # failed receipt so the persisted barrier can retry in-process;
                # the source remains paused until a notice is delivered.
                self._skip_notice_receipts.pop(session_id, None)
                self._schedule_skip_retry(session_id)
                continue
            if not receipt.commit_ready:
                continue
            intent = self.state.pending_skips.get(session_id)
            if intent is None or not self._skip_is_current(intent, verify_content=True):
                # A topic may rebind after the notice was queued or delivered.
                # Never advance the old source watermark across that boundary.
                self._cancel_pending_skip(session_id)
                continue
            if self.state.complete_skip(session_id):
                # Rebuild provider context through the skipped boundary before
                # parsing the next suffix, which may begin a new Codex turn.
                self._transcript_reader.replay_from_watermark(session_id)
                self.state.save_if_dirty()
                self._delivery_receipts.pop(session_id, None)
            else:
                # The tracked session was removed or re-keyed. Do not retain a
                # barrier that can no longer be committed or replay safely.
                self._cancel_pending_skip(session_id)
            self._skip_notice_receipts.pop(session_id, None)
            self._clear_skip_retry(session_id)

    def record_hook_activity(self, window_id: str) -> None:
        """Record hook-based activity for a window (resets idle timers)."""
        session_id = session_lifecycle.resolve_session_id(window_id)
        if session_id:
            self._idle_tracker.record_activity(session_id)

    def commit_delivered_watermarks(self) -> None:
        """Persist receipts acknowledged by the delivery boundary.

        Called after a normal monitor cycle and after the bounded shutdown
        drain. It intentionally has no queue implementation knowledge.
        """
        self._commit_watermark_prefixes()

    def _commit_watermark_prefixes(self) -> None:
        """Commit each session's longest settled receipt run (#205).

        Waiting for every receipt of a session to close (the previous
        policy) never commits under sustained output: in-flight tasks hold
        back the whole settled run, so a restart replays it in full into
        the outbound queue. The run policy lives in
        delivery_contract.settled_prefix / settled_run_offset (including
        the shared-batch-checkpoint tie rule: persistence lags delivery by
        at most one in-flight batch); this coordinator groups receipts by
        session, keeps the pending-tool, tool-delivery, and pending-skip
        fences, and persists one batched commit per cycle. The settled run is
        consumed even when the tie rule defers its commit: replay then
        re-delivers those messages, which at-least-once permits.
        """
        delivered_offsets: dict[str, int] = {}
        consumed: dict[str, int] = {}
        self._commit_pending_skips()
        for session_id, receipts in self._delivery_receipts.items():
            if (
                not receipts
                or session_id in self.state.pending_skips
                or session_id in self._pending_tools
            ):
                continue
            tool_fence = self._transcript_reader._tool_delivery_fences.get(session_id)
            if tool_fence is not None:
                release_checkpoint = tool_fence.release_checkpoint
                fenced_receipts = [
                    receipt
                    for receipt in receipts
                    if receipt.checkpoint is not None
                    and receipt.checkpoint >= tool_fence.call_checkpoint
                    and (
                        release_checkpoint is None
                        or receipt.checkpoint <= release_checkpoint
                    )
                ]
                if (
                    release_checkpoint is None
                    or not any(
                        receipt.checkpoint == release_checkpoint
                        for receipt in fenced_receipts
                    )
                    or not all(receipt.commit_ready for receipt in fenced_receipts)
                ):
                    continue
                self._transcript_reader._tool_delivery_fences.pop(session_id, None)
            prefix = settled_prefix(receipts)
            if not prefix:
                continue
            fence = receipts[len(prefix)] if len(prefix) < len(receipts) else None
            offset = settled_run_offset(prefix, fence)
            if offset is not None:
                delivered_offsets[session_id] = offset
            consumed[session_id] = len(prefix)
        if delivered_offsets and self.state.commit_parsed_offsets(
            set(delivered_offsets), delivered_offsets=delivered_offsets
        ):
            self.state.save_if_dirty()
        for session_id, count in consumed.items():
            remainder = self._delivery_receipts[session_id][count:]
            if remainder:
                self._delivery_receipts[session_id] = remainder
            else:
                self._delivery_receipts.pop(session_id, None)

    def _reserve_replay_start(
        self, window_id: str, session_id: str, path: Path
    ) -> bool:
        """Persist offset zero before consuming a session-map replay marker."""
        tracked = self.state.get_session(session_id)
        if tracked is None or tracked.file_path != str(path):
            self.state.update_session(
                TrackedSession(
                    session_id=session_id,
                    file_path=str(path),
                    last_byte_offset=0,
                )
            )
        if not self.state.save_if_dirty():
            return False
        if not acknowledge_replay_from_start(window_id, session_id):
            logger.warning(
                "Could not consume replay-from-start marker for session %s",
                session_id,
            )
        return True

    async def check_for_updates(self, current_map: dict) -> list[NewMessage]:
        """Check all sessions for new assistant messages.

        Routes sessions to _process_session_file (allowing test spying) and
        delegates the actual I/O to TranscriptReader. Uses _get_active_cwds()
        for fallback session discovery so tests can stub tmux calls.
        """
        new_messages: list[NewMessage] = []
        sid_to_wid = {v["session_id"]: wid for wid, v in current_map.items()}
        replay_cap_bytes = _replay_cap_bytes()

        direct_sessions: list[tuple[str, Path]] = []
        fallback_session_ids: set[str] = set()

        for window_id, details in current_map.items():
            session_id = details["session_id"]
            transcript_path = details.get("transcript_path", "")
            if transcript_path:
                path = Path(transcript_path)
                replay_from_start = details.get("replay_from_start") is True
                if replay_from_start and not self._reserve_replay_start(
                    window_id, session_id, path
                ):
                    continue
                if path.exists():
                    direct_sessions.append((session_id, path))
                    continue
                if replay_from_start:
                    # The explicit hook path is authoritative. Scanning can
                    # find a different generation and seed it at EOF before
                    # this file is created.
                    continue
            fallback_session_ids.add(session_id)

        for session_id, file_path in direct_sessions:
            window_id = sid_to_wid[session_id]
            if replay_cap_bytes:
                try:
                    await self._maybe_skip_replay_cap(
                        session_id,
                        file_path,
                        window_id,
                        current_map.get(window_id, {}),
                        replay_cap_bytes,
                    )
                except Exception:
                    logger.exception(
                        "Replay-cap check failed for session %s", session_id
                    )
            if session_id in self.state.pending_skips:
                continue
            try:
                await self._process_session_file(
                    session_id,
                    file_path,
                    new_messages,
                    window_id=window_id,
                )
            except Exception:
                logger.exception("Error processing session %s", session_id)

        if fallback_session_ids:
            active_cwds = await self._get_active_cwds()
            sessions = self._scan_projects_sync(active_cwds) if active_cwds else []
            for session_info in sessions:
                session_id = session_info.session_id
                if session_id not in fallback_session_ids:
                    continue
                window_id = sid_to_wid[session_id]
                if replay_cap_bytes:
                    try:
                        await self._maybe_skip_replay_cap(
                            session_id,
                            session_info.file_path,
                            window_id,
                            current_map.get(window_id, {}),
                            replay_cap_bytes,
                        )
                    except Exception:
                        logger.exception(
                            "Replay-cap check failed for session %s", session_id
                        )
                if session_id in self.state.pending_skips:
                    continue
                try:
                    await self._process_session_file(
                        session_id,
                        session_info.file_path,
                        new_messages,
                        window_id=window_id,
                    )
                except Exception:
                    logger.exception("Error processing session %s", session_id)

        self.state.save_if_dirty()
        return new_messages

    async def _process_session_file(
        self, session_id: str, file_path: Path, new_messages: list, window_id: str = ""
    ) -> None:
        """Process a single session file (delegates to TranscriptReader)."""
        await self._transcript_reader._process_session_file(
            session_id, file_path, new_messages, window_id=window_id
        )

    def _scan_projects_sync(self, active_cwds: set) -> list:
        """Scan projects synchronously (delegates to TranscriptReader)."""
        return self._transcript_reader._scan_projects_sync(
            self.projects_path, active_cwds
        )

    async def _get_active_cwds(self) -> set[str]:
        """Get normalized cwds of all active tmux windows (delegates to TranscriptReader)."""
        return await self._transcript_reader._get_active_cwds()

    async def _read_new_lines(
        self, session: Any, file_path: Path, window_id: str = ""
    ) -> list:
        """Read new lines from session file (delegates to TranscriptReader)."""
        return await self._transcript_reader._read_new_lines(
            session, file_path, window_id
        )

    async def _read_hook_events(self) -> None:
        """Read new lines from events.jsonl and dispatch via callback."""
        if not self._hook_event_callback:
            return

        offset_before = self.state.events_offset
        events, new_offset = await read_new_events(
            config.events_file, self.state.events_offset
        )
        self.state.events_offset = new_offset
        if new_offset != offset_before:
            self.state._dirty = True

        for event in events:
            window_id = strip_session_map_prefix(event.window_key, session_map_prefix())
            if window_id and not identity_state.accepts_provider_observation(
                window_id, observed_provider(event.data)
            ):
                continue
            try:
                await self._hook_event_callback(event)
            except _CallbackError:
                logger.exception("Hook event callback error for %s", event.event_type)

    async def _load_current_session_map(
        self, raw: dict | None = None
    ) -> dict[str, dict[str, str]]:
        """Load a validated session_map mapping.

        Callers that reconcile or prune first read the raw map and explicitly
        preserve a failed read. This compatibility helper keeps its historical
        mapping return type for callers that only need a parsed snapshot.
        """
        if raw is None:
            raw = await read_session_map_raw()
        if not isinstance(raw, dict):
            return {}
        prefix = session_map_prefix()
        return parse_session_map(raw, prefix)

    async def _cleanup_all_stale_sessions(self) -> None:
        """Clean up all tracked sessions not in current session_map (startup)."""
        raw = await read_session_map_raw()
        if raw is None:
            logger.warning("Startup cleanup skipped: session_map is unreadable")
            return
        current_map = await self._load_current_session_map(raw)
        active_session_ids = {v["session_id"] for v in current_map.values()}

        stale_sessions = [
            sid for sid in self.state.tracked_sessions if sid not in active_session_ids
        ]
        if stale_sessions:
            logger.info(
                "[Startup cleanup] Removing %d stale sessions", len(stale_sessions)
            )
            for session_id in stale_sessions:
                self._transcript_reader.clear_session(session_id)
                self._idle_tracker.clear_session(session_id)
            self.state.save_if_dirty()

    async def _detect_and_cleanup_changes(
        self,
        raw: dict | None = None,
        *,
        adoptable_window_ids: set[str] | None,
    ) -> dict[str, dict[str, str]]:
        """Reconcile session_map; clean up replaced/removed sessions; fire new-window events.

        ``adoptable_window_ids`` is this cycle's verdict, required rather than
        defaulted: it is per-cycle input with one production caller, so holding
        it as monitor state let a stale cycle's answer be reused and let the
        first cycle run with none at all. ``None`` means no listing was
        available, and then nothing is adopted — a hook entry is not evidence
        that ccgram may adopt the window it names.
        """
        if raw is None:
            raw = await read_session_map_raw()
        if raw is None:
            logger.warning("Session-map reconciliation skipped: map is unreadable")
            return session_lifecycle.last_session_map
        current_map = await self._load_current_session_map(raw)
        result = session_lifecycle.reconcile(current_map, self._idle_tracker)

        for session_id in result.sessions_to_remove:
            self._transcript_reader.clear_session(session_id)
        if result.sessions_to_remove:
            self.state.save_if_dirty()

        adoption_windows = dict(result.new_windows)
        # Lazy: thread_router is wired into session_manager which imports
        # session_monitor; hoisting forms a startup cycle.
        # Lazy: proxies wired by SessionManager constructor
        from .thread_router import thread_router

        for window_id, details in result.changed_windows.items():
            if not thread_router.has_window(window_id):
                adoption_windows[window_id] = details

        if adoption_windows:
            # Lazy: session.py imports session_monitor at top; hoisting
            # session_manager forms a hard cycle on bootstrap.
            from .session import session_manager as _sm

            adoptable_lookup = (
                None
                if adoptable_window_ids is None
                else _adoption_lookup(adoptable_window_ids)
            )
            for window_id, details in adoption_windows.items():
                provider_name = details.get("provider_name", "")
                if provider_name:
                    _sm.set_window_provider(window_id, provider_name)

                if thread_router.has_window(window_id):
                    # A key that is new to the map is not a window that is new
                    # to ccgram. Identity folding runs first (``_monitor_loop``),
                    # so a re-keyed or late-published identity already carries
                    # the topic it was bound under; announcing it here would
                    # ask for a second topic for the same agent. Both other
                    # discovery paths skip bound windows for the same reason.
                    continue

                if (
                    adoptable_lookup is None
                    or canonical_window_id(window_id) not in adoptable_lookup
                ):
                    # A hook entry proves an agent ran there, not that ccgram
                    # may adopt it. Installed globally, the hook writes entries
                    # for windows outside the configured scope too.
                    continue

                if self._new_window_callback:
                    event = NewWindowEvent(
                        window_id=window_id,
                        session_id=details["session_id"],
                        window_name=details.get("window_name", ""),
                        cwd=details.get("cwd", ""),
                    )
                    try:
                        await self._new_window_callback(event)
                    except _CallbackError:
                        logger.exception(
                            "New window callback error (session_map path) for %s",
                            window_id,
                        )

        return result.current_map

    async def _emit_unbound_window_events(
        self, all_windows: list, known_window_ids: set[str]
    ) -> None:
        """Fire a NewWindowEvent for each live window not in session_map / bound.

        Surfaces windows the hook never registered (no session_map entry) so
        they can become topics. What qualifies is the backend's own verdict,
        carried per window in ``WindowRef.topic_eligible``: herdr marks records
        holding a guarded target and an agent label, agterm marks in-scope
        sessions running an agent, tmux marks everything except the placeholder
        main window, its own window and ``_``-prefixed ones, which its complete
        listing returns ineligible instead of dropping. No capability flag
        gates this; keying it on one produced two bugs in a row.
        """
        if not self._new_window_callback:
            return
        # Lazy: thread_router is wired into session_manager which imports
        # session_monitor; hoisting forms a startup cycle.
        from .thread_router import thread_router

        caps = tmux_manager.capabilities
        known_lookup = _adoption_lookup(known_window_ids)
        bound_lookup = _adoption_lookup(
            {wid for _, _, wid in thread_router.iter_thread_bindings()}
        )
        for window in all_windows:
            window_key = canonical_window_id(window.window_id)
            if window_key in known_lookup:
                continue
            if window_key in bound_lookup:
                continue
            if not is_agent_topic_window(window, caps):
                continue
            event = NewWindowEvent(
                window_id=window.window_id,
                session_id="",
                window_name=window.window_name,
                cwd=window.cwd,
            )
            try:
                await self._new_window_callback(event)
            except _CallbackError:
                logger.exception(
                    "New window callback error (unbound window path) for %s",
                    window.window_id,
                )

    async def _emit_known_unbound_window_events(
        self,
        current_map: dict,
        adoptable_window_ids: set[str],
    ) -> None:
        """Fire a NewWindowEvent for each session_map window that is not bound.

        Steady-state self-heal: a tab that was in session_map at startup (known,
        so never a delta) but not yet bound to a Telegram topic retries on every
        poll until it succeeds. ``handle_new_window`` is idempotent — it skips
        windows that are already bound — so this generates no spam for bound tabs.

        ``adoptable_window_ids`` holds the live windows whose backend marked them
        ``WindowRef.topic_eligible``. It is NOT the plain live set: the listing
        this is built from is the complete reconciliation one, because pruning
        needs every window that exists. Filtering here is what keeps a window
        the backend excluded — out of scope, ccgram's own, hidden by convention
        — from being adopted through the session-map path, which reaches this
        code whenever a globally installed agent hook writes an entry for it.
        """
        if not self._new_window_callback:
            return
        # Lazy: thread_router is wired into session_manager which imports
        # session_monitor; hoisting forms a startup cycle.
        from .thread_router import thread_router

        bound_window_ids = {wid for _, _, wid in thread_router.iter_thread_bindings()}
        bound_lookup = _adoption_lookup(bound_window_ids)
        adoptable_lookup = _adoption_lookup(adoptable_window_ids)
        for window_id, details in current_map.items():
            if canonical_window_id(window_id) not in adoptable_lookup:
                continue  # dead, or the backend says it may not be adopted
            if canonical_window_id(window_id) in bound_lookup:
                continue  # already has a topic
            event = NewWindowEvent(
                window_id=window_id,
                session_id=details.get("session_id", ""),
                window_name=details.get("window_name", ""),
                cwd=details.get("cwd", ""),
            )
            try:
                await self._new_window_callback(event)
            except _CallbackError:
                logger.exception(
                    "New window callback error (known-unbound path) for %s",
                    window_id,
                )

    def _register_delivery_receipts(
        self, batches: tuple[TranscriptBatch, ...]
    ) -> list[tuple[TranscriptBatch, str, DeliveryReceipt]]:
        """Register each parse-cycle batch per consumer before any await.

        All records in a batch share its real source checkpoint. Keeping one
        receipt per batch and consumer preserves the settled-prefix tie rule:
        a failed sibling consumer cannot let that checkpoint advance.
        """
        pending: list[tuple[TranscriptBatch, str, DeliveryReceipt]] = []
        consumer_names = self._event_hub.durable_consumer_names()
        for batch in batches:
            for consumer_name in consumer_names:
                receipt = new_delivery_receipt(checkpoint=batch.byte_checkpoint)
                self._delivery_receipts.setdefault(batch.session_id, []).append(receipt)
                pending.append((batch, consumer_name, receipt))
        return pending

    async def _dispatch_message_with_receipt(
        self,
        batch: TranscriptBatch,
        receipt: DeliveryReceipt | None = None,
        consumer_name: str = "telegram",
    ) -> None:
        """Run one durable consumer under its source-checkpoint receipt."""
        if receipt is None:
            receipt = new_delivery_receipt(checkpoint=batch.byte_checkpoint)
            self._delivery_receipts.setdefault(batch.session_id, []).append(receipt)
        token = activate_delivery_receipt(receipt)
        try:
            await self._event_hub.dispatch_durable(consumer_name, batch)
        except asyncio.CancelledError:
            receipt.fail()
            raise
        except _CallbackError:
            receipt.fail()
            logger.exception(
                "Durable delivery callback error for session=%s consumer=%s",
                batch.session_id,
                consumer_name,
            )
        finally:
            deactivate_delivery_receipt(token)
            receipt.close()

    async def _read_and_sync_session_map(self) -> dict | None:
        """Discard snapshots invalidated by provider selection during I/O."""
        # Lazy: session_map is wired by SessionManager during bootstrap.
        from .session_map import session_map_sync

        revision = session_map_sync.selection_revision
        raw = await read_session_map_raw()
        if raw is None or revision != session_map_sync.selection_revision:
            logger.debug(
                "Session-map read unconfirmed or selection changed; deferring poll"
            )
            return None
        await session_map_sync.load_session_map(raw)
        return raw

    async def _monitor_loop(self) -> None:
        """Background poll loop."""
        logger.info("Session monitor started, polling every %ss", self.poll_interval)

        # Lazy: session_map imports session_monitor types via shared
        # state cycle; keep at call site.
        # Lazy: proxies wired by SessionManager constructor
        from .session_map import session_map_sync

        await self._cleanup_all_stale_sessions()
        initial_raw = await read_session_map_raw()
        initial_map = await self._load_current_session_map(initial_raw)
        session_lifecycle.initialize(initial_map)

        error_streak = 0
        while self._running:
            try:
                # The same long-lived task handles every cycle. Do not attach a
                # prior message's session_id to reconciliation and hook logs.
                structlog.contextvars.clear_contextvars()

                # Fold backend-attested aliases before hook dispatch and map
                # loading, both of which require canonical window identities.
                all_windows = await list_windows_for_reconciliation(tmux_manager)
                # Set before the session-map paths run, not after: they consume
                # it. None while a listing is unavailable, so adoption fails
                # closed instead of reusing a stale cycle's verdict.
                adoptable_window_ids = (
                    None
                    if all_windows is None
                    else {w.window_id for w in all_windows if w.topic_eligible}
                )
                if all_windows is None:
                    logger.warning(
                        "Multiplexer listing unavailable; skipping window reconciliation"
                    )
                else:
                    # Before anything keys off these ids, let the backend
                    # reconcile only aliases it explicitly attests as safe.
                    # Herdr publishes no raw locator aliases, so a missing or
                    # changed session target remains unresolved until an
                    # operator explicitly rebinds it.
                    # Lazy: importing session_manager at module scope forms a
                    # hard cycle on bootstrap (same reason as below).
                    from .session import session_manager as _sm

                    # Lazy: thread routing imports monitor-facing helpers.
                    from .thread_router import thread_router

                    _sm.reconcile_window_aliases(all_windows)
                    note_live_windows(all_windows, thread_router.all_bound_window_ids())

                # Hook routing is exact-bound: consuming a canonical event
                # before moving a legacy topic binding would drop it.
                await self._read_hook_events()

                raw_session_map = await self._read_and_sync_session_map()
                if raw_session_map is None:
                    await asyncio.sleep(self.poll_interval)
                    continue
                current_map = await self._detect_and_cleanup_changes(
                    raw_session_map, adoptable_window_ids=adoptable_window_ids
                )

                monitored_map = current_map
                if all_windows is not None:
                    live_window_ids = {
                        canonical_window_id(w.window_id) for w in all_windows
                    }
                    session_map_sync.prune_session_map(live_window_ids)
                    # Pruning needs every live window; adoption needs only the
                    # ones the backend says may be adopted. The listing is
                    # complete on purpose, so the two sets differ.
                    known_window_ids = set(current_map.keys())
                    await self._emit_unbound_window_events(
                        all_windows, known_window_ids
                    )
                    await self._emit_known_unbound_window_events(
                        current_map, adoptable_window_ids or set()
                    )
                    monitored_map = {
                        window_id: details
                        for window_id, details in current_map.items()
                        if canonical_window_id(window_id) in live_window_ids
                    }

                # A persisted barrier must be noticed before its source is read
                # again; this preserves the exact EOF snapshot across restarts.
                await self._advance_skip_barriers()
                await self.check_for_updates(monitored_map)
                transcript_batches = self._transcript_reader.drain_batches()
                # Register every normalized batch before the next await. A
                # shutdown cancellation between parse and dispatch must leave
                # a non-ready receipt so its source offset remains replayable.
                pending_dispatches = self._register_delivery_receipts(
                    transcript_batches
                )

                for batch, consumer_name, receipt in pending_dispatches:
                    structlog.contextvars.clear_contextvars()
                    structlog.contextvars.bind_contextvars(session_id=batch.session_id)
                    logger.debug(
                        "Dispatching %s transcript records for session=%s to %s",
                        len(batch.records),
                        batch.session_id,
                        consumer_name,
                    )
                    await self._dispatch_message_with_receipt(
                        batch, receipt, consumer_name
                    )

                self.commit_delivered_watermarks()

            except _LoopError:
                logger.exception("Monitor loop error")
                backoff_delay = min(_BACKOFF_MAX, _BACKOFF_MIN * (2**error_streak))
                error_streak += 1
                await asyncio.sleep(backoff_delay)
                continue
            except Exception:
                logger.exception("Unexpected error in monitor loop")
                backoff_delay = min(_BACKOFF_MAX, _BACKOFF_MIN * (2**error_streak))
                error_streak += 1
                await asyncio.sleep(backoff_delay)
                continue

            error_streak = 0
            await asyncio.sleep(self.poll_interval)

        logger.info("Session monitor stopped")

    def start(self) -> None:
        if self._running:
            logger.debug("Monitor already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        self._task.add_done_callback(task_done_callback)

    def stop(self) -> None:
        """Request producer cancellation; use ``stop_and_wait`` before drain."""
        self._running = False
        if self._task:
            self._task.cancel()
        self.state.save()
        # Distinct from the loop's "Session monitor stopped" (logged when the
        # poll loop actually exits) — this marks the stop request + state save.
        logger.info("Session monitor stop requested; state saved")

    async def stop_and_wait(self) -> None:
        """Cancel the monitor producer and wait until it cannot enqueue again."""
        self.stop()
        task = self._task
        if task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await task
            if self._task is task:
                self._task = None


_active_monitor: SessionMonitor | None = None


def set_active_monitor(monitor: SessionMonitor) -> None:
    """Set the active SessionMonitor instance (called by bot.py post_init)."""
    global _active_monitor  # noqa: PLW0603
    _active_monitor = monitor


def clear_active_monitor() -> None:
    """Clear the active SessionMonitor singleton (shutdown / test reset)."""
    global _active_monitor  # noqa: PLW0603
    _active_monitor = None


def get_active_monitor() -> SessionMonitor | None:
    """Return the active SessionMonitor instance."""
    return _active_monitor
