"""Opt-in auto-restore of dead topics at bot startup.

With ``CCGRAM_AUTO_RESTORE_ON_START=true`` the bot resumes, at startup, every
topic whose window died (a reboot kills them all) and whose provider session is
still on disk, through the replacement flow the Resume button uses.

One confirmed window listing builds a plan before anything is created: window
ids restart with the server, so a stale id may already name a live window, and
such a binding is never resumed into. Before the first create the multiplexer
advances its window-id counter past every id any binding holds, so no new window
takes another topic's old id. The plan is saved to a ledger keyed by the server
boot id: one attempt per (chat, topic, session) per boot, and the next start
finishes a plan a crash cut short.

Public surface: :func:`start_auto_restore`.
"""

from __future__ import annotations

import asyncio
import glob
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import structlog

from ... import window_query
from ...config import config
from ...multiplexer import multiplexer, reserve_window_ids_past
from ...multiplexer.base import WindowRef
from ...multiplexer.reconciliation import list_windows_for_reconciliation
from ...providers._resume import discover_claude_sessions
from ...providers.codex import codex_sessions_dir
from ...thread_router import thread_router
from ...utils import atomic_write_json, task_done_callback
from ..topics import topic_orchestration
from .recovery_banner import create_and_bind_window
from .recovery_ui import HeadlessUi, ReplacementRequest

if TYPE_CHECKING:
    from telegram import Bot

    from ...session_protocol.telegram import TelegramSessionAdapter

logger = structlog.get_logger()

GAP_S = 3.0
MAX_RATE_LIMIT_WAITS = 3
MAX_RATE_LIMIT_WAIT_S = 300.0
_LEDGER_FILE = "auto_restore.json"
_LEDGER_VERSION = 1
_PENDING, _ATTEMPTING, _DONE, _FAILED = "pending", "attempting", "done", "failed"
_STATUSES = frozenset({_PENDING, _ATTEMPTING, _DONE, _FAILED})
_ATTEMPTED = frozenset({_ATTEMPTING, _DONE, _FAILED})
_WHERE = ("chat_id", "thread_id", "window_id")


@dataclass(frozen=True)
class RestoreEntry:
    """Everything needed to restore one topic, captured before any create."""

    user_id: int
    chat_id: int
    thread_id: int
    old_window_id: str
    cwd: str
    session_id: str
    provider_name: str
    approval_mode: Literal["normal", "yolo"]
    display: str

    @property
    def key(self) -> str:
        return f"{self.chat_id}:{self.thread_id}:{self.session_id}"

    @property
    def where(self) -> tuple[int, int, str]:
        return self.chat_id, self.thread_id, self.old_window_id


class _Ledger:
    """Restore attempts of one server boot, written to disk on every change."""

    def __init__(self, path: Path, boot_id: str) -> None:
        self._path = path
        self._boot_id = boot_id
        self._rows: dict[str, tuple[RestoreEntry, str]] = {}

    @classmethod
    def load(cls, path: Path, boot_id: str) -> _Ledger:
        """Read the ledger; another boot's rows (or an unreadable file) are void.

        A row with an unknown status counts as attempted: it is never retried.
        """
        ledger = cls(path, boot_id)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if raw["boot_id"] == boot_id:
                if raw["version"] != _LEDGER_VERSION:
                    raise ValueError(f"unknown ledger version {raw['version']!r}")
                for row in raw["entries"]:
                    status = row.pop("status")
                    entry = RestoreEntry(**row)
                    ledger._rows[entry.key] = (
                        entry,
                        status if status in _STATUSES else _FAILED,
                    )
        except FileNotFoundError:
            pass
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            logger.warning("auto_restore_ledger_unreadable", error=str(exc))
            ledger._rows.clear()
        return ledger

    def status(self, key: str) -> str | None:
        return self._rows[key][1] if key in self._rows else None

    def add_pending(self, entries: list[RestoreEntry]) -> None:
        """Record planned entries that have no row yet, then save."""
        for entry in entries:
            self._rows.setdefault(entry.key, (entry, _PENDING))
        self._save()

    def set_status(self, key: str, status: str) -> None:
        self._rows[key] = (self._rows[key][0], status)
        self._save()

    def _save(self) -> None:
        entries = [{**asdict(e), "status": st} for e, st in self._rows.values()]
        atomic_write_json(
            self._path,
            {"version": _LEDGER_VERSION, "boot_id": self._boot_id, "entries": entries},
        )


@dataclass(frozen=True)
class _World:
    live: list[WindowRef]
    live_sessions: frozenset[str]


def _skip(reason: str, where: tuple[int | None, int, str]) -> None:
    log = logger.debug if reason == "window_alive" else logger.info
    log("auto_restore_skip", reason=reason, **dict(zip(_WHERE, where, strict=True)))


def _codex_rollout_exists(session_id: str) -> bool:
    pattern = f"rollout-????-??-??T??-??-??-{glob.escape(session_id)}.jsonl"
    return any(codex_sessions_dir().glob(f"**/{pattern}"))


def _claude_transcript_exists(entry: RestoreEntry) -> bool:
    # By the cwd inside each transcript: the folder name also encodes dots.
    found = discover_claude_sessions(config.claude_projects_path, cwd=entry.cwd)
    return any(session.session_id == entry.session_id for session in found)


async def _session_exists(entry: RestoreEntry) -> bool | None:
    """Whether this account still holds the session; None if unresumable here."""
    if entry.provider_name == "claude":
        return await asyncio.to_thread(_claude_transcript_exists, entry)
    if entry.provider_name == "codex":
        return await asyncio.to_thread(_codex_rollout_exists, entry.session_id)
    return None


async def _unrestorable(
    entry: RestoreEntry, world: _World, ledger: _Ledger
) -> str | None:
    """Why this entry must not be restored now, or None when it may be."""
    if not Path(entry.cwd).is_dir():
        return "cwd_missing"
    exists = await _session_exists(entry)
    if exists is None:
        return "provider_unsupported"
    if not exists:
        return "session_not_found"
    if (
        thread_router.has_topic_provisioning(entry.chat_id, entry.thread_id)
        or thread_router.has_target_provisioning(entry.old_window_id)
        or topic_orchestration.is_pending_creation(entry.old_window_id)
    ):
        return "provisioning_claim"
    if entry.session_id in world.live_sessions:
        return "session_already_live"
    if ledger.status(entry.key) in _ATTEMPTED:
        return "already_attempted"
    return None


async def _entry_for_binding(
    binding: tuple[int, int | None, int, str], world: _World, ledger: _Ledger
) -> RestoreEntry | str:
    """Build the restore entry for a binding, or the reason it is skipped."""
    user_id, chat_id, thread_id, old_wid = binding
    if chat_id is None:
        return "chat_unknown"
    display = thread_router.get_display_name(old_wid)
    holder = next((w for w in world.live if w.matches(old_wid)), None)
    if holder is not None:
        same = holder.window_name == display
        return "window_alive" if same else "stale_id_collision_suspected"
    view = window_query.view_window(old_wid)
    if view is None or not view.session_id:
        return "no_session"
    if not view.cwd:
        return "no_cwd"
    entry = RestoreEntry(
        user_id=user_id,
        chat_id=chat_id,
        thread_id=thread_id,
        old_window_id=old_wid,
        cwd=view.cwd,
        session_id=view.session_id,
        provider_name=view.provider_name,
        approval_mode="yolo" if view.approval_mode == "yolo" else "normal",
        display=display,
    )
    return await _unrestorable(entry, world, ledger) or entry


async def _live_world() -> _World | None:
    """One confirmed window listing, or None when tmux cannot confirm it."""
    live = await list_windows_for_reconciliation(multiplexer)
    if live is None:
        return None
    views = (window_query.view_window(w.window_id) for w in live)
    return _World(live, frozenset(v.session_id for v in views if v is not None))


async def _build_plan(ledger: _Ledger) -> list[RestoreEntry]:
    world = await _live_world()
    if world is None:
        logger.warning("auto_restore_listing_unconfirmed")
        return []
    planned: dict[str, RestoreEntry] = {}  # by session id: a session runs once
    for binding in list(thread_router.iter_thread_bindings_with_chat()):
        found = await _entry_for_binding(binding, world, ledger)
        if isinstance(found, str):
            _skip(found, (binding[1], binding[2], binding[3]))
        elif found.session_id in planned:
            _skip("duplicate_session", found.where)
        else:
            planned[found.session_id] = found
    return list(planned.values())


async def _pause(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _request(entry: RestoreEntry) -> ReplacementRequest:
    return ReplacementRequest(
        user_id=entry.user_id,
        thread_id=entry.thread_id,
        cwd=entry.cwd,
        chat_id=entry.chat_id,
        resume_id=entry.session_id,
        success_label="Session resumed after restart.",
        old_window_id=entry.old_window_id,
        provider_name=entry.provider_name,
        offer_verified=True,
        approval_mode=entry.approval_mode,
    )


async def _skip_reason_now(entry: RestoreEntry) -> str | None:
    """Re-check a planned entry against the world as it is right now."""
    bound = thread_router.get_window_for_chat_thread(entry.chat_id, entry.thread_id)
    if bound != entry.old_window_id:
        return "rebound"
    world = await _live_world()
    if world is None:
        return "listing_unconfirmed"
    if any(w.matches(entry.old_window_id) for w in world.live):
        return "stale_id_collision_suspected"
    if entry.session_id in world.live_sessions:
        return "session_live"
    return None


async def _restore_one(
    entry: RestoreEntry,
    ledger: _Ledger,
    bot: Bot,
    get_adapter: Callable[[], TelegramSessionAdapter],
) -> float | None:
    """Run one replacement. Returns Telegram's retry-after seconds, if it asked."""
    reason = await _skip_reason_now(entry)
    if reason is not None:
        _skip(reason, entry.where)
        return None
    ledger.set_status(entry.key, _ATTEMPTING)
    ui = HeadlessUi()
    try:
        restored = await create_and_bind_window(_request(entry), ui, bot, get_adapter)
    except Exception:
        logger.exception("auto_restore_failed", key=entry.key)
        ledger.set_status(entry.key, _FAILED)
        return None
    ledger.set_status(entry.key, _DONE if restored else _FAILED)
    logger.info("auto_restore_done", key=entry.key, restored=restored)
    return ui.retry_after


async def _run(
    plan: list[RestoreEntry],
    stale_ids: list[str],
    ledger: _Ledger,
    bot: Bot,
    get_adapter: Callable[[], TelegramSessionAdapter],
) -> None:
    """Reserve ids (can take seconds), then restore the plan.

    Waits out a rate limit up to a bounded number of times.
    """
    if not await reserve_window_ids_past(stale_ids):
        logger.warning("auto_restore_skipped", reason="id_reservation_unavailable")
        return
    ledger.add_pending(plan)
    waits = 0
    for index, entry in enumerate(plan):
        if index:
            await _pause(GAP_S)
        retry_after = await _restore_one(entry, ledger, bot, get_adapter)
        if retry_after is None:
            continue
        if waits == MAX_RATE_LIMIT_WAITS:
            logger.warning("auto_restore_rate_limited_giving_up", waits=waits)
            return
        waits += 1
        logger.warning(
            "auto_restore_rate_limited", retry_after=retry_after, waits=waits
        )
        await _pause(min(retry_after, MAX_RATE_LIMIT_WAIT_S))


async def start_auto_restore(
    bot: Bot, get_adapter: Callable[[], TelegramSessionAdapter]
) -> asyncio.Task[None] | None:
    """Plan the restores now and run them in the background.

    Returns the running task, or None when the feature is off, cannot run, or
    has nothing to do. Never raises: a failure here must not stop the bot.
    """
    if not config.auto_restore_on_start:
        return None
    if config.autodelete_dead_topics:
        # Status polling would delete the dead topics before they are resumed.
        logger.warning("auto_restore_skipped", reason="autodelete_dead_topics_enabled")
        return None
    try:
        boot_id = await multiplexer.server_boot_id()
        if boot_id is None:
            logger.warning("boot_id_unavailable")
            return None
        ledger = _Ledger.load(config.config_dir / _LEDGER_FILE, boot_id)
        plan = await _build_plan(ledger)
        stale_ids = [b[3] for b in thread_router.iter_thread_bindings_with_chat()]
    except Exception:
        logger.exception("auto_restore_failed_to_start")
        return None
    if not plan:
        return None
    task = asyncio.create_task(
        _run(plan, stale_ids, ledger, bot, get_adapter), name="auto_restore"
    )
    task.add_done_callback(task_done_callback)
    return task
