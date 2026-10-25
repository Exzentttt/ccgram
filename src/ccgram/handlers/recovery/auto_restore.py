"""Opt-in auto-restore of dead topics at bot startup.

With ``CCGRAM_AUTO_RESTORE_ON_START=true`` the bot resumes, at startup, every
topic whose window died (a reboot kills them all) and whose provider session is
still on disk, through the replacement flow the Resume button uses.

One confirmed window listing builds a plan before anything is created: window
ids restart with the server, so a stale id may already name a live window, and
such a binding is never resumed into. The plan is saved to a ledger keyed by the
server boot id, which makes an attempt once per (chat, topic, session) per boot
and lets the next start finish a plan a crash cut short, including a topic whose
binding an earlier create evicted by reusing its old window id.

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
from ...multiplexer import multiplexer
from ...multiplexer.base import WindowRef
from ...multiplexer.reconciliation import list_windows_for_reconciliation
from ...providers import get_provider_for_window, is_known_provider
from ...providers.codex import codex_sessions_dir
from ...thread_router import thread_router
from ...utils import atomic_write_json, task_done_callback
from ..topics import topic_orchestration
from .recovery_banner import _create_and_bind_window
from .recovery_ui import HeadlessUi, ReplacementRequest
from .resume_picker import scan_sessions_for_cwd

if TYPE_CHECKING:
    from telegram import Bot

    from ...session_protocol.telegram import TelegramSessionAdapter

logger = structlog.get_logger()

GAP_S = 3.0
_LEDGER_FILE = "auto_restore.json"
_PENDING, _ATTEMPTING, _DONE, _FAILED = "pending", "attempting", "done", "failed"
_WHERE = ("chat_id", "thread_id", "window_id")
_ATTEMPTED = frozenset({_ATTEMPTING, _DONE, _FAILED})


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
        self.boot_id = boot_id
        self.rows: dict[str, tuple[RestoreEntry, str]] = {}

    @classmethod
    def load(cls, path: Path, boot_id: str) -> _Ledger:
        """Read the ledger; another boot's rows (or an unreadable file) are void."""
        ledger = cls(path, boot_id)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if raw["boot_id"] == boot_id:
                for row in raw["entries"]:
                    status = row.pop("status")
                    entry = RestoreEntry(**row)
                    ledger.rows[entry.key] = (entry, status)
        except FileNotFoundError:
            pass
        except (OSError, ValueError, KeyError, TypeError) as exc:
            logger.warning("auto_restore_ledger_unreadable", error=str(exc))
            ledger.rows.clear()
        return ledger

    def status(self, key: str) -> str | None:
        return self.rows[key][1] if key in self.rows else None

    def set_status(self, key: str, status: str) -> None:
        self.rows[key] = (self.rows[key][0], status)
        self.save()

    def save(self) -> None:
        entries = [{**asdict(e), "status": st} for e, st in self.rows.values()]
        atomic_write_json(self._path, {"boot_id": self.boot_id, "entries": entries})


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


async def _session_exists(entry: RestoreEntry) -> bool | None:
    """Whether this account still holds the session; None if unresumable here."""
    name = entry.provider_name
    if name == "codex":
        return await asyncio.to_thread(_codex_rollout_exists, entry.session_id)
    if not is_known_provider(name):
        return None
    caps = get_provider_for_window("", provider_name=name).capabilities
    if not (caps.supports_resume and caps.supports_resume_picker):
        return None
    sessions = await asyncio.to_thread(scan_sessions_for_cwd, entry.cwd, name)
    return any(s.session_id == entry.session_id for s in sessions)


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


def _bound_to_live_window(entry: RestoreEntry, world: _World) -> bool:
    bound = thread_router.get_window_for_chat_thread(entry.chat_id, entry.thread_id)
    return bound is not None and any(w.matches(bound) for w in world.live)


async def _collect(
    planned: dict[str, RestoreEntry], world: _World, ledger: _Ledger
) -> None:
    """Fill ``planned`` (by session id: a session runs once) from live bindings,
    then from pending ledger rows whose topic lost its binding."""
    for binding in list(thread_router.iter_thread_bindings_with_chat()):
        found = await _entry_for_binding(binding, world, ledger)
        if isinstance(found, str):
            _skip(found, (binding[1], binding[2], binding[3]))
        else:
            _admit(found, planned)
    for entry, status in list(ledger.rows.values()):
        if status != _PENDING or entry.session_id in planned:
            continue
        if thread_router.get_window_for_chat_thread(entry.chat_id, entry.thread_id):
            continue
        reason = await _unrestorable(entry, world, ledger)
        if reason is None:
            _admit(entry, planned)
        else:
            _skip(reason, entry.where)


def _admit(entry: RestoreEntry, planned: dict[str, RestoreEntry]) -> None:
    if entry.session_id in planned:
        _skip("duplicate_session", entry.where)
    else:
        planned[entry.session_id] = entry


async def _build_plan(ledger: _Ledger) -> list[RestoreEntry]:
    live = await list_windows_for_reconciliation(multiplexer)
    if live is None:
        logger.warning("auto_restore_listing_unconfirmed")
        return []
    sessions = (window_query.view_window(w.window_id) for w in live)
    world = _World(live, frozenset(v.session_id for v in sessions if v is not None))
    for key, (entry, status) in list(ledger.rows.items()):
        if status == _PENDING and _bound_to_live_window(entry, world):
            del ledger.rows[key]
    planned: dict[str, RestoreEntry] = {}
    await _collect(planned, world, ledger)
    return list(planned.values())


async def _pause() -> None:
    await asyncio.sleep(GAP_S)


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


async def _restore_one(
    entry: RestoreEntry,
    ledger: _Ledger,
    bot: Bot,
    get_adapter: Callable[[], TelegramSessionAdapter],
) -> bool:
    """Run one replacement. Returns True when the whole run must stop."""
    ledger.set_status(entry.key, _ATTEMPTING)
    ui = HeadlessUi()
    try:
        restored = await _create_and_bind_window(_request(entry), ui, bot, get_adapter)
    except Exception:
        logger.exception("auto_restore_failed", key=entry.key)
        ledger.set_status(entry.key, _FAILED)
        return False
    ledger.set_status(entry.key, _DONE if restored else _FAILED)
    logger.info("auto_restore_done", key=entry.key, restored=restored)
    if ui.retry_after is None:
        return False
    logger.warning("auto_restore_rate_limited", retry_after=ui.retry_after)
    return True


async def _run(
    plan: list[RestoreEntry],
    ledger: _Ledger,
    bot: Bot,
    get_adapter: Callable[[], TelegramSessionAdapter],
) -> None:
    for index, entry in enumerate(plan):
        if index:
            await _pause()
        if await _restore_one(entry, ledger, bot, get_adapter):
            return


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
        for entry in plan:
            ledger.rows.setdefault(entry.key, (entry, _PENDING))
        ledger.save()
    except Exception:
        logger.exception("auto_restore_failed_to_start")
        return None
    if not plan:
        return None
    task = asyncio.create_task(
        _run(plan, ledger, bot, get_adapter), name="auto_restore"
    )
    task.add_done_callback(task_done_callback)
    return task
