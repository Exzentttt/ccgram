"""Opt-in auto-restore of dead topics at bot startup.

With ``CCGRAM_AUTO_RESTORE_ON_START=true`` the bot resumes, at startup, every
topic whose window died (a reboot kills them all) and whose provider session is
still on disk, through the replacement flow the Resume button uses.

One confirmed window listing builds a plan before anything is created: window
ids restart with the server, so a stale id may already name a live window, and
such a binding is never resumed into. At every start, flag on or off, if a
binding is dead the window-id counter advances past every bound id, so no new
window takes another topic's old id. A session running outside the bot's tmux
session (Claude pid registry, Codex rollout open per ``lsof``) is never resumed;
a live Claude or Codex session whose window :mod:`adopt_live` can prove is
reconnected to its topic instead.
The plan is saved to a ledger keyed by the server boot id: one attempt per
(chat, topic, session) per boot, and the next start finishes a plan a crash
cut short.

Public surface: :func:`start_auto_restore`.
"""

from __future__ import annotations

import asyncio
import contextlib
import glob
import json
import os
import subprocess
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple

import structlog

from ... import window_query
from ...config import config
from ...multiplexer import (
    can_reserve_window_ids,
    multiplexer,
    reserve_window_ids_past,
    server_view,
)
from ...multiplexer.base import ServerView, WindowRef
from ...multiplexer.reconciliation import list_windows_for_reconciliation
from ...providers.codex import codex_sessions_dir, codex_thread_name
from ...thread_router import thread_router
from ...utils import atomic_write_json, task_done_callback
from ..topics import topic_orchestration
from . import adopt_live
from .recovery_banner import create_and_bind_window
from .recovery_ui import HeadlessUi, ReplacementRequest
from .restore_notices import CREATION_FAILED, adopt_text, send_notice, skip_text

if TYPE_CHECKING:
    from telegram import Bot

    from ...session_protocol.telegram import TelegramSessionAdapter

logger = structlog.get_logger()

GAP_S = 3.0
_LSOF_TIMEOUT_S = 10.0
MAX_RATE_LIMIT_WAITS = 3
MAX_RATE_LIMIT_WAIT_S = 300.0
_LEDGER_FILE = "auto_restore.json"
_LEDGER_VERSION = 1
_PENDING, _ATTEMPTING, _DONE, _FAILED = "pending", "attempting", "done", "failed"
_STATUSES = frozenset({_PENDING, _ATTEMPTING, _DONE, _FAILED})
_ATTEMPTED = frozenset({_ATTEMPTING, _DONE, _FAILED})
_WHERE = ("chat_id", "thread_id", "window_id")
_ADOPTABLE = frozenset({"session_live", "cwd_missing"})
_ADOPT_MISS_TTL_S = 30.0
_adopt_misses: dict[str, float] = {}  # session id -> monotonic time to retry at


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
        self.noticed: set[str] = set()

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
                ledger.noticed = set(raw.get("notices", []))
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
            ledger.noticed.clear()
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

    def add_notice(self, key: str) -> None:
        self.noticed.add(key)
        self._save()

    def _save(self) -> None:
        entries = [{**asdict(e), "status": st} for e, st in self._rows.values()]
        atomic_write_json(
            self._path,
            {
                "version": _LEDGER_VERSION,
                "boot_id": self._boot_id,
                "entries": entries,
                "notices": sorted(self.noticed),
            },
        )


class _Notice(NamedTuple):
    topic: tuple[int, int]
    text: str
    once: bool = True  # skip notices: one per topic per boot

    @property
    def key(self) -> str:
        return "{}:{}".format(*self.topic)


@dataclass(frozen=True)
class _World:
    live: list[WindowRef]
    live_sessions: frozenset[str]


def _skip(reason: str, where: tuple[int | None, int, str]) -> None:
    log = logger.debug if reason == "window_alive" else logger.info
    log("auto_restore_skip", reason=reason, **dict(zip(_WHERE, where, strict=True)))


def _notice_for(reason: str, where: tuple[int | None, int, str]) -> _Notice | None:
    chat_id, thread_id, window_id = where
    view = window_query.view_window(window_id)
    text = skip_text(reason, view.cwd if view else "")
    if text is None or chat_id is None:
        return None
    return _Notice((chat_id, thread_id), text)


async def _send(notice: _Notice, ledger: _Ledger, bot: Bot) -> float | None:
    """Send a notice (a skip notice once per boot); the topic counts as told after."""
    if notice.once and notice.key in ledger.noticed:
        return None
    retry_after = await send_notice(bot, *notice.topic, notice.text)
    if retry_after is None and notice.once:
        ledger.add_notice(notice.key)
    return retry_after


def _codex_rollouts(session_id: str) -> list[Path]:
    pattern = f"rollout-????-??-??T??-??-??-{glob.escape(session_id)}.jsonl"
    return list(codex_sessions_dir().glob(f"**/{pattern}"))


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except ProcessLookupError:
        return False
    return True


def _run_ps(argv: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, env=env, capture_output=True, text=True, timeout=5, check=False
    )


async def _spawn_lsof(path: Path) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        "lsof",
        "-t",
        "--",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )


def _ps_start(pid: int) -> str | None:
    """Process start time as the registry writes it; None when ps cannot say."""
    env = {**os.environ, "TZ": "UTC", "LC_ALL": "C"}
    try:
        done = _run_ps(["ps", "-o", "lstart=", "-p", str(pid)], env)
    except OSError, subprocess.TimeoutExpired:
        return None
    return (done.stdout.strip() or None) if done.returncode == 0 else None


def _claude_registry_scan(session_id: str) -> tuple[int | None, bool]:
    """The confirmed live pid of the session, and whether an entry stayed unsettled."""
    unknown = False
    registry = config.claude_config_dir / "sessions"
    for path in registry.glob("*.json"):  # never the *.key files beside them
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            pid = int(data["pid"])
            if data["sessionId"] != session_id or pid <= 0 or not _pid_alive(pid):
                continue
            started = data.get("procStart")
        except OSError, ValueError, KeyError, TypeError, OverflowError:
            continue
        # A reboot leaves stale entries whose pids other processes now hold.
        actual = _ps_start(pid) if started else None
        if actual is None:
            unknown = True
        elif actual == started:
            return pid, unknown
    return None, unknown


def _claude_registry_live(session_id: str) -> bool | None:
    """True/False from the pid registry; None when an entry cannot be settled."""
    pid, unknown = _claude_registry_scan(session_id)
    return True if pid is not None else None if unknown else False


def _parent_pids() -> dict[int, int]:
    """pid -> parent pid of every process; empty when ps cannot say."""
    try:
        done = _run_ps(["ps", "-A", "-o", "pid=,ppid="], {**os.environ, "LC_ALL": "C"})
    except OSError, subprocess.TimeoutExpired:
        return {}
    table: dict[int, int] = {}
    for line in done.stdout.splitlines():
        pid, _, ppid = line.strip().partition(" ")
        if pid.isdigit() and ppid.strip().isdigit():
            table[int(pid)] = int(ppid)
    return table


async def _rollout_open(path: Path) -> bool | None:
    """Whether any process has the file open; None when lsof cannot say."""
    proc = None
    try:
        proc = await _spawn_lsof(path)
        out, _ = await asyncio.wait_for(proc.communicate(), _LSOF_TIMEOUT_S)
    except OSError, TimeoutError:
        if proc is not None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
                await proc.wait()
        return None
    if proc.returncode == 0 and out.strip():
        return True
    return False if proc.returncode == 1 and not out.strip() else None


async def _rollout_live(session_id: str) -> bool | None:
    """Whether a process holds a rollout of the Codex session open; None if unsure."""
    paths = await asyncio.to_thread(_codex_rollouts, session_id)
    opened = [await _rollout_open(path) for path in paths]
    return True if True in opened else None if None in opened else False


async def _outside_liveness(entry: RestoreEntry) -> str | None:
    """Skip reason when the session runs outside the bot's own tmux session."""
    if entry.provider_name == "claude":
        live = await asyncio.to_thread(_claude_registry_live, entry.session_id)
    elif entry.provider_name == "codex":
        live = await _rollout_live(entry.session_id)
    else:
        return None
    if live is None:
        return "liveness_unconfirmed"
    return "session_live" if live else None


async def _adopt_claude(
    view: ServerView, session_id: str, topic: tuple[int, int, int], old_window_id: str
) -> str | None:
    pid, _ = await asyncio.to_thread(_claude_registry_scan, session_id)
    if pid is None:
        return None
    parents = await asyncio.to_thread(_parent_pids)
    return await adopt_live.adopt(view, pid, parents, old_window_id, topic)


async def _adopt_codex(
    view: ServerView,
    session_id: str,
    cwd: str,
    topic: tuple[int, int, int],
    old_window_id: str,
) -> str | None:
    """Live rollout and a thread name no live session shares; the pane must then show it."""
    if not cwd or not await _rollout_live(session_id):
        return None
    found = await asyncio.to_thread(codex_thread_name, session_id)
    if found is None:
        return None
    name, namesakes = found
    for other in namesakes:  # a live namesake could be the one showing the name
        if await _rollout_live(other) is not False:
            return None
    return await adopt_live.adopt_codex(view, name, Path(cwd), old_window_id, topic)


async def adopt_live_session(
    provider_name: str,
    session_id: str,
    topic: tuple[int, int, int],
    old_window_id: str,
    cwd: str = "",
) -> str | None:
    """Reconnect ``topic`` (user, chat, thread) to the window its live session runs in.

    Claude is found by its registry pid, Codex by its open rollout, unique thread
    name and a pane in ``cwd`` showing that name. Returns the ``session:window``
    label, or None when nothing was adopted. A miss is remembered for 30 s, so a
    message to a topic with no live session elsewhere costs one lookup, not one
    per message. Never raises: a failure leaves the binding as it was.
    """
    if provider_name not in ("claude", "codex") or (view := server_view()) is None:
        return None
    if time.monotonic() < _adopt_misses.get(session_id, 0.0):
        return None
    label = None
    try:
        if provider_name == "claude":
            label = await _adopt_claude(view, session_id, topic, old_window_id)
        else:
            label = await _adopt_codex(view, session_id, cwd, topic, old_window_id)
    except Exception:
        logger.exception("adopt_live_failed", session_id=session_id)
    if label is None:
        _adopt_misses[session_id] = time.monotonic() + _ADOPT_MISS_TTL_S
    return label


async def adopt_for_topic(
    user_id: int, chat_id: int, thread_id: int, old_window_id: str
) -> str | None:
    """:func:`adopt_live_session` for a topic bound to a window that is gone."""
    view = window_query.view_window(old_window_id)
    if view is None or not view.session_id:
        return None
    topic = (user_id, chat_id, thread_id)
    return await adopt_live_session(
        view.provider_name, view.session_id, topic, old_window_id, view.cwd
    )


async def _adopted_notice(entry: RestoreEntry, reason: str | None) -> _Notice | None:
    """The notice of a topic reconnected to its live session instead of skipped."""
    if reason not in _ADOPTABLE:
        return None
    topic = (entry.user_id, entry.chat_id, entry.thread_id)
    label = await adopt_live_session(
        entry.provider_name, entry.session_id, topic, entry.old_window_id, entry.cwd
    )
    return None if label is None else _Notice(topic[1:], adopt_text(label))


def _first_cwd(transcript: Path) -> str:
    """The cwd of the first line carrying one; no line cap, bad JSON skipped."""
    with transcript.open(encoding="utf-8", errors="replace") as lines:
        for line in lines:
            with contextlib.suppress(ValueError):
                data = json.loads(line)
                if isinstance(data, dict) and isinstance(data.get("cwd"), str):
                    return data["cwd"]
    return ""


def _claude_transcript_exists(entry: RestoreEntry) -> bool:
    # By file name: folder names encode dots, discovery reads only 20 lines.
    wanted = Path(entry.cwd).resolve()
    for path in config.claude_projects_path.glob(
        f"*/{glob.escape(entry.session_id)}.jsonl"
    ):
        with contextlib.suppress(OSError):
            if (cwd := _first_cwd(path)) and Path(cwd).resolve() == wanted:
                return True
    return False


async def _session_exists(entry: RestoreEntry) -> bool | None:
    """Whether this account still holds the session; None if unresumable here."""
    if entry.provider_name == "claude":
        return await asyncio.to_thread(_claude_transcript_exists, entry)
    if entry.provider_name == "codex":
        return bool(await asyncio.to_thread(_codex_rollouts, entry.session_id))
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
    return await _outside_liveness(entry)


async def _entry_for_binding(
    binding: tuple[int, int | None, int, str], world: _World, ledger: _Ledger
) -> RestoreEntry | str | _Notice:
    """Build the restore entry for a binding, the reason it is skipped, or the
    notice of a topic reconnected to its live session."""
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
    reason = await _unrestorable(entry, world, ledger)
    if notice := await _adopted_notice(entry, reason):
        _skip("adopted", entry.where)
        return notice
    return reason or entry


async def _live_world() -> _World | None:
    """One confirmed window listing, or None when tmux cannot confirm it."""
    live = await list_windows_for_reconciliation(multiplexer)
    if live is None:
        return None
    views = (window_query.view_window(w.window_id) for w in live)
    return _World(live, frozenset(v.session_id for v in views if v is not None))


async def _build_plan(
    ledger: _Ledger, notices: dict[tuple[int, int], _Notice]
) -> list[RestoreEntry]:
    world = await _live_world()
    if world is None:
        logger.warning("auto_restore_listing_unconfirmed")
        return []
    planned: dict[str, RestoreEntry] = {}  # by session id: a session runs once
    for binding in list(thread_router.iter_thread_bindings_with_chat()):
        found = await _entry_for_binding(binding, world, ledger)
        if isinstance(found, _Notice):
            notices.setdefault(found.topic, found)
        elif isinstance(found, str):
            where = (binding[1], binding[2], binding[3])
            _skip(found, where)
            if notice := _notice_for(found, where):
                notices.setdefault(notice.topic, notice)
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
    return await _outside_liveness(entry)


async def _restore_one(
    entry: RestoreEntry,
    ledger: _Ledger,
    bot: Bot,
    get_adapter: Callable[[], TelegramSessionAdapter],
) -> tuple[float | None, _Notice | None]:
    """Run one replacement. Returns the rename's retry-after and the notice to send."""
    reason = await _skip_reason_now(entry)
    if reason is not None:
        _skip(reason, entry.where)
        return None, _notice_for(reason, entry.where)
    ledger.set_status(entry.key, _ATTEMPTING)
    ui = HeadlessUi()
    try:
        restored = await create_and_bind_window(_request(entry), ui, bot, get_adapter)
    except Exception:
        logger.exception("auto_restore_failed", key=entry.key)
        ledger.set_status(entry.key, _FAILED)
        return None, _Notice(entry.where[:2], CREATION_FAILED, once=False)
    ledger.set_status(entry.key, _DONE if restored else _FAILED)
    logger.info("auto_restore_done", key=entry.key, restored=restored)
    text = ui.final_text or CREATION_FAILED
    return ui.retry_after, _Notice(entry.where[:2], text, once=False)


async def _back_off(
    retry_after: float, waits: int, held: _Notice | None = None
) -> int | None:
    """Wait out a rate limit; None once the cap is hit (``held`` is then left unsent)."""
    if waits == MAX_RATE_LIMIT_WAITS:
        logger.warning("auto_restore_rate_limited_giving_up", waits=waits)
        if held:
            logger.warning("auto_restore_notice_unsent", key=held.key)
        return None
    logger.warning(
        "auto_restore_rate_limited", retry_after=retry_after, waits=waits + 1
    )
    await _pause(min(retry_after, MAX_RATE_LIMIT_WAIT_S))
    return waits + 1


async def _step(
    item: _Notice | RestoreEntry,
    ledger: _Ledger,
    bot: Bot,
    get_adapter: Callable[[], TelegramSessionAdapter],
) -> tuple[float | None, _Notice | None]:
    """Restore or notify once. Returns the wait owed and the notice that hit a limit."""
    if isinstance(item, _Notice):
        retry_after, notice = None, item
    else:
        retry_after, notice = await _restore_one(item, ledger, bot, get_adapter)
    held = await _send(notice, ledger, bot) if notice else None
    if notice is None or held is None:
        return retry_after, None
    return max(retry_after or 0, held), notice


async def _resend(held: _Notice, ledger: _Ledger, bot: Bot, waits: int) -> int | None:
    """Send a held notice once more; None once the wait cap is hit."""
    again = await _send(held, ledger, bot)
    if again is None:
        return waits
    logger.warning("auto_restore_notice_unsent", key=held.key)
    # the next send waits out this limit too
    return await _back_off(again, waits)


async def _run(
    work: list[_Notice | RestoreEntry],
    ledger: _Ledger,
    bot: Bot,
    get_adapter: Callable[[], TelegramSessionAdapter],
) -> None:
    """Tell skipped topics, restore the plan; wait out rate limits a bounded number of times."""
    waits = restores = 0
    for item in work:
        if isinstance(item, RestoreEntry):
            if restores:
                await _pause(GAP_S)
            restores += 1
        retry_after, held = await _step(item, ledger, bot, get_adapter)
        if retry_after is None:
            continue
        if (used := await _back_off(retry_after, waits, held)) is None:
            return
        waits = used
        if held and (used := await _resend(held, ledger, bot, waits)) is None:
            return
        waits = used


async def _reserve_stale_ids() -> bool:
    """Reserve ids past all bindings if any is dead or unconfirmed; never raises."""
    try:
        bound = [wid for *_, wid in thread_router.iter_thread_bindings_with_chat()]
        live = await list_windows_for_reconciliation(multiplexer) if bound else []
        stale = live is None or not all(any(w.matches(b) for w in live) for b in bound)
        if stale and not can_reserve_window_ids():
            logger.info("window_id_reservation", reason="not_applicable")
            return False
        reserved = await reserve_window_ids_past(bound) if stale else True
    except Exception as exc:
        logger.exception("window_id_reservation", reserved=False, error=str(exc))
        return False
    log = logger.info if reserved else logger.warning
    log("window_id_reservation", stale=stale, reserved=reserved, bound=len(bound))
    return reserved


async def start_auto_restore(
    bot: Bot, get_adapter: Callable[[], TelegramSessionAdapter]
) -> asyncio.Task[None] | None:
    """Plan the restores now and run them in the background.

    Returns the running task, or None when the feature is off, cannot run, or
    has nothing to do. Never raises: a failure here must not stop the bot.
    """
    # INVARIANT(auto-restore): reservation must finish before polling starts,
    # else a queued Resume or /new can take a stale id and evict another topic.
    # It runs with the resume loop off too: a manual Resume needs it.
    reserved = await _reserve_stale_ids()
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
        notices: dict[tuple[int, int], _Notice] = {}
        plan = await _build_plan(ledger, notices)
        if plan and not reserved:
            logger.warning("auto_restore_skipped", reason="id_reservation_unavailable")
            text = skip_text("id_reservation_unavailable") or ""
            notices.update((e.where[:2], _Notice(e.where[:2], text)) for e in plan)
            plan = []
        ledger.add_pending(plan)
    except Exception:
        logger.exception("auto_restore_failed_to_start")
        return None
    if not plan and not notices:
        return None
    task = asyncio.create_task(
        _run([*notices.values(), *plan], ledger, bot, get_adapter), name="auto_restore"
    )
    task.add_done_callback(task_done_callback)
    return task
