"""Reconnect a topic to its session when it runs in a tmux session outside the bot's.

A reboot can leave the user's agents running in tmux sessions of their own on
the bot's tmux server while the topic stays bound to a dead window id. Given the
live pid of the session, the pid's parent chain is walked to a pane of the
server-wide listing; exactly one candidate window is linked into the bot's
session (``link-window -d``, so it stays where the user started it) and the topic
is rebound to it. A window of a bot session (marked by the bot itself, or a
reserve session) or already bound to a topic is never a candidate. Nothing is
killed, respawned or sent keys; a failed rebind unlinks the window again.

A Codex session has no pid to follow (its rollout is held by a shared daemon), so
:func:`adopt_codex` proves identity from the screen: a pane running ``codex`` in
the topic's folder whose status line names the session's thread; exactly one
window must prove it.

Public surface: :func:`adopt`, :func:`adopt_codex`.
"""

from __future__ import annotations

from pathlib import Path

import structlog

from ... import session_query
from ...multiplexer.base import ServerPane, ServerView
from ...session import session_manager
from ..polling.polling_state import lifecycle_strategy
from ..topics import topic_orchestration

logger = structlog.get_logger()


def _ancestors(pid: int, parents: dict[int, int]) -> set[int]:
    """``pid`` and every parent above it (loops end, pid 1 is the root)."""
    chain: set[int] = set()
    while pid > 1 and pid not in chain:
        chain.add(pid)
        pid = parents.get(pid, 0)
    return chain


def _taken_windows(panes: list[ServerPane]) -> set[str]:
    """Windows of bot sessions or bound to a topic: never candidates."""
    taken = {p.window_id for p in panes if p.bot_owned}
    taken.update(wid for *_, wid in session_query.iter_bound_topics())
    return taken


def _candidate_windows(
    panes: list[ServerPane], pid: int, parents: dict[int, int]
) -> set[str]:
    """Windows of non-bot sessions, bound to no topic, with a pane above ``pid``."""
    taken = _taken_windows(panes)
    chain = _ancestors(pid, parents)
    return {
        p.window_id for p in panes if p.pane_pid in chain and p.window_id not in taken
    }


async def _link_and_rebind(
    view: ServerView, window_id: str, old_window_id: str, topic: tuple[int, int, int]
) -> bool:
    """Link the window in and move the topic; a failed rebind unlinks it again."""
    # The new window must not be adopted as an unbound topic mid-rebind.
    topic_orchestration.register_pending_creation(window_id)
    if not await view.link_window_into_session(window_id):
        logger.warning("adopt_link_failed", window_id=window_id)
        topic_orchestration.clear_pending_creation(window_id)
        return False
    if session_manager.rebind_window(old_window_id, window_id):
        topic_orchestration.clear_pending_creation(window_id)
        lifecycle_strategy.clear_dead_notification(topic[0], topic[2])
        return True
    logger.warning("adopt_rebind_failed", window_id=window_id)
    if await view.unlink_window_from_session(window_id):
        topic_orchestration.clear_pending_creation(window_id)
    else:  # linked and unbound: keep the poller away from it
        logger.error("adopt_unlink_failed", window_id=window_id)
    return False


async def _adopt_single(
    view: ServerView,
    panes: list[ServerPane],
    found: set[str],
    old_window_id: str,
    topic: tuple[int, int, int],
) -> str | None:
    """Link and rebind when exactly one window qualified; the label, else None."""
    if len(found) != 1:
        logger.info("adopt_ambiguous", candidates=len(found))
        return None
    (window_id,) = found
    if not await _link_and_rebind(view, window_id, old_window_id, topic):
        return None
    session = min(p.session_name for p in panes if p.window_id == window_id)
    return f"{session}:{window_id}"


async def adopt(
    view: ServerView,
    pid: int,
    parents: dict[int, int],
    old_window_id: str,
    topic: tuple[int, int, int],
) -> str | None:
    """Rebind ``topic`` (user, chat, thread) to the window above ``pid``.

    Returns the ``session:window`` label for the user, or None when no single
    window qualified or the rebind did not apply.
    """
    panes = await view.list_server_panes()
    if panes is None:
        logger.warning("adopt_listing_unavailable")
        return None
    found = _candidate_windows(panes, pid, parents)
    return await _adopt_single(view, panes, found, old_window_id, topic)


def _codex_candidates(panes: list[ServerPane], cwd: Path) -> list[ServerPane]:
    """Free ``codex`` panes in ``cwd``, one per pane id."""
    taken = _taken_windows(panes)
    wanted = cwd.resolve()
    seen: dict[str, ServerPane] = {}
    for p in panes:
        if (
            p.current_command == "codex"
            and p.window_id not in taken
            and p.current_path
            and Path(p.current_path).resolve() == wanted
        ):
            seen.setdefault(p.pane_id, p)
    return list(seen.values())


def status_line_proves(screen: str, thread_name: str, cwd: Path) -> bool:
    """Whether the Codex status line at the foot of ``screen`` names this thread.

    Only the last four non-blank lines count (a hint line follows the status
    line). Fields split on `` · ``; the folder field (a path, possibly
    ``~``-abbreviated) must end in the folder name of ``cwd`` (as given or
    resolved) and the very next field must equal ``thread_name``. Any other layout proves nothing.
    """
    folders = {cwd.name, cwd.resolve().name}
    lines = [line for line in screen.splitlines() if line.strip()][-4:]
    for line in lines:
        fields = [f.strip() for f in line.split(" · ")]
        for here, after in zip(fields, fields[1:]):
            if "/" in here and Path(here).name in folders and after == thread_name:
                return True
    return False


async def _windows_showing(
    view: ServerView, candidates: list[ServerPane], thread_name: str, cwd: Path
) -> set[str]:
    """Windows with a candidate pane whose status line proves ``thread_name``."""
    shown: set[str] = set()
    for p in candidates:
        screen = await view.capture_server_pane(p.pane_id)
        if screen is not None and status_line_proves(screen, thread_name, cwd):
            shown.add(p.window_id)
    return shown


async def adopt_codex(
    view: ServerView,
    thread_name: str,
    cwd: Path,
    old_window_id: str,
    topic: tuple[int, int, int],
) -> str | None:
    """Rebind ``topic`` to the one Codex pane in ``cwd`` showing ``thread_name``.

    Returns the ``session:window`` label, or None when no single pane proved
    itself or the rebind did not apply.
    """
    panes = await view.list_server_panes()
    if panes is None:
        logger.warning("adopt_listing_unavailable")
        return None
    candidates = _codex_candidates(panes, cwd)
    found = await _windows_showing(view, candidates, thread_name, cwd)
    return await _adopt_single(view, panes, found, old_window_id, topic)
