"""Reconnect a topic to its session when it runs in a tmux session outside the bot's.

A reboot can leave the user's agents running in tmux sessions of their own on
the bot's tmux server while the topic stays bound to a dead window id. Given the
live pid of the session, the pid's parent chain is walked to a pane of the
server-wide listing; exactly one candidate window is linked into the bot's
session (``link-window -d``, so it stays where the user started it) and the topic
is rebound to it. A window of a bot session (marked by the bot itself, or a
reserve session) or already bound to a topic is never a candidate. Nothing is
killed, respawned or sent keys; a failed rebind unlinks the window again.

Public surface: :func:`adopt`.
"""

from __future__ import annotations

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


def _candidate_windows(
    panes: list[ServerPane], pid: int, parents: dict[int, int]
) -> set[str]:
    """Windows of non-bot sessions, bound to no topic, with a pane above ``pid``."""
    taken = {p.window_id for p in panes if p.bot_owned}
    taken.update(wid for *_, wid in session_query.iter_bound_topics())
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
    if len(found) != 1:
        logger.info("adopt_ambiguous", candidates=len(found))
        return None
    (window_id,) = found
    if not await _link_and_rebind(view, window_id, old_window_id, topic):
        return None
    session = min(p.session_name for p in panes if p.window_id == window_id)
    return f"{session}:{window_id}"
