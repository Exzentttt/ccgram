"""tmux's ``ServerView`` capability: panes of every session, link and unlink.

A mixin of ``TmuxManager`` (needs its ``server`` and ``session_name``). Every
command runs as a subprocess under a timeout and is killed when it hangs, so a
stuck tmux never leaves a blocked worker thread behind.
"""

from __future__ import annotations

import asyncio
import contextlib

import libtmux
import structlog

from .base import ServerPane

logger = structlog.get_logger()

_CMD_TIMEOUT_S = 10.0
_BOT_OPTION = "@ccgram_bot"
RESERVE_SESSION_PREFIX = "_ccgram-reserve-"
_OWN_WINDOW = "_ccgram"
_PANE_FORMAT = f"#{{window_id}}|#{{pane_id}}|#{{pane_pid}}|#{{{_BOT_OPTION}}}|#{{window_name}}|#{{session_name}}"


class TmuxServerViewMixin:
    """``ServerView`` for tmux, plus marking the bot's own session."""

    session_name: str

    @property
    def server(self) -> libtmux.Server:
        """The tmux server connection; ``TmuxManager`` provides it."""
        raise NotImplementedError

    async def _tmux(self, *args: str) -> list[str] | None:
        """Stdout lines of one tmux command on this server; None on any failure."""
        socket = (
            ["-L", self.server.socket_name]
            if self.server.socket_name
            else ["-S", str(self.server.socket_path)]
            if self.server.socket_path
            else []
        )
        proc: asyncio.subprocess.Process | None = None
        try:
            proc = await asyncio.create_subprocess_exec(
                "tmux",
                *socket,
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            async with asyncio.timeout(_CMD_TIMEOUT_S):
                out, _ = await proc.communicate()
        except TimeoutError:
            if proc is not None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                    await proc.wait()
            logger.warning("tmux command timed out", command=args[0])
            return None
        except OSError:
            return None
        if proc.returncode != 0:
            return None
        return out.decode("utf-8", errors="replace").splitlines()

    async def list_server_panes(self) -> list[ServerPane] | None:
        """Every pane of every session on the server, or None when tmux cannot say.

        A session is bot-owned when marked, when it is this bot's own, when it
        holds the bot's own window (another bot not yet restarted on a version
        that marks), or when it is a reserve session.
        """
        lines = await self._tmux("list-panes", "-a", "-F", _PANE_FORMAT)
        if lines is None:
            return None
        rows = [line.split("|", 5) for line in lines]
        owned = {
            session
            for *_, mark, name, session in rows
            if mark == "1" or name == _OWN_WINDOW
        }
        owned.add(self.session_name)
        return [
            ServerPane(
                session,
                window_id,
                pane_id,
                int(pid),
                session in owned or session.startswith(RESERVE_SESSION_PREFIX),
            )
            for window_id, pane_id, pid, mark, name, session in rows
            if pid.isdigit()
        ]

    async def mark_bot_session(self) -> bool:
        """Set the bot-session option on this session; adoption skips marked sessions."""
        done = await self._tmux(
            "set-option", "-t", f"={self.session_name}:", _BOT_OPTION, "1"
        )
        return done is not None

    async def link_window_into_session(self, window_id: str) -> bool:
        """Link a window of another session into this one, keeping it in both."""
        done = await self._tmux(
            "link-window", "-d", "-s", window_id, "-t", f"={self.session_name}:"
        )
        return done is not None

    async def unlink_window_from_session(self, window_id: str) -> bool:
        """Remove the window from this session; tmux refuses when it is the last link."""
        done = await self._tmux(
            "unlink-window", "-t", f"={self.session_name}:{window_id}"
        )
        return done is not None
