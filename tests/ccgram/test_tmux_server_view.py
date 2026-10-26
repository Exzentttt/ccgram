"""Server-wide pane listing, linking and bot-session marking, on a private tmux socket."""

import asyncio
import os
import shutil
from unittest.mock import AsyncMock, MagicMock, patch

import libtmux
import pytest

from pathlib import Path

from structlog.testing import capture_logs

from ccgram.multiplexer import server_view, tmux, tmux_server_view
from ccgram.multiplexer.base import ServerPane, ServerView
from ccgram.multiplexer.tmux import TmuxManager

needs_tmux = pytest.mark.skipif(shutil.which("tmux") is None, reason="no tmux")


@pytest.fixture()
def mux():
    """A manager on its own tmux server (never the user's), killed by its socket."""
    socket = f"ccgram-adopt-test-{os.getpid()}"
    server = libtmux.Server(socket_name=socket)
    server.new_session(session_name="bot", window_name="main", attach=False)
    server.new_session(session_name="2", window_name="user", attach=False)
    server.new_session(session_name="ccgram-foo", window_name="user", attach=False)
    server.new_session(session_name="other-bot", window_name="main", attach=False)
    server.new_session(session_name="_ccgram-reserve-x", window_name="r", attach=False)
    manager = TmuxManager(session_name="bot")
    manager._server = server
    yield manager
    server.kill()


async def panes_of(mux, session):
    return [p for p in await mux.list_server_panes() if p.session_name == session]


@needs_tmux
class TestListing:
    async def test_panes_of_every_session_are_listed(self, mux) -> None:
        panes = await mux.list_server_panes()

        assert panes is not None
        assert {p.session_name for p in panes} >= {"bot", "2", "ccgram-foo"}
        assert all(isinstance(p, ServerPane) and p.pane_pid > 0 for p in panes)

    async def test_a_dead_server_lists_nothing(self, mux) -> None:
        mux.server.kill()

        assert await mux.list_server_panes() is None

    async def test_a_marked_session_is_bot_owned_whatever_its_name(self, mux) -> None:
        mux.server.cmd("set-option", "-t", "=other-bot:", "@ccgram_bot", "1")

        assert all(p.bot_owned for p in await panes_of(mux, "other-bot"))

    async def test_an_unmarked_session_named_like_the_bot_is_not_bot_owned(
        self, mux
    ) -> None:
        assert not any(p.bot_owned for p in await panes_of(mux, "ccgram-foo"))

    async def test_the_bots_own_session_is_bot_owned_without_a_mark(self, mux) -> None:
        panes = await panes_of(mux, "bot")

        assert panes and all(p.bot_owned for p in panes)

    async def test_a_session_with_the_bots_own_window_is_bot_owned(self, mux) -> None:
        mux.server.new_session(
            session_name="old-bot", window_name="_ccgram", attach=False
        )

        panes = await panes_of(mux, "old-bot")

        assert panes and all(p.bot_owned for p in panes)

    async def test_a_window_name_with_a_pipe_does_not_shift_the_fields(
        self, mux
    ) -> None:
        mux.server.new_session(session_name="pipe", window_name="a|b", attach=False)

        panes = await panes_of(mux, "pipe")

        assert panes and not any(p.bot_owned for p in panes)

    async def test_a_session_with_the_configured_main_window_is_bot_owned(
        self, mux
    ) -> None:
        mux.server.new_session(
            session_name="legacy", window_name="__main__", attach=False
        )

        panes = await panes_of(mux, "legacy")

        assert panes and all(p.bot_owned for p in panes)

    async def test_a_reserve_session_is_bot_owned_without_a_mark(self, mux) -> None:
        panes = await panes_of(mux, "_ccgram-reserve-x")

        assert panes and all(p.bot_owned for p in panes)


@needs_tmux
class TestMarking:
    async def test_the_bot_marks_its_own_session(self, mux) -> None:
        assert await mux.mark_bot_session() is True

        assert all(p.bot_owned for p in await panes_of(mux, "bot"))
        assert not any(p.bot_owned for p in await panes_of(mux, "2"))

    async def test_ensure_session_marks_the_session(self, mux) -> None:
        await mux.ensure_session()

        assert all(p.bot_owned for p in await panes_of(mux, "bot"))

    async def test_a_failed_mark_is_logged(self, mux) -> None:
        with (
            patch.object(mux, "get_or_create_session"),
            patch.object(mux, "mark_bot_session", AsyncMock(return_value=False)),
            capture_logs() as logs,
        ):
            await mux.ensure_session()

        assert "bot_session_mark_failed" in [log["event"] for log in logs]

    def test_the_reserve_prefix_is_defined_once(self) -> None:
        assert tmux.RESERVE_SESSION_PREFIX is tmux_server_view.RESERVE_SESSION_PREFIX
        assert '"_ccgram-reserve-"' not in Path(tmux.__file__).read_text()

    async def test_marking_a_missing_session_reports_failure(self, mux) -> None:
        mux.session_name = "nope"

        assert await mux.mark_bot_session() is False


@needs_tmux
class TestLinking:
    async def test_a_linked_window_stays_in_its_own_session_too(self, mux) -> None:
        (foreign,) = await panes_of(mux, "2")

        assert await mux.link_window_into_session(foreign.window_id) is True

        sessions = {
            p.session_name
            for p in await mux.list_server_panes()
            if p.window_id == foreign.window_id
        }
        assert sessions == {"bot", "2"}

    async def test_unlink_removes_it_from_the_bot_session_only(self, mux) -> None:
        (foreign,) = await panes_of(mux, "2")
        await mux.link_window_into_session(foreign.window_id)

        assert await mux.unlink_window_from_session(foreign.window_id) is True

        sessions = {
            p.session_name
            for p in await mux.list_server_panes()
            if p.window_id == foreign.window_id
        }
        assert sessions == {"2"}

    async def test_unlink_never_destroys_a_window_that_is_in_one_session(
        self, mux
    ) -> None:
        (only,) = await panes_of(mux, "bot")

        assert await mux.unlink_window_from_session(only.window_id) is False
        assert await panes_of(mux, "bot")

    async def test_linking_a_missing_window_reports_failure(self, mux) -> None:
        assert await mux.link_window_into_session("@9999") is False


class TestCommandRunner:
    async def test_a_hung_tmux_is_killed_not_abandoned(self, mux) -> None:
        proc = MagicMock()

        async def hang():
            await asyncio.sleep(3600)

        proc.communicate = hang
        proc.kill = MagicMock()
        proc.wait = AsyncMock()
        with (
            patch.object(tmux_server_view, "_CMD_TIMEOUT_S", 0.05),
            patch(
                "asyncio.create_subprocess_exec", AsyncMock(return_value=proc)
            ) as spawn,
        ):
            assert await mux.list_server_panes() is None

        proc.kill.assert_called_once()
        assert spawn.call_args.args[:3] == (
            "tmux",
            "-L",
            mux.server.socket_name,
        )

    async def test_a_missing_tmux_binary_lists_nothing(self, mux) -> None:
        with patch(
            "asyncio.create_subprocess_exec", AsyncMock(side_effect=FileNotFoundError)
        ):
            assert await mux.list_server_panes() is None


class TestCapability:
    def test_tmux_is_a_server_view(self) -> None:
        assert isinstance(TmuxManager(session_name="bot"), ServerView)

    def test_the_forwarder_returns_the_wired_tmux_backend(self) -> None:
        backend = TmuxManager(session_name="bot")
        with patch("ccgram.multiplexer._active_multiplexer", backend):
            assert server_view() is backend

    def test_a_backend_without_the_capability_gives_none(self) -> None:
        with patch("ccgram.multiplexer._active_multiplexer", MagicMock(spec=[])):
            assert server_view() is None
