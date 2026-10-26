"""Finding the window a Claude session runs in outside the bot's session, and moving the topic."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

from ccgram.handlers.recovery import adopt_live
from ccgram.handlers.topics import topic_orchestration
from ccgram.multiplexer.base import ServerPane

_AL = "ccgram.handlers.recovery.adopt_live"
_TOPIC = (100, -1001, 42)


def pane(
    session, window, pid, owned=False, command="", path="", pane_id=None
) -> ServerPane:
    return ServerPane(
        session, window, pane_id or f"%{window[1:]}", pid, owned, command, path
    )


class World:
    def __init__(self) -> None:
        self.panes = [pane("ccgram", "@0", 10, True), pane("2", "@2", 100)]
        self.parents = {200: 100, 300: 200, 100: 1, 10: 1}
        self.bound: list[str] = ["@1"]
        self.rebound = True
        self.unlinked = True
        self.shield_at_unlink: bool | None = None
        self.manager = MagicMock()
        self.lifecycle = MagicMock()
        self.view = MagicMock()
        self.view.list_server_panes = AsyncMock(side_effect=lambda: self.panes)
        self.view.link_window_into_session = AsyncMock(return_value=True)
        self.view.unlink_window_from_session = AsyncMock(side_effect=self._unlink)

    async def _unlink(self, window_id: str) -> bool:
        self.shield_at_unlink = topic_orchestration.is_pending_creation(window_id)
        return self.unlinked

    async def adopt(self, pid: int = 300):
        return await adopt_live.adopt(self.view, pid, self.parents, "@1", _TOPIC)


@pytest.fixture
def world(monkeypatch):
    w = World()
    w.manager.rebind_window.side_effect = lambda *_a: w.rebound
    monkeypatch.setattr(f"{_AL}.session_manager", w.manager)
    monkeypatch.setattr(
        f"{_AL}.session_query.iter_bound_topics",
        lambda: [(100, -1001, 1, b) for b in w.bound],
    )
    monkeypatch.setattr(f"{_AL}.lifecycle_strategy", w.lifecycle)
    yield w
    topic_orchestration.clear_pending_creation("@2")


class TestFinding:
    async def test_the_pid_chain_reaches_a_foreign_pane(self, world) -> None:
        assert await world.adopt() == "2:@2"

        world.view.link_window_into_session.assert_awaited_once_with("@2")
        world.manager.rebind_window.assert_called_once_with("@1", "@2")

    async def test_a_user_session_named_like_the_bot_is_a_candidate(
        self, world
    ) -> None:
        world.panes = [pane("ccgram", "@0", 10, True), pane("ccgram-foo", "@2", 100)]

        assert await world.adopt() == "ccgram-foo:@2"

    async def test_another_bots_marked_session_is_not_a_candidate(self, world) -> None:
        world.panes = [pane("ccgram", "@0", 10, True), pane("zzz", "@2", 100, True)]

        assert await world.adopt() is None

    async def test_a_window_also_linked_into_a_bot_session_is_excluded(
        self, world
    ) -> None:
        world.panes.append(pane("zzz", "@2", 100, True))

        assert await world.adopt() is None

    async def test_a_window_bound_to_another_topic_is_excluded(self, world) -> None:
        world.bound.append("@2")

        assert await world.adopt() is None

    async def test_a_pid_outside_every_pane_has_no_window(self, world) -> None:
        world.parents[999] = 1

        assert await world.adopt(999) is None

    async def test_two_windows_on_the_chain_are_ambiguous(self, world) -> None:
        world.panes.append(pane("3", "@3", 200))

        with capture_logs() as logs:
            assert await world.adopt() is None

        assert [
            log["candidates"] for log in logs if log["event"] == "adopt_ambiguous"
        ] == [2]
        world.view.link_window_into_session.assert_not_called()

    async def test_the_label_names_the_lowest_session_when_linked_twice(
        self, world
    ) -> None:
        world.panes.append(pane("4", "@2", 100))

        assert await world.adopt() == "2:@2"

    async def test_a_parent_loop_ends(self, world) -> None:
        world.parents.update({500: 600, 600: 500})

        assert await world.adopt(500) is None

    async def test_an_unreadable_listing_adopts_nothing(self, world) -> None:
        world.view.list_server_panes = AsyncMock(return_value=None)

        assert await world.adopt() is None


class TestRebinding:
    async def test_the_poller_is_shielded_while_the_window_is_linked(
        self, world
    ) -> None:
        seen: list[bool] = []

        async def link(window_id: str) -> bool:
            seen.append(topic_orchestration.is_pending_creation(window_id))
            return True

        world.view.link_window_into_session = link

        await world.adopt()

        assert seen == [True]
        assert not topic_orchestration.is_pending_creation("@2")

    async def test_a_failed_link_adopts_nothing_and_drops_the_shield(
        self, world
    ) -> None:
        world.view.link_window_into_session = AsyncMock(return_value=False)

        assert await world.adopt() is None

        world.manager.rebind_window.assert_not_called()
        assert not topic_orchestration.is_pending_creation("@2")

    async def test_a_failed_rebind_unlinks_the_window_under_the_shield(
        self, world
    ) -> None:
        world.rebound = False

        with capture_logs() as logs:
            assert await world.adopt() is None

        world.view.unlink_window_from_session.assert_awaited_once_with("@2")
        assert world.shield_at_unlink is True
        assert not topic_orchestration.is_pending_creation("@2")
        assert [
            log["window_id"] for log in logs if log["event"] == "adopt_rebind_failed"
        ] == ["@2"]

    async def test_the_shield_stays_while_the_window_cannot_be_unlinked(
        self, world
    ) -> None:
        world.rebound = False
        world.unlinked = False

        with capture_logs() as logs:
            assert await world.adopt() is None

        assert topic_orchestration.is_pending_creation("@2")
        assert "adopt_unlink_failed" in [log["event"] for log in logs]

    async def test_success_clears_the_dead_marker(self, world) -> None:
        await world.adopt()

        world.lifecycle.clear_dead_notification.assert_called_once_with(100, 42)


_NAME = "agentspore-oss"


@pytest.fixture
def codex(world, tmp_path):
    """A topic cwd, one codex pane of a foreign session showing the thread name."""
    cwd = tmp_path / "proj"
    cwd.mkdir()
    world.cwd = cwd
    world.panes = [
        pane("ccgram", "@0", 10, True, "python", str(cwd)),
        pane("2", "@2", 100, command="codex", path=str(cwd)),
    ]
    world.screens = {"%2": f"... · {cwd} · {_NAME} · GPT-5 · ..."}
    world.view.capture_server_pane = AsyncMock(
        side_effect=lambda pane_id: world.screens.get(pane_id)
    )

    async def adopt_codex(name=_NAME, where=None):
        return await adopt_live.adopt_codex(
            world.view, name, where or world.cwd, "@1", _TOPIC
        )

    world.adopt_codex = adopt_codex
    return world


class TestCodexIdentity:
    async def test_the_pane_showing_the_thread_name_is_adopted(self, codex) -> None:
        assert await codex.adopt_codex() == "2:@2"

        codex.view.link_window_into_session.assert_awaited_once_with("@2")
        codex.manager.rebind_window.assert_called_once_with("@1", "@2")

    async def test_a_symlinked_working_folder_still_matches(
        self, codex, tmp_path
    ) -> None:
        link = tmp_path / "link"
        link.symlink_to(codex.cwd)

        assert await codex.adopt_codex(where=link) == "2:@2"

    async def test_a_pane_without_the_thread_name_is_not_adopted(self, codex) -> None:
        codex.screens["%2"] = "some other thread"

        assert await codex.adopt_codex() is None

        codex.view.link_window_into_session.assert_not_called()

    async def test_an_unreadable_pane_is_not_adopted(self, codex) -> None:
        codex.screens.clear()

        assert await codex.adopt_codex() is None

    async def test_another_folder_is_not_a_candidate_even_with_the_name(
        self, codex, tmp_path
    ) -> None:
        codex.panes[1] = pane("2", "@2", 100, command="codex", path=str(tmp_path))

        assert await codex.adopt_codex() is None

        codex.view.capture_server_pane.assert_not_called()

    async def test_another_command_is_not_a_candidate(self, codex) -> None:
        codex.panes[1] = pane("2", "@2", 100, command="zsh", path=str(codex.cwd))

        assert await codex.adopt_codex() is None

    async def test_a_bot_owned_pane_is_not_a_candidate(self, codex) -> None:
        codex.panes[1] = pane("2", "@2", 100, True, "codex", str(codex.cwd))

        assert await codex.adopt_codex() is None

    async def test_a_window_also_in_a_bot_session_is_excluded(self, codex) -> None:
        codex.panes.append(pane("ccgram", "@2", 100, True, "codex", str(codex.cwd)))

        assert await codex.adopt_codex() is None

    async def test_a_window_bound_to_another_topic_is_excluded(self, codex) -> None:
        codex.bound.append("@2")

        assert await codex.adopt_codex() is None

    async def test_two_proven_windows_are_ambiguous(self, codex) -> None:
        codex.panes.append(pane("3", "@3", 101, command="codex", path=str(codex.cwd)))
        codex.screens["%3"] = f"x · {codex.cwd} · {_NAME} · y"

        assert await codex.adopt_codex() is None

        codex.view.link_window_into_session.assert_not_called()

    async def test_the_pane_without_the_name_does_not_make_it_ambiguous(
        self, codex
    ) -> None:
        codex.panes.append(pane("3", "@3", 101, command="codex", path=str(codex.cwd)))
        codex.screens["%3"] = "another codex thread"

        assert await codex.adopt_codex() == "2:@2"

    async def test_a_window_listed_twice_is_one_candidate(self, codex) -> None:
        codex.panes.append(pane("9", "@2", 100, command="codex", path=str(codex.cwd)))

        assert await codex.adopt_codex() == "2:@2"

    async def test_an_unreadable_listing_adopts_nothing(self, codex) -> None:
        codex.panes = None

        assert await codex.adopt_codex() is None

    async def test_a_failed_rebind_unlinks_again(self, codex) -> None:
        codex.rebound = False

        assert await codex.adopt_codex() is None

        codex.view.unlink_window_from_session.assert_awaited_once_with("@2")


_REAL = (
    "› Ask Codex to do anything\n\n"
    "  GPT-6.1-Sol medium · ~/projects/startups/projects/Platform/agentsspore · "
    "agentspore-oss · GPT-6.1-Sol · medium · Full Access · weekly 75% left · "
    "0.164.0 · Main [default]  Pursuing goal (4h 12m)\n"
    "  ← for agents · ? for shortcuts        ⚠ 2 warnings · f2 to view\n\n"
)
_CWD = Path("/Users/x/projects/startups/projects/Platform/agentsspore")


def proves(screen: str, name: str = _NAME, cwd: Path = _CWD) -> bool:
    return adopt_live.status_line_proves(screen, name, cwd)


class TestStatusLineProof:
    def test_the_real_status_line_proves_its_thread(self) -> None:
        assert proves(_REAL)

    def test_a_prefix_of_the_shown_thread_name_does_not(self) -> None:
        assert not proves(_REAL, "agentspore")

    def test_a_name_equal_to_the_folder_whose_next_field_differs_does_not(
        self,
    ) -> None:
        assert not proves(_REAL, "agentsspore")

    def test_a_name_only_in_body_text_above_the_last_lines_does_not(self) -> None:
        body = "we renamed it to agentspore-oss\n" * 5
        shown = _REAL.replace("agentspore-oss", "other")

        assert not proves(body + shown)

    def test_a_fixed_field_not_right_after_the_path_does_not(self) -> None:
        assert not proves(_REAL, "medium")

    def test_a_status_line_without_a_path_field_does_not(self) -> None:
        assert not proves("GPT · agentspore-oss · medium")

    def test_a_path_field_of_another_folder_does_not(self) -> None:
        assert not proves(_REAL, cwd=Path("/Users/x/other"))

    def test_an_empty_screen_does_not(self) -> None:
        assert not proves("")

    def test_a_folder_name_without_a_path_marker_is_not_a_path_field(self) -> None:
        assert not proves("GPT · agentsspore · agentspore-oss · medium")

    def test_a_status_line_scrolled_above_the_last_four_lines_does_not(self) -> None:
        old = "  GPT · ~/p/agentsspore · agentspore-oss · medium\n"

        assert proves(old + "a\nb\nc\n")
        assert not proves(old + "a\nb\nc\nd\n")

    def test_only_the_bottom_line_with_a_path_field_is_judged(self) -> None:
        screen = (
            "› draft\n  echo: m · ~/p/proj · topic-thread · x\n› Ask Codex\n"
            "  GPT · ~/p/proj · other-thread · medium\n  ? for shortcuts\n"
        )

        assert not proves(screen, "topic-thread", Path("/u/p/proj"))
        assert proves(screen, "other-thread", Path("/u/p/proj"))
