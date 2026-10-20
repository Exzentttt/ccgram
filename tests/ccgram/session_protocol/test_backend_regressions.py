from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call
from uuid import uuid4

import pytest

from ccgram.multiplexer.base import ForegroundInfo, Multiplexer, PaneInfo, WindowRef
from ccgram.multiplexer.tmux import TmuxManager
from ccgram.multiplexer.vim_state import notify_vim_insert_seen, reset_vim_state
from ccgram.session_protocol.backend import MultiplexerSessionBackend
from ccgram.session_protocol.models import Action, ProtocolError
from ccgram.session_protocol.service import LocalSessionService
from ccgram.window_state_ports import identity_state
from ccgram.window_state_ports.identity_state import IdentityProjection


@pytest.fixture
def mux(monkeypatch):
    monkeypatch.setattr("ccgram.config.config.prompt_mode", "wrap")
    mux = AsyncMock(spec=Multiplexer)
    mux.capabilities = SimpleNamespace(
        native_worktrees=False, native_topic_targets=False
    )
    mux.list_windows_for_reconciliation.return_value = [
        WindowRef("@1", "project", "/tmp")
    ]
    mux.foreground.return_value = ForegroundInfo(0, 0, ["claude"], "/tmp")
    mux.list_panes.return_value = [PaneInfo("%1", 0, True, "claude", "/tmp", 80, 24)]
    mux.agent_status.return_value = None
    mux.capture_pane.return_value = "Ready"
    mux.capture_pane_by_id.return_value = "Ready"
    mux.get_pane_title.return_value = ""
    mux.send_to_pane.return_value = True
    monkeypatch.setattr(
        identity_state,
        "get_identity",
        lambda _: IdentityProjection(
            "@1", "claude", "conv", "/tmp", None, "project", "normal"
        ),
    )
    return mux


async def test_verified_pidless_foreground_can_act_and_argv_change_invalidates_binding(
    mux, store
):
    service = LocalSessionService(MultiplexerSessionBackend(mux, "/tmp"), store)
    session = (await service.list_sessions())[0]
    result, status = await service.execute(
        session.id,
        Action(
            str(uuid4()), session.binding_version, "terminal.key", {"keys": ["Escape"]}
        ),
    )
    assert status == 200 and result.outcome == "sent"
    mux.send_to_pane.assert_awaited_once()
    mux.send_to_pane.reset_mock()
    mux.foreground.return_value = ForegroundInfo(
        0, 0, ["claude", "--resume", "other"], "/tmp"
    )
    result, status = await service.execute(
        session.id,
        Action(
            str(uuid4()), session.binding_version, "terminal.key", {"keys": ["Enter"]}
        ),
    )
    assert status == 409 and result.outcome == "rejected"
    mux.send_to_pane.assert_not_awaited()


@pytest.mark.parametrize("foreground", [None, ForegroundInfo(23, 23, [], "/tmp")])
async def test_unknown_foreground_does_not_advertise_actions(mux, store, foreground):
    mux.foreground.return_value = foreground
    service = LocalSessionService(MultiplexerSessionBackend(mux, "/tmp"), store)
    session = (await service.list_sessions())[0]
    assert session.capabilities == ("terminal.view",)


@pytest.mark.parametrize(
    "argv,screen,allowed",
    [
        (["bash", "./script.sh"], "⌘0⌘", False),
        (["bash", "-c", "read value"], "⌘0⌘", False),
        (["bash"], "⌘0⌘read value", False),
        (["bash"], "Enter value:", False),
        (["bash"], "⌘0⌘", True),
    ],
)
async def test_resume_requires_a_bare_shell_and_idle_prompt(
    mux, store, argv, screen, allowed
):
    mux.foreground.return_value = ForegroundInfo(23, 23, argv, "/tmp")
    mux.capture_pane.return_value = screen
    mux.capture_pane_by_id.return_value = screen
    service = LocalSessionService(MultiplexerSessionBackend(mux, "/tmp"), store)
    session = (await service.list_sessions())[0]
    assert ("session.resume" in session.capabilities) is allowed


async def test_resume_revalidates_shell_before_injection(mux):
    mux.foreground.return_value = ForegroundInfo(23, 23, ["bash"], "/tmp")
    mux.capture_pane.return_value = mux.capture_pane_by_id.return_value = "⌘0⌘"
    backend = MultiplexerSessionBackend(mux, "/tmp")
    target = await backend.inspect("@1")
    mux.foreground.return_value = ForegroundInfo(23, 23, ["bash", "script.sh"], "/tmp")
    with pytest.raises(ProtocolError):
        await backend.resume(target, "conv")
    mux.send_to_pane.assert_not_awaited()


async def test_gemini_working_title_is_used_for_snapshot(mux):
    mux.foreground.return_value = ForegroundInfo(23, 23, ["gemini"], "/tmp")
    mux.get_pane_title.return_value = "Working: ✦"
    target = await MultiplexerSessionBackend(mux, "/tmp").inspect("@1")
    assert target.status.state == "working"


async def test_native_worktree_rejects_selected_workspace(mux, tmp_path):
    mux.capabilities.native_worktrees = True
    with pytest.raises(ProtocolError, match="workspace") as error:
        await MultiplexerSessionBackend(mux, "/tmp").create(
            {
                "directory": str(tmp_path),
                "provider": "claude",
                "worktree": True,
                "workspaceId": "selected",
            }
        )
    assert error.value.status == 422
    mux.create_worktree_window.assert_not_awaited()


@pytest.mark.parametrize(
    "kind,raw", [("message.send", False), ("session.resume", True)]
)
async def test_action_uses_agent_submission_or_raw_shell_input(
    mux, store, kind, raw, monkeypatch
):
    conversation = str(uuid4())
    identity = identity_state.get_identity("@1")
    assert identity is not None
    monkeypatch.setattr(
        identity_state,
        "get_identity",
        lambda _: replace(identity, session_id=conversation),
    )
    if raw:
        mux.foreground.return_value = ForegroundInfo(23, 23, ["bash"], "/tmp")
        mux.capture_pane.return_value = mux.capture_pane_by_id.return_value = "⌘0⌘"
    service = LocalSessionService(MultiplexerSessionBackend(mux, "/tmp"), store)
    session = (await service.list_sessions())[0]
    data = {"conversationId": conversation} if raw else {"text": "hello"}
    result, status = await service.execute(
        session.id, Action(str(uuid4()), session.binding_version, kind, data)
    )
    assert status == 200 and result.outcome == "sent"
    assert mux.send_to_pane.await_args.kwargs["raw"] is raw
    assert mux.send_to_pane.await_args.kwargs["window_id"] == "@1"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("hello", [("i", False, True), ("hello", False, True), ("", True, False)]),
        (
            "!ls",
            [
                ("i", False, True),
                ("!", False, True),
                ("ls", False, True),
                ("", True, False),
            ],
        ),
    ],
)
async def test_tmux_safe_message_stays_on_pinned_pane(monkeypatch, text, expected):
    manager = TmuxManager.__new__(TmuxManager)
    pane, other = MagicMock(), MagicMock()
    pane.pane_id, other.pane_id = "%1", "%2"
    pane.capture_pane.return_value = ["-- NORMAL --"]
    window = MagicMock()
    window.window_id, window.panes, window.active_pane = "@1", [pane, other], other
    session = MagicMock()
    session.windows.__iter__.side_effect = lambda: iter([window])
    session.windows.get.return_value = window
    manager.get_session = MagicMock(return_value=session)
    reset_vim_state()
    notify_vim_insert_seen("@1")
    sleeps = AsyncMock()
    monkeypatch.setattr("ccgram.multiplexer.tmux.asyncio.sleep", sleeps)
    try:
        assert await manager.send_to_pane("%1", text, window_id="@1", raw=False)
        assert pane.send_keys.call_args_list == [
            call(value, enter=enter, literal=literal)
            for value, enter, literal in expected
        ]
        other.send_keys.assert_not_called()
        assert call(0.5) in sleeps.await_args_list
    finally:
        reset_vim_state()
