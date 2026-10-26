"""A message to a topic whose window is gone reconnects it to a live session first."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ccgram.handlers.recovery.restore_notices import adopt_text
from ccgram.handlers.text import text_handler
from ccgram.thread_router import thread_router

_TH = "ccgram.handlers.text.text_handler"
_USER, _CHAT, _THREAD = 100, -1001, 42


@pytest.fixture
def world(monkeypatch):
    thread_router.reset()
    thread_router.bind_thread(_USER, _THREAD, "@31", window_name="proj", chat_id=_CHAT)
    monkeypatch.setattr("ccgram.config.config.allowed_users", {_USER}, raising=False)
    w = SimpleNamespace(reply=AsyncMock(), forward=AsyncMock(), adopt=AsyncMock())
    w.adopt.side_effect = lambda *_a: self_rebind()
    with (
        patch(f"{_TH}.safe_reply", w.reply),
        patch(f"{_TH}._forward_message", w.forward),
        patch(f"{_TH}.adopt_for_topic", w.adopt),
        patch(f"{_TH}.protocol_adapter_for"),
        patch("ccgram.session_protocol.telegram.adapter_for"),
        patch(
            "ccgram.multiplexer.reconciliation.window_snapshot",
            AsyncMock(return_value=(True, None)),
        ),
        patch(f"{_TH}._handle_unbound_topic", AsyncMock(return_value=False)),
        patch(f"{_TH}.get_provider_for_window") as provider,
    ):
        provider.return_value.capabilities.chat_first_command_path = False
        yield w


def self_rebind() -> str:
    thread_router.bind_thread(_USER, _THREAD, "@2", window_name="proj", chat_id=_CHAT)
    return "2:@2"


def update():
    message = MagicMock(text="hello", chat=MagicMock(id=_CHAT, type="supergroup"))
    message.message_thread_id = _THREAD
    message.is_topic_message = True
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=_USER),
        message=message,
        effective_chat=SimpleNamespace(id=_CHAT),
    ), message


async def send(world):
    upd, message = update()
    context = SimpleNamespace(user_data={}, bot_data={}, bot=MagicMock())
    with patch(f"{_TH}._get_thread_id", return_value=_THREAD):
        await text_handler.handle_text_message(cast(Any, upd), cast(Any, context))
    return message


class TestAdoptGoneWindow:
    async def test_the_message_is_delivered_to_the_adopted_window(self, world) -> None:
        message = await send(world)

        world.adopt.assert_awaited_once_with(_USER, _CHAT, _THREAD, "@31")
        assert world.reply.await_args_list[0].args[1] == adopt_text("2:@2")
        assert world.forward.await_args.args[0] == "@2"
        assert world.forward.await_args.args[3] == message.text

    async def test_without_a_live_session_the_recovery_flow_runs_as_before(
        self, world
    ) -> None:
        world.adopt.side_effect = None
        world.adopt.return_value = None

        await send(world)

        world.forward.assert_not_called()
        assert "Reconnected" not in str(world.reply.await_args_list[0].args[1])

    async def test_a_live_window_is_never_adopted_over(self, world) -> None:
        alive = SimpleNamespace(pane_current_command="claude")
        with (
            patch(
                "ccgram.multiplexer.reconciliation.window_snapshot",
                AsyncMock(return_value=(True, alive)),
            ),
            patch(
                f"{_TH}.agent_origin_returned_to_shell", AsyncMock(return_value=False)
            ),
        ):
            await send(world)

        world.adopt.assert_not_called()
        assert world.forward.await_args.args[0] == "@31"

    async def test_it_works_with_auto_restore_on_start_off(
        self, world, monkeypatch
    ) -> None:
        monkeypatch.setattr("ccgram.config.config.auto_restore_on_start", False)

        await send(world)

        world.adopt.assert_awaited_once_with(_USER, _CHAT, _THREAD, "@31")
        assert world.forward.await_args.args[0] == "@2"

    async def test_one_notice_is_sent_and_the_message_is_delivered_once(
        self, world
    ) -> None:
        await send(world)

        notices = [c for c in world.reply.await_args_list if "Reconnected" in str(c)]
        assert len(notices) == 1
        world.forward.assert_awaited_once()
