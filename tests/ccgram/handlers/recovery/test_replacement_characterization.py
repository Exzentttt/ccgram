"""Characterization of the recovery window replacement, driven by its buttons.

Every scenario taps a real Fresh / Resume-pick handler and asserts the exact
ordered sequence of observable calls: provisioning claim, state flushes,
backend create arguments, topic binding, UI edits and answers. The inputs are
the real callback shapes (``rec:f:@0`` data, ``user_data`` recovery keys); the
outward boundaries (backend, Telegram, multiplexer, router, state store) are
recorded mocks. The same file passes on the pre-refactor implementation, which
is what makes it a parity proof rather than a snapshot of the new code.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

import pytest
from structlog.testing import capture_logs
from telegram.error import BadRequest, RetryAfter

from ccgram.handlers.callback_data import CB_RECOVERY_FRESH, CB_RECOVERY_PICK
from ccgram.handlers.recovery.recovery_banner import (
    create_and_bind_window,
    _handle_fresh,
    _stale_recovery_offer as _real_stale_recovery_offer,
)
from ccgram.handlers.recovery.recovery_ui import HeadlessUi, ReplacementRequest
from ccgram.handlers.recovery.resume_picker import _handle_resume_pick
from ccgram.handlers.status.topic_emoji import format_topic_name_for_mode
from ccgram.handlers.topics import topic_orchestration
from ccgram.handlers.user_state import (
    PENDING_THREAD_ID,
    PENDING_THREAD_TEXT,
    RECOVERY_SESSIONS,
    RECOVERY_WINDOW_ID,
)
from ccgram.session_protocol.models import ProtocolError
from ccgram.window_state_store import CCGRAM_CREATED_WINDOW_ORIGIN

_RC = "ccgram.handlers.recovery.recovery_banner"
_RP = "ccgram.handlers.recovery.resume_picker"
_CWD = "/tmp"
_UNREGISTERED = (
    "⚠ Session is still starting; ccgram kept it quarantined for recovery. "
    "Try again after it finishes registering."
)


@pytest.fixture()
def flow():
    """One recorded replacement run: ``parent.mock_calls`` is the ordered log."""
    parent = MagicMock()
    router, session = parent.router, parent.session
    router.resolve_chat_id.return_value = -100
    router.begin_topic_provisioning.return_value = SimpleNamespace(claim_id="claim-1")
    router.commit_topic_provisioning.return_value = True

    adapter = MagicMock()
    adapter.create = AsyncMock(
        return_value=SimpleNamespace(target_id="@5", title="project")
    )
    adapter.close = AsyncMock(return_value=(True, "Closed"))
    parent.attach_mock(adapter, "adapter")

    async def record_edit(target, text, **kwargs):
        assert target is query, "the progress message must be the tapped query"
        parent.edit(text, **kwargs)

    edit = AsyncMock(side_effect=record_edit)
    parent.stale = AsyncMock(return_value=None)
    parent.wait = AsyncMock(return_value=True)
    parent.presence = AsyncMock(return_value=True)
    parent.send = AsyncMock(return_value=(True, "sent"))
    parent.safe_send = AsyncMock()
    adapter_for = MagicMock(return_value=adapter)

    provider = MagicMock()
    provider.capabilities.name = "claude"
    provider.capabilities.supports_hook = True
    view = SimpleNamespace(provider_name="claude", approval_mode="normal", cwd=_CWD)
    window_query = MagicMock()
    window_query.view_window.return_value = view
    window_query.resolve_window_alias.side_effect = lambda wid: wid

    query = AsyncMock()
    parent.answer = query.answer = AsyncMock()
    query.message = MagicMock()
    query.message.chat.id = -100999
    query.message.chat.type = "supergroup"
    query.message.chat.is_forum = True
    query.message.is_topic_message = True
    query.message.message_thread_id = 42
    update = MagicMock()
    update.effective_user = MagicMock(id=100)
    update.message = None
    update.callback_query = query
    context = MagicMock()
    context.bot = AsyncMock()
    context.user_data = {
        PENDING_THREAD_ID: 42,
        PENDING_THREAD_TEXT: "hello",
        RECOVERY_WINDOW_ID: "@0",
    }
    client_factory = MagicMock(return_value=parent.client)
    parent.client.edit_forum_topic = AsyncMock()

    with (
        patch(f"{_RC}.tmux_manager"),
        patch(f"{_RC}.thread_router", router),
        patch(f"{_RC}.session_manager", session),
        patch(f"{_RC}.safe_edit", edit),
        patch(f"{_RC}.window_query", window_query),
        patch(f"{_RP}.window_query", window_query),
        patch(f"{_RC}._cwd_for_window", return_value=_CWD),
        patch(f"{_RC}.get_provider", return_value=provider),
        patch(f"{_RC}.get_provider_for_window", return_value=provider),
        patch(f"{_RC}.adapter_for", adapter_for),
        patch(f"{_RC}.safe_send", parent.safe_send),
        patch(f"{_RC}._stale_recovery_offer", parent.stale),
        patch(f"{_RC}.session_map_sync.wait_for_session_map_entry", parent.wait),
        patch(f"{_RC}.send_telegram_to_window", parent.send),
        patch(f"{_RC}.PTBTelegramClient", client_factory),
        patch("ccgram.multiplexer.reconciliation.window_presence", parent.presence),
    ):
        yield SimpleNamespace(
            parent=parent,
            adapter=adapter,
            adapter_for=adapter_for,
            router=router,
            session=session,
            window_query=window_query,
            stale=parent.stale,
            wait=parent.wait,
            presence=parent.presence,
            view=view,
            provider=provider,
            query=query,
            update=update,
            context=context,
            tap_fresh=lambda: _handle_fresh(
                query, 100, f"{CB_RECOVERY_FRESH}@0", update, context
            ),
            tap_pick=lambda: _handle_resume_pick(
                query, 100, f"{CB_RECOVERY_PICK}0", update, context
            ),
        )
    topic_orchestration.clear_pending_creation("@5")
    topic_orchestration.clear_pending_creation("@6")


_CLAIM = [
    call.router.begin_topic_provisioning(
        100, -100999, thread_id=42, kind="replacement", previous_target_id="@0"
    ),
    call.session.flush_state(),
]
_STALE_CHECKED = [call.stale("@0")]
_CREATE_NORMAL = call.adapter.create(
    {"provider": "claude", "directory": _CWD},
    approval_mode="normal",
    resume_id=None,
    continue_session=False,
)
_ATTACHED = [
    call.router.attach_provisioning_target("claim-1", "@5"),
    call.session.flush_state(),
]
_REGISTERED = [call.wait("@5", resolve_window_id=ANY)]
_ABORTED = [
    call.router.abort_topic_provisioning("claim-1", target_confirmed_absent=True),
    call.session.flush_state(),
]
_UNCERTAIN = [
    call.router.mark_provisioning_uncertain("claim-1"),
    call.session.flush_state(),
]


def _bound(
    mode: str, label: str, pending: str | None = "hello", wid: str = "@5"
) -> list:
    forwarded = [call.send(100, wid, 42, pending, -100999, adapter=ANY)]
    return [
        call.session.set_window_origin(wid, CCGRAM_CREATED_WINDOW_ORIGIN),
        call.session.set_window_provider(wid, "claude"),
        call.session.set_window_approval_mode(wid, mode),
        call.router.commit_topic_provisioning("claim-1", window_name="project"),
        call.session.flush_state(),
        call.router.resolve_chat_id(100, 42),
        call.client.edit_forum_topic(
            chat_id=-100,
            message_thread_id=42,
            name=format_topic_name_for_mode("project", mode),
        ),
        call.edit(f"✅ Created session 'project'\n\n{label}"),
        *(forwarded if pending else []),
        call.answer("Created"),
    ]


class TestSuccess:
    async def test_fresh_creates_binds_renames_and_forwards_the_pending_text(
        self, flow
    ) -> None:
        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            *_bound("normal", "Fresh session started."),
        ]
        assert flow.context.user_data == {}
        assert not topic_orchestration.is_pending_creation("@5")

    async def test_resume_pick_launches_the_picked_session_in_yolo_mode(
        self, flow
    ) -> None:
        flow.view.approval_mode = "yolo"
        flow.context.user_data[RECOVERY_SESSIONS] = [
            {
                "session_id": "sess-1",
                "summary": "fix the parser",
                "mtime": 1.0,
                "provider_name": "claude",
            }
        ]

        await flow.tap_pick()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            call.adapter.create(
                {"provider": "claude", "directory": _CWD},
                approval_mode="yolo",
                resume_id="sess-1",
                continue_session=False,
            ),
            *_ATTACHED,
            *_REGISTERED,
            *_bound("yolo", "Resuming session: fix the parser"),
        ]

    async def test_provider_without_hooks_skips_the_registration_wait(
        self, flow
    ) -> None:
        flow.provider.capabilities.supports_hook = False
        flow.context.user_data.pop(PENDING_THREAD_TEXT)

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_bound("normal", "Fresh session started.", pending=None),
        ]


class TestCreateFailure:
    async def test_definitive_failure_aborts_the_claim_and_keeps_the_old_binding(
        self, flow
    ) -> None:
        flow.adapter.create = AsyncMock(
            side_effect=ProtocolError(
                400, "invalid_request", "Directory does not exist: /gone"
            )
        )

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            call.adapter.create(
                {"provider": "claude", "directory": _CWD},
                approval_mode="normal",
                resume_id=None,
                continue_session=False,
            ),
            *_ABORTED,
            call.edit("❌ Directory does not exist: /gone"),
            call.answer("Failed"),
        ]
        assert flow.context.user_data == {}

    async def test_server_error_leaves_the_claim_uncertain_and_raises(
        self, flow
    ) -> None:
        flow.adapter.create = AsyncMock(
            side_effect=ProtocolError(503, "backend_unavailable", "unconfirmed")
        )

        with pytest.raises(ProtocolError):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_UNCERTAIN,
        ]

    async def test_backend_without_target_id_is_a_failed_create(self, flow) -> None:
        flow.adapter.create = AsyncMock(
            return_value=SimpleNamespace(target_id="", title="project")
        )

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_UNCERTAIN,
            call.edit("❌ Backend returned no target ID"),
            call.answer("Failed"),
        ]
        assert flow.context.user_data == {}


class TestReadinessTimeout:
    async def test_unregistered_but_live_target_stays_quarantined(self, flow) -> None:
        flow.wait.return_value = False
        flow.presence.return_value = True

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            call.presence("@5", ANY),
            *_UNCERTAIN,
            call.edit(_UNREGISTERED),
            call.answer("Still starting"),
        ]
        assert flow.context.user_data[PENDING_THREAD_TEXT] == "hello"
        assert not topic_orchestration.is_pending_creation("@5")

    async def test_unregistered_and_gone_target_aborts_the_claim(self, flow) -> None:
        flow.wait.return_value = False
        flow.presence.return_value = False

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            call.presence("@5", ANY),
            *_ABORTED,
            call.edit("❌ Session did not register with ccgram and is gone"),
            call.answer("Failed"),
        ]
        assert flow.context.user_data == {}

    async def test_unverifiable_presence_quarantines_with_its_own_message(
        self, flow
    ) -> None:
        flow.wait.return_value = False
        flow.presence.side_effect = OSError("multiplexer down")

        await flow.tap_fresh()

        assert flow.parent.mock_calls[-4:] == [
            *_UNCERTAIN,
            call.edit(
                "⚠ Could not verify the new session; ccgram kept it quarantined "
                "for recovery."
            ),
            call.answer("Still starting"),
        ]

    async def test_hook_wait_error_closes_the_target_aborts_and_reraises(
        self, flow
    ) -> None:
        flow.wait.side_effect = RuntimeError("hook boom")

        with pytest.raises(RuntimeError, match="hook boom"):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            call.adapter.close("@5"),
            *_ABORTED,
        ]


class TestStaleWindowAlive:
    """The offer is re-read from a confirmed snapshot (real ``_stale_recovery_offer``)."""

    @pytest.fixture()
    def real_offer(self, flow):
        flow.stale.side_effect = _real_stale_recovery_offer

    async def test_agent_running_again_refuses_and_creates_nothing(
        self, flow, real_offer
    ) -> None:
        with (
            patch(
                "ccgram.multiplexer.reconciliation.window_snapshot",
                AsyncMock(return_value=(True, SimpleNamespace())),
            ),
            patch(
                "ccgram.handlers.telegram_origin.agent_origin_returned_to_shell",
                AsyncMock(return_value=False),
            ),
        ):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            *_ABORTED,
            call.edit(
                "That session is running again, so it was left alone.",
                reply_markup=None,
            ),
        ]
        assert flow.context.user_data[PENDING_THREAD_TEXT] == "hello"

    async def test_unreachable_multiplexer_refuses_and_creates_nothing(
        self, flow, real_offer
    ) -> None:
        with patch(
            "ccgram.multiplexer.reconciliation.window_snapshot",
            AsyncMock(return_value=(False, None)),
        ):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            *_ABORTED,
            call.edit(
                "\u26a0 Could not reach the multiplexer, so nothing was changed. "
                "Try again in a moment.",
                reply_markup=None,
            ),
        ]

    async def test_cancelled_probe_releases_the_claim_and_propagates(
        self, flow
    ) -> None:
        flow.stale.side_effect = asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [*_CLAIM, *_STALE_CHECKED, *_ABORTED]


@pytest.fixture()
def cancelled_create(flow):
    """Cancel the tap while the backend create is in flight; create then returns."""
    started, release = asyncio.Event(), asyncio.Event()

    async def create(*_args, **_kwargs):
        started.set()
        await release.wait()
        return SimpleNamespace(target_id=flow.created_id, title="project")

    flow.created_id = ""
    flow.adapter.create = AsyncMock(side_effect=create)

    async def tap_and_cancel():
        task = asyncio.create_task(flow.tap_fresh())
        await started.wait()
        task.cancel()
        release.set()
        await task

    return tap_and_cancel


class TestCancellation:
    async def test_cancelled_create_without_target_reports_failure_and_reraises(
        self, flow, cancelled_create
    ) -> None:
        with pytest.raises(asyncio.CancelledError):
            await cancelled_create()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_UNCERTAIN,
            call.edit("❌ Backend returned no target ID"),
            call.answer("Failed"),
        ]
        assert flow.context.user_data == {}

    async def test_cancelled_create_with_target_closes_it_and_reraises(
        self, flow, cancelled_create
    ) -> None:
        flow.created_id = "@5"

        with pytest.raises(asyncio.CancelledError):
            await cancelled_create()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            call.adapter.close("@5"),
            *_ABORTED,
        ]
        assert not topic_orchestration.is_pending_creation("@5")

    async def test_cancelled_presence_probe_quarantines_and_reraises(
        self, flow
    ) -> None:
        flow.wait.return_value = False
        flow.presence.side_effect = asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            call.presence("@5", ANY),
            *_UNCERTAIN,
        ]
        assert not topic_orchestration.is_pending_creation("@5")


class TestCommit:
    async def test_unbindable_target_stays_quarantined_and_is_not_pending(
        self, flow
    ) -> None:
        flow.router.commit_topic_provisioning.return_value = False

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            *_bound("normal", "")[:5],
            *_UNCERTAIN,
            call.edit("❌ Session could not be bound to this topic"),
            call.answer("Failed"),
        ]
        assert flow.context.user_data == {}
        assert not topic_orchestration.is_pending_creation("@5")

    async def test_commit_error_quarantines_clears_pending_and_reraises(
        self, flow
    ) -> None:
        flow.router.commit_topic_provisioning.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            *_bound("normal", "")[:4],
            *_UNCERTAIN,
        ]
        assert not topic_orchestration.is_pending_creation("@5")


class TestPendingTextForwarding:
    async def test_failed_forward_tells_the_user_and_still_succeeds(self, flow) -> None:
        flow.parent.send.return_value = (False, "window busy")

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            *_bound("normal", "Fresh session started.")[:-1],
            call.router.resolve_chat_id(100, 42),
            call.safe_send(
                flow.parent.client,
                -100,
                "❌ Failed to send pending message: window busy",
                message_thread_id=42,
            ),
            call.answer("Created"),
        ]
        assert flow.context.user_data == {}


class TestSupersededTarget:
    async def test_replaced_window_id_is_followed_through_to_the_binding(
        self, flow
    ) -> None:
        flow.window_query.resolve_window_alias.side_effect = {"@5": "@6"}.get

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            call.router.attach_provisioning_target("claim-1", "@6"),
            call.session.flush_state(),
            *_bound("normal", "Fresh session started.", wid="@6"),
        ]
        assert not topic_orchestration.is_pending_creation("@5")
        assert not topic_orchestration.is_pending_creation("@6")


class TestPersistenceFailureWhileFailing:
    async def test_flush_error_while_marking_uncertain_keeps_the_original_failure(
        self, flow
    ) -> None:
        flow.adapter.create = AsyncMock(
            side_effect=ProtocolError(503, "backend_unavailable", "unconfirmed")
        )
        flow.session.flush_state.side_effect = [None, RuntimeError("disk full")]

        with pytest.raises(ProtocolError):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_UNCERTAIN,
        ]

    async def test_flush_error_while_aborting_still_reports_the_failure(
        self, flow
    ) -> None:
        flow.adapter.create = AsyncMock(
            side_effect=ProtocolError(
                400, "invalid_request", "Directory does not exist: /gone"
            )
        )
        flow.session.flush_state.side_effect = [None, RuntimeError("disk full")]

        await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ABORTED,
            call.edit("❌ Directory does not exist: /gone"),
            call.answer("Failed"),
        ]


class TestSessionServiceMissing:
    async def test_failing_adapter_lookup_surfaces_at_the_create_step(
        self, flow
    ) -> None:
        flow.adapter_for.side_effect = RuntimeError("not initialized")

        with pytest.raises(RuntimeError, match="not initialized"):
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [*_CLAIM, *_STALE_CHECKED, *_UNCERTAIN]


class TestRenameRateLimit:
    async def test_button_path_swallows_it_exactly_as_before(self, flow) -> None:
        flow.parent.client.edit_forum_topic.side_effect = RetryAfter(7)

        with capture_logs() as logs:
            await flow.tap_fresh()

        assert flow.parent.mock_calls == [
            *_CLAIM,
            *_STALE_CHECKED,
            _CREATE_NORMAL,
            *_ATTACHED,
            *_REGISTERED,
            *_bound("normal", "Fresh session started."),
        ]
        assert [log["event"] for log in logs if log["log_level"] == "debug"] == [
            f"Failed to rename topic: {RetryAfter(7)}"
        ]

    async def test_headless_path_reports_the_wait_and_still_succeeds(
        self, flow
    ) -> None:
        flow.parent.client.edit_forum_topic.side_effect = RetryAfter(7)
        ui = HeadlessUi()
        request = ReplacementRequest(user_id=100, thread_id=42, cwd=_CWD, chat_id=-100)

        ok = await create_and_bind_window(request, ui, MagicMock(), flow.adapter_for)

        assert ok is True
        assert ui.retry_after == 7

    async def test_other_telegram_errors_are_not_a_rate_limit(self, flow) -> None:
        flow.parent.client.edit_forum_topic.side_effect = BadRequest("nope")
        ui = HeadlessUi()
        request = ReplacementRequest(user_id=100, thread_id=42, cwd=_CWD, chat_id=-100)

        assert await create_and_bind_window(request, ui, MagicMock(), flow.adapter_for)
        assert ui.retry_after is None


class TestRequestOverrides:
    async def test_verified_offer_skips_the_liveness_check(self, flow) -> None:
        request = ReplacementRequest(
            user_id=100,
            thread_id=42,
            cwd=_CWD,
            chat_id=-100,
            old_window_id="@0",
            offer_verified=True,
        )

        assert await create_and_bind_window(
            request, HeadlessUi(), MagicMock(), flow.adapter_for
        )
        flow.stale.assert_not_called()

    async def test_unverified_offer_is_still_checked(self, flow) -> None:
        request = ReplacementRequest(
            user_id=100, thread_id=42, cwd=_CWD, chat_id=-100, old_window_id="@0"
        )

        await create_and_bind_window(
            request, HeadlessUi(), MagicMock(), flow.adapter_for
        )
        flow.stale.assert_awaited_once_with("@0")

    async def test_request_approval_mode_beats_the_old_window_state(self, flow) -> None:
        # The old id may now name another topic's window, so its state lies.
        flow.view.approval_mode = "normal"
        request = ReplacementRequest(
            user_id=100,
            thread_id=42,
            cwd=_CWD,
            chat_id=-100,
            old_window_id="@0",
            approval_mode="yolo",
            offer_verified=True,
        )

        await create_and_bind_window(
            request, HeadlessUi(), MagicMock(), flow.adapter_for
        )

        assert flow.adapter.create.await_args.kwargs["approval_mode"] == "yolo"
