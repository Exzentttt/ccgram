from structlog.testing import capture_logs

from ccgram.handlers.recovery.recovery_ui import HeadlessUi, ReplacementRequest


class TestHeadlessUi:
    async def test_progress_and_outcome_are_logged_in_order(self) -> None:
        ui = HeadlessUi()

        with capture_logs() as logs:
            await ui.edit("working", reply_markup=None)
            await ui.answer("Created")

        assert logs == [
            {"event": "recovery_progress", "text": "working", "log_level": "info"},
            {"event": "recovery_outcome", "text": "Created", "log_level": "debug"},
        ]

    def test_holds_no_recovery_state(self) -> None:
        ui = HeadlessUi()

        ui.clear_state()

        assert ui.pending_text() is None
        assert ui.user_data is None

    def test_rate_limit_is_recorded_per_instance(self) -> None:
        ui, other = HeadlessUi(), HeadlessUi()
        assert ui.retry_after is None

        ui.note_rate_limit(12.0)

        assert ui.retry_after == 12.0
        assert other.retry_after is None


class TestReplacementRequest:
    def test_defaults_describe_a_fresh_session_without_a_previous_window(self) -> None:
        request = ReplacementRequest(user_id=1, thread_id=2, cwd="/tmp/p")

        assert request.old_window_id == ""
        assert request.resume_id is None
        assert request.continue_session is False
        assert request.chat_id is None
        assert request.offer_verified is False
        assert request.approval_mode == ""
