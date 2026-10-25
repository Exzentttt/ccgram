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


class TestReplacementRequest:
    def test_defaults_describe_a_fresh_session_without_a_previous_window(self) -> None:
        request = ReplacementRequest(user_id=1, thread_id=2, cwd="/tmp/p")

        assert request.old_window_id == ""
        assert request.resume_id is None
        assert request.continue_session is False
        assert request.chat_id is None
