from ccgram.handlers.recovery.recovery_ui import HeadlessUi, ReplacementRequest


class TestHeadlessUi:
    async def test_reports_nothing_back_and_holds_no_state(self) -> None:
        ui = HeadlessUi()

        assert await ui.edit("working", reply_markup=None) is None
        assert await ui.answer("Created") is None
        assert ui.pending_text() is None
        assert ui.clear_state() is None
        assert ui.user_data is None


class TestReplacementRequest:
    def test_defaults_describe_a_fresh_session_without_a_previous_window(self) -> None:
        request = ReplacementRequest(user_id=1, thread_id=2, cwd="/tmp/p")

        assert request.old_window_id == ""
        assert request.resume_id is None
        assert request.continue_session is False
        assert request.chat_id is None
