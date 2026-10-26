"""``SessionManager.rebind_window``: a topic moves to another window of the same session."""

import json

import pytest

from ccgram.session import SessionManager
from ccgram.session_map import session_map_prefix, session_map_sync
from ccgram.thread_router import thread_router
from ccgram.window_state_store import WindowState, window_store

_USER, _CHAT, _THREAD = 7, -100, 41


@pytest.fixture
def mgr(monkeypatch, tmp_path) -> SessionManager:
    thread_router.reset()
    window_store.window_states.clear()
    monkeypatch.setattr(SessionManager, "_load_state", lambda self: None)
    monkeypatch.setattr(SessionManager, "_save_state", lambda self: None)
    monkeypatch.setattr("ccgram.session.config.multiplexer_name", "tmux")
    monkeypatch.setattr(
        "ccgram.session.config.session_map_file", tmp_path / "session_map.json"
    )
    manager = SessionManager()
    window_store.window_states["@1"] = WindowState(
        session_id="sid-1", cwd="/repo", transcript_path="/t.jsonl", window_name="repo"
    )
    thread_router.bind_thread(_USER, _THREAD, "@1", window_name="repo", chat_id=_CHAT)
    return manager


class TestRebindWindow:
    def test_the_topic_and_its_state_move_to_the_new_window(self, mgr) -> None:
        assert mgr.rebind_window("@1", "@9") is True

        assert thread_router.get_window_for_thread(_USER, _THREAD) == "@9"
        assert "@1" not in window_store.window_states
        assert window_store.window_states["@9"].session_id == "sid-1"

    def test_the_session_map_gets_an_entry_for_the_new_window(
        self, mgr, tmp_path
    ) -> None:
        mgr.rebind_window("@1", "@9")

        raw = json.loads((tmp_path / "session_map.json").read_text())
        entry = raw[f"{session_map_prefix()}@9"]
        assert (entry["session_id"], entry["cwd"]) == ("sid-1", "/repo")

    def test_an_existing_entry_is_re_keyed_not_duplicated(self, mgr, tmp_path) -> None:
        path = tmp_path / "session_map.json"
        old_key = f"{session_map_prefix()}@1"
        path.write_text(json.dumps({old_key: {"session_id": "sid-1", "cwd": "/repo"}}))

        mgr.rebind_window("@1", "@9")

        assert list(json.loads(path.read_text())) == [f"{session_map_prefix()}@9"]

    def test_a_failed_fold_reports_false_and_leaves_the_binding(
        self, mgr, monkeypatch
    ) -> None:
        monkeypatch.setattr(
            session_map_sync, "rename_session_map_entries", lambda _moves: False
        )

        assert mgr.rebind_window("@1", "@9") is False

        assert thread_router.get_window_for_thread(_USER, _THREAD) == "@1"
        assert "@9" not in window_store.window_states

    def test_an_unknown_old_window_reports_false(self, mgr) -> None:
        assert mgr.rebind_window("@5", "@9") is False

    def test_a_window_state_without_a_topic_changes_nothing(
        self, mgr, tmp_path
    ) -> None:
        window_store.window_states["@5"] = WindowState(session_id="sid-5", cwd="/x")

        assert mgr.rebind_window("@5", "@9") is False

        assert set(window_store.window_states) == {"@1", "@5"}
        assert thread_router.get_window_for_thread(_USER, _THREAD) == "@1"
        assert not (tmp_path / "session_map.json").exists()

    def test_a_bound_topic_without_window_state_reports_what_happened(
        self, mgr
    ) -> None:
        del window_store.window_states["@1"]

        result = mgr.rebind_window("@1", "@9")

        assert (result, thread_router.get_window_for_thread(_USER, _THREAD)) == (
            False,
            "@9",
        )
