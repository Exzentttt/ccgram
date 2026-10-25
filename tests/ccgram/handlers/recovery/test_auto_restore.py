"""Auto-restore of dead topics at startup, driven through the real replacement.

The unit under test is ``auto_restore``: selection, the ledger and the loop.
Below it everything is real except the outward boundaries: the multiplexer
(window listing, boot id, window creation through the adapter), the Telegram
client, the session-map hook wait and the claude transcript scan. The router is
a real ``ThreadRouter``, so the id-collision and eviction scenarios exercise
the router's own binding rules instead of a recording of them.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from structlog.testing import capture_logs
from telegram.error import RetryAfter

from ccgram.config import config
from ccgram.handlers.recovery import auto_restore
from ccgram.handlers.topics import topic_orchestration
from ccgram.multiplexer.base import WindowRef
from ccgram.session_protocol.backend import CreationOptions, MultiplexerSessionBackend
from ccgram.session_protocol.models import ProtocolError
from ccgram.thread_router import ThreadRouter

_AR = "ccgram.handlers.recovery.auto_restore"
_RC = "ccgram.handlers.recovery.recovery_banner"
_CHAT = -1001
_USER = 100
_TOPIC_OPTIONS = frozenset(
    {"name", "session", "provider", "mode", "chat", "cwd", "known_session"}
)
_UUID = "3f2b8c1e-9d4a-4e6b-8a71-5c0d2e9f1a47"


class Env:
    """The world around the bot: windows, window state, sessions, a router."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.boot = "boot-1"
        self.live: list[WindowRef] | None = [WindowRef("@0", "_ccgram", str(tmp_path))]
        self.states: dict[str, SimpleNamespace] = {}
        self.claude_sessions: dict[str, list[str]] = {}
        self.next_ids: list[str] = []
        self.created: list[tuple[dict, dict]] = []
        self.rename_error: Exception | None = None
        self.create_error: BaseException | None = None
        self.router = ThreadRouter(
            schedule_save=lambda: None,
            has_window_state=lambda wid: wid in self.states,
        )
        self.stale = AsyncMock(return_value=None)
        self.pause = AsyncMock()
        self.mux = MagicMock()
        self.mux.server_boot_id = AsyncMock(side_effect=lambda: self.boot)
        self.adapter = MagicMock()
        self.adapter.create = AsyncMock(side_effect=self.create)
        self.adapter.close = AsyncMock(return_value=(True, "Closed"))

    def project(self, name: str) -> str:
        path = self.tmp / name
        path.mkdir(exist_ok=True)
        return str(path)

    def dead_topic(self, thread: int, window: str, **options: Any) -> None:
        """A topic bound to ``window`` whose window is gone.

        Options: name, session, provider, mode, chat, cwd, known_session.
        """
        opts = {
            "name": f"proj{thread}",
            "session": "sess",
            "provider": "claude",
            "mode": "normal",
            "chat": _CHAT,
            "cwd": None,
            "known_session": True,
            **options,
        }
        assert opts.keys() <= _TOPIC_OPTIONS
        name, session, provider = opts["name"], opts["session"], opts["provider"]
        cwd = self.project(name) if opts["cwd"] is None else opts["cwd"]
        self.router.bind_thread(
            _USER, thread, window, window_name=name, chat_id=opts["chat"]
        )
        self.states[window] = SimpleNamespace(
            session_id=session or "",
            cwd=cwd,
            provider_name=provider,
            approval_mode=opts["mode"],
        )
        if session and opts["known_session"] and provider == "claude":
            self.claude_sessions.setdefault(cwd, []).append(session)

    def codex_rollout(self, session: str) -> None:
        day = Path.home() / ".codex" / "sessions" / "2026" / "10" / "26"
        day.mkdir(parents=True, exist_ok=True)
        (day / f"rollout-2026-10-26T10-00-00-{session}.jsonl").write_text("{}\n")

    def bound(self, thread: int) -> str | None:
        return self.router.get_window_for_chat_thread(_CHAT, thread)

    async def create(self, data: dict, **kwargs):
        if self.create_error is not None:
            raise self.create_error
        self.created.append((data, kwargs))
        wid = self.next_ids.pop(0)
        title = Path(data["directory"]).name
        assert self.live is not None
        self.live.append(WindowRef(wid, title, data["directory"]))
        self.states[wid] = SimpleNamespace(
            session_id=kwargs["resume_id"] or "",
            cwd=data["directory"],
            provider_name=data["provider"],
            approval_mode=kwargs["approval_mode"],
        )
        return SimpleNamespace(target_id=wid, title=title)

    @property
    def ledger(self) -> dict:
        return json.loads((self.tmp / "auto_restore.json").read_text())

    def ledger_status(self) -> dict[int, str]:
        return {row["thread_id"]: row["status"] for row in self.ledger["entries"]}

    async def start(self):
        task = await auto_restore.start_auto_restore(MagicMock(), lambda: self.adapter)
        if task is not None:
            await task
        return task


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(config, "config_dir", tmp_path)
    monkeypatch.setattr(config, "auto_restore_on_start", True)
    monkeypatch.setattr(config, "autodelete_dead_topics", False)
    world = Env(tmp_path)

    async def listing(_backend=None):
        return world.live

    def view_window(wid):
        return world.states.get(wid)

    def scan(cwd, provider_name="claude"):
        return [
            SimpleNamespace(session_id=sid, provider_name="claude")
            for sid in world.claude_sessions.get(cwd, [])
        ]

    async def rename(**_kwargs):
        if world.rename_error is not None:
            raise world.rename_error
        return True

    client = MagicMock()
    client.edit_forum_topic = AsyncMock(side_effect=rename)
    mux = world.mux

    with (
        patch(f"{_AR}.thread_router", world.router),
        patch(f"{_RC}.thread_router", world.router),
        patch(f"{_AR}.multiplexer", mux),
        patch(f"{_AR}.list_windows_for_reconciliation", listing),
        patch(f"{_AR}.window_query.view_window", view_window),
        patch(f"{_AR}.scan_sessions_for_cwd", scan),
        patch(f"{_AR}._pause", world.pause),
        patch(f"{_RC}.session_manager"),
        patch(f"{_RC}.PTBTelegramClient", MagicMock(return_value=client)),
        patch(f"{_RC}._stale_recovery_offer", world.stale),
        patch(
            f"{_RC}.session_map_sync.wait_for_session_map_entry",
            AsyncMock(return_value=True),
        ),
    ):
        yield world
    for wid in ("@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9"):
        topic_orchestration.clear_pending_creation(wid)


def _skips(logs: Sequence[Mapping[str, Any]]) -> list[str]:
    return [log["reason"] for log in logs if log["event"] == "auto_restore_skip"]


class TestGates:
    async def test_flag_off_touches_nothing(self, env, monkeypatch) -> None:
        monkeypatch.setattr(config, "auto_restore_on_start", False)
        env.dead_topic(1, "@1")
        env.next_ids = ["@7"]

        assert await env.start() is None

        env.adapter.create.assert_not_called()
        env.mux.server_boot_id.assert_not_called()
        assert not (env.tmp / "auto_restore.json").exists()

    async def test_autodelete_on_warns_and_skips_everything(
        self, env, monkeypatch
    ) -> None:
        monkeypatch.setattr(config, "autodelete_dead_topics", True)
        env.dead_topic(1, "@1")

        with capture_logs() as logs:
            assert await env.start() is None

        env.adapter.create.assert_not_called()
        warnings = [log for log in logs if log["log_level"] == "warning"]
        assert [w["reason"] for w in warnings] == ["autodelete_dead_topics_enabled"]
        assert not (env.tmp / "auto_restore.json").exists()

    async def test_unknown_boot_id_skips_the_feature(self, env) -> None:
        env.mux.server_boot_id = AsyncMock(return_value=None)
        env.dead_topic(1, "@1")

        with capture_logs() as logs:
            assert await env.start() is None

        env.adapter.create.assert_not_called()
        assert any(log["event"] == "boot_id_unavailable" for log in logs)

    async def test_unconfirmed_listing_restores_nothing(self, env) -> None:
        env.dead_topic(1, "@1")
        env.live = None

        with capture_logs() as logs:
            assert await env.start() is None

        env.adapter.create.assert_not_called()
        assert any(log["event"] == "auto_restore_listing_unconfirmed" for log in logs)

    async def test_a_planning_crash_never_stops_the_bot(self, env) -> None:
        env.dead_topic(1, "@1")
        env.mux.server_boot_id = AsyncMock(side_effect=RuntimeError("tmux gone"))

        with capture_logs() as logs:
            assert await env.start() is None

        assert any(log["event"] == "auto_restore_failed_to_start" for log in logs)


class TestHappyPath:
    async def test_claude_topic_resumes_its_own_session(self, env) -> None:
        env.dead_topic(1, "@1", session="sess-1", mode="yolo")
        env.next_ids = ["@7"]

        await env.start()

        ((data, kwargs),) = env.created
        assert data == {"provider": "claude", "directory": env.states["@7"].cwd}
        assert kwargs == {
            "approval_mode": "yolo",
            "resume_id": "sess-1",
            "continue_session": False,
        }
        assert env.bound(1) == "@7"
        env.stale.assert_not_called()
        assert env.ledger_status() == {1: "done"}

    async def test_codex_topic_resumes_when_its_rollout_exists(self, env) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-1")
        env.next_ids = ["@7"]

        await env.start()

        ((data, kwargs),) = env.created
        assert data["provider"] == "codex"
        assert kwargs["resume_id"] == "cdx-1"
        assert env.bound(1) == "@7"

    async def test_candidates_are_paused_between_not_before(self, env) -> None:
        for thread in (1, 2, 3):
            env.dead_topic(thread, f"@{thread}", session=f"s{thread}")
        env.next_ids = ["@7", "@8", "@9"]

        await env.start()

        assert len(env.created) == 3
        assert env.pause.await_count == 2

    async def test_the_plan_is_on_disk_before_the_first_create(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.dead_topic(2, "@2", session="s2")
        env.next_ids = ["@7", "@8"]
        seen: list[dict[int, str]] = []
        original = env.create

        async def spy(data, **kwargs):
            seen.append(env.ledger_status())
            return await original(data, **kwargs)

        env.adapter.create = AsyncMock(side_effect=spy)

        await env.start()

        assert seen[0] == {1: "attempting", 2: "pending"}
        row = next(r for r in env.ledger["entries"] if r["thread_id"] == 2)
        assert row == {
            "user_id": _USER,
            "chat_id": _CHAT,
            "thread_id": 2,
            "old_window_id": "@2",
            "cwd": env.states["@2"].cwd,
            "session_id": "s2",
            "provider_name": "claude",
            "approval_mode": "normal",
            "display": "proj2",
            "status": "done",
        }
        assert env.ledger["boot_id"] == "boot-1"


_SKIPS = {
    "chat_unknown": lambda e: e.dead_topic(1, "@1", chat=None),
    "no_session": lambda e: e.dead_topic(1, "@1", session=None),
    "cwd_missing": lambda e: e.dead_topic(1, "@1", cwd="/nonexistent/ccgram-test"),
    "session_not_found": lambda e: e.dead_topic(1, "@1", known_session=False),
    "provider_unsupported": lambda e: e.dead_topic(1, "@1", provider="gemini"),
}


class TestSkips:
    @pytest.mark.parametrize("reason", list(_SKIPS))
    async def test_each_reason_creates_nothing_and_is_logged(
        self, env, reason: str
    ) -> None:
        _SKIPS[reason](env)
        env.next_ids = ["@7"]

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == [reason]

    async def test_no_window_state_is_logged_as_no_session(self, env) -> None:
        env.dead_topic(1, "@1")
        del env.states["@1"]

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["no_session"]

    async def test_no_cwd_is_its_own_reason(self, env) -> None:
        env.dead_topic(1, "@1")
        env.states["@1"].cwd = ""

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["no_cwd"]

    async def test_codex_without_a_rollout_is_not_restored(self, env) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["session_not_found"]

    async def test_codex_rollout_of_another_session_does_not_count(self, env) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-10")
        env.codex_rollout("other-cdx-1")

        await env.start()

        env.adapter.create.assert_not_called()

    async def test_a_live_window_with_the_same_name_is_left_alone(self, env) -> None:
        env.dead_topic(1, "@1", name="proj1")
        assert env.live is not None
        env.live.append(WindowRef("@1", "proj1", env.states["@1"].cwd))

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["window_alive"]

    async def test_stale_id_held_by_another_window_is_never_resumed_into(
        self, env
    ) -> None:
        env.dead_topic(1, "@0", name="proj1")  # the bot's own `_ccgram` is @0
        env.next_ids = ["@7"]

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["stale_id_collision_suspected"]
        assert env.bound(1) == "@0"

    async def test_a_provisioning_claim_on_the_topic_blocks_it(self, env) -> None:
        env.dead_topic(1, "@1")
        env.router.begin_topic_provisioning(
            _USER, _CHAT, thread_id=1, kind="replacement", previous_target_id="@1"
        )

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["provisioning_claim"]

    async def test_a_claim_on_the_stale_target_blocks_it(self, env) -> None:
        env.dead_topic(1, "@1")
        env.router.begin_topic_provisioning(
            _USER, _CHAT, thread_id=99, kind="replacement", previous_target_id="@1"
        )

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["provisioning_claim"]

    async def test_a_pending_creation_blocks_it(self, env) -> None:
        env.dead_topic(1, "@1")
        topic_orchestration.register_pending_creation("@1")

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["provisioning_claim"]

    async def test_a_session_already_running_elsewhere_is_not_started_twice(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        assert env.live is not None
        env.live.append(WindowRef("@8", "other", env.states["@1"].cwd))
        env.states["@8"] = SimpleNamespace(
            session_id="sess-1",
            cwd="/x",
            provider_name="claude",
            approval_mode="normal",
        )

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["session_already_live"]

    async def test_one_session_bound_to_two_dead_topics_is_restored_once(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="same", name="proj")
        env.dead_topic(2, "@2", session="same", name="proj")
        env.next_ids = ["@7", "@8"]

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 1
        assert _skips(logs) == ["duplicate_session"]


class TestIdCollisions:
    async def test_a_new_window_taking_another_topics_old_id_does_not_lose_it(
        self, env
    ) -> None:
        # tmux ids restart: A's new window is @2, which B was bound to.
        env.dead_topic(1, "@1", session="sess-A", name="alpha", mode="yolo")
        env.dead_topic(2, "@2", session="sess-B", name="beta", mode="normal")
        env.next_ids = ["@2", "@3"]

        await env.start()

        (_, a_kwargs), (b_data, b_kwargs) = env.created
        assert a_kwargs["resume_id"] == "sess-A"
        assert b_kwargs == {
            "approval_mode": "normal",
            "resume_id": "sess-B",
            "continue_session": False,
        }
        assert b_data["directory"].endswith("/beta")
        assert env.bound(1) == "@2"
        assert env.bound(2) == "@3"
        assert env.ledger_status() == {1: "done", 2: "done"}

    async def test_plan_members_are_not_rechecked_for_liveness(self, env) -> None:
        env.dead_topic(1, "@1", session="sess-A")
        env.dead_topic(2, "@2", session="sess-B")
        env.next_ids = ["@2", "@3"]

        await env.start()

        # B's old id @2 is a live window by the time B runs; the plan proved
        # it dead once, and only our own create changed that.
        assert len(env.created) == 2
        env.stale.assert_not_called()


class TestRateLimit:
    async def test_the_first_retry_after_stops_the_run(self, env) -> None:
        for thread in (1, 2, 3):
            env.dead_topic(thread, f"@{thread}", session=f"s{thread}")
        env.next_ids = ["@7", "@8", "@9"]
        env.rename_error = RetryAfter(30)

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 1
        assert env.ledger_status() == {1: "done", 2: "pending", 3: "pending"}
        assert any(log["event"] == "auto_restore_rate_limited" for log in logs)

    async def test_the_next_start_continues_where_it_stopped(self, env) -> None:
        for thread in (1, 2, 3):
            env.dead_topic(thread, f"@{thread}", session=f"s{thread}")
        env.next_ids = ["@7", "@8", "@9"]
        env.rename_error = RetryAfter(30)
        await env.start()

        env.rename_error = None
        await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["s1", "s2", "s3"]
        assert env.ledger_status() == {1: "done", 2: "done", 3: "done"}


class TestFailures:
    async def test_a_failing_create_is_recorded_and_the_next_topic_still_runs(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.dead_topic(2, "@2", session="s2")
        env.create_error = ProtocolError(503, "backend_unavailable", "down")

        await env.start()

        assert env.ledger_status() == {1: "failed", 2: "failed"}
        assert env.bound(1) == "@1"
        assert env.bound(2) == "@2"

    async def test_cancellation_propagates_and_leaves_the_attempt_recorded(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.create_error = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await env.start()

        assert env.ledger_status() == {1: "attempting"}


class TestLedger:
    async def test_second_startup_creates_nothing(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.next_ids = ["@7", "@8"]
        await env.start()
        assert len(env.created) == 1

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 1
        assert _skips(logs) == ["window_alive"]
        assert env.ledger_status() == {1: "done"}

    async def test_a_window_killed_later_in_the_same_boot_is_not_restored_again(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.next_ids = ["@7", "@8"]
        await env.start()
        assert env.bound(1) == "@7"
        await env.start()  # a restart while the window is alive
        assert env.live is not None
        env.live[:] = [w for w in env.live if w.window_id != "@7"]  # user kills it

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 1
        assert _skips(logs) == ["already_attempted"]
        assert env.ledger_status() == {1: "done"}

    async def test_a_new_boot_replaces_the_done_rows_of_the_old_one(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.next_ids = ["@7", "@8"]
        await env.start()
        assert env.live is not None
        env.live[:] = [w for w in env.live if w.window_id != "@7"]
        env.boot = "boot-2"

        await env.start()

        assert len(env.created) == 2
        assert env.ledger["boot_id"] == "boot-2"
        assert env.ledger_status() == {1: "done"}

    async def test_a_failed_topic_is_not_retried_in_the_same_boot(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.next_ids = ["@7", "@8"]
        env.create_error = ProtocolError(400, "invalid_request", "bad")
        await env.start()
        env.create_error = None

        with capture_logs() as logs:
            await env.start()

        assert env.created == []
        assert _skips(logs) == ["already_attempted"]

    async def test_a_new_boot_allows_another_attempt(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.next_ids = ["@7"]
        env.create_error = ProtocolError(400, "invalid_request", "bad")
        await env.start()
        env.create_error = None
        env.boot = "boot-2"

        await env.start()

        assert len(env.created) == 1
        assert env.ledger["boot_id"] == "boot-2"
        assert env.bound(1) == "@7"

    async def test_a_crash_after_a_commits_restores_the_evicted_topic_from_the_ledger(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-A", name="alpha")
        env.dead_topic(2, "@2", session="sess-B", name="beta", mode="yolo")
        env.next_ids = ["@2", "@3"]
        env.pause.side_effect = asyncio.CancelledError  # the "crash" after A

        with pytest.raises(asyncio.CancelledError):
            await env.start()
        assert env.bound(1) == "@2"
        assert env.bound(2) is None  # A's commit evicted B
        assert env.ledger_status() == {1: "done", 2: "pending"}
        env.pause.side_effect = None

        await env.start()

        assert len(env.created) == 2
        data, kwargs = env.created[1]
        assert kwargs["resume_id"] == "sess-B"
        assert kwargs["approval_mode"] == "yolo"
        assert data["directory"].endswith("/beta")
        assert env.bound(2) == "@3"

    async def test_a_pending_row_is_dropped_once_its_topic_has_a_live_window(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-A")
        env.next_ids = ["@7"]
        await env.start()
        raw = env.ledger  # pretend the "done" write was lost
        raw["entries"][0]["status"] = "pending"
        (env.tmp / "auto_restore.json").write_text(json.dumps(raw))

        await env.start()

        assert len(env.created) == 1
        assert env.ledger["entries"] == []

    async def test_a_ledger_row_whose_session_vanished_is_not_restored(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-A", name="alpha")
        env.dead_topic(2, "@2", session="sess-B", name="beta")
        env.next_ids = ["@2", "@3"]
        env.pause.side_effect = asyncio.CancelledError
        with pytest.raises(asyncio.CancelledError):
            await env.start()
        env.pause.side_effect = None
        env.claude_sessions.clear()

        await env.start()

        assert len(env.created) == 1

    async def test_a_corrupt_ledger_starts_empty(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.next_ids = ["@7"]
        (env.tmp / "auto_restore.json").write_text("{not json")

        await env.start()

        assert len(env.created) == 1


class TestLaunchCommand:
    async def test_the_configured_provider_command_reaches_the_launch(
        self, env, monkeypatch
    ) -> None:
        monkeypatch.setenv("CCGRAM_CLAUDE_COMMAND", "my-claude --flag")
        env.dead_topic(1, "@1", session=_UUID)
        launches: list[dict] = []

        class FakeMux:
            capabilities = SimpleNamespace(native_topic_targets=False)

            async def create_window(self, path, **kwargs):
                launches.append({"path": path, **kwargs})
                return True, "ok", "proj1", "@7"

        backend = MultiplexerSessionBackend(FakeMux(), str(env.tmp))  # type: ignore[arg-type]  # FakeMux has only create_window, the one call used

        async def create_via_backend(data, **kwargs):
            options = CreationOptions(
                kwargs["approval_mode"],
                kwargs["resume_id"],
                kwargs["continue_session"],
            )
            with patch.object(backend, "_created_target", AsyncMock()) as created:
                created.return_value = SimpleNamespace(id="@7")
                target = await backend.create(data, trusted_options=options)
            return SimpleNamespace(target_id=target.id, title="proj1")

        env.adapter.create = AsyncMock(side_effect=create_via_backend)

        await env.start()

        (launch,) = launches
        assert launch["launch_command"] == "my-claude --flag"
        assert "--resume" in launch["agent_args"]
        assert _UUID in launch["agent_args"]
