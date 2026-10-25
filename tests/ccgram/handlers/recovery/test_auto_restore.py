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
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from structlog.testing import capture_logs
from telegram.error import RetryAfter

from ccgram.config import config
from ccgram.handlers.recovery import auto_restore
from ccgram.handlers.recovery.recovery_ui import HeadlessUi
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
        self.transcripts: list[Path] = []
        self.reserved: list[list[str]] = []
        self.reserve_ok = True
        self.next_id = 1  # a fresh tmux server hands out @1 next
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
        self.reserve_call = AsyncMock(side_effect=self.reserve)
        self.adapter = MagicMock()
        self.adapter.create = AsyncMock(side_effect=self.create)
        self.adapter.close = AsyncMock(return_value=(True, "Closed"))
        self.lsof_result: tuple[int, bytes] | BaseException = (1, b"")
        self.lsof_hangs = False
        self.lsof_calls: list[tuple[Any, ...]] = []
        self.lsof_exec = AsyncMock(side_effect=self.fake_lsof)
        self.ps_start: str | BaseException | None = "T0"
        self.ps_run = MagicMock(side_effect=self.fake_ps)

    def fake_ps(self, *_args: Any, **_kwargs: Any) -> SimpleNamespace:
        if isinstance(self.ps_start, BaseException):
            raise self.ps_start
        out = "" if self.ps_start is None else f"{self.ps_start}\n"
        return SimpleNamespace(returncode=1 if self.ps_start is None else 0, stdout=out)

    async def fake_lsof(self, *argv: str, **_kwargs: Any) -> MagicMock:
        self.lsof_calls.append(argv)
        if isinstance(self.lsof_result, BaseException):
            raise self.lsof_result
        code, out = self.lsof_result
        proc = MagicMock(returncode=None)
        proc.wait = AsyncMock()

        async def communicate():
            if self.lsof_hangs:
                await asyncio.sleep(3600)
            proc.returncode = code
            return out, b""

        proc.communicate = communicate
        return proc

    def claude_registry(self, session: str, pid: int, start: str | None = "T0") -> None:
        folder = self.tmp / "claude" / "sessions"
        folder.mkdir(parents=True, exist_ok=True)
        data: dict[str, Any] = {"pid": pid, "sessionId": session}
        if start is not None:
            data["procStart"] = start
        (folder / f"{pid}.json").write_text(json.dumps(data))

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
            self.transcript(cwd, session)

    def transcript(self, cwd: str, session: str, head: int = 0) -> None:
        folder = self.tmp / "claude" / "projects" / re.sub(r"[^A-Za-z0-9]", "-", cwd)
        folder.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps({"type": "api-request-blob"})] * head
        (folder / f"{session}.jsonl").write_text(
            "\n".join([*lines, json.dumps({"cwd": cwd})]) + "\n"
        )
        self.transcripts.append(folder / f"{session}.jsonl")

    def forget_sessions(self) -> None:
        for path in self.transcripts:
            path.unlink()
        self.transcripts.clear()

    async def reserve(self, window_ids: Iterable[str]) -> bool:
        ids = list(window_ids)
        self.reserved.append(ids)
        if self.reserve_ok:
            highest = max((int(wid[1:]) for wid in ids), default=0)
            self.next_id = max(self.next_id, highest + 1)
        return self.reserve_ok

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
        wid = f"@{self.next_id}"
        self.next_id += 1
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
    monkeypatch.setattr(
        config, "claude_projects_path", tmp_path / "claude" / "projects"
    )
    monkeypatch.setattr(config, "auto_restore_on_start", True)
    monkeypatch.setattr(config, "autodelete_dead_topics", False)
    world = Env(tmp_path)

    async def listing(_backend=None):
        return world.live

    def view_window(wid):
        return world.states.get(wid)

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
        patch(f"{_AR}.reserve_window_ids_past", world.reserve_call),
        patch(f"{_AR}.list_windows_for_reconciliation", listing),
        patch(f"{_AR}.window_query.view_window", view_window),
        patch(f"{_AR}._pause", world.pause),
        patch(f"{_AR}.asyncio.create_subprocess_exec", world.lsof_exec),
        patch(f"{_AR}.subprocess.run", world.ps_run),
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

        await env.start()

        ((data, kwargs),) = env.created
        assert data == {"provider": "claude", "directory": env.states["@2"].cwd}
        assert kwargs == {
            "approval_mode": "yolo",
            "resume_id": "sess-1",
            "continue_session": False,
        }
        assert env.bound(1) == "@2"
        env.stale.assert_not_called()
        assert env.ledger_status() == {1: "done"}

    async def test_an_old_session_among_many_newer_ones_is_still_found(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="old-one")
        for n in range(8):  # the resume picker lists only the newest few
            env.transcript(env.states["@1"].cwd, f"newer-{n}")

        await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["old-one"]

    async def test_a_cwd_with_dots_finds_its_transcript(self, env) -> None:
        cwd = env.tmp / "repo" / ".worktrees" / "v1.2"
        cwd.mkdir(parents=True)
        env.dead_topic(1, "@1", session="dotted", cwd=str(cwd))

        await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["dotted"]

    async def test_a_cwd_after_many_leading_lines_is_still_found(self, env) -> None:
        env.dead_topic(1, "@1", session="long", known_session=False)
        env.transcript(env.states["@1"].cwd, "long", head=25)

        await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["long"]

    async def test_an_unreadable_transcript_never_blocks_another_topic(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="bad", name="alpha", known_session=False)
        (env.tmp / "claude" / "projects" / "x" / "bad.jsonl").mkdir(parents=True)
        env.dead_topic(2, "@2", session="good", name="beta")

        await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["good"]

    async def test_an_undecodable_transcript_never_blocks_another_topic(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="bad", name="alpha", known_session=False)
        bad = env.tmp / "claude" / "projects" / "x" / "bad.jsonl"
        bad.parent.mkdir(parents=True)
        bad.write_bytes(b"\xff\xfe\n")
        env.dead_topic(2, "@2", session="good", name="beta")

        await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["good"]

    @pytest.mark.parametrize("topic_cwd_is_link", [True, False])
    async def test_a_symlinked_cwd_finds_the_transcript(
        self, env, topic_cwd_is_link: bool
    ) -> None:
        real = Path(env.project("real"))
        link = env.tmp / "link"
        link.symlink_to(real)
        topic_cwd, transcript_cwd = (link, real) if topic_cwd_is_link else (real, link)
        env.dead_topic(
            1, "@1", session="linked", cwd=str(topic_cwd), known_session=False
        )
        env.transcript(str(transcript_cwd), "linked")

        await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["linked"]

    async def test_a_transcript_of_another_cwd_does_not_vouch_for_the_session(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="mine", known_session=False)
        env.transcript(env.project("elsewhere"), "mine")

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["session_not_found"]

    async def test_other_transcripts_in_the_folder_do_not_vouch_for_the_session(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="gone", known_session=False)
        env.transcript(env.states["@1"].cwd, "someone-else")

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["session_not_found"]

    async def test_codex_topic_resumes_when_its_rollout_exists(self, env) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-1")

        await env.start()

        ((data, kwargs),) = env.created
        assert data["provider"] == "codex"
        assert kwargs["resume_id"] == "cdx-1"
        assert env.bound(1) == "@2"

    async def test_candidates_are_paused_between_not_before(self, env) -> None:
        for thread in (1, 2, 3):
            env.dead_topic(thread, f"@{thread}", session=f"s{thread}")

        await env.start()

        assert len(env.created) == 3
        assert env.pause.await_count == 2

    async def test_the_plan_is_on_disk_before_the_first_create(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.dead_topic(2, "@2", session="s2")
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

    async def test_claude_session_live_in_another_tmux_is_not_resumed(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid())

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["session_live"]

    async def test_claude_registry_entry_of_a_dead_pid_does_not_block(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        gone = subprocess.Popen([sys.executable, "-c", "pass"])
        gone.wait()
        env.claude_registry("sess-1", gone.pid)
        (env.tmp / "claude" / "sessions" / "junk.json").write_text("{not json")
        (env.tmp / "claude" / "sessions" / "x.key").write_text("never-read")

        await env.start()

        assert len(env.created) == 1

    async def test_a_reused_pid_with_another_start_time_does_not_block(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid(), start="T-before-reboot")

        await env.start()

        assert len(env.created) == 1
        assert env.ps_run.call_args.kwargs["env"]["TZ"] == "UTC"

    @pytest.mark.parametrize(
        ("registered", "ps_start"),
        [
            ("T0", None),
            ("T0", subprocess.TimeoutExpired("ps", 5)),
            ("T0", FileNotFoundError("ps")),
            (None, "T0"),
        ],
    )
    async def test_claude_start_time_unconfirmed_is_never_not_live(
        self, env, registered, ps_start
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid(), start=registered)
        env.ps_start = ps_start

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["liveness_unconfirmed"]

    async def test_codex_rollout_open_by_a_process_is_live(self, env) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-1")
        env.lsof_result = (0, b"4242\n")

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["session_live"]
        assert env.lsof_calls[0][:3] == ("lsof", "-t", "--")

    @pytest.mark.parametrize(
        "outcome",
        [(2, b""), (0, b""), (1, b"4242\n"), FileNotFoundError("lsof"), "timeout"],
    )
    async def test_codex_lsof_failure_is_unconfirmed_never_not_live(
        self, env, monkeypatch, outcome
    ) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-1")
        if outcome == "timeout":
            env.lsof_hangs = True
            monkeypatch.setattr(auto_restore, "_LSOF_TIMEOUT_S", 0.01)
        else:
            env.lsof_result = outcome

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["liveness_unconfirmed"]

    async def test_codex_rollout_not_open_is_resumed(self, env) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-1")
        env.lsof_result = (1, b"")

        await env.start()

        assert len(env.created) == 1

    async def test_session_started_elsewhere_after_planning_is_caught_at_restore(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        entry = auto_restore.RestoreEntry(
            user_id=_USER,
            chat_id=_CHAT,
            thread_id=1,
            old_window_id="@1",
            cwd=env.states["@1"].cwd,
            session_id="sess-1",
            provider_name="claude",
            approval_mode="normal",
            display="proj1",
        )
        env.claude_registry("sess-1", os.getpid())

        assert await auto_restore._skip_reason_now(entry) == "session_live"

    async def test_one_session_bound_to_two_dead_topics_is_restored_once(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="same", name="proj")
        env.dead_topic(2, "@2", session="same", name="proj")

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 1
        assert _skips(logs) == ["duplicate_session"]


class TestIdReservation:
    async def test_ids_are_reserved_past_every_binding_before_the_first_create(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-A", name="alpha")
        env.dead_topic(2, "@2", session="sess-B", name="beta")
        env.dead_topic(3, "@5", session="sess-C", name="gamma", known_session=False)

        await env.start()

        assert [sorted(ids) for ids in env.reserved] == [["@1", "@2", "@5"]]
        assert [env.bound(t) for t in (1, 2, 3)] == ["@6", "@7", "@5"]

    async def test_a_failed_resume_never_retires_another_topic_for_deletion(
        self, env
    ) -> None:
        env.dead_topic(1, "@2", session="sess-A", name="alpha")
        env.dead_topic(2, "@1", session="sess-B", name="beta")
        original = env.create
        calls: list[str] = []

        async def create(data, **kwargs):
            calls.append(kwargs["resume_id"])
            if len(calls) == 2:
                raise ProtocolError(400, "invalid_request", "directory does not exist")
            return await original(data, **kwargs)

        env.adapter.create = AsyncMock(side_effect=create)

        await env.start()

        assert list(env.router.iter_retired_topics()) == []
        assert env.bound(2) == "@1"

    async def test_an_unavailable_reservation_creates_nothing(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.reserve_ok = False

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert not (env.tmp / "auto_restore.json").exists()
        assert [
            log["reason"] for log in logs if log["event"] == "auto_restore_skipped"
        ] == ["id_reservation_unavailable"]

    async def test_ids_are_reserved_before_start_returns_and_before_any_create(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="s1")
        done = False

        async def reserve(window_ids):
            nonlocal done
            await asyncio.sleep(0)
            done = True
            return True

        env.reserve_call.side_effect = reserve

        task = await auto_restore.start_auto_restore(MagicMock(), lambda: env.adapter)

        assert task is not None
        assert done  # post_init awaits start: polling cannot begin before this
        env.adapter.create.assert_not_called()
        await task
        assert len(env.created) == 1

    async def test_an_empty_plan_still_reserves_for_the_dead_binding(self, env) -> None:
        env.dead_topic(1, "@1", known_session=False)

        await env.start()

        assert env.reserved == [["@1"]]
        env.adapter.create.assert_not_called()


class TestStartupReservation:
    @pytest.fixture(autouse=True)
    def flag_off(self, env, monkeypatch) -> None:
        monkeypatch.setattr(config, "auto_restore_on_start", False)

    async def test_flag_off_reserves_before_start_returns(self, env) -> None:
        env.dead_topic(1, "@1")
        env.dead_topic(2, "@4")
        done = False

        async def reserve(window_ids):
            nonlocal done
            await asyncio.sleep(0)
            done = True
            return True

        env.reserve_call.side_effect = reserve

        assert (
            await auto_restore.start_auto_restore(MagicMock(), lambda: env.adapter)
            is None
        )

        assert done
        env.adapter.create.assert_not_called()

    async def test_autodelete_on_still_reserves(self, env, monkeypatch) -> None:
        monkeypatch.setattr(config, "autodelete_dead_topics", True)
        env.dead_topic(1, "@3")

        await env.start()

        assert env.reserved == [["@3"]]

    async def test_every_binding_live_reserves_nothing(self, env) -> None:
        env.dead_topic(1, "@1", name="proj1")
        assert env.live is not None
        env.live.append(WindowRef("@1", "proj1", env.states["@1"].cwd))

        await env.start()

        env.reserve_call.assert_not_called()

    async def test_no_bindings_reserves_nothing(self, env) -> None:
        await env.start()

        env.reserve_call.assert_not_called()

    async def test_a_manual_replacement_gets_an_id_above_every_stale_one(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-A", name="alpha")
        env.dead_topic(2, "@5", session="sess-B", name="beta")
        await env.start()
        entry = auto_restore.RestoreEntry(
            user_id=_USER,
            chat_id=_CHAT,
            thread_id=1,
            old_window_id="@1",
            cwd=env.states["@1"].cwd,
            session_id="sess-A",
            provider_name="claude",
            approval_mode="normal",
            display="alpha",
        )

        restored = await auto_restore.create_and_bind_window(
            auto_restore._request(entry), HeadlessUi(), MagicMock(), lambda: env.adapter
        )

        assert restored
        assert env.bound(1) == "@6"
        assert env.bound(2) == "@5"


class TestChangesDuringTheRun:
    @pytest.fixture()
    def second_topic(self, env):
        env.dead_topic(1, "@1", session="sess-A", name="alpha")
        env.dead_topic(2, "@2", session="sess-B", name="beta")
        return env.states["@2"].cwd

    def start_session_b(self, env, cwd: str, *, bind: bool):
        async def meanwhile(_seconds):
            assert env.live is not None
            env.live.append(WindowRef("@9", "beta", cwd))
            env.states["@9"] = SimpleNamespace(
                session_id="sess-B",
                cwd=cwd,
                provider_name="claude",
                approval_mode="normal",
            )
            if bind:
                env.router.bind_thread(
                    _USER, 2, "@9", window_name="beta", chat_id=_CHAT
                )

        env.pause.side_effect = meanwhile

    async def test_a_topic_the_user_resumed_by_hand_is_not_resumed_again(
        self, env, second_topic
    ) -> None:
        self.start_session_b(env, second_topic, bind=True)

        with capture_logs() as logs:
            await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["sess-A"]
        assert env.bound(2) == "@9"
        assert _skips(logs) == ["rebound"]

    async def test_a_session_started_meanwhile_is_not_started_twice(
        self, env, second_topic
    ) -> None:
        self.start_session_b(env, second_topic, bind=False)

        with capture_logs() as logs:
            await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["sess-A"]
        assert _skips(logs) == ["session_live"]

    async def test_an_old_id_taken_meanwhile_is_never_resumed_into(
        self, env, second_topic
    ) -> None:
        async def meanwhile(_seconds):
            assert env.live is not None
            env.live.append(WindowRef("@2", "unrelated", second_topic))

        env.pause.side_effect = meanwhile

        with capture_logs() as logs:
            await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == ["sess-A"]
        assert _skips(logs) == ["stale_id_collision_suspected"]


class TestRateLimit:
    @pytest.fixture()
    def five_topics(self, env):
        for thread in range(1, 6):
            env.dead_topic(thread, f"@{thread}", session=f"s{thread}")
        env.rename_error = RetryAfter(30)

    async def test_a_retry_after_is_waited_out_and_the_run_goes_on(self, env) -> None:
        for thread in (1, 2):
            env.dead_topic(thread, f"@{thread}", session=f"s{thread}")
        env.rename_error = RetryAfter(30)

        await env.start()

        assert len(env.created) == 2
        assert [c.args[0] for c in env.pause.await_args_list] == [
            30.0,
            auto_restore.GAP_S,
            30.0,
        ]

    async def test_a_long_retry_after_wait_is_capped(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.rename_error = RetryAfter(3600)

        await env.start()

        env.pause.assert_awaited_once_with(auto_restore.MAX_RATE_LIMIT_WAIT_S)

    async def test_the_run_gives_up_after_three_waits(self, env, five_topics) -> None:
        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 4
        assert env.ledger_status()[5] == "pending"
        assert any(
            log["event"] == "auto_restore_rate_limited_giving_up" for log in logs
        )

    async def test_the_next_start_continues_where_it_stopped(
        self, env, five_topics
    ) -> None:
        await env.start()

        env.rename_error = None
        await env.start()

        assert [kw["resume_id"] for _, kw in env.created] == [
            f"s{n}" for n in range(1, 6)
        ]
        assert set(env.ledger_status().values()) == {"done"}


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
        await env.start()
        assert env.bound(1) == "@2"
        await env.start()  # a restart while the window is alive
        assert env.live is not None
        env.live[:] = [w for w in env.live if w.window_id != "@2"]  # user kills it

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 1
        assert _skips(logs) == ["already_attempted"]
        assert env.ledger_status() == {1: "done"}

    async def test_a_new_boot_replaces_the_done_rows_of_the_old_one(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        await env.start()
        assert env.live is not None
        env.live[:] = [w for w in env.live if w.window_id != "@2"]
        env.boot = "boot-2"

        await env.start()

        assert len(env.created) == 2
        assert env.ledger["boot_id"] == "boot-2"
        assert env.ledger_status() == {1: "done"}

    async def test_a_failed_topic_is_not_retried_in_the_same_boot(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.create_error = ProtocolError(400, "invalid_request", "bad")
        await env.start()
        env.create_error = None

        with capture_logs() as logs:
            await env.start()

        assert env.created == []
        assert _skips(logs) == ["already_attempted"]

    async def test_a_new_boot_allows_another_attempt(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.create_error = ProtocolError(400, "invalid_request", "bad")
        await env.start()
        env.create_error = None
        env.boot = "boot-2"

        await env.start()

        assert len(env.created) == 1
        assert env.ledger["boot_id"] == "boot-2"
        assert env.bound(1) == "@2"

    async def test_a_crash_between_topics_leaves_the_rest_for_the_next_start(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-A", name="alpha")
        env.dead_topic(2, "@2", session="sess-B", name="beta", mode="yolo")
        env.pause.side_effect = asyncio.CancelledError  # the "crash" after A

        with pytest.raises(asyncio.CancelledError):
            await env.start()
        assert env.ledger_status() == {1: "done", 2: "pending"}
        env.pause.side_effect = None

        await env.start()

        assert len(env.created) == 2
        data, kwargs = env.created[1]
        assert kwargs["resume_id"] == "sess-B"
        assert kwargs["approval_mode"] == "yolo"
        assert data["directory"].endswith("/beta")
        assert env.bound(2) == "@4"

    async def test_a_ledger_row_whose_session_vanished_is_not_restored(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-A", name="alpha")
        env.dead_topic(2, "@2", session="sess-B", name="beta")
        env.pause.side_effect = asyncio.CancelledError
        with pytest.raises(asyncio.CancelledError):
            await env.start()
        env.pause.side_effect = None
        env.forget_sessions()

        await env.start()

        assert len(env.created) == 1

    def write_ledger(self, env, **fields) -> None:
        body = {"version": 1, "boot_id": "boot-1", "entries": []} | fields
        (env.tmp / "auto_restore.json").write_text(json.dumps(body))

    async def test_the_ledger_file_carries_a_version(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")

        await env.start()

        assert env.ledger["version"] == 1

    async def test_an_unknown_ledger_version_is_discarded_with_a_warning(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="s1")
        self.write_ledger(env, version=2)

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 1
        assert any(log["event"] == "auto_restore_ledger_unreadable" for log in logs)

    async def test_a_row_that_is_not_an_object_never_breaks_startup(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        self.write_ledger(env, entries=["junk"])

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 1
        assert any(log["event"] == "auto_restore_ledger_unreadable" for log in logs)

    async def test_a_row_with_an_unknown_status_counts_as_attempted(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
        env.create_error = ProtocolError(400, "invalid_request", "bad")
        await env.start()
        env.create_error = None
        raw = env.ledger
        raw["entries"][0]["status"] = "bogus"
        (env.tmp / "auto_restore.json").write_text(json.dumps(raw))

        with capture_logs() as logs:
            await env.start()

        assert env.created == []
        assert _skips(logs) == ["already_attempted"]

    async def test_a_corrupt_ledger_starts_empty(self, env) -> None:
        env.dead_topic(1, "@1", session="s1")
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
