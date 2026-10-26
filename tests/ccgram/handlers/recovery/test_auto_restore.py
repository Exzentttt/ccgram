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
from telegram.error import BadRequest, NetworkError, RetryAfter

from ccgram.config import config
from ccgram.handlers.recovery import auto_restore
from ccgram.handlers.recovery.recovery_ui import HeadlessUi
from ccgram.handlers.recovery.restore_notices import adopt_text, skip_text
from ccgram.handlers.topics import topic_orchestration
from ccgram.multiplexer.base import WindowRef
from ccgram.session_protocol.backend import CreationOptions, MultiplexerSessionBackend
from ccgram.session_protocol.models import ProtocolError
from ccgram.thread_router import ThreadRouter

_AR = "ccgram.handlers.recovery.auto_restore"
_RC = "ccgram.handlers.recovery.recovery_banner"
_RN = "ccgram.handlers.recovery.restore_notices"
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
        self.lsof_script: list[tuple[int, bytes]] = []
        self.lsof_procs: list[MagicMock] = []
        self.can_reserve = MagicMock(return_value=True)
        self.server_view = MagicMock(return_value=None)
        self.lsof_hangs = False
        self.lsof_calls: list[tuple[Any, ...]] = []
        self.lsof_exec = AsyncMock(side_effect=self.fake_lsof)
        self.ps_start: str | BaseException | None = "T0"
        self.ps_code: int | None = None
        self.ps_run = MagicMock(side_effect=self.fake_ps)
        self.sent: list[dict[str, Any]] = []
        self.send_errors: list[BaseException | None] = []

    async def fake_send(self, **kwargs: Any) -> SimpleNamespace:
        if self.send_errors and (error := self.send_errors.pop(0)) is not None:
            raise error
        self.sent.append(kwargs)
        return SimpleNamespace(message_id=len(self.sent))

    def notices(self) -> list[tuple[int, str]]:
        """(topic thread, text) of every message sent, in order."""
        return [(kw["message_thread_id"], kw["text"]) for kw in self.sent]

    def fake_ps(self, argv: list[str], env: dict[str, str]) -> SimpleNamespace:
        if isinstance(self.ps_start, BaseException):
            raise self.ps_start
        out = "" if self.ps_start is None else f"{self.ps_start}\n"
        code = self.ps_code if self.ps_code is not None else int(self.ps_start is None)
        return SimpleNamespace(returncode=code, stdout=out)

    async def fake_lsof(self, path: Path) -> MagicMock:
        argv = (path,)
        self.lsof_calls.append(argv)
        if isinstance(self.lsof_result, BaseException):
            raise self.lsof_result
        code, out = self.lsof_script.pop(0) if self.lsof_script else self.lsof_result
        proc = MagicMock(returncode=None)
        proc.wait = AsyncMock()
        self.lsof_procs.append(proc)

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

    def codex_rollout(self, session: str, day: str = "26") -> None:
        day_dir = Path.home() / ".codex" / "sessions" / "2026" / "10" / day
        day_dir.mkdir(parents=True, exist_ok=True)
        name = f"rollout-2026-10-{day}T10-00-00-{session}.jsonl"
        (day_dir / name).write_text("{}\n")

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
    monkeypatch.setattr(config, "claude_config_dir", tmp_path / "claude")
    monkeypatch.setattr(config, "auto_restore_on_start", True)
    monkeypatch.setattr(config, "autodelete_dead_topics", False)
    monkeypatch.setattr(auto_restore, "_adopt_misses", {})
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
    sender = MagicMock()
    sender.send_message = AsyncMock(side_effect=world.fake_send)

    with (
        patch(f"{_RN}.PTBTelegramClient", MagicMock(return_value=sender)),
        patch(f"{_AR}.thread_router", world.router),
        patch(f"{_RC}.thread_router", world.router),
        patch(f"{_AR}.multiplexer", mux),
        patch(f"{_AR}.reserve_window_ids_past", world.reserve_call),
        patch(f"{_AR}.list_windows_for_reconciliation", listing),
        patch(f"{_AR}.window_query.view_window", view_window),
        patch(f"{_AR}._pause", world.pause),
        patch(f"{_AR}._spawn_lsof", world.lsof_exec),
        patch(f"{_AR}._run_ps", world.ps_run),
        patch(f"{_AR}.can_reserve_window_ids", world.can_reserve),
        patch(f"{_AR}.server_view", world.server_view),
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
        assert env.ledger["entries"] == []  # refused at planning, not at create

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

    @pytest.mark.parametrize("pid", [0, -1, 2**70])
    async def test_a_registry_pid_outside_the_pid_range_never_counts(
        self, env, pid: int
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", pid)

        await env.start()

        assert len(env.created) == 1

    def test_a_pid_we_may_not_signal_is_alive_one_that_is_gone_is_not(
        self, monkeypatch
    ) -> None:
        kill = MagicMock(side_effect=PermissionError)
        monkeypatch.setattr(auto_restore.os, "kill", kill)
        assert auto_restore._pid_alive(7) is True
        kill.side_effect = ProcessLookupError
        assert auto_restore._pid_alive(7) is False

    async def test_ps_that_fails_but_prints_a_time_is_not_trusted(self, env) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid())
        env.ps_code = 1

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["liveness_unconfirmed"]

    async def test_a_reused_pid_with_another_start_time_does_not_block(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid(), start="T-before-reboot")

        await env.start()

        assert len(env.created) == 1
        ps_env = env.ps_run.call_args.args[1]
        assert (ps_env["TZ"], ps_env["LC_ALL"]) == ("UTC", "C")

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
        assert env.lsof_calls[0][0].name.endswith("-cdx-1.jsonl")
        assert env.ledger["entries"] == []

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

    async def test_one_open_rollout_wins_over_an_unknown_one(self, env) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-1", day="25")
        env.codex_rollout("cdx-1", day="26")
        env.lsof_script = [(2, b""), (0, b"4242\n")]

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert _skips(logs) == ["session_live"]

    async def test_a_timed_out_lsof_is_killed_and_reaped(
        self, env, monkeypatch
    ) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-1")
        env.lsof_hangs = True
        monkeypatch.setattr(auto_restore, "_LSOF_TIMEOUT_S", 0.01)

        await env.start()

        proc = env.lsof_procs[0]
        proc.kill.assert_called_once_with()
        proc.wait.assert_awaited_once_with()

    async def test_an_attempted_entry_never_probes_for_liveness(self, env) -> None:
        env.dead_topic(1, "@1", session="cdx-1", provider="codex")
        env.codex_rollout("cdx-1")
        await env.start()
        assert len(env.lsof_calls) == 2  # plan + re-check of the first run
        env.live[:] = env.live[:1]  # the resumed window dies again, same boot
        env.router.bind_thread(_USER, 1, "@1", window_name="proj1", chat_id=_CHAT)
        env.lsof_calls.clear()

        with capture_logs() as logs:
            await env.start()

        assert _skips(logs) == ["already_attempted"]
        assert env.lsof_calls == []

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
        assert env.ledger["entries"] == []
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

    async def test_an_unconfirmed_listing_reserves(self, env) -> None:
        env.dead_topic(1, "@1")
        env.live = None

        await env.start()

        assert env.reserved == [["@1"]]

    async def test_a_backend_without_reservation_logs_info_not_a_warning(
        self, env
    ) -> None:
        env.dead_topic(1, "@1")
        env.can_reserve.return_value = False

        with capture_logs() as logs:
            await env.start()

        env.reserve_call.assert_not_called()
        events = [log for log in logs if log["event"] == "window_id_reservation"]
        assert [(e["log_level"], e["reason"]) for e in events] == [
            ("info", "not_applicable")
        ]
        assert not [log for log in logs if log["log_level"] == "warning"]

    async def test_a_backend_without_reservation_skips_a_planned_restore(
        self, env, monkeypatch
    ) -> None:
        monkeypatch.setattr(config, "auto_restore_on_start", True)
        env.dead_topic(1, "@1", session="s1")
        env.can_reserve.return_value = False

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert [
            log["reason"] for log in logs if log["event"] == "auto_restore_skipped"
        ] == ["id_reservation_unavailable"]

    async def test_a_live_listing_needs_no_backend_capability(self, env) -> None:
        env.dead_topic(1, "@1", name="proj1")
        assert env.live is not None
        env.live.append(WindowRef("@1", "proj1", env.states["@1"].cwd))
        env.can_reserve.return_value = False

        with capture_logs() as logs:
            await env.start()

        assert not [
            log
            for log in logs
            if log["event"] == "window_id_reservation"
            and log.get("reason") == "not_applicable"
        ]

    async def test_a_failing_reservation_is_logged_and_resumes_nothing(
        self, env, monkeypatch
    ) -> None:
        monkeypatch.setattr(config, "auto_restore_on_start", True)
        env.dead_topic(1, "@1", session="s1")
        env.reserve_call.side_effect = RuntimeError("tmux gone")

        with capture_logs() as logs:
            await env.start()

        failed = [log for log in logs if log["event"] == "window_id_reservation"]
        assert [(e["reserved"], e["error"]) for e in failed] == [(False, "tmux gone")]
        assert [
            log["reason"] for log in logs if log["event"] == "auto_restore_skipped"
        ] == ["id_reservation_unavailable"]
        env.adapter.create.assert_not_called()

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


class TestSubprocessSeams:
    async def test_lsof_gets_exactly_its_argv(self, tmp_path) -> None:
        spawn = AsyncMock()
        target = tmp_path / "rollout.jsonl"

        with patch.object(asyncio, "create_subprocess_exec", spawn):
            await auto_restore._spawn_lsof(target)

        assert spawn.call_args.args == ("lsof", "-t", "--", str(target))

    def test_ps_gets_exactly_its_argv_in_a_fixed_locale(self) -> None:
        done = subprocess.CompletedProcess([], 0, stdout="x\n", stderr="")

        with patch.object(subprocess, "run", return_value=done) as run:
            assert auto_restore._ps_start(4242) == "x"

        assert run.call_args.args == (["ps", "-o", "lstart=", "-p", "4242"],)
        env = run.call_args.kwargs["env"]
        assert (env["TZ"], env["LC_ALL"]) == ("UTC", "C")


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


class TestNoticeTexts:
    @pytest.mark.parametrize(
        "reason",
        [
            "rebound",
            "duplicate_session",
            "session_already_live",
            "already_attempted",
            "listing_unconfirmed",
            "boot_id_unavailable",
            "autodelete_dead_topics_enabled",
            "window_alive",
            "stale_id_collision_suspected",
            "provisioning_claim",
            "no_session",
            "no_cwd",
            "chat_unknown",
        ],
    )
    def test_internal_reasons_have_no_text(self, reason) -> None:
        assert skip_text(reason) is None

    @pytest.mark.parametrize(
        "reason",
        [
            "session_live",
            "cwd_missing",
            "session_not_found",
            "liveness_unconfirmed",
            "provider_unsupported",
            "id_reservation_unavailable",
        ],
    )
    def test_user_reasons_start_with_the_warning_marker(self, reason) -> None:
        assert (skip_text(reason, "/p") or "").startswith("⚠ Not restored")


class TestNotices:
    async def test_a_restored_topic_gets_the_flows_final_text(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")

        await env.start()

        [(thread, text)] = env.notices()
        assert thread == 7
        assert "Session resumed after restart." in text
        assert env.sent[0]["chat_id"] == _CHAT

    async def test_only_the_final_outcome_is_sent_not_progress(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")

        await env.start()

        assert len(env.sent) == 1

    async def test_a_failed_creation_is_reported_in_the_topic(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.create_error = ProtocolError(503, "backend_unavailable", "down")

        await env.start()

        [(thread, text)] = env.notices()
        assert thread == 7
        assert text.startswith("❌ Not restored")
        assert "/restore" in text

    async def test_a_cwd_missing_skip_names_the_path(self, env) -> None:
        env.dead_topic(7, "@1", session="s1", cwd=str(env.tmp / "gone"))

        await env.start()

        [(thread, text)] = env.notices()
        assert thread == 7
        assert str(env.tmp / "gone") in text

    @pytest.mark.parametrize(
        ("setup", "needle"),
        [
            ("forget", "session not found"),
            ("codex_unknown", "unsupported provider"),
        ],
    )
    async def test_plan_skips_with_a_user_reason_are_told(
        self, env, setup, needle
    ) -> None:
        if setup == "forget":
            env.dead_topic(7, "@1", session="s1")
            env.forget_sessions()
        else:
            env.dead_topic(7, "@1", session="s1", provider="gemini")

        await env.start()

        [(thread, text)] = env.notices()
        assert thread == 7
        assert needle in text
        env.adapter.create.assert_not_called()

    async def test_a_session_live_elsewhere_is_told(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.claude_registry("s1", os.getpid())

        await env.start()

        [(_, text)] = env.notices()
        assert "already running elsewhere" in text

    async def test_unconfirmed_liveness_suggests_restore(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.claude_registry("s1", os.getpid())
        env.ps_start = None

        await env.start()

        [(_, text)] = env.notices()
        assert "/restore" in text

    async def test_a_session_that_went_live_before_its_turn_is_told(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.dead_topic(8, "@2", session="s2")

        async def meanwhile(_seconds):
            env.claude_registry("s2", os.getpid())

        env.pause.side_effect = meanwhile

        await env.start()

        sent = dict(env.notices())
        assert "resumed" in sent[7]
        assert "already running elsewhere" in sent[8]

    async def test_unreservable_ids_tell_every_planned_topic(self, env) -> None:
        env.reserve_ok = False
        env.dead_topic(7, "@1", session="s1")
        env.dead_topic(8, "@2", session="s2")

        await env.start()

        assert sorted(thread for thread, _ in env.notices()) == [7, 8]
        env.adapter.create.assert_not_called()

    async def test_internal_skip_reasons_send_nothing(self, env) -> None:
        env.dead_topic(7, "@1", session="dup")
        env.dead_topic(8, "@2", session="dup")
        env.live = [WindowRef("@0", "_ccgram", str(env.tmp))]
        env.router.bind_thread(_USER, 9, "@0", window_name="live", chat_id=_CHAT)

        await env.start()

        assert [thread for thread, _ in env.notices()] == [7]

    async def test_a_bot_restart_in_the_same_boot_does_not_resend(self, env) -> None:
        env.dead_topic(7, "@1", session="s1", cwd=str(env.tmp / "gone"))

        await env.start()
        await env.start()

        assert len(env.sent) == 1

    async def test_a_new_boot_may_notify_again(self, env) -> None:
        env.dead_topic(7, "@1", session="s1", cwd=str(env.tmp / "gone"))

        await env.start()
        env.boot = "boot-2"
        await env.start()

        assert len(env.sent) == 2

    async def test_a_restored_topic_is_not_told_again_after_a_restart(
        self, env
    ) -> None:
        env.dead_topic(7, "@1", session="s1")

        await env.start()
        env.live = [WindowRef("@0", "_ccgram", str(env.tmp))]
        await env.start()

        assert len(env.sent) == 1

    @pytest.mark.parametrize(
        "error",
        [
            NetworkError("connection reset"),
            BadRequest("Message thread not found"),
            RuntimeError("not a telegram error"),
        ],
        ids=["network", "topic_deleted", "other"],
    )
    async def test_a_send_failure_is_logged_and_the_run_goes_on(
        self, env, error
    ) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.dead_topic(8, "@2", session="s2")
        # safe_send retries a Telegram error once as plain text
        env.send_errors = [error, error] if type(error) is NetworkError else [error]

        with capture_logs() as logs:
            await env.start()

        assert len(env.created) == 2
        assert [thread for thread, _ in env.notices()] == [8]
        failed = [log for log in logs if log["event"] == "auto_restore_notice_failed"]
        assert [(f["chat_id"], f["thread_id"]) for f in failed] == [(_CHAT, 7)]

    async def test_a_notice_retry_after_is_waited_out(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.send_errors = [RetryAfter(30)]

        await env.start()

        assert [c.args[0] for c in env.pause.await_args_list] == [30.0]
        assert len(env.created) == 1

    async def test_a_rate_limited_notice_is_resent_once_after_the_wait(
        self, env
    ) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.send_errors = [RetryAfter(30)]

        await env.start()

        assert len(env.created) == 1
        assert len(env.sent) == 1
        assert "Session resumed" in env.sent[0]["text"]

    async def test_a_skip_notice_rate_limited_twice_is_unclaimed(self, env) -> None:
        env.dead_topic(7, "@1", session="s1", cwd=str(env.tmp / "gone"))
        env.send_errors = [RetryAfter(1), RetryAfter(1)]

        with capture_logs() as logs:
            await env.start()

        assert env.sent == []
        assert env.ledger["notices"] == []
        assert any(log["event"] == "auto_restore_notice_unsent" for log in logs)
        await env.start()
        assert [thread for thread, _ in env.notices()] == [7]

    async def test_a_restore_outcome_rate_limited_twice_does_not_rerun_it(
        self, env
    ) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.send_errors = [RetryAfter(1), RetryAfter(1)]

        await env.start()
        env.live = [WindowRef("@0", "_ccgram", str(env.tmp))]
        await env.start()

        assert len(env.created) == 1
        assert env.sent == []

    async def test_notices_are_not_paced_but_restores_are(self, env) -> None:
        env.dead_topic(1, "@1", session="s1", cwd=str(env.tmp / "gone1"))
        env.dead_topic(2, "@2", session="s2", cwd=str(env.tmp / "gone2"))
        env.dead_topic(3, "@3", session="s3")
        env.dead_topic(4, "@4", session="s4")

        await env.start()

        assert len(env.sent) == 4
        assert [c.args[0] for c in env.pause.await_args_list] == [auto_restore.GAP_S]

    async def test_a_session_live_in_another_window_is_not_announced(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")
        assert env.live is not None
        env.live.append(WindowRef("@5", "elsewhere", str(env.tmp)))
        env.states["@5"] = SimpleNamespace(
            session_id="s1", cwd=str(env.tmp), provider_name="claude", approval_mode=""
        )

        with capture_logs() as logs:
            await env.start()

        assert "session_already_live" in _skips(logs)
        assert env.sent == []

    async def test_a_topic_is_queued_once_even_with_two_bindings(self, env) -> None:
        env.reserve_ok = False
        env.dead_topic(7, "@1", session="s1")
        env.router.bind_thread(101, 7, "@2", window_name="proj7", chat_id=_CHAT)
        env.states["@2"] = SimpleNamespace(
            session_id="s2",
            cwd=env.states["@1"].cwd,
            provider_name="claude",
            approval_mode="normal",
        )
        env.transcript(env.states["@1"].cwd, "s2")
        spy = AsyncMock(wraps=auto_restore._send)

        with patch(f"{_AR}._send", spy):
            await env.start()

        assert spy.await_count == 1

    async def test_an_unreadable_ledger_forgets_its_notices_too(self, env) -> None:
        env.dead_topic(7, "@1", session="s1", cwd=str(env.tmp / "gone"))
        row = {"version": 1, "boot_id": env.boot, "entries": [{"bogus": 1}]}
        (env.tmp / "auto_restore.json").write_text(
            json.dumps({**row, "notices": [f"{_CHAT}:7"]})
        )

        await env.start()

        assert [thread for thread, _ in env.notices()] == [7]

    async def test_a_creation_failure_notice_is_resent_after_a_rate_limit(
        self, env
    ) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.create_error = ProtocolError(503, "backend_unavailable", "down")
        env.send_errors = [RetryAfter(1)]

        await env.start()

        assert [thread for thread, _ in env.notices()] == [7]
        assert env.sent[0]["text"].startswith("❌ Not restored")

    async def test_notice_waits_count_against_the_rate_limit_cap(self, env) -> None:
        for thread in range(1, 6):
            env.dead_topic(
                thread,
                f"@{thread}",
                cwd=str(env.tmp / f"gone{thread}"),
                session=f"s{thread}",
            )
        env.send_errors = [RetryAfter(5), None] * 4

        with capture_logs() as logs:
            await env.start()

        assert [c.args[0] for c in env.pause.await_args_list] == [5.0, 5.0, 5.0]
        assert len(env.sent) == 3
        assert len(env.ledger["notices"]) == 3
        assert any(
            log["event"] == "auto_restore_rate_limited_giving_up" for log in logs
        )

    async def test_the_outcome_notice_and_the_rename_wait_once(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.rename_error = RetryAfter(30)
        env.send_errors = [RetryAfter(10)]

        await env.start()

        assert [c.args[0] for c in env.pause.await_args_list] == [30.0]
        assert len(env.sent) == 1

    async def test_the_longer_of_two_waits_is_the_one_waited(self, env) -> None:
        env.dead_topic(7, "@1", session="s1")
        env.rename_error = RetryAfter(30)
        env.send_errors = [RetryAfter(40)]

        await env.start()

        assert [c.args[0] for c in env.pause.await_args_list] == [40.0]

    async def test_a_cancel_during_the_wait_releases_the_skip_notice(self, env) -> None:
        env.dead_topic(7, "@1", session="s1", cwd=str(env.tmp / "gone"))
        env.send_errors = [RetryAfter(1)]
        env.pause.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await env.start()

        env.pause.side_effect = None
        await env.start()
        assert [thread for thread, _ in env.notices()] == [7]

    async def test_a_cancel_during_the_send_releases_the_skip_notice(self, env) -> None:
        env.dead_topic(7, "@1", session="s1", cwd=str(env.tmp / "gone"))
        env.send_errors = [asyncio.CancelledError()]

        with pytest.raises(asyncio.CancelledError):
            await env.start()

        await env.start()
        assert [thread for thread, _ in env.notices()] == [7]

    async def test_a_resend_limited_again_is_waited_before_the_next_send(
        self, env
    ) -> None:
        for thread in (1, 2):
            env.dead_topic(
                thread,
                f"@{thread}",
                cwd=str(env.tmp / f"gone{thread}"),
                session=f"s{thread}",
            )
        env.send_errors = [RetryAfter(5), RetryAfter(7)]
        sent_at_pause: list[int] = []

        async def record(_seconds):
            sent_at_pause.append(len(env.sent))

        env.pause.side_effect = record

        await env.start()

        assert [c.args[0] for c in env.pause.await_args_list] == [5.0, 7.0]
        assert sent_at_pause == [0, 0]
        assert [thread for thread, _ in env.notices()] == [2]

    async def test_giving_up_logs_the_notice_it_held_back(self, env) -> None:
        for thread in range(1, 5):
            env.dead_topic(
                thread,
                f"@{thread}",
                cwd=str(env.tmp / f"gone{thread}"),
                session=f"s{thread}",
            )
        env.send_errors = [RetryAfter(5), None] * 3 + [RetryAfter(5)]

        with capture_logs() as logs:
            await env.start()

        unsent = [
            log["key"] for log in logs if log["event"] == "auto_restore_notice_unsent"
        ]
        assert unsent == [f"{_CHAT}:4"]


class TestAdoptLive:
    """A Claude session running in another tmux session is reconnected, not skipped."""

    @pytest.fixture()
    def adopt(self):
        with patch(f"{_AR}.adopt_live_session", AsyncMock(return_value="2:@2")) as m:
            yield m

    async def test_a_live_session_elsewhere_is_adopted_instead_of_skipped(
        self, env, adopt
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid())

        with capture_logs() as logs:
            await env.start()

        env.adapter.create.assert_not_called()
        assert env.notices() == [(1, adopt_text("2:@2"))]
        assert _skips(logs) == ["adopted"]
        assert env.ledger["entries"] == []
        adopt.assert_awaited_once_with(
            "claude", "sess-1", (_USER, _CHAT, 1), "@1", env.states["@1"].cwd
        )

    async def test_a_live_session_with_a_missing_folder_is_adopted_too(
        self, env, adopt
    ) -> None:
        env.dead_topic(1, "@1", session="s1", cwd=str(env.tmp / "gone"))

        await env.start()

        assert env.notices() == [(1, adopt_text("2:@2"))]

    async def test_a_failed_adoption_keeps_todays_skip(self, env, adopt) -> None:
        adopt.return_value = None
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid())

        with capture_logs() as logs:
            await env.start()

        assert _skips(logs) == ["session_live"]
        assert env.notices() == [(1, skip_text("session_live"))]

    async def test_a_session_not_live_is_never_offered_to_adoption(
        self, env, adopt
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1")

        await env.start()

        adopt.assert_not_called()
        env.adapter.create.assert_awaited_once()

    async def test_the_adoption_notice_is_sent_once_per_boot(self, env, adopt) -> None:
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid())

        await env.start()
        await env.start()

        assert env.notices() == [(1, adopt_text("2:@2"))]

    async def test_a_dead_session_with_a_missing_folder_keeps_its_skip(
        self, env
    ) -> None:
        env.dead_topic(1, "@1", session="s1", cwd=str(env.tmp / "gone"))
        env.server_view.return_value = MagicMock()

        with (
            patch(f"{_AR}.adopt_live.adopt", AsyncMock()) as adopt_it,
            capture_logs() as logs,
        ):
            await env.start()

        adopt_it.assert_not_called()
        assert _skips(logs) == ["cwd_missing"]

    async def test_auto_restore_off_does_not_stop_the_message_path(
        self, env, monkeypatch
    ) -> None:
        monkeypatch.setattr(config, "auto_restore_on_start", False)
        env.dead_topic(1, "@1", session="sess-1")
        env.claude_registry("sess-1", os.getpid())
        env.server_view.return_value = MagicMock()
        target = "ccgram.handlers.recovery.adopt_live.adopt"

        with patch(target, AsyncMock(return_value="2:@2")) as adopt_it:
            label = await auto_restore.adopt_for_topic(_USER, _CHAT, 1, "@1")

        assert label == "2:@2"
        assert adopt_it.call_args.args[3:] == ("@1", (_USER, _CHAT, 1))


class TestAdoptLiveSession:
    """The orchestration around the finder: capability first, pid, cache, no raising."""

    @pytest.fixture()
    def live(self, env):
        env.claude_registry("sess-1", os.getpid())
        env.server_view.return_value = view = MagicMock()
        with (
            patch(f"{_AR}._parent_pids", return_value={9: 1}),
            patch(
                f"{_AR}.adopt_live.adopt", AsyncMock(return_value="2:@2")
            ) as adopt_it,
        ):
            yield SimpleNamespace(view=view, adopt=adopt_it)

    async def call(self, provider="claude"):
        return await auto_restore.adopt_live_session(
            provider, "sess-1", (_USER, _CHAT, 1), "@1"
        )

    async def test_claude_gets_its_registry_pid_and_is_adopted(self, env, live) -> None:
        assert await self.call() == "2:@2"

        live.adopt.assert_awaited_once_with(
            live.view, os.getpid(), {9: 1}, "@1", (_USER, _CHAT, 1)
        )

    async def test_an_unknown_provider_is_not_adopted(self, env, live) -> None:
        assert await self.call("pi") is None

        env.server_view.assert_not_called()
        live.adopt.assert_not_called()

    async def test_without_the_backend_capability_nothing_is_scanned(
        self, env, live
    ) -> None:
        env.server_view.return_value = None

        with patch(f"{_AR}._claude_registry_scan") as scan:
            assert await self.call() is None

        scan.assert_not_called()

    async def test_a_session_that_is_not_live_is_not_adopted(self, env, live) -> None:
        env.ps_start = "T-other"

        assert await self.call() is None

        live.adopt.assert_not_called()

    async def test_a_failure_is_logged_and_returns_none(self, env, live) -> None:
        live.adopt.side_effect = RuntimeError("boom")

        with capture_logs() as logs:
            assert await self.call() is None

        assert "adopt_live_failed" in [log["event"] for log in logs]

    async def test_a_miss_is_remembered_for_thirty_seconds(self, env, live) -> None:
        live.adopt.return_value = None
        clock = MagicMock(return_value=1000.0)

        with (
            patch(f"{_AR}.time.monotonic", clock),
            patch(
                f"{_AR}._claude_registry_scan", return_value=(os.getpid(), False)
            ) as scan,
        ):
            await self.call()
            clock.return_value = 1029.0
            await self.call()
            assert scan.call_count == 1
            clock.return_value = 1031.0
            await self.call()
            assert scan.call_count == 2

    async def test_a_hit_is_not_remembered(self, env, live) -> None:
        await self.call()
        await self.call()

        assert live.adopt.await_count == 2


class TestAdoptCodexSession:
    """A Codex session: its rollout must be open, its thread name unique."""

    @pytest.fixture()
    def live(self, env):
        env.codex_rollout("sess-1")
        env.lsof_result = (0, b"p1\n")
        index = Path.home() / ".codex" / "session_index.jsonl"
        index.write_text(
            json.dumps({"id": "sess-1", "thread_name": "thread-1", "updated_at": ""})
            + "\n"
        )
        env.server_view.return_value = view = MagicMock()
        with patch(
            f"{_AR}.adopt_live.adopt_codex", AsyncMock(return_value="2:@2")
        ) as adopt_it:
            yield SimpleNamespace(view=view, adopt=adopt_it, cwd=str(env.tmp / "p"))

    async def call(self, live, cwd=None):
        return await auto_restore.adopt_live_session(
            "codex", "sess-1", (_USER, _CHAT, 1), "@1", live.cwd if cwd is None else cwd
        )

    async def test_an_open_rollout_with_a_name_is_adopted(self, env, live) -> None:
        assert await self.call(live) == "2:@2"

        live.adopt.assert_awaited_once_with(
            live.view, "thread-1", Path(live.cwd), "@1", (_USER, _CHAT, 1)
        )

    async def test_a_rollout_nobody_holds_open_is_not_adopted(self, env, live) -> None:
        env.lsof_result = (1, b"")

        assert await self.call(live) is None

        live.adopt.assert_not_called()

    async def test_an_unconfirmed_rollout_is_not_adopted(self, env, live) -> None:
        env.lsof_result = (2, b"")

        assert await self.call(live) is None

        live.adopt.assert_not_called()

    async def test_a_session_without_a_rollout_is_not_adopted(self, env, live) -> None:
        for path in (Path.home() / ".codex" / "sessions").rglob("*.jsonl"):
            path.unlink()

        assert await self.call(live) is None

        live.adopt.assert_not_called()

    def namesakes(self, *ids: str) -> None:
        index = Path.home() / ".codex" / "session_index.jsonl"
        rows = [{"id": i, "thread_name": "thread-1"} for i in ("sess-1", *ids)]
        index.write_text("".join(json.dumps(r) + "\n" for r in rows))

    async def test_a_name_shared_with_a_session_without_rollout_is_not_adopted(
        self, env, live
    ) -> None:
        self.namesakes("old-missing")

        assert await self.call(live) is None

        live.adopt.assert_not_called()

    def archived(self, session: str) -> None:
        folder = Path.home() / ".codex" / "archived_sessions"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"rollout-2026-10-02T03-50-46-{session}.jsonl").write_text("{}\n")

    async def test_a_namesake_archived_and_closed_is_cleared(self, env, live) -> None:
        self.archived("old-archived")
        self.namesakes("old-archived")
        env.lsof_script = [(0, b"p1\n"), (1, b"")]

        assert await self.call(live) == "2:@2"

    async def test_a_namesake_archived_and_open_is_not_adopted(self, env, live) -> None:
        self.archived("old-archived")
        self.namesakes("old-archived")
        env.lsof_script = [(0, b"p1\n"), (0, b"p2\n")]

        assert await self.call(live) is None

        live.adopt.assert_not_called()

    async def test_a_name_shared_with_a_closed_rollout_is_adopted(
        self, env, live
    ) -> None:
        env.codex_rollout("old-closed")
        self.namesakes("old-closed")
        env.lsof_script = [(0, b"p1\n"), (1, b"")]

        assert await self.call(live) == "2:@2"

        assert len(env.lsof_calls) == 2

    async def test_a_name_shared_with_an_open_rollout_is_not_adopted(
        self, env, live
    ) -> None:
        env.codex_rollout("other-open")
        self.namesakes("other-open")
        env.lsof_script = [(0, b"p1\n"), (0, b"p2\n")]

        assert await self.call(live) is None

        live.adopt.assert_not_called()

    async def test_a_name_shared_with_an_unconfirmed_rollout_is_not_adopted(
        self, env, live
    ) -> None:
        env.codex_rollout("other-unknown")
        self.namesakes("other-unknown")
        env.lsof_script = [(0, b"p1\n"), (2, b"")]

        assert await self.call(live) is None

        live.adopt.assert_not_called()

    async def test_a_session_without_a_name_is_not_adopted(self, env, live) -> None:
        (Path.home() / ".codex" / "session_index.jsonl").write_text("")

        assert await self.call(live) is None

        live.adopt.assert_not_called()

    async def test_a_topic_without_a_folder_is_not_adopted(self, env, live) -> None:
        assert await self.call(live, cwd="") is None

        live.adopt.assert_not_called()

    async def test_without_the_backend_capability_nothing_is_read(
        self, env, live
    ) -> None:
        env.server_view.return_value = None

        assert await self.call(live) is None

        assert env.lsof_calls == []

    async def test_a_failure_is_logged_and_returns_none(self, env, live) -> None:
        live.adopt.side_effect = RuntimeError("boom")

        with capture_logs() as logs:
            assert await self.call(live) is None

        assert "adopt_live_failed" in [log["event"] for log in logs]

    async def test_a_miss_is_remembered_for_thirty_seconds(self, env, live) -> None:
        live.adopt.return_value = None
        clock = MagicMock(return_value=1000.0)

        with patch(f"{_AR}.time.monotonic", clock):
            await self.call(live)
            clock.return_value = 1029.0
            await self.call(live)
            assert live.adopt.await_count == 1
            clock.return_value = 1031.0
            await self.call(live)
            assert live.adopt.await_count == 2

    async def test_a_dead_codex_topic_is_adopted_by_the_message_path(
        self, env, live
    ) -> None:
        env.dead_topic(1, "@1", session="sess-1", provider="codex")

        assert await auto_restore.adopt_for_topic(_USER, _CHAT, 1, "@1") == "2:@2"

        assert live.adopt.call_args.args[2] == Path(env.states["@1"].cwd)

    async def test_auto_restore_adopts_a_live_codex_session(self, env, live) -> None:
        env.dead_topic(1, "@1", session="sess-1", provider="codex")

        await env.start()

        assert env.notices() == [(1, adopt_text("2:@2"))]
        env.adapter.create.assert_not_called()


class TestAdoptForTopic:
    async def test_a_topic_without_a_known_session_is_not_adopted(self, env) -> None:
        env.dead_topic(1, "@1", session="")

        with patch(f"{_AR}.adopt_live_session", AsyncMock()) as adopt_it:
            assert await auto_restore.adopt_for_topic(_USER, _CHAT, 1, "@1") is None

        adopt_it.assert_not_called()


class TestParentPids:
    def test_ps_output_becomes_a_pid_to_parent_table(self) -> None:
        done = SimpleNamespace(stdout="  1     0\n 20     1\njunk\n300    20\n")

        with patch(f"{_AR}._run_ps", return_value=done):
            assert auto_restore._parent_pids() == {1: 0, 20: 1, 300: 20}

    def test_an_unrunnable_ps_gives_an_empty_table(self) -> None:
        with patch(f"{_AR}._run_ps", side_effect=subprocess.TimeoutExpired("ps", 5)):
            assert auto_restore._parent_pids() == {}
