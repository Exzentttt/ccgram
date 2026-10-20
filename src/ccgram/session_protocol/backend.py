"""Backend port and production adapter of the existing Multiplexer seam."""

from __future__ import annotations

import asyncio
import hashlib
import json
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ..command_catalog import CommandCatalog
from ..multiplexer.base import Multiplexer, WindowRef
from ..providers import get_provider_for_window, resolve_launch_command
from ..providers.process_detection import classify_provider_from_argv
from ..window_state_ports import identity_state
from ..worktrees import (
    check_worktree_eligibility,
    create_worktree,
    slug_for_path,
    suggest_branch_name,
    validate_branch_name,
    worktree_path_for,
    WorktreeError,
)
from .models import ProtocolError, Status, utc_now


@dataclass(frozen=True, slots=True)
class Target:
    id: str
    title: str
    cwd: str
    provider: str
    conversation_id: str
    transcript: Path | None
    pane_id: str
    pane_title: str
    rows: int
    cols: int
    process_id: int
    approval_mode: str
    status: Status
    aliases: tuple[str, ...] = ()
    host: str = ""
    resume_provider: str = ""
    transcript_provider: str = ""
    history_generation: int = 0

    @property
    def binding(self) -> str:
        return json.dumps(
            [
                self.id.casefold(),
                self.provider,
                self.conversation_id,
                self.pane_id,
                self.process_id,
                self.history_generation,
            ],
            separators=(",", ":"),
        )


@dataclass(frozen=True, slots=True)
class Prompt:
    text: str
    kind: str

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.text.encode()).hexdigest()


class SessionBackend(Protocol):
    async def targets(self) -> list[Target]: ...
    async def inspect(self, target_id: str) -> Target: ...
    async def prompt(self, target: Target) -> Prompt | None: ...
    async def terminal(self, target: Target, pane: str) -> dict[str, Any]: ...
    async def send_text(self, target: Target, text: str, *, enter: bool) -> bool: ...
    async def send_key(self, target: Target, key: str) -> bool: ...
    async def close(self, target: Target) -> bool: ...
    async def rename(self, target: Target, title: str) -> bool: ...
    async def commands(self, target: Target) -> list[dict[str, str]]: ...
    async def create(self, data: dict[str, Any]) -> Target: ...
    async def resume(self, target: Target, conversation_id: str) -> bool: ...


class MultiplexerSessionBackend:
    """No provider is launched through an SDK; every operation uses the seam."""

    def __init__(self, multiplexer: Multiplexer, command_dir: str) -> None:
        self.mux = multiplexer
        self.command_dir = command_dir
        self.catalog = CommandCatalog()
        self.host = socket.gethostname()

    async def targets(self) -> list[Target]:
        windows = await self.mux.list_windows_for_reconciliation()
        if windows is None:
            raise ProtocolError(
                503, "backend_unavailable", "Multiplexer listing could not be confirmed"
            )
        result = []
        for window in windows:
            if (
                window.topic_eligible
                or identity_state.get_identity(window.window_id) is not None
            ):
                result.append(await self._target(window))
        return result

    async def inspect(self, target_id: str) -> Target:
        windows = await self.mux.list_windows_for_reconciliation()
        if windows is None:
            raise ProtocolError(
                503, "backend_unavailable", "Multiplexer is unavailable"
            )
        window = next((w for w in windows if w.matches(target_id)), None)
        if window is None:
            raise ProtocolError(404, "not_found", "Session target no longer exists")
        return await self._target(window)

    async def _target(self, window: WindowRef) -> Target:
        identity = identity_state.get_identity(window.window_id)
        fg = await self.mux.foreground(window.window_id)
        provider_name = classify_provider_from_argv(fg.argv) if fg else ""
        provider_name = provider_name or (
            identity.provider_name if identity and fg is None else "unknown"
        )
        provider = get_provider_for_window(
            window.window_id, provider_name=provider_name or "shell"
        )
        resume_provider = (
            identity.provider_name
            if identity
            and provider_name == "shell"
            and identity.provider_name not in {"shell", "unknown"}
            else ""
        )
        if (
            resume_provider
            and not get_provider_for_window(
                window.window_id, provider_name=resume_provider
            ).capabilities.supports_resume
        ):
            resume_provider = ""
        history_provider = resume_provider or provider_name
        conversation_id = (
            identity.session_id
            if identity and identity.provider_name == history_provider
            else ""
        )
        transcript = identity.transcript_path if conversation_id and identity else None
        if history_provider not in {"claude", "codex", "gemini", "pi"}:
            transcript = None
        panes = await self.mux.list_panes(window.window_id)
        active = next((p for p in panes if p.active), None)
        native = await self.mux.agent_status(window.window_id)
        now = utc_now()
        status = Status("unknown", "", "screen", now)
        if native and native.state in {
            "starting",
            "working",
            "blocked",
            "done",
            "idle",
            "dead",
            "unknown",
        }:
            status = Status(native.state, native.custom_status, "native", now)
        elif provider_name != "unknown":
            screen = await self.mux.capture_pane(window.window_id)
            if screen:
                parsed = provider.parse_terminal_status(screen)
                status = Status(
                    "working" if parsed and not parsed.is_interactive else "idle",
                    parsed.display_label if parsed else "",
                    "screen",
                    now,
                )
        return Target(
            window.window_id,
            window.window_name,
            window.cwd,
            provider_name or "unknown",
            conversation_id,
            transcript,
            active.pane_id if active else "",
            active.command if active else "",
            active.height if active else window.pane_height,
            active.width if active else window.pane_width,
            fg.pid if fg else 0,
            identity.approval_mode if identity else "normal",
            status,
            window.alias_window_ids,
            self.host,
            resume_provider,
            history_provider,
        )

    async def prompt(self, target: Target) -> Prompt | None:
        text = (
            await self.mux.capture_pane_by_id(target.pane_id, window_id=target.id)
            if target.pane_id
            else await self.mux.capture_pane(target.id)
        )
        if not text:
            raise ProtocolError(
                503, "backend_unavailable", "Cannot verify the active pane"
            )
        provider = get_provider_for_window(target.id, provider_name=target.provider)
        title = (
            await self.mux.get_pane_title(target.id)
            if provider.capabilities.uses_pane_title
            else ""
        )
        status = provider.parse_terminal_status(text, pane_title=title)
        if status and status.is_interactive and not status.ui_advisory:
            return Prompt(status.raw_text, status.ui_type or "prompt")
        return None

    async def terminal(self, target: Target, pane: str) -> dict[str, Any]:
        if pane not in {"p0", "", target.pane_id}:
            raise ProtocolError(404, "not_found", "Pane not found")
        text = (
            await self.mux.capture_pane_by_id(
                target.pane_id, window_id=target.id, with_ansi=True
            )
            if target.pane_id
            else await self.mux.capture_pane(target.id, with_ansi=True)
        )
        if text is None:
            raise ProtocolError(503, "backend_unavailable", "Pane capture failed")
        return {
            "pane": "p0",
            "rows": target.rows,
            "cols": target.cols,
            "ansi": text,
            "hash": hashlib.sha256(text.encode()).hexdigest(),
        }

    async def send_text(self, target: Target, text: str, *, enter: bool) -> bool:
        if target.pane_id:
            return await self.mux.send_to_pane(
                target.pane_id, text, enter=enter, literal=True, window_id=target.id
            )
        return await self.mux.send(target.id, text, enter=enter, literal=True)

    async def send_key(self, target: Target, key: str) -> bool:
        if target.pane_id:
            return await self.mux.send_to_pane(
                target.pane_id, key, enter=False, literal=False, window_id=target.id
            )
        return await self.mux.send(target.id, key, enter=False, literal=False, raw=True)

    async def close(self, target: Target) -> bool:
        return await self.mux.kill_window(target.id)

    async def rename(self, target: Target, title: str) -> bool:
        return await self.mux.rename_window(target.id, title)

    async def commands(self, target: Target) -> list[dict[str, str]]:
        provider = get_provider_for_window(target.id, provider_name=target.provider)
        commands = await asyncio.to_thread(
            self.catalog.get_provider_commands, provider, self.command_dir
        )
        return [
            {"name": c.name, "description": c.description, "source": c.source}
            for c in commands
        ]

    async def resume(self, target: Target, conversation_id: str) -> bool:
        if (
            target.provider != "shell"
            or not target.resume_provider
            or conversation_id != target.conversation_id
        ):
            raise ProtocolError(
                422,
                "unsupported_action",
                "Resume requires a verified shell and its previous conversation",
            )
        provider = get_provider_for_window(
            target.id, provider_name=target.resume_provider
        )
        if not provider.capabilities.supports_resume:
            raise ProtocolError(
                422, "unsupported_action", "Provider does not support resume"
            )
        command = resolve_launch_command(target.resume_provider, approval_mode="normal")
        arguments = provider.make_launch_args(resume_id=conversation_id)
        return await self.send_text(
            target, f"{command} {arguments}".strip(), enter=True
        )

    async def create(self, data: dict[str, Any]) -> Target:
        directory = Path(data["directory"]).expanduser()
        if not directory.is_absolute() or not directory.is_dir():
            raise ProtocolError(
                400,
                "invalid_request",
                "directory must be an existing absolute directory",
            )
        if data.get("mode", "normal") != "normal":
            raise ProtocolError(
                422,
                "unsupported_action",
                "Remote creation supports the normal approval mode only",
            )
        provider = data["provider"]
        candidate = get_provider_for_window("", provider_name=provider)
        if candidate.capabilities.name != provider:
            raise ProtocolError(400, "invalid_request", "Unknown provider")
        command = resolve_launch_command(provider, approval_mode="normal")
        worktree = data.get("worktree")
        branch = ""
        worktree_path = directory
        if worktree:
            eligibility = await asyncio.to_thread(check_worktree_eligibility, directory)
            if not eligibility.eligible or eligibility.repo_path is None:
                raise ProtocolError(
                    400, "invalid_request", "Directory is not eligible for a worktree"
                )
            branch = data.get("branch") or await asyncio.to_thread(
                suggest_branch_name, data.get("title"), eligibility.repo_path
            )
            if not isinstance(branch, str) or not await asyncio.to_thread(
                validate_branch_name, branch
            ):
                raise ProtocolError(400, "invalid_request", "Invalid worktree branch")
            worktree_path = worktree_path_for(
                eligibility.repo_path, slug_for_path(branch)
            )
            if self.mux.capabilities.native_worktrees:
                ok, _, _, target_id = await self.mux.create_worktree_window(
                    str(eligibility.repo_path),
                    str(worktree_path),
                    branch,
                    launch_command=command,
                )
                return await self._created_target(ok, target_id)
            try:
                await asyncio.to_thread(
                    create_worktree, eligibility.repo_path, branch, worktree_path
                )
            except WorktreeError as exc:
                raise ProtocolError(400, "invalid_request", str(exc)) from exc
        if self.mux.capabilities.native_topic_targets:
            created = await self.mux.create_topic_target(
                str(worktree_path),
                launch_command=command,
                workspace_id=data.get("workspaceId"),
            )
            return await self.inspect(created.target_id)
        ok, _, _, target_id = await self.mux.create_window(
            str(worktree_path),
            launch_command=command,
            workspace_id=data.get("workspaceId"),
        )
        return await self._created_target(ok, target_id)

    async def _created_target(self, ok: bool, target_id: str) -> Target:
        if not ok or not target_id:
            raise ProtocolError(
                503,
                "backend_unavailable",
                "Could not confirm session creation; inspect the terminal before retrying",
            )
        return await self.inspect(target_id)
