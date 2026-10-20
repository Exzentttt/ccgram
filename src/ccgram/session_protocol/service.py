"""Session operations independent of HTTP and Telegram identities."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from collections.abc import AsyncIterator, Collection
from dataclasses import replace
from datetime import datetime
from typing import Any, Protocol

from ..providers.base import HookEvent
from .backend import Prompt, SessionBackend, Target
from .events import Event, EventHub
from .messages import Message, build_messages, read_entries
from .models import (
    Action,
    ActionResult,
    Conversation,
    HistoryPage,
    Pane,
    ProtocolError,
    SessionSnapshot,
    Status,
    utc_now,
)
from .requests import PendingHook, RequestStore
from .store import ProtocolStore
from .uploads import UploadStore

MAX_HISTORY_PAGE = 200

SUPPORTED_ACTIONS = (
    "message.send",
    "run.interrupt",
    "request.respond",
    "terminal.key",
    "terminal.paste",
    "session.close",
    "session.rename",
    "session.resume",
)


class SessionService(Protocol):
    async def list_sessions(self) -> list[SessionSnapshot]: ...
    async def get_session(self, session_id: str) -> SessionSnapshot: ...
    async def history(
        self,
        session_id: str,
        cursor: str | None,
        limit: int,
        *,
        conversation: str | None = None,
    ) -> HistoryPage: ...
    def watch(self, session_ids: Collection[str] | None) -> AsyncIterator[Event]: ...
    async def execute(
        self, session_id: str, action: Action
    ) -> tuple[ActionResult, int]: ...


class LocalSessionService:
    def __init__(
        self, backend: SessionBackend, store: ProtocolStore, hub: EventHub | None = None
    ) -> None:
        self.backend = backend
        self.store = store
        self.uploads = UploadStore(store.path.parent / "uploads", store)
        self.hub = hub or EventHub()
        self.requests = RequestStore(self.hub)
        self._snapshots: dict[str, SessionSnapshot] = {}
        self._targets: dict[str, Target] = {}
        self._refresh_lock = asyncio.Lock()
        self._transcript_stamps: dict[str, tuple[int, int, int]] = {}
        self._revisions: dict[str, dict[str, int]] = {}
        # Lazy: ActionExecutor imports the service only for type checking.
        from .actions import ActionExecutor

        self.executor = ActionExecutor(self)

    async def refresh(self) -> None:
        async with self._refresh_lock:
            targets = await self.backend.targets()
            live = set()
            for target in targets:
                _, snapshot = await self._update(target)
                live.add(snapshot.id)
            for session_id in self._snapshots.keys() - live:
                self.requests.close(session_id, "vanished")
                self.hub.publish(
                    "session.removed", session_id, {"reason": "target_gone"}
                )
                self._snapshots.pop(session_id)
                self._targets.pop(session_id)
                self._transcript_stamps.pop(session_id, None)
                self._revisions.pop(session_id, None)

    async def _update(self, target: Target) -> tuple[Target, SessionSnapshot]:
        try:
            target = await self._source_version(target)
            prompt = await self.backend.prompt(target)
            sid, version = self.store.bind(target.id, target.binding, target.aliases)
            return target, await self._observe(sid, version, target, prompt)
        except (ProtocolError, OSError) as exc:
            target = replace(
                target,
                available=False,
                status=Status("unknown", str(exc), "screen", utc_now()),
            )
            sid, version = self.store.bind(target.id, target.binding, target.aliases)
            return target, await self._observe(sid, version, target, None)

    async def _observe(
        self, session_id: str, version: int, target: Target, prompt: Prompt | None
    ) -> SessionSnapshot:
        old = self._snapshots.get(session_id)
        if old and old.binding_version != version:
            self.requests.close(session_id, "vanished")
            self._revisions.pop(session_id, None)
            self._transcript_stamps.pop(session_id, None)
        self._targets[session_id] = target
        opened = self.requests.reconcile(
            session_id, target.conversation_id, version, prompt
        )
        status = target.status
        if opened:
            status = Status(
                "blocked", opened.request.title, opened.request.source, status.since
            )
        if old and (old.status.state, old.status.detail, old.status.source) == (
            status.state,
            status.detail,
            status.source,
        ):
            status = replace(status, since=old.status.since)
        last_activity = old.last_activity if old else utc_now()
        if old and old.status != status:
            last_activity = utc_now()
        capabilities = tuple(
            k
            for k in SUPPORTED_ACTIONS
            if target.foreground_verified
            and (k != "message.send" or target.provider not in {"shell", "unknown"})
            and (
                k != "request.respond"
                or (opened is not None and bool(opened.option_keys))
            )
            and (k != "session.resume" or bool(target.resume_provider))
        )
        snapshot = SessionSnapshot(
            session_id,
            version,
            target.title,
            target.host,
            target.cwd,
            None,
            target.provider,
            Conversation(target.conversation_id, "full", target.history_generation)
            if target.transcript
            else None,
            status,
            "default" if target.approval_mode == "normal" else target.approval_mode,
            (Pane("p0", target.pane_title, True),),
            (opened.request,) if opened else (),
            (*capabilities, "terminal.view"),
            last_activity,
        )
        self._snapshots[session_id] = snapshot
        if old != snapshot:
            self.hub.publish(
                "session.snapshot" if old is None else "session.updated",
                session_id,
                snapshot,
            )
        if target.available:
            await self._publish_messages(session_id, target)
        return self._snapshots[session_id]

    async def _source_version(self, target: Target) -> Target:
        if target.transcript is None:
            return target
        try:
            generation = await self.store.transcript_generation(
                f"{target.id.casefold()}:{target.conversation_id}", target.transcript
            )
        except OSError as exc:
            raise ProtocolError(
                503, "backend_unavailable", "Transcript source is unavailable"
            ) from exc
        return replace(target, history_generation=generation)

    async def _messages(self, target: Target) -> tuple[Message, ...]:
        if target.transcript is None:
            return ()
        try:
            entries = await asyncio.to_thread(read_entries, target.transcript)
        except OSError as exc:
            raise ProtocolError(
                503, "backend_unavailable", "Transcript is unavailable"
            ) from exc
        messages = await asyncio.to_thread(
            build_messages,
            entries,
            target.conversation_id,
            target.transcript_provider or target.provider,
            target.history_generation,
        )
        sid = next((sid for sid, t in self._targets.items() if t.id == target.id), "")
        return tuple(self._correlate(sid, message) for message in messages)

    async def _publish_messages(self, session_id: str, target: Target) -> None:
        if target.transcript is None:
            return
        try:
            stat = await asyncio.to_thread(target.transcript.stat)
        except OSError:
            return
        stamp = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        if self._transcript_stamps.get(session_id) == stamp:
            return
        messages = await self._messages(target)
        revisions = self._revisions.setdefault(session_id, {})
        changed = False
        for message in messages:
            if revisions.get(message.id) != message.revision:
                changed = True
                self.hub.publish("message.upsert", session_id, message)
                revisions[message.id] = message.revision
        self._transcript_stamps[session_id] = stamp
        if changed:
            snapshot = replace(self._snapshots[session_id], last_activity=utc_now())
            self._snapshots[session_id] = snapshot
            self.hub.publish(
                "session.updated",
                session_id,
                {
                    "bindingVersion": snapshot.binding_version,
                    "lastActivity": snapshot.last_activity,
                },
            )

    def _correlate(self, session_id: str, message: Message) -> Message:
        if message.role != "user" or not message.time:
            return message
        try:
            timestamp = datetime.fromisoformat(message.time).timestamp()
        except ValueError:
            return message
        text = " ".join(p.text or "" for p in message.parts if p.type == "text")
        action_id, fresh = self.store.match_delivery(
            session_id, message.conversation_id, message.id, text, timestamp
        )
        if action_id and fresh:
            self.hub.publish(
                "action.updated",
                session_id,
                {"actionId": action_id, "outcome": "sent", "messageId": message.id},
            )
        return replace(message, action_id=action_id) if action_id else message

    async def list_sessions(self) -> list[SessionSnapshot]:
        await self.refresh()
        return list(self._snapshots.values())

    async def get_session(self, session_id: str) -> SessionSnapshot:
        await self.refresh()
        try:
            return self._snapshots[session_id]
        except KeyError as exc:
            raise ProtocolError(404, "not_found", "Session not found") from exc

    async def current(self, session_id: str) -> tuple[Target, SessionSnapshot]:
        if session_id not in self._targets:
            await self.refresh()
        async with self._refresh_lock:
            target = self._targets.get(session_id)
            if target is None:
                raise ProtocolError(404, "not_found", "Session not found")
            return await self._update(await self.backend.inspect(target.id))

    async def history(
        self,
        session_id: str,
        cursor: str | None,
        limit: int,
        *,
        conversation: str | None = None,
    ) -> HistoryPage:
        if not 1 <= limit <= MAX_HISTORY_PAGE:
            raise ProtocolError(
                400, "invalid_request", "limit must be between 1 and 200"
            )
        target, _ = await self.current(session_id)
        if conversation is not None and conversation != target.conversation_id:
            raise ProtocolError(
                404, "not_found", "Conversation is no longer bound to this session"
            )
        messages = await self._messages(target)
        before = len(messages)
        if cursor:
            try:
                decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()))
                if (
                    decoded["conversation"] != target.conversation_id
                    or decoded.get("generation", 0) != target.history_generation
                ):
                    raise ValueError
                before = next(
                    i for i, m in enumerate(messages) if m.id == decoded["before"]
                )
            except (ValueError, KeyError, TypeError, StopIteration) as exc:
                raise ProtocolError(
                    400, "invalid_request", "Invalid cursor for this conversation"
                ) from exc
        start = max(0, before - limit)
        page = messages[start:before]
        prev = (
            base64.urlsafe_b64encode(
                json.dumps(
                    {
                        "conversation": target.conversation_id,
                        "before": messages[start].id,
                        "generation": target.history_generation,
                    }
                ).encode()
            ).decode()
            if start
            else None
        )
        return HistoryPage(
            target.conversation_id or None,
            page,
            prev,
            "full" if target.transcript else "scrollback",
            target.history_generation,
        )

    async def watch(
        self, session_ids: Collection[str] | None = None
    ) -> AsyncIterator[Event]:
        subscriber = self.hub.subscribe(session_ids)
        try:
            while True:
                yield await subscriber.get()
        finally:
            self.hub.unsubscribe(subscriber)

    async def execute(
        self, session_id: str, action: Action
    ) -> tuple[ActionResult, int]:
        return await self.executor.execute(session_id, action)

    async def create_session(self, data: dict[str, Any]) -> SessionSnapshot:
        async with self._refresh_lock:
            _, snapshot = await self._update(await self.backend.create(data))
            return snapshot

    async def terminal(self, session_id: str, pane: str = "p0") -> dict[str, Any]:
        target, _ = await self.current(session_id)
        return await self.backend.terminal(target, pane)

    async def commands(self, session_id: str) -> list[dict[str, str]]:
        target, _ = await self.current(session_id)
        return await self.backend.commands(target)

    async def observe_hook(self, event: HookEvent, prefix: str) -> None:
        async with self._refresh_lock:
            await self._observe_hook(event, prefix)

    async def _observe_hook(self, event: HookEvent, prefix: str) -> None:
        target_id = event.window_key.removeprefix(prefix)
        if not event.window_key.startswith(prefix):
            return
        for sid, target in list(self._targets.items()):
            if (
                target.id.casefold() != target_id.casefold()
                or target.conversation_id != event.session_id
            ):
                continue
            if event.event_type in {"Stop", "SessionEnd"}:
                self.requests.close(sid, "turn_ended")
                continue
            if event.event_type not in {"PermissionRequest", "PreToolUse"}:
                continue
            tool_name = event.data.get("tool_name")
            if event.event_type == "PreToolUse" and tool_name != "AskUserQuestion":
                continue
            await self._match_hook(sid, target, event)

    async def _match_hook(self, sid: str, target: Target, event: HookEvent) -> None:
        tool_name = event.data.get("tool_name")
        if not isinstance(tool_name, str):
            return
        for message in reversed(await self._messages(target)):
            for part in message.parts:
                if (
                    part.type != "tool"
                    or part.name != tool_name
                    or not isinstance(part.input, dict)
                ):
                    continue
                digest = hashlib.sha256(
                    json.dumps(
                        part.input,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode()
                ).hexdigest()
                if digest == event.data.get("tool_input_hash"):
                    self.requests.observe_hook(
                        sid,
                        PendingHook(
                            event.session_id,
                            "question"
                            if tool_name == "AskUserQuestion"
                            else "permission",
                            tool_name,
                            part.input,
                            event.timestamp,
                        ),
                    )
                    return
