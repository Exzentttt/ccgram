"""Validate and serialize actions, reserving each ID durably before side effects."""

from __future__ import annotations

import asyncio
import re
import time
from weakref import WeakValueDictionary
from typing import TYPE_CHECKING, Any
from uuid import UUID

from .backend import Target
from .input_lock import input_lock
from .models import (
    ACTION_KINDS,
    MAX_TEXT_BYTES,
    Action,
    ActionResult,
    ProtocolError,
    wire,
)

if TYPE_CHECKING:
    from .service import LocalSessionService

_MAX_KEYS = 32
_MAX_ATTACHMENTS = 10

_KEY_RE = re.compile(
    r"^(?:[CMS]-){0,3}(?:[a-zA-Z0-9]|Enter|Escape|Tab|Space|BSpace|DC|Up|Down|Left|Right|Home|End|PPage|NPage|F(?:[1-9]|1[0-2]))$"
)


def parse_action(body: Any) -> Action:
    if not isinstance(body, dict):
        raise ProtocolError(400, "invalid_request", "Action must be an object")
    action_id, version, kind, data = (
        body.get(k) for k in ("id", "bindingVersion", "kind", "data")
    )
    try:
        if not isinstance(action_id, str) or str(UUID(action_id)) != action_id.lower():
            raise ValueError
    except ValueError as exc:
        raise ProtocolError(400, "invalid_request", "id must be a UUID") from exc
    if (
        type(version) is not int
        or version < 1
        or not isinstance(kind, str)
        or kind not in ACTION_KINDS
        or not isinstance(data, dict)
    ):
        raise ProtocolError(
            400, "invalid_request", "Invalid bindingVersion, kind, or data"
        )
    _validate_data(kind, data)
    return Action(action_id.lower(), version, kind, data)


def _string(data: dict[str, Any], name: str, *, maximum: int = MAX_TEXT_BYTES) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ProtocolError(
            400, "invalid_request", f"{name} must be nonempty text without NUL"
        )
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise ProtocolError(
            400, "invalid_request", f"{name} must be valid UTF-8"
        ) from exc
    if size > maximum:
        raise ProtocolError(
            400, "invalid_request", f"{name} exceeds {maximum} UTF-8 bytes"
        )
    return value


def _validate_data(kind: str, data: dict[str, Any]) -> None:
    if kind in {"message.send", "terminal.paste"}:
        _string(data, "text")
    if kind == "message.send":
        _validate_message(data)
    if kind == "terminal.key":
        keys = data.get("keys")
        if (
            not isinstance(keys, list)
            or not 1 <= len(keys) <= _MAX_KEYS
            or any(not isinstance(k, str) or not _KEY_RE.fullmatch(k) for k in keys)
        ):
            raise ProtocolError(
                400, "invalid_request", "keys must contain 1 to 32 terminal key names"
            )
    if kind == "request.respond":
        _validate_response_data(data)
    if kind == "session.rename":
        _string(data, "title", maximum=200)
    if kind == "session.resume":
        _string(data, "conversationId", maximum=200)


def _validate_message(data: dict[str, Any]) -> None:
    if "dismissRequest" in data and type(data["dismissRequest"]) is not bool:
        raise ProtocolError(400, "invalid_request", "dismissRequest must be boolean")
    files = data.get("files", [])
    if (
        not isinstance(files, list)
        or len(files) > _MAX_ATTACHMENTS
        or any(
            not isinstance(f, dict) or not isinstance(f.get("id"), str) for f in files
        )
    ):
        raise ProtocolError(
            400,
            "invalid_request",
            "files must be a list of at most ten upload references",
        )


def _validate_response_data(data: dict[str, Any]) -> None:
    _string(data, "requestId", maximum=128)
    if "optionId" in data:
        _string(data, "optionId", maximum=128)
    elif "answer" not in data:
        raise ProtocolError(
            400, "invalid_request", "A response needs optionId or answer"
        )
    if "optionId" in data and "answer" in data:
        raise ProtocolError(400, "invalid_request", "Choose optionId or answer")


class ActionExecutor:
    def __init__(self, service: LocalSessionService) -> None:
        self.service = service
        self._action_locks: WeakValueDictionary[str, asyncio.Lock] = (
            WeakValueDictionary()
        )

    async def execute(
        self, session_id: str, action: Action
    ) -> tuple[ActionResult, int]:
        # Validate in-process clients too; the HTTP decoder is not a trust boundary.
        action = parse_action(wire(action))
        lock = self._action_locks.setdefault(action.id, asyncio.Lock())
        async with lock:
            return await self._once(session_id, action)

    async def _once(self, session_id: str, action: Action) -> tuple[ActionResult, int]:
        store = self.service.store
        fingerprint = store.action_fingerprint(session_id, action)
        previous = store.action(action.id, fingerprint)
        if previous is not None:
            return previous
        store.reserve(action.id, fingerprint)
        attempted = False
        try:
            target, _ = await self.service.current(session_id)
            async with input_lock(target.id):
                target = await self._preflight(session_id, action)
                sent_at = time.time()
                attempted = True
                async with asyncio.timeout(10):
                    success = await self._perform(session_id, target, action)
                result = ActionResult(action.id, "sent" if success else "unknown")
                if success:
                    self._record_delivery(session_id, target, action, sent_at)
        except ProtocolError as exc:
            if attempted:
                result, status = (
                    ActionResult(action.id, "unknown", exc.body()["error"]),
                    200,
                )
            else:
                result, status = (
                    ActionResult(action.id, "rejected", exc.body()["error"]),
                    exc.status,
                )
            store.finish(result, status)
            return result, status
        except OSError, TimeoutError:
            result = ActionResult(
                action.id,
                "unknown" if attempted else "rejected",
                {
                    "code": "backend_unavailable",
                    "message": "Could not confirm delivery",
                    "details": {},
                },
            )
        store.finish(result)
        self.service.hub.publish("action.updated", session_id, result)
        return result, 200

    async def _preflight(self, session_id: str, action: Action) -> Target:
        target, snapshot = await self.service.current(session_id)
        if snapshot.binding_version != action.binding_version:
            raise ProtocolError(
                409, "binding_conflict", "Session binding changed", snapshot
            )
        if not target.foreground_verified:
            raise ProtocolError(
                503,
                "backend_unavailable",
                "Foreground process could not be verified",
            )
        if action.kind == "request.respond":
            await self._validate_response(session_id, target, action)
        if action.kind == "session.resume" and (
            target.provider != "shell"
            or action.data["conversationId"] != target.conversation_id
        ):
            raise ProtocolError(
                422,
                "unsupported_action",
                "Resume requires the previous conversation in a shell",
            )
        if action.kind not in snapshot.capabilities:
            raise ProtocolError(
                422,
                "unsupported_action",
                "Session does not support this action",
            )
        if (
            action.kind == "message.send"
            and self.service.requests.get(session_id)
            and not action.data.get("dismissRequest", False)
        ):
            raise ProtocolError(
                409, "request_open", "Sending text requires dismissRequest"
            )
        if action.kind == "message.send":
            self.service.uploads.message_text(
                action.data["text"], action.data.get("files", [])
            )
        return target

    def _record_delivery(
        self, session_id: str, target: Target, action: Action, sent_at: float
    ) -> None:
        if action.kind == "message.send":
            text = self.service.uploads.message_text(
                action.data["text"], action.data.get("files", [])
            )
            self.service.store.remember_delivery(
                action.id, session_id, target.conversation_id, text, sent_at
            )

    async def _validate_response(
        self, session_id: str, target: Target, action: Action
    ) -> None:
        current = self.service.requests.get(session_id)
        if current is None or current.request.id != action.data["requestId"]:
            raise ProtocolError(409, "request_closed", "Request has closed")
        answer = action.data.get("answer")
        valid = (
            action.data.get("optionId") in current.option_keys
            if "optionId" in action.data
            else isinstance(answer, str) and answer in current.answer_keys
        )
        if current.request.source != "hook" or not valid:
            raise ProtocolError(
                422, "unsupported_action", "Use the terminal for this response"
            )
        prompt = await self.service.backend.prompt(target)
        if self.service.requests.get(session_id) is not current:
            raise ProtocolError(
                409, "request_closed", "Request changed during validation"
            )
        if prompt is None or prompt.fingerprint != current.fingerprint:
            self.service.requests.close(session_id, "vanished")
            raise ProtocolError(
                409, "request_closed", "Prompt changed before the response"
            )

    async def _perform(self, session_id: str, target: Target, action: Action) -> bool:
        backend = self.service.backend
        kind, data = action.kind, action.data
        if kind == "message.send":
            return await self._send_message(session_id, target, action)
        if kind == "request.respond":
            return await self._respond(session_id, target, action)
        if kind == "terminal.key":
            return await self._send_keys(target, data["keys"])
        if kind == "run.interrupt":
            success = await backend.send_key(target, "C-c")
            if success:
                self.service.requests.close(session_id, "interrupted")
            return success
        if kind == "terminal.paste":
            return await backend.send_text(target, data["text"], enter=False)
        if kind == "session.close":
            return await backend.close(target)
        if kind == "session.rename":
            return await backend.rename(target, data["title"])
        return await backend.resume(target, data["conversationId"])

    async def _send_keys(self, target: Target, keys: list[str]) -> bool:
        for key in keys:
            if not await self.service.backend.send_key(target, key):
                return False
        return True

    async def _send_message(
        self, session_id: str, target: Target, action: Action
    ) -> bool:
        backend = self.service.backend
        request = self.service.requests.get(session_id)
        if request:
            if not await backend.send_key(target, "Escape"):
                return False
            if not await self._prompt_gone(target):
                return False
            current = self.service.requests.get(session_id)
            if current is not None and current is not request:
                raise ProtocolError(
                    409, "request_open", "A new request opened during dismissal"
                )
            if current is request:
                self.service.requests.close(session_id, "dismissed")
        text = self.service.uploads.message_text(
            action.data["text"], action.data.get("files", [])
        )
        return await backend.send_text(target, text, enter=True)

    async def _respond(self, session_id: str, target: Target, action: Action) -> bool:
        current = self.service.requests.get(session_id)
        if current is None or current.request.id != action.data["requestId"]:
            raise ProtocolError(
                409, "request_closed", "Request changed before dispatch"
            )
        key = (
            current.option_keys[action.data["optionId"]]
            if "optionId" in action.data
            else current.answer_keys[action.data["answer"]]
        )
        if not await self.service.backend.send_text(target, key, enter=False):
            return False
        if not await self._prompt_gone(target):
            return False
        if self.service.requests.get(session_id) is current:
            self.service.requests.close(session_id, "answered")
        return True

    async def _prompt_gone(self, target: Target) -> bool:
        for _ in range(3):
            await asyncio.sleep(0.3)
            if await self.service.backend.prompt(target) is None:
                return True
        return False
