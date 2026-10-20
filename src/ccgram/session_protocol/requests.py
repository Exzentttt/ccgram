"""Open requests keyed by control session, with server-owned response evidence."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

from .backend import Prompt
from .events import EventHub
from .messages import tool_kind
from .models import CLOSE_REASONS, Request, RequestOption, utc_now
from .store import new_id

_HOOK_MATCH_TTL = 30
_MIN_MENU_OPTIONS = 2
_NUMBERED = re.compile(r"^\s*[❯>●]?\s*([1-8])[.)]\s+(.+?)\s*$")


def _compact(text: str) -> str:
    return "".join(text.split())


def _choices(text: str) -> tuple[tuple[str, str], ...]:
    choices = []
    for line in text.splitlines():
        match = _NUMBERED.fullmatch(line)
        if match:
            choices.append((match[1], match[2]))
    if len(choices) < _MIN_MENU_OPTIONS or [k for k, _ in choices] != [
        str(i + 1) for i in range(len(choices))
    ]:
        return ()
    return tuple(choices)


@dataclass(frozen=True, slots=True)
class PendingHook:
    conversation_id: str
    kind: str
    tool_name: str
    tool_input: dict[str, Any]
    timestamp: float


@dataclass(frozen=True, slots=True)
class OpenRequest:
    request: Request
    binding_version: int
    fingerprint: str
    option_keys: dict[str, str]
    answer_keys: dict[str, str]


def _question_label(hook: PendingHook, label: str) -> str | None:
    question = hook.tool_input["questions"][0]
    return next(
        (
            o["label"]
            for o in question.get("options", [])
            if isinstance(o, dict)
            and isinstance(o.get("label"), str)
            and label.startswith(o["label"])
        ),
        None,
    )


def _response_options(
    hook: PendingHook | None, prompt: Prompt
) -> tuple[list[RequestOption], dict[str, str], dict[str, str], dict[str, Any] | None]:
    options: list[RequestOption] = []
    keys: dict[str, str] = {}
    answers: dict[str, str] = {}
    response_schema = None
    if hook:
        for key, label in _choices(prompt.text):
            lower = label.casefold()
            if any(
                word in lower
                for word in (
                    "always",
                    "session",
                    "don't ask",
                    "don’t ask",
                    "remember",
                )
            ):
                continue
            if hook.kind == "question":
                matched = _question_label(hook, label)
                if matched:
                    answers[matched] = key
                    options.append(RequestOption(key, label, "allow_once"))
                    keys[key] = key
            elif lower.startswith(("yes", "allow", "approve", "accept")):
                options.append(RequestOption(key, label, "allow_once"))
                keys[key] = key
            elif lower.startswith(("no", "deny", "reject", "cancel")):
                options.append(RequestOption(key, label, "reject_once"))
                keys[key] = key
        if answers:
            response_schema = {"type": "string", "enum": list(answers)}
    return options, keys, answers, response_schema


class RequestStore:
    def __init__(self, hub: EventHub) -> None:
        self.hub = hub
        self._open: dict[str, OpenRequest] = {}
        self._hooks: dict[str, PendingHook] = {}

    def get(self, session_id: str) -> OpenRequest | None:
        return self._open.get(session_id)

    def observe_hook(self, session_id: str, hook: PendingHook) -> None:
        self._hooks[session_id] = hook

    def close(self, session_id: str, reason: str) -> None:
        if reason not in CLOSE_REASONS:
            raise ValueError(f"Unknown request close reason: {reason}")
        current = self._open.pop(session_id, None)
        self._hooks.pop(session_id, None)
        if current:
            self.hub.publish(
                "request.closed",
                session_id,
                {"requestId": current.request.id, "reason": reason},
            )

    def reconcile(
        self, session_id: str, conversation_id: str, version: int, prompt: Prompt | None
    ) -> OpenRequest | None:
        current = self.get(session_id)
        if prompt is None:
            if current:
                self.close(session_id, "vanished")
            return None
        hook = self._hooks.get(session_id)
        structured = self._match(hook, conversation_id, prompt)
        if (
            current
            and current.binding_version == version
            and current.fingerprint == prompt.fingerprint
            and (current.request.source == "hook" or not structured)
        ):
            return current
        if current:
            # Retain a matching observation when promoting screen -> hook.
            self._open.pop(session_id)
            self.hub.publish(
                "request.closed",
                session_id,
                {"requestId": current.request.id, "reason": "vanished"},
            )
        options, keys, answers, response_schema = _response_options(
            hook if structured else None, prompt
        )
        request = Request(
            new_id("r_"),
            session_id,
            hook.kind if structured and hook else "prompt",
            "hook" if structured else "screen",
            prompt.text,
            {
                "name": hook.tool_name,
                "kind": tool_kind(hook.tool_name),
                "input": hook.tool_input,
            }
            if structured and hook
            else None,
            tuple(options),
            response_schema,
            utc_now(),
        )
        opened = OpenRequest(request, version, prompt.fingerprint, keys, answers)
        self._open[session_id] = opened
        self.hub.publish("request.opened", session_id, request)
        return opened

    @staticmethod
    def _match(hook: PendingHook | None, conversation_id: str, prompt: Prompt) -> bool:
        if (
            hook is None
            or hook.conversation_id != conversation_id
            or abs(time.time() - hook.timestamp) > _HOOK_MATCH_TTL
        ):
            return False
        if hook.kind == "question":
            questions = hook.tool_input.get("questions")
            if not isinstance(questions, list) or len(questions) != 1:
                return False
            question = questions[0]
            return (
                isinstance(question, dict)
                and bool(question.get("question"))
                and _compact(str(question["question"])) in _compact(prompt.text)
            )
        value = next(
            (
                hook.tool_input[k]
                for k in ("command", "file_path", "path")
                if isinstance(hook.tool_input.get(k), str) and hook.tool_input[k]
            ),
            None,
        )
        return bool(
            value and _compact(value) in _compact(prompt.text) and _choices(prompt.text)
        )
