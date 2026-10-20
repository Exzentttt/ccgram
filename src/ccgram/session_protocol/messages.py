"""Provider-native records to messages. History and live use this one builder.

No Telegram formatting or mutable counters. Rebuilding a transcript prefix gives
identical IDs/revisions after a restart. Tool pairing precedes pagination.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from datetime import UTC, datetime
from typing import Any

from .models import Message, Part


@dataclass(frozen=True, slots=True)
class SourceEntry:
    offset: int
    data: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _Record:
    key: str
    role: str
    time: str | None
    parts: tuple[dict[str, Any], ...]
    results: tuple[tuple[str, Any, bool], ...] = ()


def read_entries(path: Path) -> list[SourceEntry]:
    """Ignore an unfinished final JSONL line; the next poll will read it."""
    result = []
    with path.open("rb") as source:
        while True:
            offset = source.tell()
            line = source.readline()
            if not line or not line.endswith(b"\n"):
                break
            try:
                data = json.loads(line)
            except ValueError, UnicodeDecodeError:
                continue
            if isinstance(data, dict):
                result.append(SourceEntry(offset, data))
    return result


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(
            str(item.get("text", "")) for item in value if isinstance(item, dict)
        )
    return json.dumps(value, ensure_ascii=False) if value is not None else ""


def tool_kind(name: str) -> str:
    lower = name.lower()
    for kind, names in (
        ("read", ("read",)),
        ("edit", ("edit", "write", "patch")),
        ("search", ("grep", "glob", "search", "find")),
        ("execute", ("bash", "shell", "exec", "command")),
        ("fetch", ("fetch", "web")),
        ("think", ("think",)),
    ):
        if any(word in lower for word in names):
            return kind
    return "other"


def _tool(call_id: str, name: str, arguments: Any) -> dict[str, Any]:
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except ValueError:
            arguments = {"text": arguments}
    title = name
    if isinstance(arguments, dict):
        title = next(
            (
                str(arguments[k])
                for k in ("command", "cmd", "file_path", "path", "query", "pattern")
                if k in arguments
            ),
            name,
        )
    return {
        "type": "tool",
        "tool_call_id": call_id,
        "name": name,
        "kind": tool_kind(name),
        "status": "running",
        "title": title,
        "input": arguments,
    }


def _content(content: Any) -> tuple[list[dict[str, Any]], list[tuple[str, Any, bool]]]:
    if isinstance(content, str):
        return ([{"type": "text", "text": content}] if content else []), []
    parts: list[dict[str, Any]] = []
    results: list[tuple[str, Any, bool]] = []
    for block in content if isinstance(content, list) else []:
        if not isinstance(block, dict):
            continue
        kind = block.get("type", "text")
        if not isinstance(kind, str):
            continue
        if kind in {"text", "input_text", "output_text"} and block.get("text"):
            parts.append({"type": "text", "text": str(block["text"])})
        elif kind in {"thinking", "reasoning"}:
            parts.append(
                {
                    "type": "thinking",
                    "text": str(block.get("thinking", block.get("text", ""))),
                }
            )
        elif kind in {"tool_use", "toolCall"}:
            parts.append(
                _tool(
                    str(block.get("id", "")),
                    str(block.get("name", "unknown")),
                    block.get("input", block.get("arguments", {})),
                )
            )
        elif kind == "tool_result":
            results.append(
                (
                    str(block.get("tool_use_id", "")),
                    block.get("content"),
                    bool(block.get("is_error")),
                )
            )
        elif kind in {"image", "image_url", "input_image"}:
            # Agent-local paths and embedded data are not public API URLs.
            parts.append(
                {
                    "type": "notice",
                    "level": "info",
                    "text": "Image in transcript; view it in the terminal",
                }
            )
    return parts, results


def _codex(entry: SourceEntry) -> _Record | None:
    raw = entry.data
    if raw.get("type") not in ("response_item", "input_item"):
        return None
    data = raw.get("payload")
    if not isinstance(data, dict):
        return None
    key = str(data.get("id") or f"o{entry.offset}")
    kind = data.get("type")
    if not isinstance(kind, str):
        return None
    timestamp = raw.get("timestamp")
    if kind == "message":
        parts, results = _content(data.get("content"))
        role = (
            "agent"
            if data.get("role") == "assistant"
            else str(data.get("role", "system"))
        )
        return _Record(key, role, timestamp, tuple(parts), tuple(results))
    if kind in {"function_call", "custom_tool_call"}:
        part = _tool(
            str(data.get("call_id", "")),
            str(data.get("name", "unknown")),
            data.get("arguments", data.get("input", {})),
        )
        return _Record(key, "agent", timestamp, (part,))
    if kind in {"function_call_output", "custom_tool_call_output"}:
        return _Record(
            key,
            "agent",
            timestamp,
            (),
            ((str(data.get("call_id", "")), data.get("output"), False),),
        )
    if kind == "reasoning":
        return _Record(
            key,
            "agent",
            timestamp,
            (
                {
                    "type": "thinking",
                    "text": _text(data.get("summary", data.get("content"))),
                },
            ),
        )
    return None


def _gemini(entry: SourceEntry) -> _Record | None:
    data = entry.data
    kind = data.get("type")
    if not isinstance(kind, str):
        return None
    if kind not in {"user", "gemini", "info", "error"}:
        return None
    key = str(data.get("id") or f"o{entry.offset}")
    content = data.get("content", data.get("displayContent", ""))
    parts, results = _content(content)
    if kind in {"info", "error"}:
        parts = [
            {
                "type": "notice",
                "level": "error" if kind == "error" else "info",
                "text": _text(content),
            }
        ]
    calls = data.get("toolCalls")
    for call in calls if isinstance(calls, list) else []:
        if not isinstance(call, dict):
            continue
        call_id = str(call.get("id", ""))
        parts.append(
            _tool(
                call_id,
                str(call.get("name", call.get("displayName", "unknown"))),
                call.get("args", {}),
            )
        )
        output = call.get("resultDisplay") or call.get("result")
        if output is not None:
            results.append((call_id, output, call.get("status") == "error"))
    return _Record(
        key,
        "user" if kind == "user" else "agent",
        data.get("timestamp"),
        tuple(parts),
        tuple(results),
    )


def _chat(entry: SourceEntry, provider: str) -> _Record | None:
    data = entry.data
    msg = data.get("message")
    if not isinstance(msg, dict):
        return None
    role = msg.get("role", data.get("type"))
    key = str(data.get("uuid" if provider == "claude" else "id") or f"o{entry.offset}")
    timestamp = data.get("timestamp") or msg.get("timestamp")
    if isinstance(timestamp, (int, float)):
        timestamp = datetime.fromtimestamp(timestamp / 1000, UTC).isoformat()
    if not isinstance(role, str):
        return None
    if role == "toolResult":
        return _Record(
            key,
            "agent",
            timestamp,
            (),
            (
                (
                    str(msg.get("toolCallId", "")),
                    msg.get("content"),
                    bool(msg.get("isError")),
                ),
            ),
        )
    if role not in {"user", "assistant", "system"}:
        return None
    parts, results = _content(msg.get("content"))
    return _Record(
        key,
        "agent" if role == "assistant" else role,
        timestamp,
        tuple(parts),
        tuple(results),
    )


def build_messages(
    entries: list[SourceEntry], conversation_id: str, provider: str, generation: int = 0
) -> tuple[Message, ...]:
    """Build full replacements, joining results before a caller slices pages.

    Repeated native IDs (Gemini updates) increment revision once per source
    record; each distinct result line contributes once to its owner message.
    """
    messages: dict[str, Message] = {}
    contributors: dict[str, set[int]] = {}
    calls: dict[str, tuple[str, int]] = {}
    results: list[tuple[int, str, Any, bool]] = []
    for entry in entries:
        record = (
            _codex(entry)
            if provider == "codex"
            else _gemini(entry)
            if provider == "gemini"
            else _chat(entry, provider)
        )
        if record is None:
            continue
        results.extend(
            (entry.offset, call_id, output, failed)
            for call_id, output, failed in record.results
        )
        if not record.parts:
            continue
        message_id = f"{conversation_id}/{'g' + str(generation) + '/' if generation else ''}{record.key}"
        contributors.setdefault(message_id, set()).add(entry.offset)
        previous = messages.get(message_id)
        parts = tuple(
            Part(id=f"{message_id}#{i}", **part) for i, part in enumerate(record.parts)
        )
        messages[message_id] = Message(
            message_id,
            previous.revision + 1 if previous else 1,
            conversation_id,
            record.role,
            record.time,
            True,
            parts,
        )
        for i, part in enumerate(parts):
            if part.tool_call_id:
                calls[part.tool_call_id] = message_id, i
    for offset, call_id, output, failed in results:
        owner = calls.get(call_id)
        if owner is None:
            continue
        message_id, index = owner
        message = messages[message_id]
        parts = list(message.parts)
        parts[index] = replace(
            parts[index],
            status="failed" if failed else "completed",
            output={"text": _text(output), "truncated": False},
        )
        contributors[message_id].add(offset)
        messages[message_id] = replace(
            message, parts=tuple(parts), revision=len(contributors[message_id])
        )
    return tuple(messages.values())
