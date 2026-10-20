import json
import time
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from ccgram.hooks.adapters import ClaudeHookAdapter
from ccgram.providers.base import HookEvent
from ccgram.session_protocol.backend import Prompt
from ccgram.session_protocol.models import Action, ProtocolError
from ccgram.session_protocol.service import LocalSessionService


def append(path, data):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(data) + "\n")


async def test_history_joins_results_beyond_page_and_rejects_cross_conversation_cursor(
    service, backend, tmp_path
):
    path = tmp_path / "transcript.jsonl"
    path.touch()
    backend.target = replace(backend.target, transcript=path)
    append(
        path,
        {
            "type": "assistant",
            "uuid": "first",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "Bash",
                        "id": "tool",
                        "input": {"command": "test"},
                    }
                ]
            },
        },
    )
    append(
        path, {"type": "assistant", "uuid": "second", "message": {"content": "later"}}
    )
    append(
        path,
        {
            "type": "user",
            "uuid": "result",
            "message": {
                "content": [
                    {"type": "tool_result", "tool_use_id": "tool", "content": "passed"}
                ]
            },
        },
    )
    session = (await service.list_sessions())[0]
    newest = await service.history(session.id, None, 1)
    assert newest.prev_cursor
    older = await service.history(session.id, newest.prev_cursor, 1)
    assert older.messages[0].revision == 2
    assert older.messages[0].parts[0].output["text"] == "passed"
    backend.target = replace(backend.target, conversation_id="new-conversation")
    with pytest.raises(ProtocolError, match="cursor"):
        await service.history(session.id, newest.prev_cursor, 1)


async def test_transcript_replacement_changes_generation_ids_and_binding(
    service, backend, tmp_path
):
    path = tmp_path / "transcript.jsonl"
    append(
        path, {"type": "assistant", "uuid": "same", "message": {"content": "before"}}
    )
    backend.target = replace(backend.target, transcript=path)
    session = (await service.list_sessions())[0]
    first = await service.history(session.id, None, 50)
    replacement = tmp_path / "replacement"
    append(
        replacement,
        {"type": "assistant", "uuid": "same", "message": {"content": "after"}},
    )
    replacement.replace(path)
    current = await service.get_session(session.id)
    second = await service.history(session.id, None, 50)
    assert current.binding_version == session.binding_version + 1
    assert current.conversation.history_generation == 1
    assert first.messages[0].id != second.messages[0].id
    restarted = LocalSessionService(backend, service.store)
    assert (await restarted.history(session.id, None, 50)).messages[
        0
    ].id == second.messages[0].id


async def test_sent_message_gets_delivered_identity_and_survives_restart(
    service, backend, tmp_path
):
    path = tmp_path / "transcript.jsonl"
    path.touch()
    backend.target = replace(backend.target, transcript=path)
    session = (await service.list_sessions())[0]
    action = Action(
        str(uuid4()), session.binding_version, "message.send", {"text": "hello   world"}
    )
    result, _ = await service.execute(session.id, action)
    assert result.outcome == "sent"
    append(
        path,
        {
            "uuid": "user-entry",
            "type": "user",
            "timestamp": datetime.now(UTC).isoformat(),
            "message": {"content": "hello world"},
        },
    )
    await service.refresh()
    messages = (await service.history(session.id, None, 50)).messages
    assert messages[0].action_id == action.id
    restarted = LocalSessionService(backend, service.store)
    assert (await restarted.history(session.id, None, 50)).messages[
        0
    ].action_id == action.id
    _, events = service.hub.replay(service.hub.epoch, 0)
    assert any(
        event.type == "action.updated"
        and event.data.get("messageId") == "conv/user-entry"
        for event in events
    )


async def test_hook_digest_matches_transcript_without_persisting_tool_input(
    service, backend, tmp_path
):
    path = tmp_path / "transcript.jsonl"
    path.touch()
    backend.target = replace(
        backend.target,
        transcript=path,
        conversation_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    )
    tool_input = {"command": "uv run pytest", "private": "not-in-hook-log"}
    append(
        path,
        {
            "uuid": "tool-entry",
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": "call",
                        "name": "Bash",
                        "input": tool_input,
                    }
                ]
            },
        },
    )
    session = (await service.list_sessions())[0]
    event = ClaudeHookAdapter().normalize(
        {
            "hook_event_name": "PermissionRequest",
            "session_id": backend.target.conversation_id,
            "tool_name": "Bash",
            "tool_input": tool_input,
        }
    )
    assert event is not None
    assert "not-in-hook-log" not in json.dumps(event.data)
    await service.observe_hook(
        HookEvent(
            event.canonical_event_name,
            "ccgram:@1",
            event.session_id,
            event.data,
            time.time(),
        ),
        "ccgram:",
    )
    backend.current_prompt = Prompt("uv run pytest\n1. Yes\n2. No", "PermissionPrompt")
    session = await service.get_session(session.id)
    assert session.open_requests[0].source == "hook"


async def test_resume_is_available_only_for_a_previous_agent_in_a_shell(
    service, backend
):
    backend.target = replace(backend.target, provider="shell", resume_provider="claude")
    session = (await service.list_sessions())[0]
    assert "session.resume" in session.capabilities
    result, status = await service.execute(
        session.id,
        Action(
            str(uuid4()),
            session.binding_version,
            "session.resume",
            {"conversationId": "conv"},
        ),
    )
    assert status == 200 and result.outcome == "sent"
    assert backend.sent == [("resume", "conv", True)]
