import json

import pytest

from ccgram.session_protocol.messages import SourceEntry, build_messages, read_entries


@pytest.mark.parametrize(
    "provider,entry",
    [
        (
            "claude",
            {
                "type": "assistant",
                "uuid": "native",
                "message": {
                    "id": "not-entry-id",
                    "role": "assistant",
                    "content": [{"type": "text", "text": "hi"}],
                },
            },
        ),
        (
            "codex",
            {
                "type": "response_item",
                "payload": {
                    "id": "native",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "hi"}],
                },
            },
        ),
        ("gemini", {"type": "gemini", "id": "native", "content": "hi"}),
        (
            "pi",
            {
                "type": "message",
                "id": "native",
                "message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "hi"}],
                },
            },
        ),
    ],
)
def test_native_message_identity_matches_history_and_live(provider, entry):
    source = [SourceEntry(200, entry)]
    live = build_messages(source, "conv", provider)
    history = build_messages(list(source), "conv", provider)
    assert live == history
    assert [(m.id, m.revision) for m in live] == [("conv/native", 1)]
    assert live[0].parts[0].id == "conv/native#0"
    assert live[0].parts[0].text == "hi"


def test_codex_no_id_uses_byte_offset_and_ignores_duplicate_event_messages():
    entries = [
        SourceEntry(
            10,
            {
                "type": "event_msg",
                "payload": {"type": "agent_message", "message": "hi"},
            },
        ),
        SourceEntry(
            200,
            {
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "hi"}],
                },
            },
        ),
    ]
    assert [m.id for m in build_messages(entries, "conv", "codex")] == ["conv/o200"]


def test_tool_pairing_updates_original_message_across_page_boundary():
    call = SourceEntry(
        0,
        {
            "type": "assistant",
            "uuid": "call",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tool1",
                        "name": "Bash",
                        "input": {"command": "uv run pytest"},
                    }
                ],
            },
        },
    )
    middle = SourceEntry(
        300, {"type": "assistant", "uuid": "middle", "message": {"content": "waiting"}}
    )
    result = SourceEntry(
        500,
        {
            "type": "user",
            "uuid": "result",
            "message": {
                "content": [
                    {"type": "tool_result", "tool_use_id": "tool1", "content": "passed"}
                ]
            },
        },
    )
    before = build_messages([call, middle], "conv", "claude")
    after = build_messages([call, middle, result], "conv", "claude")
    assert before[0].revision == 1
    assert after[0].revision == 2
    assert len(after) == 2
    assert after[0].parts[0].status == "completed"
    assert after[0].parts[0].output == {"text": "passed", "truncated": False}


def test_gemini_repeated_native_id_increments_revision_for_content_updates():
    first = SourceEntry(0, {"id": "m1", "type": "gemini", "content": "first"})
    second = SourceEntry(100, {"id": "m1", "type": "gemini", "content": "second"})
    assert build_messages([first, second], "conv", "gemini")[0].revision == 2
    assert (
        build_messages([first, second], "conv", "gemini")[0].parts[0].text == "second"
    )


def test_reader_uses_utf8_byte_offsets_and_retries_partial_line(tmp_path):
    path = tmp_path / "session.jsonl"
    first = (
        json.dumps({"type": "gemini", "content": "שלום"}, ensure_ascii=False).encode()
        + b"\n"
    )
    path.write_bytes(first + b'{"type":"user","content":"hi"}')
    assert len(read_entries(path)) == 1
    with path.open("ab") as file:
        file.write(b"\n")
    assert [e.offset for e in read_entries(path)] == [0, len(first)]
