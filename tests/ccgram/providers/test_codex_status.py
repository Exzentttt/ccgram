import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from ccgram.providers import codex_status
from ccgram.providers.codex_status import (
    build_codex_status_snapshot,
    has_codex_assistant_output_since,
)


def _write_jsonl(path, entries) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry) + "\n")


def test_build_codex_status_snapshot_with_token_stats(tmp_path) -> None:
    transcript = tmp_path / "codex.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "timestamp": "2026-03-02T17:00:00.000Z",
                "type": "session_meta",
                "payload": {
                    "id": "sess-abc",
                    "cwd": "/Users/alexei/Workspace/repo",
                    "cli_version": "0.106.0",
                },
            },
            {
                "timestamp": "2026-03-02T17:00:01.000Z",
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "model_context_window": 1000,
                        "total_token_usage": {
                            "input_tokens": 10,
                            "cached_input_tokens": 3,
                            "output_tokens": 4,
                            "reasoning_output_tokens": 2,
                            "total_tokens": 14,
                        },
                        "rate_limits": {
                            "primary": {"used_percent": 12.5, "resets_at": 1772485454},
                            "secondary": {
                                "used_percent": 20.0,
                                "resets_at": 1772792459,
                            },
                        },
                    },
                },
            },
        ],
    )

    result = build_codex_status_snapshot(
        str(transcript),
        display_name="repo",
        session_id="",
        cwd="",
    )

    assert result is not None
    assert "[repo] Codex status snapshot" in result
    assert "session: `sess-abc`" in result
    assert "tokens (total): in `10`" in result
    assert "context window: `14` / `1,000` (1.4%)" in result
    assert "primary limit: `12.5%` used" in result


def test_build_codex_status_snapshot_missing_file() -> None:
    result = build_codex_status_snapshot(
        "/tmp/ccgram-missing-codex-status.jsonl",
        display_name="repo",
    )
    assert result is None


def test_build_codex_status_snapshot_without_token_count(tmp_path) -> None:
    transcript = tmp_path / "codex.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "timestamp": "2026-03-02T17:00:00.000Z",
                "type": "session_meta",
                "payload": {
                    "id": "sess-xyz",
                    "cwd": "/tmp/repo",
                    "cli_version": "0.106.0",
                },
            }
        ],
    )

    result = build_codex_status_snapshot(
        str(transcript),
        display_name="repo",
    )

    assert result is not None
    assert "token stats: unavailable" in result


def test_has_codex_assistant_output_since_detects_response(tmp_path) -> None:
    transcript = tmp_path / "codex.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "timestamp": "2026-03-02T17:00:00.000Z",
                "type": "input_item",
                "payload": {"role": "user", "content": "/status"},
            },
            {
                "timestamp": "2026-03-02T17:00:01.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "status text"}],
                },
            },
        ],
    )

    assert has_codex_assistant_output_since(str(transcript), 0) is True


def test_has_codex_assistant_output_since_handles_midline_offset(tmp_path) -> None:
    transcript = tmp_path / "codex.jsonl"
    _write_jsonl(
        transcript,
        [
            {
                "timestamp": "2026-03-02T17:00:00.000Z",
                "type": "event_msg",
                "payload": {"type": "token_count", "info": {"total_token_usage": {}}},
            },
            {
                "timestamp": "2026-03-02T17:00:02.000Z",
                "type": "event_msg",
                "payload": {"type": "agent_message", "message": "ready"},
            },
        ],
    )

    first_line = transcript.read_text(encoding="utf-8").splitlines()[0]
    mid_offset = len(first_line) // 2
    assert has_codex_assistant_output_since(str(transcript), mid_offset) is True


def _token_entry(total: int) -> dict:
    return {
        "timestamp": f"2026-03-02T17:00:{total:02d}.000Z",
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {"total_token_usage": {"total_tokens": total}},
        },
    }


def _append_jsonl(path, entries) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry) + "\n")


def test_status_snapshot_parses_only_appended_lines_on_repeat_poll(tmp_path) -> None:
    transcript = tmp_path / "codex.jsonl"
    _write_jsonl(transcript, [_token_entry(n) for n in range(1, 21)])
    build_codex_status_snapshot(str(transcript), display_name="repo")

    _append_jsonl(transcript, [_token_entry(21), _token_entry(22)])
    with patch(
        "ccgram.providers.codex_status._parse_json_object",
        wraps=codex_status._parse_json_object,
    ) as parse:
        result = build_codex_status_snapshot(str(transcript), display_name="repo")

    assert parse.call_count == 2
    assert result is not None
    assert "transcript entries: `22`" in result
    assert "total `22`" in result


def test_status_snapshot_counts_unterminated_last_line_without_caching_it(
    tmp_path,
) -> None:
    transcript = tmp_path / "codex.jsonl"
    _write_jsonl(transcript, [_token_entry(1)])
    with transcript.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_token_entry(2)))

    first = build_codex_status_snapshot(str(transcript), display_name="repo")
    with transcript.open("a", encoding="utf-8") as handle:
        handle.write("\n")
    second = build_codex_status_snapshot(str(transcript), display_name="repo")

    assert first is not None
    assert "transcript entries: `2`" in first
    assert second is not None
    assert "transcript entries: `2`" in second


def test_status_snapshot_rescans_after_truncation(tmp_path) -> None:
    transcript = tmp_path / "codex.jsonl"
    _write_jsonl(transcript, [_token_entry(n) for n in range(1, 11)])
    build_codex_status_snapshot(str(transcript), display_name="repo")

    _write_jsonl(transcript, [_token_entry(50)])
    result = build_codex_status_snapshot(str(transcript), display_name="repo")

    assert result is not None
    assert "transcript entries: `1`" in result
    assert "total `50`" in result


def test_status_scan_cache_evicts_least_recently_used(tmp_path) -> None:
    codex_status._scan_cache.clear()
    paths = []
    for n in range(codex_status._MAX_CACHED_SCANS + 1):
        path = tmp_path / f"codex-{n}.jsonl"
        _write_jsonl(path, [_token_entry(1)])
        paths.append(path)
    for path in paths[: codex_status._MAX_CACHED_SCANS]:
        build_codex_status_snapshot(str(path), display_name="repo")
    build_codex_status_snapshot(str(paths[0]), display_name="repo")
    build_codex_status_snapshot(str(paths[-1]), display_name="repo")

    assert str(paths[0]) in codex_status._scan_cache
    assert str(paths[1]) not in codex_status._scan_cache


def test_status_snapshot_is_consistent_under_concurrent_polls(tmp_path) -> None:
    """Smoke test only: it also passes without the lock, it does not prove it."""
    transcript = tmp_path / "codex.jsonl"
    _write_jsonl(transcript, [_token_entry(n) for n in range(1, 31)])

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda _: build_codex_status_snapshot(
                    str(transcript), display_name="repo"
                ),
                range(32),
            )
        )

    assert all(r is not None and "transcript entries: `30`" in r for r in results)
