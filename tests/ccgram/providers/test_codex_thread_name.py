"""Thread name of a Codex session from ``session_index.jsonl``, with the ids sharing it."""

import json

import pytest

from ccgram.providers.codex import codex_thread_name


@pytest.fixture()
def index(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    path = tmp_path / ".codex" / "session_index.jsonl"
    path.parent.mkdir()

    def write(*rows: object, raw: str = "") -> None:
        lines = [r if isinstance(r, str) else json.dumps(r) for r in rows]
        path.write_text("\n".join(lines) + "\n" + raw)

    return write


def row(session_id: str, name: str) -> dict[str, str]:
    return {"id": session_id, "thread_name": name, "updated_at": "2026-10-26T10:00:00Z"}


class TestThreadName:
    def test_the_name_of_the_id(self, index) -> None:
        index(row("a", "alpha"), row("b", "beta"))

        assert codex_thread_name("b") == ("beta", ())

    def test_the_last_line_of_an_id_wins(self, index) -> None:
        index(row("a", "old"), row("a", "new"))

        assert codex_thread_name("a") == ("new", ())

    def test_an_unknown_id_has_no_name(self, index) -> None:
        index(row("a", "alpha"))

        assert codex_thread_name("zzz") is None

    def test_a_missing_index_has_no_name(self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))

        assert codex_thread_name("a") is None

    def test_an_unreadable_index_has_no_name(self, index, tmp_path) -> None:
        index(row("a", "alpha"))
        (tmp_path / ".codex" / "session_index.jsonl").write_bytes(b"\xff\xfe\n")

        assert codex_thread_name("a") is None

    def test_a_name_shared_with_other_ids_lists_them(self, index) -> None:
        index(row("a", "same"), row("c", "same"), row("b", "same"), row("d", "x"))

        assert codex_thread_name("a") == ("same", ("b", "c"))
        assert codex_thread_name("b") == ("same", ("a", "c"))

    def test_a_name_the_other_id_no_longer_has_is_not_shared(self, index) -> None:
        index(row("b", "same"), row("b", "renamed"), row("a", "same"))

        assert codex_thread_name("a") == ("same", ())

    def test_bad_lines_are_skipped(self, index) -> None:
        index(
            "not json",
            "[1]",
            {"id": "a"},
            {"id": "a", "thread_name": 5},
            row("a", "ok"),
        )

        assert codex_thread_name("a") == ("ok", ())

    def test_an_empty_name_is_no_name(self, index) -> None:
        index(row("a", ""))

        assert codex_thread_name("a") is None
