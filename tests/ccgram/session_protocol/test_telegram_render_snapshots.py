import json
from pathlib import Path

import pytest

from ccgram.session_protocol.messages import (
    SourceEntry,
    TranscriptParserState,
    parse_records,
)
from ccgram.transcript_reader import render_protocol_records

SNAPSHOTS = json.loads((Path(__file__).parent / "telegram_snapshots.json").read_text())


@pytest.mark.parametrize("provider_name", SNAPSHOTS)
def test_canonical_records_preserve_telegram_render_snapshot(provider_name):
    fixture = SNAPSHOTS[provider_name]
    source = fixture["source"]
    state = TranscriptParserState()
    entries = [SourceEntry(i * 100, raw) for i, raw in enumerate(source)]
    records, state = parse_records(
        entries,
        provider_name,
        state,
        deduplicate_updates=True,
    )
    parsed = render_protocol_records("snapshot", provider_name, records, state)
    assert [
        {"text": m.text, "role": m.role, "contentType": m.content_type} for m in parsed
    ] == fixture["telegram"]
