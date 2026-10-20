import json
from pathlib import Path

import pytest

from ccgram.providers import get_provider_for_window
from ccgram.session_protocol.messages import SourceEntry, build_messages

SNAPSHOTS = json.loads((Path(__file__).parent / "telegram_snapshots.json").read_text())


@pytest.mark.parametrize("provider_name", SNAPSHOTS)
def test_mobile_builder_preserves_telegram_render_snapshot(provider_name):
    fixture = SNAPSHOTS[provider_name]
    provider = get_provider_for_window("snapshot", provider_name=provider_name)
    source = fixture["source"]
    build_messages(
        [SourceEntry(i * 100, raw) for i, raw in enumerate(source)],
        "conv",
        provider_name,
    )
    entries = [
        entry
        for raw in source
        if (entry := provider.parse_transcript_line(json.dumps(raw))) is not None
    ]
    parsed, _ = provider.parse_transcript_entries(entries, pending_tools={})
    assert [
        {"text": m.text, "role": m.role, "contentType": m.content_type} for m in parsed
    ] == fixture["telegram"]
