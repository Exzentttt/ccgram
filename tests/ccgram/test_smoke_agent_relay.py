import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from ccgram.monitor_state import MonitorState, TrackedSession

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/smoke_agent_relay.py"


def load_smoke():
    spec = importlib.util.spec_from_file_location("smoke_agent_relay", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_smoke_import_does_not_create_native_sessions(monkeypatch):
    monkeypatch.setattr(
        "subprocess.run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unexpected process")
        ),
    )
    assert load_smoke().USER_ID == 999001


def test_reply_requires_the_real_persisted_checkpoint_and_resolved_source(tmp_path):
    smoke = load_smoke()
    path = tmp_path / "session.jsonl"
    row = {
        "type": "message",
        "id": "reply",
        "message": {
            "role": "assistant",
            "content": [{"type": "text", "text": "nonce"}],
        },
    }
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")
    state = MonitorState(state_file=tmp_path / "state.json")
    tracked = TrackedSession(session_id="session", file_path=str(path))
    state.update_session(tracked)
    monitor = SimpleNamespace(state=state)
    mapping = {"session_id": "session", "provider_name": "pi", "transcript_path": ""}

    assert smoke.acknowledged_reply(mapping, "nonce", monitor) is None
    tracked.last_byte_offset = path.stat().st_size - 1
    assert smoke.acknowledged_reply(mapping, "nonce", monitor) is None
    tracked.last_byte_offset = path.stat().st_size
    receipt = smoke.acknowledged_reply(mapping, "nonce", monitor)
    assert receipt == {
        "reply_end": path.stat().st_size,
        "watermark": path.stat().st_size,
        "internal_request_records": 0,
    }
