import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

from ccgram.api.runtime import start_if_enabled
from ccgram.cli import cli
from ccgram.providers.base import HookEvent
from ccgram.session_protocol.store import ProtocolStore


def test_pair_command_generates_shared_persistent_code_and_qr(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "pair",
            "--url",
            "https://mac.example.ts.net",
            "--config-dir",
            str(tmp_path),
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["protocol"] == "ccgram/1" and len(payload["code"]) == 43
    store = ProtocolStore(tmp_path / "protocol.sqlite3")
    assert store.pair(payload["code"], "phone")["fingerprint"] == payload["fingerprint"]
    store.close()
    qr = runner.invoke(
        cli,
        ["pair", "--url", "https://mac.example.ts.net", "--config-dir", str(tmp_path)],
    )
    assert qr.exit_code == 0, qr.output
    assert len(qr.output.splitlines()) > 10


@pytest.mark.parametrize(
    "url",
    [
        "http://public.example",
        "https://user:password@example.test",
        "https://example.test/v1",
        "https://example.test?token=x",
    ],
)
def test_pair_rejects_invalid_origins(tmp_path, url):
    result = CliRunner().invoke(
        cli, ["pair", "--url", url, "--config-dir", str(tmp_path), "--json"]
    )
    assert result.exit_code != 0
    assert not (tmp_path / "protocol.sqlite3").exists()


async def test_api_is_disabled_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("CCGRAM_API_PORT", raising=False)
    assert await start_if_enabled(AsyncMock(), tmp_path / "unused", "", "test:") is None
    assert not (tmp_path / "unused").exists()


async def test_runtime_lock_and_shutdown_are_real(
    tmp_path, monkeypatch, unused_tcp_port
):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setenv("CCGRAM_API_PORT", str(unused_tcp_port))
    mux = AsyncMock()
    mux.capabilities = SimpleNamespace(name="test")
    mux.list_windows_for_reconciliation.return_value = []
    runtime = await start_if_enabled(mux, tmp_path / "state", "", "test:")
    assert runtime is not None and runtime.task is not None
    try:
        with pytest.raises(RuntimeError, match="already using"):
            await start_if_enabled(mux, tmp_path / "other-state", "", "test:")
        for _ in range(300):
            runtime.observe_hook(HookEvent("Stop", "test:@1", "conv", {}, 0))
        assert runtime.hooks.qsize() == 256
    finally:
        await runtime.close()
    assert runtime.task.done()
