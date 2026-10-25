import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

from ccgram.api.runtime import start_if_enabled
from ccgram.cli import cli
from ccgram.providers.base import HookEvent
from ccgram.session_protocol import runtime as runtime_module
from ccgram.session_protocol.runtime import _lock_name, start_runtime
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


async def test_core_is_available_when_api_is_disabled(tmp_path, monkeypatch):
    monkeypatch.delenv("CCGRAM_API_PORT", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    mux = AsyncMock()
    mux.capabilities = SimpleNamespace(name="test")
    mux.list_windows_for_reconciliation.return_value = []

    runtime = await start_runtime(mux, tmp_path / "state", "", "test:")
    try:
        cleanup = await start_if_enabled(runtime.service, runtime.store)
        assert cleanup is None
        assert runtime.task is not None
        assert runtime.service.store is runtime.store
        assert (tmp_path / "state" / "protocol.sqlite3").exists()
    finally:
        await runtime.close()
    assert runtime.task.done()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        _ = runtime.store.fingerprint


async def test_runtime_lock_and_shutdown_are_real(
    tmp_path, monkeypatch, unused_tcp_port
):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setenv("CCGRAM_API_PORT", str(unused_tcp_port))
    mux = AsyncMock()
    mux.capabilities = SimpleNamespace(name="test")
    mux.list_windows_for_reconciliation.return_value = []
    runtime = await start_runtime(mux, tmp_path / "state", "", "test:")
    assert runtime is not None and runtime.task is not None
    try:
        runtime.set_listener_cleanup(
            await start_if_enabled(runtime.service, runtime.store)
        )
        with pytest.raises(RuntimeError, match="already using"):
            await start_runtime(mux, tmp_path / "other-state", "", "test:")
        for _ in range(300):
            runtime.observe_hook(HookEvent("Stop", "test:@1", "conv", {}, 0))
        assert runtime.hooks.qsize() == 256
    finally:
        await runtime.close()
    assert runtime.task.done()


def _mux(name: str = "tmux", session: str = "work") -> AsyncMock:
    mux = AsyncMock()
    mux.capabilities = SimpleNamespace(name=name)
    mux.session_name = session
    mux.list_windows_for_reconciliation.return_value = []
    return mux


@pytest.fixture
def session_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setenv("TMUX", f"{tmp_path}/tmux-sock,1,0")
    monkeypatch.setenv("CCGRAM_PROTOCOL_LOCK_SCOPE", "session")


async def test_default_lock_is_backend_wide_across_config_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.delenv("CCGRAM_PROTOCOL_LOCK_SCOPE", raising=False)
    first = await start_runtime(_mux(session="a"), tmp_path / "a", "", "a:")
    try:
        with pytest.raises(RuntimeError, match="already using"):
            await start_runtime(_mux(session="b"), tmp_path / "b", "", "b:")
    finally:
        await first.close()


async def test_session_scope_allows_different_tmux_sessions(tmp_path, session_scope):
    first = await start_runtime(_mux(session="work"), tmp_path / "a", "", "a:")
    try:
        second = await start_runtime(_mux(session="personal"), tmp_path / "b", "", "b:")
        await second.close()
    finally:
        await first.close()


async def test_session_scope_conflicts_on_same_tmux_session(tmp_path, session_scope):
    first = await start_runtime(_mux(session="work"), tmp_path / "a", "", "a:")
    try:
        with pytest.raises(RuntimeError, match="already using"):
            await start_runtime(_mux(session="work"), tmp_path / "b", "", "b:")
    finally:
        await first.close()


async def test_session_scope_allows_same_session_on_other_server(
    tmp_path, session_scope, monkeypatch
):
    first = await start_runtime(_mux(session="work"), tmp_path / "a", "", "a:")
    try:
        monkeypatch.setenv("TMUX", f"{tmp_path}/other-sock,1,0")
        second = await start_runtime(_mux(session="work"), tmp_path / "b", "", "b:")
        await second.close()
    finally:
        await first.close()


async def test_session_scope_is_ignored_on_non_tmux_backend(tmp_path, session_scope):
    first = await start_runtime(_mux("herdr", "work"), tmp_path / "a", "", "a:")
    try:
        with pytest.raises(RuntimeError, match="already using"):
            await start_runtime(_mux("herdr", "personal"), tmp_path / "b", "", "b:")
    finally:
        await first.close()


def test_session_scope_key_is_canonical_across_tmux_env_forms(
    tmp_path, session_scope, monkeypatch
):
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    uid = os.getuid()
    monkeypatch.setenv("TMUX", f"{real}/tmux-{uid}/default,1,0")
    inside = _lock_name(_mux())

    monkeypatch.delenv("TMUX")
    monkeypatch.setenv("TMUX_TMPDIR", str(link))
    assert _lock_name(_mux()) == inside


def test_session_scope_key_treats_empty_tmux_tmpdir_as_unset(
    session_scope, monkeypatch
):
    uid = os.getuid()
    monkeypatch.setenv("TMUX", f"/tmp/tmux-{uid}/default,1,0")
    inside = _lock_name(_mux())

    monkeypatch.delenv("TMUX")
    monkeypatch.setenv("TMUX_TMPDIR", "")
    assert _lock_name(_mux()) == inside


async def test_store_constructor_failure_releases_lock_with_retained_traceback(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    mux = AsyncMock()
    mux.capabilities = SimpleNamespace(name="test")
    mux.list_windows_for_reconciliation.return_value = []

    with monkeypatch.context() as scoped:

        def fail_store(_path):
            raise RuntimeError("store construction failed")

        scoped.setattr(runtime_module, "ProtocolStore", fail_store)
        with pytest.raises(RuntimeError, match="store construction failed") as caught:
            await start_runtime(mux, tmp_path / "state", "", "test:")
        retained_error = caught.value

    runtime = await start_runtime(mux, tmp_path / "state", "", "test:")
    try:
        assert runtime.task is not None
        assert retained_error.__traceback__ is not None
    finally:
        await runtime.close()


async def test_service_constructor_failure_closes_store_and_releases_lock(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    mux = AsyncMock()
    mux.capabilities = SimpleNamespace(name="test")
    mux.list_windows_for_reconciliation.return_value = []
    stores = []

    with monkeypatch.context() as scoped:

        def fail_service(_backend, store):
            stores.append(store)
            raise RuntimeError("service construction failed")

        scoped.setattr(runtime_module, "LocalSessionService", fail_service)
        with pytest.raises(RuntimeError, match="service construction failed") as caught:
            await start_runtime(mux, tmp_path / "state", "", "test:")
        retained_error = caught.value

    assert retained_error.__traceback__ is not None
    assert len(stores) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        _ = stores[0].fingerprint

    runtime = await start_runtime(mux, tmp_path / "state", "", "test:")
    await runtime.close()


async def test_retarget_failure_closes_store_and_releases_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    mux = AsyncMock()
    mux.capabilities = SimpleNamespace(name="test")
    mux.list_windows_for_reconciliation.return_value = []
    stores = []

    with monkeypatch.context() as scoped:

        def failing_store(path):
            store = ProtocolStore(path)
            stores.append(store)

            def fail_retarget(_previous, _current):
                raise RuntimeError("target recovery failed")

            scoped.setattr(store, "retarget", fail_retarget)
            return store

        scoped.setattr(runtime_module, "ProtocolStore", failing_store)
        with pytest.raises(RuntimeError, match="target recovery failed") as caught:
            await start_runtime(
                mux,
                tmp_path / "state",
                "",
                "test:",
                recovered_targets=[("old", "new")],
            )
        retained_error = caught.value

    assert retained_error.__traceback__ is not None
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        _ = stores[0].fingerprint
    runtime = await start_runtime(mux, tmp_path / "state", "", "test:")
    await runtime.close()
