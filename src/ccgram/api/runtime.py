"""Optional mobile API lifecycle inside the existing Telegram runtime."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import logging
import os
from pathlib import Path
from typing import IO

from aiohttp import web

from ..multiplexer.base import Multiplexer
from ..providers.base import HookEvent
from ..session_protocol.backend import MultiplexerSessionBackend
from ..session_protocol.models import ProtocolError
from ..session_protocol.service import LocalSessionService
from ..session_protocol.store import ProtocolStore
from .server import start_server

logger = logging.getLogger(__name__)
_MAX_PORT = 65535


class ProtocolRuntime:
    def __init__(
        self, store: ProtocolStore, service: LocalSessionService, lock: IO[bytes]
    ) -> None:
        self.store = store
        self.service = service
        self.lock = lock
        self.runner: web.AppRunner | None = None
        self.task: asyncio.Task | None = None
        self.hooks: asyncio.Queue[HookEvent] = asyncio.Queue(256)

    def observe_hook(self, event: HookEvent) -> None:
        # A remote observer never blocks the Telegram monitor or its receipts.
        with contextlib.suppress(asyncio.QueueFull):
            self.hooks.put_nowait(event)

    async def poll(self, prefix: str) -> None:
        while True:
            try:
                await self.service.refresh()
                while not self.hooks.empty():
                    await self.service.observe_hook(self.hooks.get_nowait(), prefix)
            except ProtocolError, OSError:
                logger.warning("Session protocol source unavailable", exc_info=True)
            await asyncio.sleep(1)

    async def close(self) -> None:
        try:
            if self.runner is not None:
                await self.runner.cleanup()
        finally:
            try:
                if self.task is not None:
                    self.task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self.task
            finally:
                self.store.close()
                self.lock.close()


async def start_if_enabled(
    multiplexer: Multiplexer,
    directory: Path,
    command_dir: str,
    prefix: str,
    *,
    recovered_targets: list[tuple[str, str]] | None = None,
) -> ProtocolRuntime | None:
    """Disabled unless CCGRAM_API_PORT is set. Never bind an external address."""
    configured = os.getenv("CCGRAM_API_PORT")
    if not configured:
        return None
    port = int(configured)
    if not 1 <= port <= _MAX_PORT:
        raise ValueError("CCGRAM_API_PORT must be between 1 and 65535")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock_dir = Path.home() / ".cache" / "ccgram" / "locks"
    lock_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock_path = lock_dir / f"protocol-{multiplexer.capabilities.name}.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    lock = os.fdopen(fd, "a+b")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        lock.close()
        raise RuntimeError(
            "A mobile API is already using this multiplexer backend"
        ) from exc
    store = ProtocolStore(directory / "protocol.sqlite3")
    for previous, current in recovered_targets or []:
        store.retarget(previous, current)
    service = LocalSessionService(
        MultiplexerSessionBackend(multiplexer, command_dir), store
    )
    runtime = ProtocolRuntime(store, service, lock)
    hosts = frozenset(
        h.strip().lower()
        for h in os.getenv(
            "CCGRAM_API_HOSTS", f"127.0.0.1:{port},localhost:{port}"
        ).split(",")
        if h.strip()
    )
    origins = frozenset(
        o.strip() for o in os.getenv("CCGRAM_API_ORIGINS", "").split(",") if o.strip()
    )
    try:
        await service.refresh()
        # Recover only fresh observations. Old hooks never recreate approvals.
        # Lazy: hook-log I/O is needed only when the optional API starts.
        from ..event_reader import read_new_events

        events_file = directory / "events.jsonl"
        offset = (
            max(0, events_file.stat().st_size - 65536) if events_file.exists() else 0
        )
        events, _ = await read_new_events(events_file, offset)
        for event in events:
            await service.observe_hook(event, prefix)
        await service.refresh()
        runtime.runner = await start_server(
            service, store, port=port, allowed_hosts=hosts, allowed_origins=origins
        )
        runtime.task = asyncio.create_task(
            runtime.poll(prefix), name="ccgram-session-protocol"
        )
    except BaseException:
        await runtime.close()
        raise
    return runtime
