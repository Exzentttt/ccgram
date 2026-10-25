"""Import boundaries between neutral event observation and application adapters."""

import ast
from importlib.util import resolve_name
from pathlib import Path

import pytest

_SOURCE = Path(__file__).resolve().parents[2] / "src" / "ccgram"


def _forbidden_imports(source: str, forbidden: tuple[str, ...]) -> list[str]:
    violations = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                base = resolve_name("." * node.level + base, "ccgram")
            names = [base, *(f"{base}.{alias.name}" for alias in node.names)]
        else:
            continue
        violations.extend(
            f"{node.lineno}: {name}" for name in names if name.startswith(forbidden)
        )
    return violations


@pytest.mark.parametrize(
    ("module", "forbidden"),
    [
        (
            "event_stream_monitor",
            (
                "ccgram.handlers",
                "ccgram.telegram",
                "ccgram.thread_router",
                "ccgram.bot",
                "telegram",
            ),
        ),
        ("bot", ("ccgram.main",)),
    ],
)
def test_runtime_dependency_direction(module, forbidden):
    source = (_SOURCE / f"{module}.py").read_text()
    violations = _forbidden_imports(source, forbidden)
    assert not violations, f"{module}.py: " + "\n".join(violations)


@pytest.mark.parametrize(
    ("source", "forbidden", "rejected"),
    [
        ("import ccgram.main as runner", ("ccgram.main",), True),
        ("from .main import _shutdown_signal", ("ccgram.main",), True),
        ("def callback():\n    from . import main", ("ccgram.main",), True),
        ("from ccgram import main as runner", ("ccgram.main",), True),
        ("from .handlers import polling", ("ccgram.handlers",), True),
        ("import telegram", ("telegram",), True),
        ("from .telegram_client import TelegramClient", ("ccgram.telegram",), True),
        ("from .multiplexer import base", ("ccgram.handlers", "telegram"), False),
    ],
)
def test_audit_rejects_planted_imports(source, forbidden, rejected):
    assert bool(_forbidden_imports(source, forbidden)) is rejected
