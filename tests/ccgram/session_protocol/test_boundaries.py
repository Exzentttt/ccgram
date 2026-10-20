import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BASELINE = Path(__file__).parent / "handler_input_baseline.json"


def input_sites(source):
    tree = ast.parse(source)
    return sorted(
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"send", "send_keys", "send_to_pane", "send_keys_to_pane"}
    )


def test_api_and_session_core_have_no_telegram_or_handler_imports():
    for directory in (ROOT / "src/ccgram/api", ROOT / "src/ccgram/session_protocol"):
        for path in directory.glob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom):
                    segments = (node.module or "").split(".")
                    assert "handlers" not in segments and "telegram" not in segments, (
                        path
                    )
                elif isinstance(node, ast.Import):
                    assert all(
                        "telegram" not in n.name.split(".")
                        and "handlers" not in n.name.split(".")
                        for n in node.names
                    ), path


def test_handlers_introduce_no_unmigrated_input_sites():
    actual = {
        str(p.relative_to(ROOT)): sites
        for p in (ROOT / "src/ccgram/handlers").rglob("*.py")
        if (sites := input_sites(p.read_text()))
    }
    assert actual == json.loads(BASELINE.read_text())


def test_input_audit_detects_a_new_direct_send():
    assert input_sites(
        "async def handler(mux):\n    await mux.send_keys('target', 'Enter')\n"
    ) == ["send_keys"]
