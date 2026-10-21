import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
for path in (ROOT, ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    """Cada teste usa o próprio arquivo de estado, para nada vazar de um teste para outro."""
    monkeypatch.setenv("GOVERNANCA_STATE_FILE", str(tmp_path / "estado.json"))
    return tmp_path / "estado.json"
