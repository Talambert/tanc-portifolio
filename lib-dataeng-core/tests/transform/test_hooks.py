from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

from dataeng_core.base.errors import ConfigError
from dataeng_core.transform.hooks import resolve_hook


def _write(root: Path, rel: str, body: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def test_resolves_function_from_module_and_package(tmp_path: Path) -> None:
    _write(tmp_path, "custom/__init__.py", "")
    _write(tmp_path, "custom/hooks.py", "def f(df):\n    return df.assign(x=1)\n")
    _write(tmp_path, "solto.py", "def g(df):\n    return df.assign(y=2)\n")
    _write(tmp_path, "pacote/__init__.py", "def h(df):\n    return df.assign(z=3)\n")
    df = pd.DataFrame({"a": [1]})
    assert resolve_hook("custom.hooks:f", project_root=tmp_path)(df)["x"].tolist() == [1]
    assert resolve_hook("solto:g", project_root=tmp_path)(df)["y"].tolist() == [2]
    assert resolve_hook("pacote:h", project_root=tmp_path)(df)["z"].tolist() == [3]


def test_hook_module_can_import_siblings_and_project_root_is_on_sys_path(tmp_path: Path) -> None:
    _write(tmp_path, "custom/__init__.py", "")
    _write(tmp_path, "custom/util.py", "K = 7\n")
    _write(tmp_path, "custom/hooks.py", "from custom.util import K\ndef f(df):\n    return df.assign(k=K)\n")
    fn = resolve_hook("custom.hooks:f", project_root=tmp_path)
    assert fn(pd.DataFrame({"a": [1]}))["k"].tolist() == [7]
    assert sys.path[0] == str(tmp_path)


def test_same_module_name_in_different_projects_does_not_collide(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    _write(a, "h.py", "def f(df):\n    return df.assign(v='a')\n")
    _write(b, "h.py", "def f(df):\n    return df.assign(v='b')\n")
    df = pd.DataFrame({"x": [1]})
    assert resolve_hook("h:f", project_root=a)(df)["v"].tolist() == ["a"]
    assert resolve_hook("h:f", project_root=b)(df)["v"].tolist() == ["b"]


def test_missing_hook_names_module_and_function(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as exc:
        resolve_hook("custom.hooks:f", project_root=tmp_path)
    msg = str(exc.value)
    assert "custom.hooks" in msg and "'f'" in msg and "custom/hooks.py" in msg
    _write(tmp_path, "custom/hooks.py", "def outra(df):\n    return df\n")
    with pytest.raises(ConfigError) as exc:
        resolve_hook("custom.hooks:f", project_root=tmp_path)
    assert "'f'" in str(exc.value) and "custom.hooks" in str(exc.value) and "outra" in str(exc.value)


def test_invalid_reference_format(tmp_path: Path) -> None:
    for ref in ("semdoispontos", "a:b:c", ":f", "m:", "m.:f", "../x:f", "m:1f"):
        with pytest.raises(ConfigError, match="modulo:funcao"):
            resolve_hook(ref, project_root=tmp_path)


def test_not_callable_and_wrong_signature(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "h.py",
        "X = 3\ndef zero():\n    return 1\ndef dois(a, b):\n    return a\ndef varargs(*a):\n    return a[0]\n",
    )
    with pytest.raises(ConfigError, match="não é uma função"):
        resolve_hook("h:X", project_root=tmp_path)
    with pytest.raises(ConfigError, match="DataFrame"):
        resolve_hook("h:zero", project_root=tmp_path)
    with pytest.raises(ConfigError, match="DataFrame"):
        resolve_hook("h:dois", project_root=tmp_path)
    assert resolve_hook("h:varargs", project_root=tmp_path) is not None


def test_import_error_in_module_is_config_error_and_leaves_no_residue(tmp_path: Path) -> None:
    _write(tmp_path, "quebrado.py", "import modulo_que_nao_existe\n")
    _write(tmp_path, "sintaxe.py", "def (:\n")
    antes = set(sys.modules)
    antes_path = list(sys.path)
    with pytest.raises(ConfigError, match="quebrado"):
        resolve_hook("quebrado:f", project_root=tmp_path)
    with pytest.raises(ConfigError, match="sintaxe"):
        resolve_hook("sintaxe:f", project_root=tmp_path)
    assert not (set(sys.modules) - antes)
    assert sys.path == antes_path


def test_hook_function_is_not_executed_on_resolve(tmp_path: Path) -> None:
    marcador = tmp_path / "rodou.txt"
    _write(tmp_path, "h.py", f"def f(df):\n    open({str(marcador)!r}, 'w').write('x')\n    return df\n")
    resolve_hook("h:f", project_root=tmp_path)
    assert not marcador.exists()


def test_module_path_cannot_escape_project_root(tmp_path: Path) -> None:
    root = tmp_path / "proj"
    root.mkdir()
    _write(tmp_path / "fora", "x.py", "def f(df):\n    return df\n")
    with pytest.raises(ConfigError):
        resolve_hook("..fora.x:f", project_root=root)


def test_project_with_stdlib_named_files_does_not_evict_stdlib_modules(tmp_path: Path) -> None:
    import types

    _write(tmp_path, "types.py", "X = 1\n")
    _write(tmp_path, "test/__init__.py", "")
    _write(tmp_path, "h.py", "def f(df):\n    return df\n")
    antes = sys.modules["types"]
    resolve_hook("h:f", project_root=tmp_path)
    assert sys.modules["types"] is antes is types
