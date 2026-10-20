"""Fixtures do template: onde fica o template e como gerar um projeto em `tmp_path`."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from cookiecutter.main import cookiecutter

TEMPLATE_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture
def bake(tmp_path: Path) -> Callable[..., Path]:
    """`bake(**contexto)` gera o projeto em uma pasta nova dentro de `tmp_path` e devolve a raiz dele."""
    counter = {"n": 0}

    def _bake(**contexto: Any) -> Path:
        counter["n"] += 1
        out = tmp_path / f"out{counter['n']}"
        out.mkdir()
        nome = contexto.setdefault("nome_projeto", "meu-pipeline")
        cookiecutter(str(TEMPLATE_DIR), no_input=True, output_dir=str(out), extra_context=contexto)
        return out / nome

    return _bake
