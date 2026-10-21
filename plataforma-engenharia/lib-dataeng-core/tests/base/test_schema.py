from pathlib import Path

import pytest

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.schema import Coluna, SchemaSpec, load_schema


def _write(tmp_path: Path, text: str) -> Path:
    f = tmp_path / "schema.yaml"
    f.write_text(text, encoding="utf-8")
    return f


def test_load_schema_reads_columns(tmp_path: Path) -> None:
    f = _write(
        tmp_path,
        "colunas:\n  - {nome: cnpj, tipo: texto, obrigatoria: true}\n  - {nome: valor, tipo: decimal}\n",
    )
    spec = load_schema(f)
    assert spec == SchemaSpec(
        colunas=[Coluna(nome="cnpj", tipo="texto", obrigatoria=True), Coluna(nome="valor", tipo="decimal")]
    )


def test_schema_loader_rejects_unknown_type(tmp_path: Path) -> None:
    f = _write(tmp_path, "colunas:\n  - {nome: valor, tipo: dinheiro}\n")
    with pytest.raises(ConfigError) as exc:
        load_schema(f)
    msg = str(exc.value)
    assert "schema.yaml" in msg
    assert "colunas[0] (valor).tipo" in msg
    for tipo in ("texto", "inteiro", "decimal", "data", "booleano", "timestamp"):
        assert tipo in msg


def test_schema_loader_rejects_duplicate_columns(tmp_path: Path) -> None:
    f = _write(tmp_path, "colunas:\n  - {nome: a, tipo: texto}\n  - {nome: a, tipo: inteiro}\n")
    with pytest.raises(ConfigError, match="repetid"):
        load_schema(f)


def test_schema_loader_rejects_unknown_field(tmp_path: Path) -> None:
    f = _write(tmp_path, "colunas:\n  - {nome: a, tipo: texto, obrigatorio: true}\n")
    with pytest.raises(ConfigError, match="obrigatorio"):
        load_schema(f)
