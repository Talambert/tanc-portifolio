"""Schemas de colunas (`schemas/*.yaml`) usados na leitura e na carga."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import load_yaml

TipoColuna = Literal["texto", "inteiro", "decimal", "data", "booleano", "timestamp"]


class Coluna(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nome: str = Field(min_length=1)
    tipo: TipoColuna
    obrigatoria: bool = False


class SchemaSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    colunas: list[Coluna] = Field(min_length=1)

    @field_validator("colunas")
    @classmethod
    def _unique_names(cls, colunas: list[Coluna]) -> list[Coluna]:
        nomes = [c.nome for c in colunas]
        repetidas = sorted({n for n in nomes if nomes.count(n) > 1})
        if repetidas:
            raise ValueError(f"colunas repetidas: {', '.join(repetidas)}")
        return colunas


def _describe(exc: ValidationError, data: dict[str, Any]) -> str:
    """Local e motivo de cada erro, sem ecoar valores de entrada."""
    colunas = data.get("colunas")
    partes = []
    for err in exc.errors():
        loc = list(err["loc"])
        where = ""
        for item in loc:
            if isinstance(item, int) and where == "colunas" and isinstance(colunas, list) and item < len(colunas):
                nome = colunas[item].get("nome") if isinstance(colunas[item], dict) else None
                where += f"[{item}]" + (f" ({nome})" if nome else "")
            elif isinstance(item, int):
                where += f"[{item}]"
            else:
                where += ("." if where else "") + str(item)
        msg = str(err["msg"]).removeprefix("Value error, ")
        if err["type"] == "extra_forbidden":
            msg = "campo não permitido"
        partes.append(f"{where or '<raiz>'}: {msg}")
    return "; ".join(partes)


def load_schema(path: Path) -> SchemaSpec:
    """Lê e valida um schema; erros viram `ConfigError` com arquivo e campos."""
    data = load_yaml(path)
    try:
        return SchemaSpec.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"arquivo {path}: {_describe(exc, data)}", operation="load_schema") from exc
