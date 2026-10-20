"""Projeto sintético para os testes das etapas (LocalStorage + LocalWarehouse)."""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from dataeng_core.base.errors import LoadError
from dataeng_core.base.params import Environment
from dataeng_core.base.pipeline import Pipeline
from dataeng_core.steps.context import StepContext

RID1 = "20260101T060000-aaaaaa"
RID2 = "20260101T120000-bbbbbb"  # mesmo dia de RID1
RID3 = "20260102T060000-cccccc"  # dia seguinte

BRONZE = "gs://bronze-teste/bronze/vendas"
TABLE = "silver.vendas"
QUARANTINE = "silver.vendas_rejeitados"
CONTROL = "silver._controle_execucoes"

ORIGEM_SCHEMA = """\
colunas:
  - {nome: id, tipo: texto, obrigatoria: true}
  - {nome: produto, tipo: texto}
  - {nome: valor, tipo: texto}
"""

DESTINO_SCHEMA = """\
colunas:
  - {nome: id, tipo: texto, obrigatoria: true}
  - {nome: produto, tipo: texto}
  - {nome: valor, tipo: decimal}
"""

# 1 e 2 válidas; 3 com valor inválido; id nulo; linha com coluna a mais; 2 duplicada.
CSV = "id;produto;valor\n1;gasolina;5,50\n2;etanol;3,99\n3;diesel;abc\n;gnv;4,00\n4;extra;1;2\n2;etanol;3,99\n"

TRANSFORM: list[dict[str, Any]] = [
    {"tipar": {"valor": {"tipo": "decimal", "separador_decimal": ","}}},
    {"rejeitar_se": [{"coluna": "id", "regra": "nao_nulo"}]},
    {"deduplicar": ["id"]},
]


def write_project(root: Path, csv: str | bytes = CSV) -> None:
    (root / "schemas").mkdir(parents=True, exist_ok=True)
    (root / "schemas" / "origem.yaml").write_text(ORIGEM_SCHEMA, encoding="utf-8")
    (root / "schemas" / "destino.yaml").write_text(DESTINO_SCHEMA, encoding="utf-8")
    write_source(root, csv)


def write_source(root: Path, csv: str | bytes, name: str = "vendas.csv") -> None:
    (root / "dados").mkdir(parents=True, exist_ok=True)
    data = csv.encode("utf-8") if isinstance(csv, str) else csv
    (root / "dados" / name).write_bytes(data)


def ingest_step(step_id: str = "ingere", **destino: Any) -> dict[str, Any]:
    origem = destino.pop("origem", None) or {
        "tipo": "arquivo",
        "localizacao": "dados/vendas.csv",
        "formato": "csv",
        "delimitador": ";",
        "schema": "schemas/origem.yaml",
    }
    extra = {k: destino.pop(k) for k in ("transformacao", "qualidade") if k in destino}
    return {
        "id": step_id,
        "tipo": "ingestao",
        "origem": origem,
        "destino": {
            "bronze": BRONZE + "/",
            "tabela": TABLE,
            "schema": "schemas/destino.yaml",
            "modo_carga": "overwrite_particao",
            **destino,
        },
        "transformacao": extra.get("transformacao", TRANSFORM),
        "qualidade": extra.get("qualidade", []),
    }


def pipeline(*etapas: dict[str, Any]) -> Pipeline:
    return Pipeline.model_validate(
        {"schema_version": 1, "nome": "teste", "agenda": "0 6 * * 1", "etapas": list(etapas)}
    )


def environment() -> Environment:
    return Environment(
        modo="local",
        projeto_gcp="projeto-teste",
        regiao="regiao-teste",
        bucket_bronze="bronze-teste",
        dataset_silver="silver",
        dataset_gold="gold",
        pasta_local=".local",
    )


def with_run(ctx: StepContext, run_id: str) -> StepContext:
    """O mesmo contexto (mesmo storage e warehouse) com outro `run_id`."""
    return dataclasses.replace(ctx, run_id=run_id, params={**ctx.params, "run_id": run_id})


def rows(ctx: StepContext, sql: str) -> list[tuple[Any, ...]]:
    df = ctx.warehouse.query(sql)
    return [tuple(None if pd.isna(v) else v for v in r) for r in df.itertuples(index=False)]


def control_rows(ctx: StepContext) -> list[tuple[Any, ...]]:
    return rows(
        ctx,
        "SELECT run_id, etapa, status, linhas_lidas, linhas_gravadas, linhas_rejeitadas "
        f"FROM {CONTROL} ORDER BY registrado_em",
    )


class FlakyWarehouse:
    """Delegação para o warehouse real; a carga em `fail_table` falha `times` vezes."""

    def __init__(self, inner: Any, fail_table: str, times: int = 1) -> None:
        self.inner = inner
        self.fail_table = fail_table
        self.remaining = times

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def load(self, table: str, data: pd.DataFrame, **kwargs: Any) -> int:
        if table == self.fail_table and self.remaining > 0:
            self.remaining -= 1
            raise LoadError("falha simulada na carga", operation="warehouse.load")
        return int(self.inner.load(table, data, **kwargs))


def list_bronze(ctx: StepContext, prefix: str = BRONZE + "/") -> list[str]:
    return ctx.storage.list(prefix)


def names(paths: Sequence[str]) -> list[str]:
    return [p.rsplit("/", 1)[-1] for p in paths]
