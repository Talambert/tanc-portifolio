"""Projeto sintético em disco para os testes de CLI e jobs (config + ambiente local + dados + SQL)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from tests.steps.helpers import ingest_step, write_project

ENV_LOCAL = """\
modo: local
projeto_gcp: projeto-teste
regiao: regiao-teste
bucket_bronze: bronze-teste
dataset_silver: silver
dataset_gold: gold
pasta_local: .local
"""

SQL_MARCA = (
    "-- marca; com ponto e vírgula em texto\n"
    "CREATE OR REPLACE TABLE ${dataset_gold}.marca AS\n"
    "SELECT COUNT(*) AS n, '${rotulo}' AS rotulo, ';' AS sep FROM ${dataset_silver}.vendas;\n"
)


def make_project(
    root: Path, *, sql: str = SQL_MARCA, extra_params: dict[str, str] | None = None, with_sql_step: bool = False
) -> Path:
    """Cria o projeto em `root` e devolve o caminho de `config/pipeline.yaml`.

    Etapas: `ingere2` (declarada primeiro, depende de `ingere`), `ingere` (com `dml_pos_carga` = sql/marca.sql)
    e, com `with_sql_step`, `resumo` (tipo sql, depende de `ingere2`, usa o mesmo arquivo).
    """
    write_project(root)
    (root / "config" / "ambientes").mkdir(parents=True, exist_ok=True)
    (root / "config" / "ambientes" / "local.yaml").write_text(ENV_LOCAL, encoding="utf-8")
    (root / "sql").mkdir(exist_ok=True)
    (root / "sql" / "marca.sql").write_text(sql, encoding="utf-8")
    etapas: list[dict[str, Any]] = [
        {
            **ingest_step("ingere2", bronze="gs://bronze-teste/bronze/vendas2/", tabela="silver.vendas2"),
            "depende_de": ["ingere"],
        },
        {**ingest_step("ingere", dml_pos_carga=["sql/marca.sql"]), "depende_de": []},
    ]
    if with_sql_step:
        etapas.append({"id": "resumo", "tipo": "sql", "sql": ["sql/marca.sql"], "depende_de": ["ingere2"]})
    config: dict[str, Any] = {
        "schema_version": 1,
        "nome": "vendas",
        "agenda": "0 6 * * 1",
        "parametros": {"rotulo": "padrao", **(extra_params or {})},
        "etapas": etapas,
    }
    path = root / "config" / "pipeline.yaml"
    path.write_text(yaml.safe_dump(config, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path
