"""Bateria de contrato reutilizável para qualquer implementação de Warehouse."""

from __future__ import annotations

import pandas as pd
import pytest

from dataeng_core.base.errors import ConfigError, LoadError
from dataeng_core.warehouse.base import Warehouse


def _frame(rows: list[tuple[int, str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["id", "nome", "dia"])


def _sorted(df: pd.DataFrame) -> list[tuple[int, str, str]]:
    return sorted((int(r.id), str(r.nome), str(r.dia)) for r in df.itertuples())


def run_warehouse_contract(
    wh: Warehouse,
    dataset: str = "contrato_ds",
    *,
    int_type: str = "INTEGER",
    text_type: str = "VARCHAR",
) -> None:
    """Executa o contrato em tabelas `<dataset>.contrato_*` e remove o que criou.

    `int_type` e `text_type` adaptam o DDL ao dialeto (ex.: INT64 e STRING no BigQuery).
    O dataset deve existir (ou ser criável pela implementação).
    """
    t_append = f"{dataset}.contrato_append"
    t_part = f"{dataset}.contrato_part"
    t_merge = f"{dataset}.contrato_merge"
    t_ddl = f"{dataset}.contrato_ddl"
    view = f"{dataset}.contrato_view"
    ddl_cols = f"id {int_type}, nome {text_type}, dia {text_type}"
    try:
        # execute_ddl cria a tabela; table_exists / row_count
        assert wh.table_exists(t_ddl) is False
        wh.execute_ddl(f"CREATE TABLE {t_ddl} ({ddl_cols})")
        assert wh.table_exists(t_ddl) is True
        assert wh.row_count(t_ddl) == 0

        # run_sql executa vários comandos em ordem
        wh.run_sql(
            [
                f"INSERT INTO {t_ddl} VALUES (1, 'a', '2026-01-01')",
                f"INSERT INTO {t_ddl} VALUES (2, 'b', '2026-01-02')",
            ]
        )
        assert wh.row_count(t_ddl) == 2

        # query devolve DataFrame
        out = wh.query(f"SELECT id, nome, dia FROM {t_ddl}")
        assert isinstance(out, pd.DataFrame)
        assert _sorted(out) == [(1, "a", "2026-01-01"), (2, "b", "2026-01-02")]

        # view criada e substituída
        wh.create_or_replace_view(view, f"SELECT id FROM {t_ddl} WHERE id = 1")
        assert wh.table_exists(view) is True
        assert [int(v) for v in wh.query(f"SELECT id FROM {view}")["id"]] == [1]
        wh.create_or_replace_view(view, f"SELECT id FROM {t_ddl} WHERE id = 2")
        assert [int(v) for v in wh.query(f"SELECT id FROM {view}")["id"]] == [2]

        # append soma (e cria a tabela se não existir)
        assert wh.load(t_append, _frame([(1, "a", "d1"), (2, "b", "d1")]), mode="append") == 2
        assert wh.load(t_append, _frame([(3, "c", "d2")]), mode="append") == 1
        assert wh.row_count(t_append) == 3
        # reexecutar append duplica (não é idempotente por desenho)
        wh.load(t_append, _frame([(3, "c", "d2")]), mode="append")
        assert wh.row_count(t_append) == 4

        # overwrite_partition troca só a partição dos valores presentes
        wh.load(t_part, _frame([(1, "a", "d1"), (2, "b", "d1"), (3, "c", "d2")]), mode="append")
        novo = _frame([(10, "x", "d1"), (11, "y", "d1"), (12, "z", "d1")])
        assert wh.load(t_part, novo, mode="overwrite_partition", partition_column="dia") == 3
        assert _sorted(wh.query(f"SELECT * FROM {t_part}")) == [
            (3, "c", "d2"),
            (10, "x", "d1"),
            (11, "y", "d1"),
            (12, "z", "d1"),
        ]
        # reexecutar a mesma carga mantém a contagem
        wh.load(t_part, novo, mode="overwrite_partition", partition_column="dia")
        assert wh.row_count(t_part) == 4

        # merge atualiza chaves existentes e insere as novas, sem duplicar
        wh.load(t_merge, _frame([(1, "a", "d1"), (2, "b", "d1")]), mode="append")
        carga = _frame([(2, "B2", "d1"), (3, "c", "d2")])
        assert wh.load(t_merge, carga, mode="merge", merge_keys=["id"]) == 2
        assert _sorted(wh.query(f"SELECT * FROM {t_merge}")) == [(1, "a", "d1"), (2, "B2", "d1"), (3, "c", "d2")]
        wh.load(t_merge, carga, mode="merge", merge_keys=["id"])
        assert wh.row_count(t_merge) == 3

        # merge sem chaves e overwrite_partition sem coluna levantam erro antes de gravar
        with pytest.raises(LoadError):
            wh.load(t_merge, carga, mode="merge")
        with pytest.raises(LoadError):
            wh.load(t_part, novo, mode="overwrite_partition")
        assert wh.row_count(t_merge) == 3

        # entradas inválidas: ConfigError, nunca exceção crua
        with pytest.raises(ConfigError):
            wh.load(t_merge, carga, mode="inexistente")  # type: ignore[arg-type]
        with pytest.raises(ConfigError):
            wh.load("tabela; DROP TABLE x", carga, mode="append")
        with pytest.raises(ConfigError):
            wh.row_count("a.b.c")
        with pytest.raises(ConfigError):
            wh.table_exists("")
    finally:
        wh.run_sql([f"DROP VIEW IF EXISTS {view}"])
        for name in (t_append, t_part, t_merge, t_ddl):
            wh.run_sql([f"DROP TABLE IF EXISTS {name}"])
