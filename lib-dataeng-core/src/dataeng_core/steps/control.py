"""Tabela de controle `<dataset_silver>._controle_execucoes`: uma linha por execução de etapa.

A tabela é um registro de auditoria (só acrescenta): reexecutar o mesmo `run_id` acrescenta outra
linha, e o estado atual de uma etapa é a linha mais recente (`registrado_em`). Assim uma falha seguida
de uma reexecução bem-sucedida aparece como "falha" e depois "ok", e nada é reescrito.
"""

from __future__ import annotations

import json
from dataclasses import asdict

import pandas as pd

from dataeng_core.base.params import Environment
from dataeng_core.steps.context import StepResult
from dataeng_core.warehouse.base import Warehouse, validate_identifier

CONTROL_TABLE = "_controle_execucoes"
CONTROL_COLUMNS = [
    "run_id",
    "etapa",
    "status",
    "linhas_lidas",
    "linhas_gravadas",
    "linhas_rejeitadas",
    "backlog",
    "qualidade",
    "detalhe",
    "registrado_em",
]


_last_stamp: list[pd.Timestamp] = []


def _stamp() -> pd.Timestamp:
    """Agora (UTC), estritamente crescente no processo: o relógio do Windows pode repetir o instante."""
    now = pd.Timestamp.now(tz="UTC").floor("us")
    if _last_stamp and now <= _last_stamp[0]:
        now = _last_stamp[0] + pd.Timedelta(microseconds=1)
    _last_stamp[:] = [now]
    return now


def control_table(env: Environment) -> str:
    dataset = validate_identifier(env.dataset_silver, what="dataset_silver", operation="record_result")
    return f"{dataset}.{CONTROL_TABLE}"


def record_result(warehouse: Warehouse, env: Environment, result: StepResult, run_id: str) -> None:
    """Acrescenta o resultado da etapa à tabela de controle.

    `qualidade` é uma lista JSON com o resultado de cada verificação. O `detail` deve chegar já sem
    segredos (quem monta o resultado aplica `redact`).
    """
    qualidade = json.dumps([asdict(q) for q in result.quality], ensure_ascii=False)
    row = pd.DataFrame(
        {
            "run_id": pd.Series([run_id], dtype="string"),
            "etapa": pd.Series([result.step_id], dtype="string"),
            "status": pd.Series([result.status], dtype="string"),
            "linhas_lidas": pd.Series([result.rows_read], dtype="Int64"),
            "linhas_gravadas": pd.Series([result.rows_written], dtype="Int64"),
            "linhas_rejeitadas": pd.Series([result.rows_rejected], dtype="Int64"),
            "backlog": pd.Series([result.backlog], dtype="Int64"),
            "qualidade": pd.Series([qualidade], dtype="string"),
            "detalhe": pd.Series([result.detail], dtype="string"),
            "registrado_em": pd.Series([_stamp()]).astype("datetime64[us, UTC]"),
        }
    )
    warehouse.load(control_table(env), row, mode="append")
