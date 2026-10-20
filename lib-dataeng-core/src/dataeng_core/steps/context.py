"""Contexto de execução das etapas e resultado de cada etapa."""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import Environment
from dataeng_core.base.pipeline import Pipeline
from dataeng_core.base.runtime import new_run_id
from dataeng_core.pubsub.base import PubSubClient, make_pubsub
from dataeng_core.quality.checks import CheckResult
from dataeng_core.storage.base import Storage, make_storage
from dataeng_core.warehouse.base import Warehouse, make_warehouse

StepStatus = Literal["ok", "falha", "nao_executada"]

# Vai para caminhos do bronze e para a tabela de controle: só caracteres seguros em GCS, disco e SQL.
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_RUN_ID_DATE = re.compile(r"(\d{4})(\d{2})(\d{2})")


@dataclass
class StepResult:
    step_id: str
    status: StepStatus
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    backlog: int | None = None
    quality: list[CheckResult] = field(default_factory=list)
    detail: str = ""


@dataclass
class StepContext:
    pipeline: Pipeline
    env: Environment
    storage: Storage
    warehouse: Warehouse
    pubsub: PubSubClient | None
    run_id: str
    params: dict[str, str]
    project_root: Path


def validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id):
        raise ConfigError(
            "run_id inválido: use de 1 a 128 caracteres entre letras, dígitos, '_', '.' e '-', "
            "começando por letra ou dígito.",
            operation="build_context",
        )
    return run_id


def resolve_ingestion_date(run_id: str, ingestion_date: dt.date | None) -> dt.date:
    """`ingestion_date` explícita, senão a data no início do `run_id` (`AAAAMMDD...`, como `new_run_id`).

    Sem nenhuma das duas é erro: cair na data de hoje faria a reexecução do mesmo run em outro dia
    gravar outra partição (duplicando os dados).
    """
    if ingestion_date is not None:
        if not isinstance(ingestion_date, dt.date) or isinstance(ingestion_date, dt.datetime):
            raise ConfigError("ingestion_date deve ser uma data (datetime.date).", operation="build_context")
        return ingestion_date
    m = _RUN_ID_DATE.match(run_id)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    raise ConfigError(
        f"o run_id '{run_id}' não contém data (AAAAMMDD...). Use new_run_id() ou informe ingestion_date "
        "explicitamente.",
        operation="build_context",
    )


def _uses_pubsub(pipeline: Pipeline) -> bool:
    return any(getattr(getattr(s, "origem", None), "tipo", None) == "pubsub" for s in pipeline.etapas)


def build_context(
    pipeline: Pipeline,
    env: Environment,
    *,
    project_root: Path,
    run_id: str | None = None,
    ingestion_date: dt.date | None = None,
    pubsub: PubSubClient | None = None,
) -> StepContext:
    """Monta o contexto: storage e warehouse do ambiente, `run_id` novo (ou o informado) e parâmetros.

    No modo local, `pasta_local` relativa é resolvida a partir da raiz do projeto (não do diretório atual).
    Os parâmetros são os do pipeline (já com os do ambiente) mais `run_id` e `ingestion_date` (AAAA-MM-DD,
    a partição `_ingestion_date` e a pasta do bronze; ver `resolve_ingestion_date`).

    Para T13/T16 (jobs e DAGs): passe um `run_id` ESTÁVEL entre tentativas e que contenha a data
    (formato de `new_run_id`, `AAAAMMDDTHHMMSS-xxxxxx`), ou informe `ingestion_date`. O run_id nativo do
    Airflow (ex.: `scheduled__2026-01-01T06:00:00+00:00`) é recusado por `validate_run_id`; a DAG deve
    mapeá-lo para esse formato (ou passar `ingestion_date` explícita).

    Cliente Pub/Sub: o `pubsub` informado (útil em testes, com um `LocalPubSub` já alimentado); senão,
    `make_pubsub(env)` SÓ quando alguma etapa usa a origem `pubsub` (nada de GCP importado sem
    necessidade). No modo local, `make_pubsub` cria uma fila em memória vazia: a etapa termina `ok` com
    0 linhas.
    """
    root = Path(project_root).resolve()
    rid = validate_run_id(run_id) if run_id is not None else new_run_id()
    day = resolve_ingestion_date(rid, ingestion_date)
    if env.modo == "local" and not Path(env.pasta_local).is_absolute():
        env = env.model_copy(update={"pasta_local": str(root / env.pasta_local)})
    params = {**env.params(), **pipeline.parametros, "run_id": rid, "ingestion_date": day.isoformat()}
    return StepContext(
        pipeline=pipeline,
        env=env,
        storage=make_storage(env),
        warehouse=make_warehouse(env),
        pubsub=pubsub if pubsub is not None else (make_pubsub(env) if _uses_pubsub(pipeline) else None),
        run_id=rid,
        params=params,
        project_root=root,
    )
