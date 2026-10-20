"""Contexto de execução das etapas e resultado de cada etapa."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import Environment
from dataeng_core.base.pipeline import Pipeline
from dataeng_core.base.runtime import new_run_id
from dataeng_core.pubsub.base import PubSubClient
from dataeng_core.quality.checks import CheckResult
from dataeng_core.storage.base import Storage, make_storage
from dataeng_core.warehouse.base import Warehouse, make_warehouse

StepStatus = Literal["ok", "falha", "nao_executada"]

# Vai para caminhos do bronze e para a tabela de controle: só caracteres seguros em GCS, disco e SQL.
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")


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


def build_context(
    pipeline: Pipeline, env: Environment, *, project_root: Path, run_id: str | None = None
) -> StepContext:
    """Monta o contexto: storage e warehouse do ambiente, `run_id` novo (ou o informado) e parâmetros.

    No modo local, `pasta_local` relativa é resolvida a partir da raiz do projeto (não do diretório atual).
    Os parâmetros são os do pipeline (já com os do ambiente) mais `run_id`.
    """
    root = Path(project_root).resolve()
    rid = validate_run_id(run_id) if run_id is not None else new_run_id()
    if env.modo == "local" and not Path(env.pasta_local).is_absolute():
        env = env.model_copy(update={"pasta_local": str(root / env.pasta_local)})
    params = {**env.params(), **pipeline.parametros, "run_id": rid}
    return StepContext(
        pipeline=pipeline,
        env=env,
        storage=make_storage(env),
        warehouse=make_warehouse(env),
        pubsub=None,
        run_id=rid,
        params=params,
        project_root=root,
    )
