"""Planejador de tarefas: traduz o `Pipeline` em especificações neutras (sem Airflow, sem Google).

Mapa de tipos de etapa para tarefas:

- `sql`, `view` -> `bigquery_job`, SEM contêiner: `command` é a lista das instruções já renderizadas e divididas.
- `ingestao`, `python` -> `cloud_run_job` ou `dataproc_batch`, conforme `motor` (padrão `cloud_run`). `command` é
  `python -m dataeng_core.jobs.<módulo> --config ... --etapa ... --ambiente ... --run-id {{ ts_nodash }}`; o módulo
  é `run_step` (Cloud Run, e `python` no Dataproc) ou `ingest_spark` (`ingestao` no Dataproc).
- `spark` -> `dataproc_batch` rodando o PRÓPRIO script da etapa: `command` é
  `python <script> --etapa <id> --run-id {{ ts_nodash }} [--param chave=valor ...]`.
- `qualidade` -> `cloud_run_job` (e NÃO `bigquery_job`; veja abaixo).

Por que `qualidade` não é `bigquery_job`: `run_checks` opera sobre um DataFrame (regex em Python, `unico` com chave
nula como distinta, bloqueio, gravação de cada resultado no controle). Reescrever isso como SQL agregado divergiria
da execução local, não gravaria o controle e dependeria de truques (`ERROR()`) para falhar a tarefa. A etapa roda
com o mesmo `run_step` das demais; o que ela consulta no BigQuery é decidido por `steps.qualidade_step`.
`qualidade` usa SEMPRE o Cloud Run, mesmo na DAG do `pipeline_dataproc.yaml` (que então também exige o Cloud Run Job).
O texto do spec 5.x que diz que `qualidade` vira operador do BigQuery está desatualizado (a Tarefa 25 corrige).

Dataproc com `ingestao`/`python`: os arquivos de entrada (`ingest_spark`/`run_step`) só existem em uma IMAGEM do lote
com o dataeng-core instalado, que a biblioteca não cria (suposição de infraestrutura). O planejador exige
`execucao_padrao.dataproc.container_image` ou, para o tipo da etapa, `ingest_entrypoint_uri`/`run_step_entrypoint_uri`;
sem isso levanta `ConfigError` em vez de gerar uma DAG que falharia no Dataproc. `main_python_file_uri` global em
`execucao_padrao.dataproc` é erro nessas etapas (a chave agora vale só por etapa `spark`).

`motor: ambos` não existe no modelo (é uma variável do template): o projeto gera dois YAMLs, um com
`motor: cloud_run` e outro com `motor: dataproc`, e duas DAGs (`<nome>_cloud_run` e `<nome>_dataproc`, via
`dag_id` em `build_dag`). Cada YAML passa pelo planejador separadamente e gera seu conjunto de tarefas.

SQL das tarefas BigQuery: o parâmetro indefinido, a sintaxe inválida e o arquivo sem instruções falham AO PLANEJAR
(`dataeng plan` e o parse da DAG), antes de qualquer execução. `run_id` e `ingestion_date` não existem no parse: o SQL
leva as macros do Airflow `{{ ts_nodash }}` e `{{ ds }}`, e o SQL final é renderizado pelo Airflow. Para o Jinja do
Airflow não interpretar o SQL do usuário, todo `{{`, `{%` e `{#` do SQL e dos valores de parâmetros vira
`{{ '{{' }}` (etc.), que o Jinja devolve como o texto original; assim nem o SQL nem um parâmetro do yaml injetam Jinja.
O mesmo escape vale para os textos de `execucao_padrao` (`container_image`, `script_base_uri`,
`main_python_file_uri`, `runtime`...), que chegam a campos de template do lote.

ATENÇÃO: o SQL renderizado (com os valores de `parametros` do yaml e do ambiente) aparece em texto claro na UI do
Airflow e nos logs do Composer, e os `--param` do script Spark aparecem nos argumentos do lote. Não coloque segredos
em `parametros`; segredos vão em variáveis de ambiente (`segredo_env`), nunca em `command` nem em `resources`.
"""

from __future__ import annotations

import re
import secrets as _secrets
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.pipeline import IngestaoStep, Pipeline, PythonStep, QualidadeStep, SparkStep, SqlStep, ViewStep
from dataeng_core.jobs.run_step import project_root_of
from dataeng_core.steps.sql_steps import check_view_name, inspect_view, prepare_script

TaskKind = Literal["cloud_run_job", "dataproc_batch", "bigquery_job"]

# Run id da execução lógica da DAG: expressão Jinja do Airflow (ex.: 20261019T060000). Estável entre as
# tentativas (retries) da mesma execução e começa com AAAAMMDD, como `validate_run_id` exige.
RUN_ID_TEMPLATE = "{{ ts_nodash }}"
# Dia lógico da execução (AAAA-MM-DD), o mesmo dia do início de `ts_nodash`.
INGESTION_DATE_TEMPLATE = "{{ ds }}"

# Módulos de ponto de entrada (`python -m <módulo>`).
RUN_STEP_MODULE = "dataeng_core.jobs.run_step"
INGEST_SPARK_MODULE = "dataeng_core.jobs.ingest_spark"

# Arquivos principais do lote do Dataproc para os dois módulos acima. SUPOSIÇÃO DE INFRAESTRUTURA: estes arquivos
# NÃO existem na biblioteca; a imagem do lote (Dataproc Serverless com contêiner próprio) precisa trazê-los, cada um
# com duas linhas (`from <módulo> import main` e `raise SystemExit(main())`). Para outro local, informe em
# `execucao_padrao.dataproc`: `run_step_entrypoint_uri` e `ingest_entrypoint_uri`.
DATAPROC_RUN_STEP_ENTRYPOINT = "local:///opt/dataeng/run_step_entrypoint.py"
DATAPROC_INGEST_ENTRYPOINT = "local:///opt/dataeng/ingest_spark_entrypoint.py"

_KIND_BY_MOTOR: dict[str, TaskKind] = {"cloud_run": "cloud_run_job", "dataproc": "dataproc_batch"}
_JINJA_OPEN = re.compile(r"\{[{%#]")


@dataclass
class TaskSpec:
    task_id: str
    kind: TaskKind
    command: list[str]
    depends_on: list[str] = field(default_factory=list)
    resources: dict[str, Any] = field(default_factory=dict)


def escape_jinja(text: str) -> str:
    """Torna o texto literal para o Jinja: `{{`, `{%` e `{#` viram `{{ '{{' }}`, `{{ '{%' }}` e `{{ '{#' }}`."""
    return _JINJA_OPEN.sub(lambda m: "{{ '" + m.group(0) + "' }}", text)


def _escape_values(value: Any) -> Any:
    """`escape_jinja` em todo texto de um valor de configuração (a `execucao_padrao` chega a campos de template)."""
    if isinstance(value, str):
        return escape_jinja(value)
    if isinstance(value, Mapping):
        return {k: _escape_values(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_escape_values(v) for v in value]
    return value


def task_args(spec: TaskSpec) -> list[str]:
    """Argumentos depois do ponto de entrada: `python -m <módulo>` (3 itens) ou `python <script>` (2 itens)."""
    prefix = 3 if len(spec.command) > 1 and spec.command[1] == "-m" else 2
    return spec.command[prefix:]


def _job_command(module: str, step_id: str, job_config: str, ambiente: str) -> list[str]:
    return [
        "python",
        "-m",
        module,
        "--config",
        escape_jinja(job_config),
        "--etapa",
        step_id,
        "--ambiente",
        escape_jinja(ambiente),
        "--run-id",
        RUN_ID_TEMPLATE,
    ]


def _sql_statements(step: SqlStep | ViewStep, root: Path, params: Mapping[str, str]) -> list[str]:
    """Instruções renderizadas, prontas para o Jinja do Airflow (veja o docstring do módulo).

    O SQL é renderizado e validado com valores-marca únicos no lugar de `run_id` e `ingestion_date`; só depois o
    texto é escapado para o Jinja e as marcas viram as macros (o usuário não consegue forjar a marca).
    """
    marks = {"run_id": "DATAENGRUN" + _secrets.token_hex(8), "ingestion_date": "DATAENGDAY" + _secrets.token_hex(8)}
    values = {**params, **marks}
    statements: list[str] = []
    if isinstance(step, ViewStep):
        check_view_name(step)
        script = prepare_script(step.id, root, step.sql, values)
        is_view_statement = inspect_view(step, script)
        for statement in script.statements:
            text = statement.text
            statements.append(text if is_view_statement else f"CREATE OR REPLACE VIEW {step.nome_view} AS {text}")
    else:
        for rel in step.sql:
            statements += [s.text for s in prepare_script(step.id, root, rel, values).statements]
    return [_with_macros(s, marks) for s in statements]


def _with_macros(text: str, marks: Mapping[str, str]) -> str:
    """Escapa o texto para o Jinja e troca as marcas pelas macros, isolando-as dos vizinhos.

    O texto entre as marcas é escapado à parte; um `{` colado antes da macro (que viraria `{{{`) ou um `}` colado
    depois dela é emitido como `{{ '{' }}`/`{{ '}' }}`, de modo que a renderização devolva exatamente o texto original.
    """
    macros = {marks["run_id"]: RUN_ID_TEMPLATE, marks["ingestion_date"]: INGESTION_DATE_TEMPLATE}
    pieces = re.split("(" + "|".join(map(re.escape, macros)) + ")", text)
    out: list[str] = []
    for index, piece in enumerate(pieces):
        if piece in macros:
            out.append(macros[piece])
            continue
        escaped = escape_jinja(piece)
        if index + 1 < len(pieces) and escaped.endswith("{"):
            escaped = escaped[:-1] + "{{ '{' }}"
        if index > 0 and escaped.startswith("}"):
            escaped = "{{ '}' }}" + escaped[1:]
        out.append(escaped)
    return "".join(out)


def _dataproc_script_uri(step: SparkStep, resources: Mapping[str, Any]) -> str:
    uri = resources.get("main_python_file_uri")
    if isinstance(uri, str) and uri:
        return uri
    base = resources.get("script_base_uri")
    if isinstance(base, str) and base:
        return f"{base.rstrip('/')}/{step.script.as_posix()}"
    raise ConfigError(
        f"etapa '{step.id}': o lote do Dataproc precisa do script em um local acessível (ex.: no GCS), e o "
        "planejador não inventa esse local. Informe em `execucao_padrao.dataproc.script_base_uri` o prefixo em que "
        f"o projeto é publicado (o script será `<prefixo>/{step.script.as_posix()}`) ou, só para esta etapa, "
        f"`dataproc.main_python_file_uri` na etapa '{step.id}'.",
        operation="plan_tasks",
    )


def _spark_command(step: SparkStep, params: Mapping[str, str]) -> list[str]:
    """`python <script> --etapa --run-id --param ...`: o mesmo contrato do modo local (`spark_submit_command`)."""
    command = [
        "python",
        escape_jinja(step.script.as_posix()),
        "--etapa",
        escape_jinja(step.id),
        "--run-id",
        RUN_ID_TEMPLATE,
    ]
    for key, value in sorted({**params, "run_id": "", "ingestion_date": ""}.items()):
        if key == "run_id":
            value = RUN_ID_TEMPLATE
        elif key == "ingestion_date":
            value = INGESTION_DATE_TEMPLATE
        else:
            value = escape_jinja(value)
        command += ["--param", f"{key}={value}"]
    return command


def plan_tasks(
    pipeline: Pipeline, *, config_path: str, ambiente: str = "prod", job_config_path: str | None = None
) -> list[TaskSpec]:
    """Uma tarefa por etapa, em ordem topológica estável; `depends_on` segue `depende_de`.

    `config_path`: onde o parse da DAG lê o yaml (use um caminho absoluto resolvido a partir do `__file__` do
    arquivo da DAG); a raiz do projeto (arquivos `.sql` e scripts) é a pasta acima de `config/`. `job_config_path`:
    caminho do MESMO arquivo dentro da imagem do job (relativo à WORKDIR ou absoluto Linux); é ele que vai em
    `--config`. Sem `job_config_path`, usa-se `config_path`. Barras invertidas viram `/` no comando (o job roda em
    Linux).

    Erros de configuração (parâmetro `${x}` indefinido, SQL inválido, arquivo sem instruções, view mal formada,
    script Spark sem local) levantam `ConfigError`/`SqlError` aqui, antes de qualquer execução.
    """
    job_config = (job_config_path if job_config_path is not None else config_path).replace("\\", "/")
    root = project_root_of(Path(config_path))
    execucao = pipeline.execucao_padrao
    tasks: list[TaskSpec] = []
    for step in pipeline.ordered_steps():
        depends = list(step.depende_de)
        if isinstance(step, SqlStep | ViewStep):
            statements = _sql_statements(step, root, pipeline.parametros)
            tasks.append(TaskSpec(step.id, "bigquery_job", statements, depends, {}))
        elif isinstance(step, SparkStep):
            resources = _escape_values({**execucao.get("dataproc", {}), **step.dataproc})
            resources["main_python_file_uri"] = _dataproc_script_uri(step, resources)
            command = _spark_command(step, pipeline.parametros)
            tasks.append(TaskSpec(step.id, "dataproc_batch", command, depends, resources))
        else:
            assert isinstance(step, IngestaoStep | PythonStep | QualidadeStep)
            motor = "cloud_run" if isinstance(step, QualidadeStep) else (step.motor or "cloud_run")
            module = INGEST_SPARK_MODULE if isinstance(step, IngestaoStep) and motor == "dataproc" else RUN_STEP_MODULE
            if motor == "dataproc":
                assert isinstance(step, IngestaoStep | PythonStep)
                _check_dataproc_entrypoint(step, execucao.get("dataproc", {}))
            command = _job_command(module, step.id, job_config, ambiente)
            tasks.append(
                TaskSpec(
                    step.id, _KIND_BY_MOTOR[motor], command, depends, _escape_values(dict(execucao.get(motor, {})))
                )
            )
    return tasks


def _check_dataproc_entrypoint(step: IngestaoStep | PythonStep, resources: Mapping[str, Any]) -> None:
    """Ingestão/python no Dataproc usam entrypoints que só existem numa imagem própria do lote: exige configuração."""
    if "main_python_file_uri" in resources:
        raise ConfigError(
            "`execucao_padrao.dataproc.main_python_file_uri` mudou de significado: agora vale só para etapas "
            f"`spark` (por etapa). A etapa '{step.id}' usa o ponto de entrada da biblioteca; use "
            "`ingest_entrypoint_uri` (ingestao) ou `run_step_entrypoint_uri` (python) e retire a chave global.",
            operation="plan_tasks",
        )
    key = "ingest_entrypoint_uri" if isinstance(step, IngestaoStep) else "run_step_entrypoint_uri"
    if not resources.get("container_image") and not resources.get(key):
        raise ConfigError(
            f"etapa '{step.id}': o motor dataproc para '{step.tipo}' roda `{key.split('_entrypoint')[0]}` "
            "num arquivo de entrada que só existe em uma IMAGEM do lote com o dataeng-core instalado (suposição de "
            "infraestrutura que a biblioteca não cria). Informe `execucao_padrao.dataproc.container_image` (a "
            f"imagem do lote) ou, se o arquivo já estiver acessível, `execucao_padrao.dataproc.{key}`.",
            operation="plan_tasks",
        )
