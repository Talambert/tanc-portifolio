# template-pipeline

Template [Cookiecutter](https://cookiecutter.readthedocs.io/) de pipelines de engenharia de dados sobre a biblioteca `dataeng-core` (`../lib-dataeng-core`). Esta versão gera só o tipo **`ingestao`**; `relatorio`/`hibrido` chegam na Tarefa 23 e a origem `pubsub` na Tarefa 19 (o gerador recusa esses valores com uma mensagem clara).

```bash
pip install cookiecutter
cookiecutter template-pipeline            # pergunta as variáveis
cookiecutter template-pipeline --no-input origens=arquivo,api motor_ingestao=ambos modo_carga=merge
```

## Variáveis

| Variável | Valores |
|---|---|
| `nome_projeto` | letras, dígitos, `-`, `_` (nome do pipeline, do Cloud Run Job e da DAG) |
| `tipo_projeto` | `ingestao` |
| `origens` | texto separado por vírgula: `arquivo`, `api` (ex.: `arquivo,api`) |
| `motor_ingestao` | `cloud_run`, `dataproc` ou `ambos` (duas DAGs e dois YAMLs) |
| `modo_carga` | `append`, `overwrite_particao`, `merge` (`merge` usa `chaves_merge: [id]`) |
| `projeto_gcp`, `regiao`, `bucket_bronze`, `dataset_silver`, `dataset_gold`, `agenda_cron` | valores de exemplo em `dev.yaml` |
| `origem_da_biblioteca` | `git` (`dataeng-core[local] @ git+<repositorio_biblioteca>@dataeng-core-vX.Y.Z#subdirectory=lib-dataeng-core`) ou `artifact_registry` (`dataeng-core[local]==X.Y.*` + `requirements.txt` com `--extra-index-url`) |
| `versao_biblioteca`, `repositorio_biblioteca`, `indice_artifact_registry` | versão fixada, URL do repositório Git e índice do Artifact Registry (exemplo fictício) |

O gancho `hooks/pre_gen_project.py` valida as respostas; `hooks/post_gen_project.py` remove o que não se aplica (DAG do outro motor, bloco da origem não escolhida, `sql/views/`, `spark/`; com a origem `api` não gera `tests/test_local_run.py`).

## Testes

```bash
pytest template-pipeline/tests -q     # a partir da raiz do repositório; precisa de dataeng-core[local]
```

Gera a matriz origem x motor x modo de carga, valida/planeja cada projeto, executa o exemplo local e roda os testes do projeto gerado.

## Não validado

DAG real no Airflow, Cloud Run Job, Dataproc, BigQuery e a instalação da biblioteca via Git/Artifact Registry não foram executados; a checagem é estática (texto do `pyproject.toml`/`Dockerfile`, AST das DAGs).
