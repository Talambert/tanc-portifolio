# template-pipeline

Template [Cookiecutter](https://cookiecutter.readthedocs.io/) de pipelines de engenharia de dados sobre a biblioteca `dataeng-core` (`../lib-dataeng-core`). Esta versão gera só o tipo **`ingestao`** (origens `arquivo`, `api` e `pubsub`); `relatorio`/`hibrido` chegam na Tarefa 23 (o gerador recusa esses valores com uma mensagem clara).

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
| `origens` | texto separado por vírgula: `arquivo`, `api`, `pubsub` (ex.: `arquivo,pubsub`) |
| `motor_ingestao` | `cloud_run`, `dataproc` ou `ambos` (duas DAGs e dois YAMLs) |
| `modo_carga` | `append`, `overwrite_particao`, `merge` (`merge` usa `chaves_merge: [id]`). No bloco `pubsub`, `overwrite_particao` vira `append` (merge por `message_id`; sobrescrever a partição apagaria os lotes anteriores) e `merge` usa `chaves_merge: [message_id]` |
| `projeto_gcp`, `regiao`, `bucket_bronze`, `dataset_silver`, `dataset_gold`, `agenda_cron` | valores de exemplo em `dev.yaml` |
| `origem_da_biblioteca` | `git` (`dataeng-core[local] @ git+<repositorio_biblioteca>@dataeng-core-vX.Y.Z#subdirectory=lib-dataeng-core`) ou `artifact_registry` (`dataeng-core[local]==X.Y.*` + `requirements.txt` com `--extra-index-url`) |
| `versao_biblioteca`, `repositorio_biblioteca`, `indice_artifact_registry` | versão fixada, URL do repositório Git e índice do Artifact Registry (exemplo fictício) |

O gancho `hooks/pre_gen_project.py` valida as respostas; `hooks/post_gen_project.py` remove o que não se aplica (DAG do outro motor, bloco da origem não escolhida, `sql/views/`, `spark/`; com a origem `api` não gera `tests/test_local_run.py`, que faria rede). Com `pubsub`, o `tests/test_local_run.py` do projeto publica mensagens sintéticas num `LocalPubSub` e o injeta em `build_context`: `dataeng run local` sozinho usa uma fila em memória vazia e termina ok com 0 linhas.

## Pub/Sub: operação

- **Dead-letter:** configure na assinatura `max_delivery_attempts` e um tópico dead-letter. Sem isso, um lote que falha sempre (ex.: verificação de qualidade bloqueante que falha por causa do conteúdo de uma mensagem) volta para sempre: a silver já foi gravada (de forma idempotente), mas nada é confirmado e o backlog não drena. Use verificação bloqueante sobre o conteúdo das mensagens com cuidado; prefira rejeitar por mensagem com `rejeitar_se` (vai para a quarentena e é confirmada).
- **Prazo de ack:** a biblioteca não estende o prazo de ack durante o processamento. Configure o `ackDeadlineSeconds` da assinatura (até 600 s) para cobrir um lote completo (pull + bronze + silver + quarentena + `dml_pos_carga`) e reduza `max_mensagens` se necessário. Um ack depois do prazo pode ser ignorado pelo serviço: as mensagens voltam (a carga é idempotente, mas o backlog não drena).
- **`modo_carga: merge` com chaves de negócio:** em cada lote fica a mensagem de maior `publish_time` por chave; as superadas são confirmadas sem ir para a silver nem para a quarentena. O merge é incondicional: um evento atrasado (mais antigo) que chegue numa execução posterior sobrescreve o mais novo já gravado.
- **Quarentena:** `_linha_original` guarda a mensagem inteira em base64 (com `attributes`), cerca de 33% maior que o original; mensagens grandes inflam a tabela `<tabela>_rejeitados`.

## Testes

```bash
pytest template-pipeline/tests -q     # a partir da raiz do repositório; precisa de dataeng-core[local]
```

Gera a matriz origem x motor x modo de carga, valida/planeja cada projeto, executa o exemplo local e roda os testes do projeto gerado.

## Não validado

DAG real no Airflow, Cloud Run Job, Dataproc, BigQuery, Pub/Sub real (assinatura, ack e reentrega) e a instalação da biblioteca via Git/Artifact Registry não foram executados; a checagem é estática (texto do `pyproject.toml`/`Dockerfile`, AST das DAGs).

Motor Dataproc (`ingestao`/`python`): exige uma IMAGEM do lote com o `dataeng-core` instalado (suposição de infraestrutura, a biblioteca não a cria). Informe `execucao_padrao.dataproc.container_image` no `pipeline_dataproc.yaml` (ou `ingest_entrypoint_uri`/`run_step_entrypoint_uri`); sem isso o planejamento da DAG falha com erro explicativo. A etapa `qualidade` roda sempre no Cloud Run, mesmo nessa DAG.

## Notas de segurança e limites

- **Validação:** `hooks/pre_gen_project.py` recusa, antes de gerar qualquer arquivo, valores fora de um formato estrito (cron, ids de projeto/bucket/dataset, região, versão `X.Y.Z`, URLs `https` sem metacaracteres de shell). Por isso os valores entram nos YAML/TOML/Dockerfile sem escape adicional. Valores de escolha (`tipo_projeto`, `motor_ingestao`, `modo_carga`, `origem_da_biblioteca`) são recusados pelo próprio Cookiecutter. Nomes com `"` falham no Windows antes mesmo do gancho (limite do sistema de arquivos).
- **Nome longo:** a biblioteca trunca o nome do Cloud Run Job em 63 caracteres; prefira nomes curtos.
- **Artifact Registry:** `--extra-index-url` mistura o índice privado com o PyPI público e abre espaço para *dependency confusion*. Prefira um índice privado com o nome `dataeng-core` reservado ou, quando possível, `--index-url` único apontando para um repositório virtual.
- **Imagem:** a imagem base (`python:3.12-slim`) não é fixada por digest e, no modo `git`, o `git` permanece na imagem. Aceitável nesta versão.
- **dev.yaml e prod.yaml:** vêm das variáveis em `dev.yaml`: `projeto_gcp`, `regiao`, `bucket_bronze`, `dataset_silver`, `dataset_gold`. Em `prod.yaml` só `regiao`, `dataset_silver` e `dataset_gold` vêm das variáveis; projeto, bucket e Service Accounts (`meu-projeto-prod...`, `pipeline-*@...`) são exemplos fixos e fictícios a editar.
