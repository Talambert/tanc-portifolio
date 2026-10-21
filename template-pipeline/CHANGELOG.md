# Changelog

## 0.1.0 (Não lançado)

- Primeiro template: tipo `ingestao` (origens `arquivo` e `api`; motores `cloud_run`, `dataproc` e `ambos`; modos de carga `append`, `overwrite_particao` e `merge`).
- Projeto gerado executável de saída (`dataeng run local` com `data/amostra.csv`).
- Origem `pubsub` (`origens=pubsub` ou combinada, ex.: `arquivo,pubsub`): bloco `ingere_eventos` com dedupe por `message_id`, schemas e DDL de eventos, e teste local com `LocalPubSub`.
- `origem_da_biblioteca`: `git` ou `artifact_registry`.
- Tipos `relatorio` e `hibrido`: `tipo_projeto` aceita `ingestao`, `relatorio` e `hibrido`; novas variáveis `usa_python` e `usa_spark` (etapas `python` e `spark`, `custom/enriquecimento.py`, `spark/`). O `relatorio` não tem origem nem `schemas/` e roda localmente sobre dados semeados (`semeia_dados`); o `hibrido` junta ingestão e relatório numa só `pipeline.yaml` (com `motor_ingestao=ambos`, duas DAGs com o relatório inteiro). SQL de relatório portátil (view e `MERGE`).
- `execucao_padrao.dataproc` ganha `container_image` (e `script_base_uri` com `spark`) de exemplo quando há ingestão em Dataproc, como o planejamento da DAG exige; os testes passam a planejar as tarefas da DAG em `prod` (sem Airflow).
- Aceite híbrido da ANP (`tests/test_hybrid_anp.py`) com amostra sintética própria.
- Guia `docs/guia-novo-pipeline.md` (de zero a uma DAG validada), com os comandos marcados executados por `tests/test_docs_commands.py`; README com os três tipos, as variáveis e as limitações.
