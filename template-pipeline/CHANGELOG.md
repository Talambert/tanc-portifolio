# Changelog

## 0.1.0 (Não lançado)

- Primeiro template: tipo `ingestao` (origens `arquivo` e `api`; motores `cloud_run`, `dataproc` e `ambos`; modos de carga `append`, `overwrite_particao` e `merge`).
- Projeto gerado executável de saída (`dataeng run local` com `data/amostra.csv`).
- Origem `pubsub` (`origens=pubsub` ou combinada, ex.: `arquivo,pubsub`): bloco `ingere_eventos` com dedupe por `message_id`, schemas e DDL de eventos, e teste local com `LocalPubSub`.
- `origem_da_biblioteca`: `git` ou `artifact_registry`.
