# 3️⃣ Plano — `combustiveis-orquestracao`

**Objetivo:** orquestrar o pipeline inteiro (ingestão → silver → dbt) com **Apache Airflow**, rodando **o mesmo DAG** em dois ambientes: Airflow local (Docker) e **Cloud Composer** (gerenciado no GCP).

**Pré-requisito:** projetos 1 e 2 funcionando manualmente.

**Entrega final:** um DAG agendado, com retries, alertas e backfill, executado com sucesso nos dois ambientes, e a comparação entre os dois.

## Estrutura da pasta

```text
combustiveis-orquestracao/
├── dags/
│   └── combustiveis_pipeline.py
├── plugins/                  # operadores/hooks próprios, se necessários
├── include/                  # configuração compartilhada
├── docker/                   # ambiente local
│   ├── docker-compose.yaml
│   └── Dockerfile
├── composer/                 # implantação no Cloud Composer
│   ├── terraform/            # cria e DESTRÓI o ambiente
│   └── deploy.sh             # envia DAGs ao bucket do Composer
├── tests/                    # testes de integridade dos DAGs
└── README.md
```

## Desenho do DAG

```text
  verificar_novos_arquivos
            │
      baixar_para_bronze                  (Cloud Run Job de ingestão)
            │
       ┌────┴─────┐
 silver_python   silver_pyspark           (opcional, escolhido por parâmetro)
       └────┬─────┘
       validar_silver                     (contagem, quarentena, equivalência)
            │
         dbt_build                        (staging → marts + testes)
            │
      notificar_resultado
```

- **Agendamento:** semanal (a ANP publica semanalmente), com `catchup` controlado e `max_active_runs=1`.
- **Retries** com *backoff* exponencial nas tarefas de rede.
- **Idempotência:** cada tarefa recebe a janela de datas (`data_interval_start/end`), nada de "hoje" fixo.
- **Disparo dos jobs:** operadores do provider Google (`CloudRunExecuteJobOperator`, `DataprocCreateBatchOperator`, `BigQueryInsertJobOperator`). O dbt roda via `BashOperator` ou Cloud Run Job.
- **Falha:** alerta por e-mail ou Slack (`on_failure_callback`).

## Fases

### Fase 6.1 — Airflow local (Docker)
- [ ] `docker-compose` com Airflow (webserver, scheduler, banco).
- [ ] Conexão com o GCP via Service Account montada como volume, **fora do git**.
- [ ] DAG com as tarefas acima; testar cada uma isoladamente (`airflow tasks test`).
- [ ] Teste de integridade: todos os DAGs importam sem erro (`pytest`).
- ✅ **Pronto quando:** execução completa verde no local e um *backfill* de dois períodos funciona.

### Fase 6.2 — Cloud Composer
- [ ] Terraform do ambiente Composer **no menor porte possível**.
- [ ] `deploy.sh` para enviar `dags/` ao bucket do ambiente.
- [ ] Ajustar apenas o que difere do local (conexões, variáveis, Service Account do ambiente).
- [ ] Executar, **capturar evidências** (capturas da UI, logs, grafo) e **destruir o ambiente no mesmo dia**.
- ⚠️ Composer cobra por hora de ambiente ligado (ordem de centenas de dólares por mês). Documentar o custo real da demonstração.
- ✅ **Pronto quando:** o mesmo DAG roda verde no Composer, sem alterações de lógica, e o ambiente foi destruído.

### Fase 6.3 — Comparativo local × Composer
- [ ] Tabela: tempo de setup, custo, manutenção, observabilidade, escalabilidade.
- [ ] Conclusão: quando usar cada um.

## Evidências para o portfólio

- Capturas do grafo do DAG, da execução verde e da tela de backfill.
- Logs de uma falha forçada e da recuperação por retry.
- Registro do custo real da execução no Composer.

## Riscos

| Risco | Mitigação |
|---|---|
| Custo do Composer | Subir só para a demonstração; Terraform `destroy`; alerta de orçamento |
| DAG funciona local e quebra no Composer | Mesma versão do Airflow e dos providers (usar a imagem/versão do Composer como referência) |
| Credenciais em imagens ou git | Montar por volume ou Secret Manager; `.gitignore` conferido |
| Tarefas não idempotentes | Parametrizar por janela de dados e testar reexecução |

## Definição de pronto do projeto

- [ ] DAG verde no local e no Composer.
- [ ] Backfill e retry demonstrados.
- [ ] Ambiente Composer destruído e custo registrado.
- [ ] README com diagrama, evidências e comparativo.
