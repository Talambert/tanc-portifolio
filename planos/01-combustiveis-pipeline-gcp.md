# 1️⃣ Plano — `combustiveis-pipeline-gcp`

**Objetivo:** pipeline ELT no GCP que leva os preços de combustíveis da ANP do arquivo cru (bronze) até uma tabela limpa e tipada no BigQuery (silver), com **dois motores** equivalentes: Python no Cloud Run Jobs e PySpark no Dataproc Serverless.

**Base de engenharia:** as Fases 1 a 3 (ingestão, silver e comparação) partem do [`template-pipeline`](../template-pipeline/) (tipo `hibrido`, origem `arquivo`) e da biblioteca [`lib-dataeng-core`](../lib-dataeng-core/). O contrato de dados abaixo vive em `config/pipeline.yaml` e `schemas/`; o template cobre a ingestão e a transformação em Python/pandas (Cloud Run) e uma etapa `spark` (Dataproc), mas o motor Spark **nunca foi executado**: a comparação Python × PySpark de tempo e custo continua sendo o objetivo e o trabalho futuro do projeto.

**Entrega final:** duas implementações com resultado idêntico, um comparativo de tempo e custo, e infraestrutura reproduzível por Terraform.

## Estrutura da pasta

```text
combustiveis-pipeline-gcp/
├── common/              # manifesto de fontes e regras em texto
│   ├── rules.md         # regras de limpeza (fonte da verdade, em texto)
│   └── sources.yaml     # manifesto explícito de URLs da ANP
├── config/              # (template) pipeline.yaml e ambientes/ local, dev, prod
├── schemas/             # (template) colunas do CSV de origem e da silver; substitui o common/schema.py
├── custom/              # (template) ganchos de transformação (endereco)
├── ingestion/           # Fase 1: download → Cloud Storage (bronze)
├── python-cloudrun/     # Fase 2: motor Python
├── pyspark-dataproc/    # Fase 3: motor PySpark
├── sql/                 # verificação de equivalência e consultas de apoio
│   └── pipeline/        # (template) DDL da silver
├── infra/               # Terraform
├── tests/
└── README.md
```

## Contrato de dados (silver)

Implementado em [`config/pipeline.yaml`](../combustiveis-pipeline-gcp/config/pipeline.yaml) e [`schemas/`](../combustiveis-pipeline-gcp/schemas/). A tabela abaixo continua sendo a referência; as diferenças entre ela e o que a biblioteca faz hoje estão em "Status".

| Coluna final | Tipo | Regra |
|---|---|---|
| `regiao`, `uf`, `municipio`, `bairro`, `cep` | STRING | `trim`, maiúsculas, CEP só com dígitos e hífen |
| `cnpj_revenda` | STRING | **só dígitos** (14), sem espaço e sem pontuação |
| `revenda`, `bandeira` | STRING | `trim`, espaços múltiplos colapsados |
| `endereco` | STRING | `rua + numero + complemento` normalizados |
| `produto` | STRING | valor de lista controlada (gasolina, etanol, diesel S10 etc.) |
| `data_coleta` | DATE | `dd/mm/aaaa` → `DATE` |
| `valor_venda` | NUMERIC | vírgula → ponto, depois `NUMERIC` |
| `valor_compra` | NUMERIC | idem, **nulo permitido** |
| `unidade_medida` | STRING | como veio |
| `arquivo_origem`, `carregado_em` | STRING, TIMESTAMP | rastreabilidade |

- **Chave natural:** `cnpj_revenda + produto + data_coleta`. Duplicatas são removidas, e a contagem removida é registrada.
- **Linhas malformadas** (campo com `;` sem aspas, colunas deslocadas) vão para uma tabela de **quarentena** (`silver_rejeitados`) com o motivo, em vez de serem descartadas em silêncio.
- **Idempotência:** a carga é `merge` pelo grão (`cnpj_revenda, produto, data_coleta`). Rodar duas vezes dá o mesmo resultado.

## Status

**Real:** `config/` e `schemas/` gerados do template e adaptados ao contrato acima; `dataeng validate-config` e `dataeng plan` passam em `local`, `dev` e `prod`; `dataeng run local` roda com uma amostra **sintética** (teste `tests/test_pipeline_config.py`): CNPJ só com dígitos, vírgula decimal, `dd/mm/aaaa`, quarentena, deduplicação e `endereco` montado.

**Não real:** nenhuma execução em GCP; nenhum dado real da ANP processado (o cabeçalho do CSV de origem segue o layout público conhecido e não foi conferido contra um arquivo real); Spark não executado.

**Diferenças deliberadas em relação ao contrato:** a carga é `merge` por `cnpj_revenda, produto, data_coleta` (idempotente por grão e segura quando vários arquivos caem no mesmo dia lógico; `overwrite_particao` por `_ingestion_date` apagaria o arquivo anterior) e não a partição por mês de `data_coleta`; a quarentena é `<tabela>_rejeitados` (`precos_combustiveis_rejeitados`); `cep` só recebe `trim` (falta "só dígitos e hífen"); `produto` ainda sem lista controlada; `DECIMAL(10, 2)` no lugar de `NUMERIC` (portável); `arquivo_origem` e `carregado_em` viram `_source_file` e `_loaded_at`; o bronze segue `bronze/precos_combustiveis/ingestion_date=.../run_id=.../`, não `bronze/ca/ano=.../`.

## Fases

### Fase 0 — Base
- [ ] Projeto GCP, billing, **alerta de orçamento**.
- [ ] APIs habilitadas: Storage, BigQuery, Cloud Run, Artifact Registry, Dataproc.
- [ ] Service Accounts por função (ingestão, silver-python, silver-spark) com menor privilégio.
- [ ] Terraform: bucket `bronze`, datasets `silver`, permissões.
- ✅ **Pronto quando:** `terraform apply` e `terraform destroy` funcionam do zero.

### Fase 1 — Ingestão (bronze)
- [ ] (base pronta no template: etapa `ingestao` com bronze, manifesto de tamanho e SHA-256 e carga idempotente por `run_id`; falta executar no GCP)
- [ ] `sources.yaml` com as URLs reais (semestrais de 2023 em diante, mensais para o semestre corrente).
- [ ] Download com retry e `User-Agent` de navegador (o servidor responde 403/404 a clientes simples).
- [ ] Descompactar ZIP e gravar no bucket em `bronze/ca/ano=AAAA/semestre=S/...`.
- [ ] Registrar tamanho e hash de cada arquivo (manifesto de ingestão).
- ✅ **Pronto quando:** reexecutar não baixa de novo o que já existe (idempotente) e o bucket tem os arquivos esperados.
- 🔗 Reaproveita ideias do `gcp-file-uploader` (upload com Service Account).

### Fase 2 — Motor Python (silver)
- [ ] Leitura em streaming (`csv` ou `pandas` por chunks), encoding `utf-8-sig`, separador `;`.
- [ ] Aplicar o contrato; separar linhas malformadas para quarentena.
- [ ] Carga no BigQuery por partição, com `WRITE_TRUNCATE` na partição.
- [ ] Empacotar em contêiner e executar como **Cloud Run Job**.
- [ ] Testes unitários das regras de limpeza (`pytest`), incluindo casos reais problemáticos.
- ✅ **Pronto quando:** contagem de linhas da silver + quarentena = linhas do arquivo de origem.

### Fase 3 — Motor PySpark (silver)
- [ ] (o template tem a etapa `spark` e o motor Spark da biblioteca, nunca executados; o gancho `custom/anp.py` precisará de versão Spark)
- [ ] Mesmo contrato em PySpark, lendo do bucket com `option("sep", ";")` e `encoding=UTF-8`.
- [ ] Gravação no BigQuery pelo conector Spark-BigQuery (testar escrita direta vs. indireta e registrar a escolha).
- [ ] Teste local com PySpark antes de gastar na nuvem.
- [ ] Submissão com `gcloud dataproc batches submit pyspark`, Service Account própria.
- [ ] Tabela de destino separada (`silver_spark`) para comparar com a do Python.
- ⚠️ **Atenção:** a versão do conector precisa ser compatível com o runtime do Dataproc.
- ✅ **Pronto quando:** job conclui e a equivalência com a fase 2 é provada (abaixo).

### Fase 4 — Comparativo
- [ ] Query de **equivalência** em `sql/`: contagem, somas e amostras por chave entre `silver_python` e `silver_spark`. Esperado: **diferença zero**.
- [ ] Medir por motor: tempo total, custo estimado, esforço de manutenção.
- [ ] Repetir com mais dados (ex.: 2020 em diante) para mostrar onde o Spark começa a compensar.
- [ ] Tabela e conclusão no README ("quando usar cada um").
- ✅ **Pronto quando:** o README tem números reais, mesmo que a conclusão seja "Spark foi exagero neste volume".

## Riscos

| Risco | Mitigação |
|---|---|
| Site da ANP muda URLs ou bloqueia | Manifesto explícito e cópia em bronze; falha clara no download |
| Linhas malformadas viram erro silencioso | Quarentena com motivo e teste de contagem |
| Custo inesperado | Alerta de orçamento, Serverless, partição, `destroy` ao fim |
| Divergência entre Python e Spark | `common/` como fonte única e query de equivalência |
| Encoding diferente em semestres antigos | Detectar e registrar por arquivo; testar 2023 em diante primeiro |

## Definição de pronto do projeto

- [ ] Infra sobe e desce com Terraform.
- [ ] Os dois motores produzem silver idêntica, comprovada por SQL.
- [ ] Comparativo documentado com números.
- [ ] Testes passando no CI.
- [ ] README com diagrama, decisões e como reproduzir.
