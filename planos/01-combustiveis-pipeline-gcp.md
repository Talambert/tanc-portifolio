# 1️⃣ Plano — `combustiveis-pipeline-gcp`

**Objetivo:** pipeline ELT no GCP que leva os preços de combustíveis da ANP do arquivo cru (bronze) até uma tabela limpa e tipada no BigQuery (silver), com **dois motores** equivalentes: Python no Cloud Run Jobs e PySpark no Dataproc Serverless.

**Entrega final:** duas implementações com resultado idêntico, um comparativo de tempo e custo, e infraestrutura reproduzível por Terraform.

## Estrutura da pasta

```text
combustiveis-pipeline-gcp/
├── common/              # contrato compartilhado entre os motores
│   ├── schema.py        # colunas, tipos e nomes finais
│   ├── rules.md         # regras de limpeza (fonte da verdade, em texto)
│   └── sources.yaml     # manifesto explícito de URLs da ANP
├── ingestion/           # Fase 1: download → Cloud Storage (bronze)
├── python-cloudrun/     # Fase 2: motor Python
├── pyspark-dataproc/    # Fase 3: motor PySpark
├── sql/                 # verificação de equivalência e consultas de apoio
├── infra/               # Terraform
├── tests/
└── README.md
```

## Contrato de dados (silver)

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
- **Idempotência:** a carga substitui por partição (`data_coleta` por mês). Rodar duas vezes dá o mesmo resultado.

## Fases

### Fase 0 — Base
- [ ] Projeto GCP, billing, **alerta de orçamento**.
- [ ] APIs habilitadas: Storage, BigQuery, Cloud Run, Artifact Registry, Dataproc.
- [ ] Service Accounts por função (ingestão, silver-python, silver-spark) com menor privilégio.
- [ ] Terraform: bucket `bronze`, datasets `silver`, permissões.
- ✅ **Pronto quando:** `terraform apply` e `terraform destroy` funcionam do zero.

### Fase 1 — Ingestão (bronze)
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
