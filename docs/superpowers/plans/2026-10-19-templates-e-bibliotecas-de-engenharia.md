# Templates e bibliotecas de engenharia de dados: plano de implementação

> **Para agentes:** SUB-SKILL OBRIGATÓRIA: use `superpowers:subagent-driven-development` (recomendado) ou `superpowers:executing-plans` para executar este plano tarefa por tarefa. Os passos usam caixas de seleção (`- [ ]`).

**Objetivo:** construir a biblioteca `dataeng-core` e o template Cookiecutter `template-pipeline` (tipos `ingestao`, `relatorio` e `hibrido`), de modo que uma demanda vire um repositório novo só preenchendo configuração.

**Arquitetura:** um pacote Python em camadas (`base → storage, warehouse, pubsub → sources, transform, quality, sql → steps → jobs → airflow, cli`) com implementações **locais** das interfaces (arquivos, DuckDB, Pub/Sub falso) para testar sem GCP. O pipeline é uma lista de etapas tipadas lida de um `pipeline.yaml`. O template é fino: configuração, schemas, SQL, DAG e ganchos.

**Stack:** Python ≥ 3.11, pydantic v2, pandas, PyYAML, requests, sqlglot, DuckDB (`[local]`), `google-cloud-storage/bigquery/pubsub` (`[gcp]`), PySpark (`[spark]`), Apache Airflow e provider Google (`[airflow]`), Cookiecutter, pytest, ruff, mypy, import-linter. Confirmar as versões atuais de cada uma ao instalar; nenhuma está fixada aqui.

**Especificação:** `docs/superpowers/specs/2026-10-19-templates-e-bibliotecas-de-engenharia-design.md` (leia antes de começar; os números de seção abaixo referem-se a ela).

## Restrições globais

Valem para todas as tarefas. Valores copiados da especificação.

- Pastas na raiz do portfólio: `lib-dataeng-core/` (distribuição `dataeng-core`, importação `dataeng_core`) e `template-pipeline/`. Cada uma com README, `pyproject.toml` ou `cookiecutter.json`, `CHANGELOG.md` e CI próprios, como se fossem repositórios independentes.
- Pacote em `lib-dataeng-core/src/dataeng_core/`; testes em `lib-dataeng-core/tests/`.
- Camadas, da base para o topo: `base → storage, warehouse, pubsub → sources, transform, quality, sql → steps → jobs → airflow, cli`. Cada camada só importa das de baixo; `import-linter` verifica no CI.
- Extras: `[local]` (duckdb), `[gcp]` (storage, bigquery, pubsub), `[spark]` (pyspark), `[airflow]`, `[dev]`. O pacote base não depende de nada de GCP.
- Tipos de etapa: `ingestao`, `sql`, `view`, `python`, `spark`, `qualidade`. Origens de ingestão: `arquivo`, `api`, `pubsub`. Motores: `cloud_run`, `dataproc` (e `ambos` só na ingestão do template). `tipo_projeto`: `ingestao`, `relatorio`, `hibrido`.
- `schema_version: 1` no YAML. Parâmetros nos arquivos SQL e nos YAML: **`${nome}`** (nunca `{{ }}`).
- Colunas de controle da silver: `_run_id`, `_source_file`, `_loaded_at`, `_ingestion_date`. Quarentena: `<tabela>_rejeitados` (linha original e motivo). Controle: `_controle_execucoes` no dataset da silver.
- Bronze: `<bucket>/bronze/<nome>/ingestion_date=AAAA-MM-DD/run_id=<id>/<arquivo>`, com manifesto (tamanho e SHA-256).
- Modos de carga: `append`, `overwrite_particao`, `merge` (o `merge` exige `destino.chaves_merge`). A carga é idempotente por `run_id` e partição.
- Pub/Sub: pull em lote; **confirma só depois de gravar no bronze**; deduplica por `message_id`; registra o backlog restante.
- Nomes de DAG com `ambos`: `<nome>_cloud_run` e `<nome>_dataproc`.
- CLI: `dataeng validate-config`, `dataeng plan`, `dataeng run local`.
- Versionamento semântico; tags `dataeng-core-vX.Y.Z`.
- Windows: o Airflow **não roda** no Windows. Localmente, DAGs só passam por checagem estática; a importação real é um job do CI em Linux. Caminhos devem funcionar com `\` e `/`.
- Sem dados de empresa em nenhum arquivo. Fixtures são sintéticas.
- Commits em português, no formato `tipo(escopo): descrição`. **Só executar `git commit` quando o usuário autorizar.**
- Ambiente de desenvolvimento: um venv por pasta. Se `python -m venv` falhar (acontece em alguns sandboxes), instalar com `pip install --target <pasta>` e usar `PYTHONPATH`.

## Foco de revisão

Entradas e falhas que a especificação implica mas que provavelmente ninguém testaria, da mais provável para a menos. Cada uma tem seu teste na tarefa dona.

1. **Arquivo vazio ou só com cabeçalho** → zero linhas, sucesso, controle com 0 (Tarefas 7 e 12).
2. **Falha entre o bronze e a silver** → reexecutar conclui sem duplicar (Tarefa 12).
3. **Pub/Sub com fila vazia e com mensagem de JSON inválido** → o lote não trava, a inválida vai para a quarentena e é confirmada (Tarefa 19).
4. **SQL com `${parametro}` não definido, `;` dentro de texto e comentário no fim** → erro claro antes de executar e divisão correta (Tarefa 11).
5. **Segredo da API** nunca aparece em log, em mensagem de erro nem na tabela de controle (Tarefas 8 e 12).

---

# Fase 1: núcleo da biblioteca e tipo `ingestao` (arquivo e API)

### Tarefa 1: esqueleto da biblioteca e ferramentas

**Arquivos:**
- Criar: `lib-dataeng-core/pyproject.toml`, `README.md`, `CHANGELOG.md`, `.gitignore`, `.importlinter`, `src/dataeng_core/__init__.py`, um `__init__.py` vazio em cada subpacote (`base`, `storage`, `warehouse`, `pubsub`, `sources`, `transform`, `quality`, `sql`, `steps`, `jobs`, `airflow`, `cli`), `tests/test_smoke.py`.

**Interfaces:**
- Produz: `dataeng_core.__version__: str = "0.1.0"`; script de console `dataeng = dataeng_core.cli:main` (ainda ausente); contrato `import-linter` do tipo `layers` com as camadas da seção de restrições.

- [ ] **Passo 1: escrever o teste** em `tests/test_smoke.py`: `test_version_is_semver` (`re.fullmatch(r"\d+\.\d+\.\d+", dataeng_core.__version__)`) e `test_all_layers_importable` (importa cada subpacote).
- [ ] **Passo 2: rodar** `pytest lib-dataeng-core/tests/test_smoke.py -v`. Esperado: FALHA (pacote inexistente).
- [ ] **Passo 3: criar** o `pyproject.toml` (`requires-python = ">=3.11"`, extras da restrição global, `[tool.ruff]`, `[tool.mypy]` estrito nos módulos `base`) e a estrutura de pastas. O `.importlinter` declara o contrato `layers` com `dataeng_core.cli | dataeng_core.airflow`, depois `jobs`, `steps`, `sources | transform | quality | sql`, `storage | warehouse | pubsub`, `base` (ordem do topo para a base).
- [ ] **Passo 4: rodar** `pytest`, `ruff check .`, `mypy src` e `lint-imports`. Esperado: tudo passa.
- [ ] **Passo 5: commit** `feat(lib): esqueleto do pacote dataeng-core`.

### Tarefa 2: erros tipados e utilidades de execução

**Arquivos:**
- Criar: `src/dataeng_core/base/errors.py`, `src/dataeng_core/base/runtime.py`
- Teste: `tests/base/test_errors.py`, `tests/base/test_runtime.py`

**Interfaces:**
- Produz (`base.errors`): `class DataEngError(Exception)` com atributos de classe `title`, `what`, `fix` (textos em português: o que aconteceu e o que fazer), construtor `__init__(self, detail: str = "", *, operation: str = "")` e atributo `reference: str` (8 caracteres hexadecimais). Subclasses: `ConfigError`, `SourceError`, `LoadError`, `QualityError`, `SqlError`, `PubSubError`.
- Produz (`base.runtime`): `new_run_id() -> str` (`AAAAMMDDTHHMMSS-<6 hex>`); `retry(fn: Callable[[], T], *, attempts: int = 3, base_delay: float = 0.5, retry_on: tuple[type[BaseException], ...] = (Exception,), sleep: Callable[[float], None] = time.sleep) -> T`; `get_logger(name: str) -> logging.Logger` (linhas JSON com `run_id` quando definido); `redact(text: str, secrets: Iterable[str]) -> str` (troca cada segredo por `***`).

- [ ] **Passo 1: testes.** `test_each_error_has_actionable_text` (todo subtipo tem `title`, `what`, `fix` não vazios e `reference` de 8 hex); `test_retry_succeeds_after_transient_failures` (falha 2×, sucesso na 3ª, `sleep` chamado com `0.5` e `1.0`); `test_retry_raises_after_attempts` ; `test_retry_only_retries_listed_exceptions`; `test_run_id_format_and_uniqueness`; `test_redact_hides_every_secret_and_ignores_empty` (`redact("t=abc", ["abc", ""]) == "t=***"`).
- [ ] **Passo 2: rodar** e ver falhar.
- [ ] **Passo 3: implementar** as assinaturas acima. Espera crescente: `base_delay * 2**tentativa`.
- [ ] **Passo 4: rodar** e ver passar.
- [ ] **Passo 5: commit** `feat(base): erros tipados, retry, run_id e redact`.

### Tarefa 3: parâmetros, ambiente e substituição de `${}`

**Arquivos:**
- Criar: `src/dataeng_core/base/params.py`
- Teste: `tests/base/test_params.py`

**Interfaces:**
- Consome: `ConfigError`.
- Produz: `substitute(value: T, params: Mapping[str, str], *, strict: bool = True) -> T` (percorre `str`, `dict` e `list`; com `strict`, `${x}` ausente levanta `ConfigError` citando o nome); `load_yaml(path: Path) -> dict[str, Any]` (erro de sintaxe vira `ConfigError` com arquivo e linha); `class Environment(BaseModel)` com `modo: Literal["local","gcp"]`, `projeto_gcp: str`, `regiao: str`, `bucket_bronze: str`, `dataset_silver: str`, `dataset_gold: str`, `service_account: str | None`, `pasta_local: str = ".local"`; `Environment.params() -> dict[str, str]`; `load_environment(path: Path) -> Environment`.

- [ ] **Passo 1: testes.** `test_substitute_nested_structures`; `test_substitute_strict_names_the_missing_param` (mensagem contém `dataset_gold`); `test_substitute_non_strict_keeps_placeholder`; `test_non_string_values_untouched`; `test_load_yaml_reports_file_on_syntax_error`; `test_environment_requires_fields_and_exposes_params`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar (regex `\$\{(\w+)\}`), rodar (passa).
- [ ] **Passo 5: commit** `feat(base): substituição de parâmetros e ambientes`.

### Tarefa 4: modelos do `pipeline.yaml` e validação

**Arquivos:**
- Criar: `src/dataeng_core/base/pipeline.py`, `src/dataeng_core/base/schema.py`
- Teste: `tests/base/test_pipeline.py`, `tests/base/test_schema.py`

**Interfaces:**
- Consome: `substitute`, `load_yaml`, `Environment`, `ConfigError`.
- Produz (`base.schema`): `Coluna(nome: str, tipo: Literal["texto","inteiro","decimal","data","booleano","timestamp"], obrigatoria: bool = False)`; `SchemaSpec(colunas: list[Coluna])`; `load_schema(path: Path) -> SchemaSpec`.
- Produz (`base.pipeline`): modelos pydantic `ArquivoOrigem`, `ApiOrigem`, `PubSubOrigem` (campos da seção 5.3), `Destino(bronze, tabela, schema, modo_carga: Literal["append","overwrite_particao","merge"], particao="_ingestion_date", chaves_merge=[], ddl=[], dml_pos_carga=[])`, `TransformOp`, `QualityCheck`, e as etapas `IngestaoStep`, `SqlStep`, `ViewStep`, `PythonStep`, `SparkStep`, `QualidadeStep` (todas com `id`, `tipo`, `depende_de: list[str]`, `motor: Literal["cloud_run","dataproc"] | None`); `Step = IngestaoStep | SqlStep | ViewStep | PythonStep | SparkStep | QualidadeStep` (união discriminada pelo campo `tipo`); `Pipeline(schema_version: int, nome: str, agenda: str, parametros: dict[str,str], etapas: list[Step])`; `Pipeline.ordered_steps() -> list[Step]` (ordem topológica estável); `Pipeline.get(step_id: str) -> Step`; `load_pipeline(config_path: Path, *, ambiente: str = "local", extra_params: Mapping[str, str] | None = None) -> Pipeline` (carrega `config/ambientes/<ambiente>.yaml`, mescla `parametros` e `extra_params`, substitui `${}`, valida).

- [ ] **Passo 1: testes.** `test_valid_hybrid_example_loads` (o YAML da seção 5.3); `test_missing_depends_on_means_previous_step`; `test_duplicate_ids_rejected`; `test_cycle_rejected_and_names_the_cycle`; `test_unknown_dependency_rejected`; `test_motor_incompatible_with_type_rejected` (`view` com `motor`); `test_merge_requires_chaves_merge`; `test_missing_sql_file_rejected`; `test_unknown_step_type_lists_valid_types`; `test_old_schema_version_warns_but_loads` (usa `schema_version` anterior suportado, quando existir; hoje só valida que `schema_version != 1` falha com mensagem clara); `test_schema_loader_rejects_unknown_type`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar (união discriminada por `tipo`; validadores de modelo), rodar (passa).
- [ ] **Passo 5: commit** `feat(base): modelos e validação do pipeline.yaml`.

### Tarefa 5: `Storage` e `LocalStorage`

**Arquivos:**
- Criar: `src/dataeng_core/storage/__init__.py` (exporta), `storage/base.py`, `storage/local.py`
- Teste: `tests/storage/contract.py` (bateria reutilizável), `tests/storage/test_local_storage.py`

**Interfaces:**
- Consome: `Environment`.
- Produz: `class Storage(Protocol)` com `read(path: str) -> bytes`, `write(path: str, data: bytes) -> None`, `list(prefix: str) -> list[str]`, `exists(path: str) -> bool`, `delete(path: str) -> None`; `class LocalStorage(root: Path)` (mapeia `gs://<bucket>/<chave>` para `<root>/<bucket>/<chave>`); `make_storage(env: Environment) -> Storage` (local quando `env.modo == "local"`; para `gcp`, importa `GcsStorage` na Tarefa 14).

- [ ] **Passo 1: testes.** A bateria `run_storage_contract(storage)` verifica: escrever e ler; `exists` antes e depois; `list` por prefixo em ordem e sem pastas; `delete` idempotente; ler caminho inexistente levanta `SourceError`; caminhos com `\` e `/` equivalentes (Windows). `test_local_storage_passes_contract` usa `tmp_path`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(storage): interface Storage e LocalStorage`.

### Tarefa 6: `Warehouse` e `LocalWarehouse` (DuckDB)

**Arquivos:**
- Criar: `src/dataeng_core/warehouse/__init__.py`, `warehouse/base.py`, `warehouse/local.py`
- Teste: `tests/warehouse/contract.py`, `tests/warehouse/test_local_warehouse.py`

**Interfaces:**
- Consome: `Environment`, `LoadError`.
- Produz: `LoadMode = Literal["append","overwrite_partition","merge"]`; `class Warehouse(Protocol)` com `execute_ddl(sql: str) -> None`, `run_sql(statements: Sequence[str]) -> None`, `create_or_replace_view(name: str, select_sql: str) -> None`, `load(table: str, rows: pd.DataFrame, *, mode: LoadMode, partition_column: str | None = None, merge_keys: Sequence[str] = ()) -> int` (devolve linhas gravadas), `query(sql: str) -> pd.DataFrame`, `table_exists(table: str) -> bool`, `row_count(table: str) -> int`; `class LocalWarehouse(db_path: Path | None = None)` (em memória quando `None`; `dataset.tabela` vira `schema.tabela`, criando o schema); `make_warehouse(env: Environment) -> Warehouse`.

- [ ] **Passo 1: testes (bateria `run_warehouse_contract`).** `append` soma; `overwrite_partition` troca só a partição dos valores presentes em `partition_column`; `merge` atualiza as chaves existentes e insere as novas, sem duplicar; reexecutar a mesma carga em `overwrite_partition` e `merge` mantém a contagem; `execute_ddl` cria a tabela; `create_or_replace_view` substitui; `query` devolve DataFrame; `table_exists`/`row_count`; `merge` sem `merge_keys` levanta `LoadError`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(warehouse): interface Warehouse e LocalWarehouse em DuckDB`.

### Tarefa 7: leitura de arquivos (`sources.files`)

**Arquivos:**
- Criar: `src/dataeng_core/sources/files.py`
- Teste: `tests/sources/test_files.py`

**Interfaces:**
- Consome: `ArquivoOrigem`, `SchemaSpec`, `SourceError`.
- Produz: `@dataclass ReadResult(rows: pd.DataFrame, rejected: pd.DataFrame)` (`rejected` com colunas `_linha_original` e `_motivo`); `read_file(data: bytes, origem: ArquivoOrigem, schema: SchemaSpec) -> ReadResult`; `extract_zip_members(data: bytes) -> dict[str, bytes]` (corrige nomes em cp437 para UTF-8 e ignora caminhos fora da pasta).

- [ ] **Passo 1: testes.** `test_csv_with_bom_semicolon_and_crlf` (cabeçalho sem BOM nas colunas); `test_latin1_encoding_when_declared`; `test_row_with_extra_semicolon_goes_to_rejected_with_reason` (`_motivo == "numero_de_colunas_inesperado"`); `test_header_only_file_returns_zero_rows_and_columns` (**foco 1**); `test_empty_file_returns_zero_rows` (**foco 1**); `test_json_lines_and_json_array`; `test_xlsx_first_sheet`; `test_zip_with_accented_member_name`; `test_zip_slip_member_is_ignored`; `test_quoted_delimiter_is_not_a_split`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar (`csv` da biblioteca padrão para detectar linhas com contagem errada; `pandas` para xlsx e JSON), rodar (passa).
- [ ] **Passo 5: commit** `feat(sources): leitura de csv, json, xlsx e zip com quarentena de linhas ruins`.

### Tarefa 8: origem API (`sources.api`)

**Arquivos:**
- Criar: `src/dataeng_core/sources/api.py`
- Teste: `tests/sources/test_api.py`

**Interfaces:**
- Consome: `ApiOrigem` (`endpoint`, `autenticacao{tipo: none|bearer|header|basic, segredo_env, nome_cabecalho}`, `paginacao{tipo: nenhuma|pagina|offset|cursor, parametro, tamanho, caminho_proximo, caminho_dados}`, `limite_taxa: float | None` em requisições por segundo), `retry`, `redact`, `SourceError`.
- Produz: `fetch_records(origem: ApiOrigem, *, session: requests.Session | None = None, env: Mapping[str, str] | None = None, sleep: Callable[[float], None] = time.sleep) -> list[dict[str, Any]]`.

- [ ] **Passo 1: testes** com uma sessão falsa que devolve respostas roteirizadas. `test_page_pagination_until_empty`; `test_cursor_pagination_follows_next_path`; `test_offset_pagination`; `test_bearer_token_read_from_env_var`; `test_rate_limit_sleeps_between_requests`; `test_retries_on_503_then_succeeds`; `test_4xx_is_not_retried_and_raises_source_error`; `test_missing_secret_env_names_the_variable`; `test_secret_never_in_error_message_or_log` (**foco 5**: com o token `S3CR3T` num cabeçalho, falha de rede e `caplog`, o texto `S3CR3T` não aparece em exceção nem em log).
- [ ] **Passo 2 a 4:** rodar (falha), implementar (`requests`; todo texto que sai passa por `redact` com o segredo), rodar (passa).
- [ ] **Passo 5: commit** `feat(sources): origem API com paginação, autenticação e limite de taxa`.

### Tarefa 9: transformação declarativa em pandas

**Arquivos:**
- Criar: `src/dataeng_core/transform/engine_pandas.py`, `transform/hooks.py`
- Teste: `tests/transform/test_engine_pandas.py`, `tests/transform/test_hooks.py`

**Interfaces:**
- Consome: `TransformOp` (`renomear`, `tipar`, `normalizar`, `deduplicar`, `rejeitar_se`, `hook`), `DataEngError`.
- Produz: `@dataclass TransformResult(rows: pd.DataFrame, rejected: pd.DataFrame)`; `apply_transform(df: pd.DataFrame, ops: Sequence[TransformOp], *, project_root: Path) -> TransformResult`; `resolve_hook(ref: str, *, project_root: Path) -> Callable[[pd.DataFrame], pd.DataFrame]` (`"modulo:funcao"`, com `project_root` no `sys.path`). Normalizações: `apenas_digitos`, `trim`, `maiusculas`, `minusculas`, `colapsar_espacos`. Tipos: `decimal` (com `separador_decimal`), `inteiro`, `texto`, `data` (com `formato`), `booleano`, `timestamp`. Regras de `rejeitar_se`: `nao_nulo`, `regex`, `faixa`.

- [ ] **Passo 1: testes.** `test_rename_and_cast_decimal_with_comma`; `test_cast_failure_rejects_row_with_reason_and_keeps_original`; `test_normalize_cnpj_with_leading_space_to_digits`; `test_dedup_keeps_first_and_counts_removed`; `test_reject_if_null_regex_and_range`; `test_ops_apply_in_declared_order`; `test_hook_runs_between_ops`; `test_missing_hook_names_module_and_function`; `test_empty_dataframe_passes_through`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(transform): motor pandas declarativo com quarentena e ganchos`.

### Tarefa 10: verificações de qualidade

**Arquivos:**
- Criar: `src/dataeng_core/quality/checks.py`
- Teste: `tests/quality/test_checks.py`

**Interfaces:**
- Consome: `QualityCheck` (`nao_nulo`, `unico`, `faixa`, `conjunto`, `formato`, cada uma com `limiar: float = 1.0` e `bloqueante: bool = False`), `QualityError`.
- Produz: `@dataclass CheckResult(nome: str, coluna: str | None, aprovado: bool, taxa: float, limiar: float, bloqueante: bool)`; `run_checks(df: pd.DataFrame, checks: Sequence[QualityCheck]) -> list[CheckResult]`; `raise_if_blocking(results: Sequence[CheckResult]) -> None` (levanta `QualityError` se algum bloqueante reprovou).

- [ ] **Passo 1: testes.** Um por verificação (aprova e reprova, com `taxa` exata); `test_threshold_allows_partial_failures`; `test_unique_over_composite_key`; `test_empty_dataframe_is_vacuously_approved`; `test_blocking_failure_raises_and_non_blocking_does_not`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(quality): verificações com limiar e bloqueio opcional`.

### Tarefa 11: módulo SQL (parâmetros, divisão, validação)

**Arquivos:**
- Criar: `src/dataeng_core/sql/render.py`, `sql/validate.py`
- Teste: `tests/sql/test_render.py`, `tests/sql/test_validate.py`

**Interfaces:**
- Consome: `substitute`, `SqlError`.
- Produz: `render_sql(sql: str, params: Mapping[str, str]) -> str` (`${x}` ausente levanta `SqlError` citando `x` e o arquivo, se informado); `split_statements(sql: str, dialect: str = "bigquery") -> list[str]` (via `sqlglot`; ignora comentários e vazios); `validate_sql(sql: str, dialect: str = "bigquery") -> list[str]` (levanta `SqlError` com linha na sintaxe inválida; devolve **avisos**, por exemplo `INSERT` sem `MERGE`, `OVERWRITE` nem `WHERE NOT EXISTS`).

- [ ] **Passo 1: testes.** `test_render_replaces_params`; `test_render_missing_param_raises_before_anything_runs` (**foco 4**); `test_split_ignores_semicolon_inside_string_literal` (**foco 4**); `test_split_ignores_trailing_comment_and_blank_statements` (**foco 4**); `test_validate_rejects_bad_syntax_with_line`; `test_validate_accepts_bigquery_merge_and_create_or_replace_view`; `test_validate_warns_on_plain_insert`; `test_validate_does_not_warn_on_merge`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(sql): renderização, divisão e validação de SQL BigQuery`.

### Tarefa 12: executor de etapas, controle e a etapa `ingestao` (arquivo e API)

**Arquivos:**
- Criar: `src/dataeng_core/steps/context.py`, `steps/registry.py`, `steps/control.py`, `steps/ingestao.py`
- Teste: `tests/steps/test_registry.py`, `tests/steps/test_ingestao_file.py`, `tests/steps/test_control.py`

**Interfaces:**
- Consome: tudo das Tarefas 2 a 11.
- Produz: `@dataclass StepContext(pipeline: Pipeline, env: Environment, storage: Storage, warehouse: Warehouse, pubsub: PubSubClient | None, run_id: str, params: dict[str, str], project_root: Path)`; `@dataclass StepResult(step_id: str, status: Literal["ok","falha"], rows_read: int = 0, rows_written: int = 0, rows_rejected: int = 0, backlog: int | None = None, quality: list[CheckResult] = [], detail: str = "")`; `StepRunner = Callable[[Step, StepContext], StepResult]`; `register_step_type(tipo: str, runner: StepRunner) -> None`; `run_step(step: Step, ctx: StepContext) -> StepResult`; `run_pipeline(pipeline: Pipeline, ctx: StepContext, *, only: Sequence[str] | None = None) -> list[StepResult]` (na ordem de `ordered_steps`; para na primeira falha e marca as seguintes como não executadas); `build_context(pipeline: Pipeline, env: Environment, *, project_root: Path, run_id: str | None = None) -> StepContext`; `record_result(warehouse: Warehouse, env: Environment, result: StepResult, run_id: str) -> None` (grava em `<dataset_silver>._controle_execucoes`). A etapa `ingestao` com origem `arquivo` e `api`: lê → grava o bronze (com manifesto) → transforma → carrega a silver com as colunas de controle → grava a quarentena (`<tabela>_rejeitados`) → roda a qualidade → roda `dml_pos_carga` → registra o controle. `modo_carga: overwrite_particao` vira `overwrite_partition` no `Warehouse.load`.

- [ ] **Passo 1: testes (todos com `LocalStorage` e `LocalWarehouse`).** `test_ingest_file_end_to_end` (bronze com manifesto, silver com `_run_id`, `_source_file`, `_loaded_at`, `_ingestion_date`, quarentena com motivo, controle com contagens); `test_rerun_does_not_duplicate_overwrite_partition`; `test_rerun_does_not_duplicate_merge`; `test_failure_between_bronze_and_silver_then_rerun_completes_without_duplicates` (**foco 2**: faz o `Warehouse.load` falhar uma vez; o bronze existe; a 2ª execução termina com a contagem certa); `test_empty_input_succeeds_with_zero_rows_and_control_row` (**foco 1**); `test_blocking_quality_failure_marks_step_failed`; `test_run_pipeline_stops_after_first_failure`; `test_unknown_step_type_raises_config_error`; `test_secret_not_in_control_table_or_detail` (**foco 5**); `test_register_step_type_adds_new_runner`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(steps): executor de etapas, controle e ingestão por arquivo e API`.

### Tarefa 13: pontos de entrada e CLI

**Arquivos:**
- Criar: `src/dataeng_core/jobs/run_step.py`, `jobs/ingest.py`, `cli/__init__.py`, `cli/main.py`
- Teste: `tests/cli/test_cli.py`, `tests/jobs/test_run_step.py`

**Interfaces:**
- Consome: `load_pipeline`, `build_context`, `run_pipeline`, `render_sql`, `validate_sql`.
- Produz: `jobs.run_step.main(argv: Sequence[str] | None = None) -> int` (`--config`, `--ambiente`, `--etapa`, `--param chave=valor` repetível; código 0 em sucesso e 1 em falha, imprimindo o erro com `title`, `what`, `fix` e `reference`); `jobs.ingest.main(argv) -> int` (igual, mas recusa etapas que não sejam `ingestao`); `cli.main(argv: Sequence[str] | None = None) -> int` com os subcomandos `validate-config`, `plan` (ordem das etapas, motor, e o SQL já renderizado quando houver) e `run local` (usa o ambiente `local`).

- [ ] **Passo 1: testes.** `test_validate_config_ok_and_error_exit_codes`; `test_validate_config_prints_actionable_error`; `test_plan_lists_steps_in_topological_order`; `test_run_local_executes_pipeline_and_returns_zero`; `test_ingest_refuses_non_ingestion_step`; `test_param_flag_overrides_parameter`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar com `argparse`, rodar (passa).
- [ ] **Passo 5: commit** `feat(cli): comandos validate-config, plan e run local, e pontos de entrada dos jobs`.

### Tarefa 14: implementações GCP de `Storage` e `Warehouse`

**Arquivos:**
- Criar: `src/dataeng_core/storage/gcs.py`, `warehouse/bigquery.py`
- Modificar: `storage/__init__.py`, `warehouse/__init__.py` (`make_storage` e `make_warehouse` devolvem as versões GCP quando `modo == "gcp"`)
- Teste: `tests/storage/test_gcs_storage.py`, `tests/warehouse/test_bigquery_warehouse.py`

**Interfaces:**
- Produz: `GcsStorage(project: str)` e `BigQueryWarehouse(project: str, location: str)` implementando os mesmos protocolos. As importações de `google.cloud` são **tardias** (dentro de métodos ou do construtor), para o pacote base continuar sem GCP.

- [ ] **Passo 1: testes.** Reaproveitam as baterias `run_storage_contract` e `run_warehouse_contract`, marcados com `@pytest.mark.gcp` e **pulados sem credenciais** (`GOOGLE_APPLICATION_CREDENTIALS` ou `DATAENG_GCP_PROJECT` ausentes). Testes que **rodam sempre**: `test_importing_package_without_gcp_extra_works` (importa `dataeng_core` com `google` ausente de `sys.modules`); `test_make_storage_gcp_without_extra_gives_actionable_error` (erro orienta instalar `dataeng-core[gcp]`).
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (os que dependem de GCP ficam pulados; o resto passa).
- [ ] **Passo 5: commit** `feat(gcp): GcsStorage e BigQueryWarehouse com testes de contrato`.

### Tarefa 15: fixtures da ANP e teste de aceite local

**Arquivos:**
- Criar: `tests/fixtures/anp/amostra.csv` (cerca de 50 linhas sintéticas), `tests/fixtures/anp/pipeline.yaml`, `tests/fixtures/anp/config/ambientes/local.yaml`, `tests/fixtures/anp/schemas/*.yaml`, `tests/fixtures/anp/sql/ddl/silver_precos.sql`
- Teste: `tests/acceptance/test_anp_ingestion_local.py`

**Interfaces:**
- Consome: `cli.main`, `load_pipeline`, `run_pipeline`.

- [ ] **Passo 1: criar a amostra** com os defeitos reais mapeados: BOM, `;`, vírgula decimal, data `dd/mm/aaaa`, CNPJ com espaço no início (pelo menos 3 linhas), 2 linhas com `;` no endereço (colunas deslocadas), 1 linha com preço negativo, 2 linhas duplicadas.
- [ ] **Passo 2: escrever o teste** `test_anp_sample_ingests_with_quarantine_and_is_idempotent`: roda `dataeng run local` duas vezes; afirma silver com `N` linhas válidas (valor exato conhecido da amostra), quarentena com as 2 linhas deslocadas e o preço negativo, CNPJ só com dígitos, nenhuma linha a mais na 2ª rodada, e 2 linhas na tabela de controle por etapa.
- [ ] **Passo 3: rodar** e ver falhar; ajustar a configuração da fixture e, se o teste revelar defeito real na biblioteca, corrigi-lo com um teste novo na tarefa dona.
- [ ] **Passo 4: rodar** `pytest tests/acceptance -v`. Esperado: passa.
- [ ] **Passo 5: commit** `test(acceptance): ingestão local da amostra da ANP`.

### Tarefa 16: planejador de tarefas e montador de DAG (ingestão)

**Arquivos:**
- Criar: `src/dataeng_core/airflow/planner.py`, `airflow/dag.py`
- Teste: `tests/airflow/test_planner.py`, `tests/airflow/test_dag_static.py`

**Interfaces:**
- Consome: `Pipeline`, `IngestaoStep`.
- Produz (`planner`, **sem importar Airflow**): `@dataclass TaskSpec(task_id: str, kind: Literal["cloud_run_job","dataproc_batch","bigquery_job"], command: list[str], depends_on: list[str], resources: dict[str, Any])`; `plan_tasks(pipeline: Pipeline) -> list[TaskSpec]`. Nesta fase, só `ingestao`: `motor: cloud_run` → `cloud_run_job` com `command = ["python","-m","dataeng_core.jobs.run_step","--config",<arquivo>,"--etapa",<id>]`; `motor: dataproc` → `dataproc_batch`.
- Produz (`dag`, importa Airflow **dentro** da função): `build_dag(config_path: str, *, ambiente: str = "prod", extra_tasks: Callable[[Any], None] | None = None, **dag_kwargs: Any) -> Any`.

- [ ] **Passo 1: testes.** `test_ingestion_cloud_run_maps_to_cloud_run_job_command`; `test_ingestion_dataproc_maps_to_dataproc_batch`; `test_dependencies_follow_depends_on`; `test_planner_does_not_import_airflow` (`"airflow" not in sys.modules` após o import); `test_dag_module_has_no_airflow_import_at_module_level` (checagem por `ast`); `test_build_dag_without_airflow_gives_actionable_error`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa). Um teste `@pytest.mark.airflow` que importa a DAG de verdade é marcado e pulado quando o Airflow não está instalado (roda no CI, Tarefa 25).
- [ ] **Passo 5: commit** `feat(airflow): planejador de tarefas e montador de DAG para ingestão`.

### Tarefa 17: `template-pipeline` com o tipo `ingestao`

**Arquivos:**
- Criar: `template-pipeline/cookiecutter.json`, `README.md`, `CHANGELOG.md`, `hooks/post_gen_project.py`, `{{cookiecutter.nome_projeto}}/` (estrutura da seção 5.2 para o tipo `ingestao`: `config/pipeline.yaml`, `config/ambientes/{local,dev,prod}.yaml`, `schemas/`, `sql/ddl/`, `custom/__init__.py`, `dags/pipeline.py`, `data/amostra.csv`, `Dockerfile`, `pyproject.toml`, `README.md`, `tests/test_config.py`, `tests/test_local_run.py`), `tests/conftest.py`, `tests/test_generation.py`

**Interfaces:**
- Consome: `dataeng_core.cli.main`.
- Produz: `cookiecutter.json` com as variáveis da seção 5.1; `hooks/post_gen_project.py` removendo o que não se aplica (DAG e bloco de origem não escolhidos, `sql/views/`, `spark/`); projeto gerado **executável de saída**: `pipeline.yaml` de exemplo com um schema genérico (`id`, `nome`, `valor`) e `data/amostra.csv` de 5 linhas, para que `dataeng run local` funcione antes de qualquer edição.

- [ ] **Passo 1: testes (`tests/test_generation.py`).** Função auxiliar `bake(**contexto) -> Path` com `cookiecutter(..., no_input=True, output_dir=tmp_path, extra_context=...)`. `test_ingestao_arquivo_cloud_run_generates_expected_files`; `test_unwanted_blocks_are_removed` (sem `sql/views/`, sem `spark/`, só a DAG do motor escolhido); `test_generated_project_validates_and_plans` (`cli.main(["validate-config", ...]) == 0` e `plan == 0`); `test_generated_project_runs_local_and_is_idempotent`; `test_generated_project_own_tests_pass` (executa o `pytest` do projeto gerado em subprocesso); `test_origem_da_biblioteca_changes_the_dependency_line` (`git` gera uma dependência `git+https://...@dataeng-core-vX.Y.Z#subdirectory=lib-dataeng-core`; `artifact_registry` gera `dataeng-core[...]==X.Y.*` com `--extra-index-url`); `test_dockerfile_installs_pinned_library_and_sets_entrypoint` (checagem estática do texto: instala a versão fixada e usa `python -m dataeng_core.jobs.run_step` como entrada); `test_matrix_origem_motor_modo_carga` (parametrizado: `arquivo`/`api` × `cloud_run`/`dataproc`/`ambos` × `append`/`overwrite_particao`/`merge`, cada combinação precisa validar e fazer `plan`).
- [ ] **Passo 2: rodar** e ver falhar.
- [ ] **Passo 3: criar** o template e o gancho (Jinja só nos nomes e valores; os `${}` dos YAML e SQL **não** sofrem renderização do Cookiecutter, pois não usam `{{ }}`).
- [ ] **Passo 4: rodar** `pytest template-pipeline/tests -v`. Esperado: passa (instalar `dataeng-core[local]` no mesmo ambiente).
- [ ] **Passo 5: commit** `feat(template): template-pipeline com o tipo ingestao`.

---

# Fase 2: Pub/Sub em pull

### Tarefa 18: `PubSubClient`, `LocalPubSub` e `GcpPubSub`

**Arquivos:**
- Criar: `src/dataeng_core/pubsub/base.py`, `pubsub/local.py`, `pubsub/gcp.py`; modificar `pubsub/__init__.py`
- Teste: `tests/pubsub/contract.py`, `tests/pubsub/test_local_pubsub.py`, `tests/pubsub/test_gcp_pubsub.py`

**Interfaces:**
- Produz: `@dataclass PubSubMessage(message_id: str, ack_id: str, data: bytes, attributes: dict[str, str], publish_time: datetime)`; `class PubSubClient(Protocol)` com `pull(subscription: str, max_messages: int, timeout_s: float) -> list[PubSubMessage]`, `ack(subscription: str, ack_ids: Sequence[str]) -> None`, `nack(subscription: str, ack_ids: Sequence[str]) -> None`, `backlog(subscription: str) -> int | None`; `class LocalPubSub` com `publish(subscription: str, data: bytes, attributes: dict[str, str] | None = None, message_id: str | None = None) -> str` (mensagem não confirmada volta no próximo `pull`; publicar o mesmo `message_id` duas vezes simula entrega duplicada); `GcpPubSub(project: str)` com importação tardia; `make_pubsub(env: Environment) -> PubSubClient`.

- [ ] **Passo 1: testes (bateria `run_pubsub_contract`).** `pull` respeita `max_messages`; mensagem sem `ack` volta; `ack` remove; `nack` devolve já; `pull` em fila vazia devolve `[]` sem esperar além de `timeout_s`; `backlog` conta as pendentes; duplicata mantém o mesmo `message_id` e `ack_id` diferentes. O teste de GCP usa `@pytest.mark.gcp` e é pulado sem credenciais.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(pubsub): interface PubSubClient, versão local e versão GCP`.

### Tarefa 19: origem `pubsub` na ingestão

**Arquivos:**
- Criar: `src/dataeng_core/sources/pubsub.py`
- Modificar: `steps/ingestao.py` (ramo da origem `pubsub`), `steps/context.py` (`build_context` preenche `pubsub` com `make_pubsub` quando alguma etapa usa a origem `pubsub`), `template-pipeline` (bloco `pubsub` no `pipeline.yaml` e na matriz). O modelo `PubSubOrigem` já existe desde a Tarefa 4.
- Teste: `tests/sources/test_pubsub.py`, `tests/steps/test_ingestao_pubsub.py`; `template-pipeline/tests/test_generation.py` (estender a matriz)

**Interfaces:**
- Consome: `PubSubClient`, `PubSubOrigem` (`assinatura`, `max_mensagens`, `tempo_maximo_s`, `formato: Literal["json"]`, `dedupe_por: Literal["message_id"]`, `schema`).
- Produz: `@dataclass PulledBatch(records: pd.DataFrame, rejected: pd.DataFrame, ack_ids: list[str], backlog: int | None)`; `pull_batch(client: PubSubClient, origem: PubSubOrigem) -> PulledBatch` (monta uma linha por mensagem com `message_id`, `publish_time`, `attributes` e o JSON achatado; JSON inválido vai para `rejected` e **seu `ack_id` entra em `ack_ids`**, para não reprocessar para sempre).

- [ ] **Passo 1: testes.** `test_pull_batch_builds_rows_with_message_metadata`; `test_invalid_json_goes_to_rejected_and_is_acked` (**foco 3**); `test_empty_queue_succeeds_with_zero_rows` (**foco 3**); `test_ack_happens_only_after_bronze_write` (a gravação no `LocalStorage` falha → nenhum `ack`, as mensagens voltam no `pull` seguinte); `test_duplicate_message_id_is_deduplicated_in_silver`; `test_backlog_is_recorded_in_control_table`; `test_respects_max_messages`; teste de geração: a matriz inclui `origens` = `["arquivo","pubsub"]` e o projeto gerado valida e faz `plan`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar (ordem: puxar → gravar bronze → carregar silver → `ack`), rodar (passa).
- [ ] **Passo 5: commit** `feat(pubsub): ingestão por pull em lote com confirmação após gravar`.

---

# Fase 3: relatório, motores e híbrido

### Tarefa 20: etapas `sql` e `view`

**Arquivos:**
- Criar: `src/dataeng_core/steps/sql_steps.py`
- Modificar: `cli/main.py` (o `plan` mostra o SQL renderizado e os avisos de idempotência)
- Teste: `tests/steps/test_sql_steps.py`, `tests/cli/test_plan_sql.py`

**Interfaces:**
- Consome: `render_sql`, `split_statements`, `validate_sql`, `Warehouse.run_sql`, `Warehouse.create_or_replace_view`, `SqlStep`, `ViewStep`.
- Produz: runners registrados para `sql` e `view`. `sql`: lê os arquivos, renderiza com `ctx.params`, divide, valida e executa na ordem. `view`: espera um `CREATE OR REPLACE VIEW` e o executa; se o arquivo trouxer só um `SELECT`, envolve com o nome informado em `nome_view` (campo opcional da etapa).

- [ ] **Passo 1: testes (SQL em subconjunto portátil, rodando no DuckDB).** `test_sql_step_runs_merge_and_is_idempotent`; `test_sql_step_renders_params`; `test_view_step_creates_and_replaces`; `test_missing_param_fails_before_executing_any_statement` (**foco 4**); `test_syntax_error_reports_file_and_line`; `test_plan_prints_rendered_sql_and_idempotency_warning`; `test_step_order_respects_depends_on`.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(steps): etapas sql e view`.

### Tarefa 21: etapas `python` e `spark`, e o motor PySpark da transformação

**Arquivos:**
- Criar: `src/dataeng_core/steps/python_step.py`, `steps/spark_step.py`, `transform/engine_spark.py`, `jobs/ingest_spark.py`
- Teste: `tests/steps/test_python_step.py`, `tests/transform/test_equivalence.py`, `tests/jobs/test_ingest_spark.py`

**Interfaces:**
- Consome: `resolve_hook`, `TransformOp`, `PythonStep` (`funcao: "modulo:funcao"`), `SparkStep` (`script`, `dataproc{runtime, executores}`).
- Produz: runner `python` (chama `funcao(ctx) -> StepResult | None`); runner `spark` (executa o script local com `spark-submit` em modo local quando `ctx.env.modo == "local"`, e delega ao planejador de DAG no modo `gcp`); `apply_transform_spark(df: pyspark.sql.DataFrame, ops: Sequence[TransformOp], *, project_root: Path) -> SparkTransformResult(rows, rejected)` (importação tardia do PySpark); `jobs.ingest_spark.main(argv) -> int`.

- [ ] **Passo 1: testes.** `test_python_step_calls_function_with_context`; `test_python_step_failure_is_marked_failed_with_reference`; **equivalência:** `test_pandas_and_spark_engines_agree` parametrizado sobre os mesmos casos da Tarefa 9 (renomear, tipar decimal com vírgula, normalizar, deduplicar, rejeitar), comparando `rows` e `rejected` ordenados; `test_spark_engine_import_is_lazy`. Os testes de PySpark ficam com `@pytest.mark.spark` e são pulados se o PySpark ou o Java não estiverem instalados (rodam no CI, Linux).
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa; os de Spark pulam localmente se faltar o Java).
- [ ] **Passo 5: commit** `feat(steps): etapas python e spark e motor PySpark da transformação`.

### Tarefa 22: etapa `qualidade` e planejador completo de DAG

**Arquivos:**
- Criar: `src/dataeng_core/steps/qualidade_step.py`
- Modificar: `airflow/planner.py`, `airflow/dag.py`
- Teste: `tests/steps/test_qualidade_step.py`, `tests/airflow/test_planner_full.py`

**Interfaces:**
- Consome: `QualidadeStep` (`verificacoes` com `tabela`), `run_checks`, `TaskSpec`.
- Produz: runner `qualidade` (consulta a tabela com `Warehouse.query`, roda `run_checks` e grava o controle). `plan_tasks` cobre todos os tipos: `sql`, `view` e `qualidade` → `bigquery_job` (sem contêiner, com o SQL renderizado em `command`); `ingestao`, `python` → `cloud_run_job` ou `dataproc_batch` conforme `motor`; `spark` → `dataproc_batch`. `build_dag` cria as tarefas, aplica `depends_on` e chama `extra_tasks(dag)` se informado.

- [ ] **Passo 1: testes.** `test_quality_step_records_results_and_fails_when_blocking`; `test_planner_maps_every_step_type_to_a_task_kind`; `test_planner_sql_task_carries_rendered_sql_without_container`; `test_planner_with_ambos_creates_two_task_sets_named_cloud_run_and_dataproc`; `test_extra_tasks_hook_is_called` (com o `build_dag` coberto no CI com Airflow, marca `@pytest.mark.airflow`).
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(airflow): planejamento completo de tarefas e etapa de qualidade`.

### Tarefa 23: tipos `relatorio` e `hibrido` no template

**Arquivos:**
- Modificar: `template-pipeline/cookiecutter.json`, `hooks/post_gen_project.py`, o diretório gerado (`sql/views/`, `sql/dml/`, `spark/`, `custom/enriquecimento.py`, exemplos de `pipeline.yaml` por tipo), `tests/test_generation.py`
- Criar: `template-pipeline/tests/test_hybrid_anp.py`

**Interfaces:**
- Consome: as etapas das Tarefas 20 a 22.
- Produz: três exemplos executáveis de `pipeline.yaml` (um por tipo), com SQL portátil; `usa_python` e `usa_spark` habilitam as etapas correspondentes.

- [ ] **Passo 1: testes.** Estender a matriz: `tipo_projeto` (`ingestao`, `relatorio`, `hibrido`) × `usa_python` × `usa_spark`; `test_relatorio_has_no_origin_schemas`; `test_hibrido_generates_everything`; `test_generated_relatorio_runs_local` (view e `MERGE` sobre dados semeados); **aceite híbrido da ANP** (`test_hybrid_anp.py`): gera um `hibrido`, troca pelo `pipeline.yaml` e schemas da ANP, roda `dataeng plan` (ordem das etapas e SQL renderizado), roda `dataeng run local` duas vezes e afirma silver, quarentena, view de preço médio por UF, agregado por `MERGE`, ausência de duplicatas e uma linha de controle por etapa.
- [ ] **Passo 2 a 4:** rodar (falha), implementar, rodar (passa).
- [ ] **Passo 5: commit** `feat(template): tipos relatorio e hibrido`.

---

# Fase 4: acabamento

### Tarefa 24: CI (GitHub Actions)

**Arquivos:**
- Criar: `.github/workflows/lib-dataeng-core.yml`, `.github/workflows/template-pipeline.yml`

**Interfaces:**
- Produz: dois fluxos com filtro por pasta (`paths:`). `lib-dataeng-core`: `ruff`, `mypy`, `lint-imports`, `pytest` (Linux e Windows), job `spark` (Java + `[spark]`, marca `spark`), job `airflow` (Linux, `[airflow]`, marca `airflow`, importa a DAG gerada), build do pacote. `template-pipeline`: instala a biblioteca da pasta e roda a matriz de geração.

- [ ] **Passo 1: escrever** os fluxos. Confirmar as versões atuais das ações e do Airflow (com o arquivo de constraints oficial) antes de fixar.
- [ ] **Passo 2: validar** a sintaxe localmente com um linter de workflows (por exemplo, `actionlint`) se disponível; caso contrário, revisar à mão e registrar que a execução real depende do GitHub.
- [ ] **Passo 3: commit** `ci: fluxos da biblioteca e do template`.

### Tarefa 25: guias, READMEs e CHANGELOG

**Arquivos:**
- Criar/modificar: `lib-dataeng-core/README.md`, `lib-dataeng-core/CHANGELOG.md`, `template-pipeline/docs/guia-novo-pipeline.md`, `template-pipeline/README.md`, `template-pipeline/tests/test_docs_commands.py`
- Modificar: `README.md` da raiz (tabela de projetos e mapa de pastas), `docs/gerar_arquitetura.py` (acrescentar uma caixa para a plataforma, se couber) e `planos/README.md`

**Interfaces:**
- Produz: guia "criar um novo pipeline" que leva de zero a uma DAG planejada (a montagem real no Airflow não foi validada) (gerar, preencher o YAML, rodar `validate-config`, `plan`, `run local`); tabela de **limitações** copiada da seção 11 da especificação ("implementado e testado com versão local; validar no GCP").

- [ ] **Passo 1: escrever** a documentação. Cada comando do guia é executado de verdade antes de ser citado.
- [ ] **Passo 2: escrever e rodar** `template-pipeline/tests/test_docs_commands.py`, com `test_guide_commands_run_on_a_generated_project`: extrai de `docs/guia-novo-pipeline.md` os blocos marcados com `<!-- testar -->` e os executa num projeto gerado, esperando código de saída 0.
- [ ] **Passo 3: commit** `docs: guia de novo pipeline, READMEs e limitações`.

### Tarefa 26: adaptar o `combustiveis-pipeline-gcp` ao template

**Arquivos:**
- Modificar: `combustiveis-pipeline-gcp/README.md`, `planos/01-combustiveis-pipeline-gcp.md`, `combustiveis-pipeline-gcp/common/sources.yaml` (mantido como manifesto das fontes)
- Criar: `combustiveis-pipeline-gcp/config/pipeline.yaml` e `schemas/` gerados a partir do template

**Interfaces:**
- Consome: o projeto `hibrido` testado localmente na Tarefa 23.

- [ ] **Passo 1: gerar** um projeto `hibrido` com as origens da ANP e trazer `config/` e `schemas/` para a pasta existente, sem apagar o `download_anp.py`.
- [ ] **Passo 2: rodar** `dataeng validate-config` e `dataeng plan` na pasta; ajustar o plano 01 para citar o template como base das Fases 1 a 3 e manter a comparação Python × PySpark.
- [ ] **Passo 3: commit** `feat(combustiveis): pipeline da ANP passa a usar o template`.

---

## Autoavaliação

- **Cobertura da especificação:** objetivo e decisões (seções 1 e 2) → Tarefas 1 e 17; camadas e extras (4.1, 4.2) → 1; interfaces (4.3) → 5, 6, 14 e 18; transformação em dois motores (4.4) → 9 e 21; regras de execução e Pub/Sub (4.5) → 12 e 19; API pública e versões (4.6) → 4 e 24; template (5.1 a 5.5) → 16, 17, 22 e 23; testes e CI (6) → 5, 6, 17, 19, 21 e 24; distribuição e versões (7) → 1 e 24; validação com a ANP (9) → 15 e 23; critérios de sucesso (10) → 15, 23, 24 e 25; limitações (11) → 14, 16 e 25. O campo `chaves_merge` (lacuna encontrada na especificação) foi incluído e a especificação atualizada.
- **Consistência de nomes:** `Storage.read/write/list/exists/delete`, `Warehouse.load/run_sql/create_or_replace_view`, `PubSubClient.pull/ack/nack/backlog`, `StepContext`, `StepResult`, `TaskSpec` e `plan_tasks` têm uma única definição e são citados com a mesma assinatura. A tradução `overwrite_particao` → `overwrite_partition` está registrada na Tarefa 12.
- **Proporção:** o plano descreve decisões, assinaturas e testes, sem corpos de função.
