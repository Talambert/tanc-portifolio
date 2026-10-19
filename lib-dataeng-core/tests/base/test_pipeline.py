import textwrap
from pathlib import Path

import pytest

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.pipeline import (
    ApiOrigem,
    ArquivoOrigem,
    IngestaoStep,
    Pipeline,
    PubSubOrigem,
    PythonStep,
    QualidadeStep,
    SparkStep,
    SqlStep,
    ViewStep,
    load_pipeline,
)

ENV_LOCAL = """\
modo: local
projeto_gcp: projeto-local
regiao: regiao-local
bucket_bronze: bucket-local
dataset_silver: silver_local
dataset_gold: gold_local
"""

# Exemplo da seção 5.3 da especificação, sem alterações.
HYBRID = """\
schema_version: 1
nome: precos_combustiveis
agenda: "0 6 * * 1"
parametros:                       # disponíveis nos SQL como ${...}
  dataset_silver: silver
  dataset_gold: gold

etapas:
  - id: ingere_precos
    tipo: ingestao
    motor: cloud_run              # ou: dataproc
    origem:
      tipo: arquivo               # ou: api | pubsub
      localizacao: "https://exemplo.gov.br/dados/ca-${ano}-${semestre}.zip"
      formato: csv
      encoding: utf-8-sig
      delimitador: ";"
      schema: schemas/origem_precos.yaml
      # tipo: api -> endpoint, autenticacao (tipo e nome do segredo), paginacao, limite_taxa
    destino:
      bronze: "gs://${bucket_bronze}/bronze/precos_combustiveis/"
      tabela: "${dataset_silver}.precos_combustiveis"
      schema: schemas/destino_precos.yaml
      modo_carga: overwrite_particao   # ou: append | merge (o merge exige chaves_merge)
      particao: _ingestion_date
      chaves_merge: []                 # obrigatória quando modo_carga = merge
      ddl: [sql/ddl/silver_precos.sql]
    transformacao:
      - renomear: {"Valor de Venda": valor_venda}
      - tipar: {valor_venda: {tipo: decimal, separador_decimal: ","}}
      - normalizar: {cnpj_revenda: [apenas_digitos]}
      - deduplicar: [cnpj_revenda, produto, data_coleta]
      - rejeitar_se: [{coluna: cnpj_revenda, regra: nao_nulo}]
      - hook: custom.ajustes:corrigir_linhas_deslocadas   # opcional
    qualidade:
      - nao_nulo: [cnpj_revenda, produto]
      - faixa: {coluna: valor_venda, minimo: 0.5, maximo: 30, limiar: 0.99}

  - id: ingere_eventos
    tipo: ingestao
    motor: cloud_run
    origem:
      tipo: pubsub
      assinatura: "projects/${projeto_gcp}/subscriptions/precos-eventos"
      max_mensagens: 100000
      tempo_maximo_s: 600
      formato: json
      dedupe_por: message_id
      schema: schemas/origem_eventos.yaml
    destino:
      bronze: "gs://${bucket_bronze}/bronze/eventos/"
      tabela: "${dataset_silver}.eventos_preco"
      schema: schemas/destino_eventos.yaml
      modo_carga: append
      particao: _ingestion_date
      ddl: [sql/ddl/silver_eventos.sql]

  - id: view_preco_medio_uf
    tipo: view
    depende_de: [ingere_precos]
    sql: sql/views/preco_medio_uf.sql           # CREATE OR REPLACE VIEW ${dataset_gold}.preco_medio_uf

  - id: agrega_semanal
    tipo: sql
    depende_de: [ingere_precos, ingere_eventos]
    sql: [sql/dml/agrega_semanal.sql]           # MERGE idempotente

  - id: enriquece
    tipo: python
    motor: cloud_run
    depende_de: [agrega_semanal]
    funcao: custom.enriquecimento:rodar

  - id: recalcula_grande
    tipo: spark
    depende_de: [agrega_semanal]
    script: spark/recalcula.py
    dataproc: {runtime: "2.2", executores: 2}

  - id: checagem_final
    tipo: qualidade
    depende_de: [view_preco_medio_uf, enriquece, recalcula_grande]
    verificacoes:
      - nao_nulo: {tabela: "${dataset_gold}.agregado_semanal", colunas: [uf, semana]}

execucao_padrao:
  cloud_run: {cpu: 1, memoria: 2Gi, timeout: 3600}
"""

HYBRID_FILES = [
    "schemas/origem_precos.yaml",
    "schemas/destino_precos.yaml",
    "schemas/origem_eventos.yaml",
    "schemas/destino_eventos.yaml",
    "sql/ddl/silver_precos.sql",
    "sql/ddl/silver_eventos.sql",
    "sql/views/preco_medio_uf.sql",
    "sql/dml/agrega_semanal.sql",
    "spark/recalcula.py",
]

HYBRID_EXTRA = {"ano": "2024", "semestre": "01"}

HEADER = 'schema_version: 1\nnome: teste\nagenda: "0 6 * * *"\n'


def make_project(root: Path, pipeline_yaml: str, files: list[str] | None = None) -> Path:
    """Cria `config/pipeline.yaml`, o ambiente `local` e os arquivos referenciados."""
    ambientes = root / "config" / "ambientes"
    ambientes.mkdir(parents=True)
    (ambientes / "local.yaml").write_text(ENV_LOCAL, encoding="utf-8")
    config = root / "config" / "pipeline.yaml"
    config.write_text(pipeline_yaml, encoding="utf-8")
    for rel in files or []:
        f = root / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("select 1\n", encoding="utf-8")
    return config


def sql_steps(*steps: str) -> str:
    """Pipeline mínimo com etapas `sql` (cada string é o corpo YAML de uma etapa)."""
    body = "".join(textwrap.indent(textwrap.dedent(s).strip() + "\n", "    ").replace("    ", "  - ", 1) for s in steps)
    return HEADER + "etapas:\n" + body


def load_ok(tmp_path: Path, steps_yaml: str) -> Pipeline:
    config = make_project(tmp_path, steps_yaml, ["a.sql"])
    return load_pipeline(config)


def load_err(tmp_path: Path, yaml_text: str, files: list[str] | None = None) -> str:
    config = make_project(tmp_path, yaml_text, ["a.sql"] if files is None else files)
    with pytest.raises(ConfigError) as exc:
        load_pipeline(config)
    return str(exc.value)


def test_valid_hybrid_example_loads(tmp_path: Path) -> None:
    config = make_project(tmp_path, HYBRID, HYBRID_FILES)
    p = load_pipeline(config, extra_params=HYBRID_EXTRA)

    assert p.schema_version == 1
    assert p.nome == "precos_combustiveis"
    assert [type(s) for s in p.etapas] == [
        IngestaoStep,
        IngestaoStep,
        ViewStep,
        SqlStep,
        PythonStep,
        SparkStep,
        QualidadeStep,
    ]
    precos = p.get("ingere_precos")
    assert isinstance(precos, IngestaoStep)
    assert isinstance(precos.origem, ArquivoOrigem)
    assert precos.origem.localizacao == "https://exemplo.gov.br/dados/ca-2024-01.zip"
    assert precos.origem.delimitador == ";"
    # O ambiente prevalece sobre os `parametros` do pipeline.
    assert precos.destino.tabela == "silver_local.precos_combustiveis"
    assert precos.destino.bronze == "gs://bucket-local/bronze/precos_combustiveis/"
    assert precos.destino.ddl == [Path("sql/ddl/silver_precos.sql")]
    assert [op.nome for op in precos.transformacao] == [
        "renomear",
        "tipar",
        "normalizar",
        "deduplicar",
        "rejeitar_se",
        "hook",
    ]
    assert precos.transformacao[0].renomear == {"Valor de Venda": "valor_venda"}
    nao_nulo, faixa = precos.qualidade
    assert (nao_nulo.tipo, nao_nulo.colunas, nao_nulo.limiar) == ("nao_nulo", ["cnpj_revenda", "produto"], 1.0)
    assert (faixa.tipo, faixa.colunas, faixa.minimo, faixa.maximo, faixa.limiar) == (
        "faixa",
        ["valor_venda"],
        0.5,
        30,
        0.99,
    )

    eventos = p.get("ingere_eventos")
    assert isinstance(eventos, IngestaoStep)
    assert isinstance(eventos.origem, PubSubOrigem)
    assert eventos.origem.assinatura == "projects/projeto-local/subscriptions/precos-eventos"
    assert eventos.depende_de == ["ingere_precos"]  # sem `depende_de`: depende da anterior

    view = p.get("view_preco_medio_uf")
    assert isinstance(view, ViewStep)
    assert view.sql == Path("sql/views/preco_medio_uf.sql")
    assert view.motor is None
    spark = p.get("recalcula_grande")
    assert isinstance(spark, SparkStep)
    assert spark.dataproc == {"runtime": "2.2", "executores": 2}
    final = p.get("checagem_final")
    assert isinstance(final, QualidadeStep)
    assert final.verificacoes[0].tabela == "gold_local.agregado_semanal"
    assert final.verificacoes[0].colunas == ["uf", "semana"]
    assert p.parametros["dataset_gold"] == "gold_local"
    assert p.parametros["ano"] == "2024"
    assert [s.id for s in p.ordered_steps()] == [
        "ingere_precos",
        "ingere_eventos",
        "view_preco_medio_uf",
        "agrega_semanal",
        "enriquece",
        "recalcula_grande",
        "checagem_final",
    ]


def test_missing_depends_on_means_previous_step(tmp_path: Path) -> None:
    p = load_ok(
        tmp_path,
        sql_steps(
            "id: a\ntipo: sql\nsql: [a.sql]",
            "id: b\ntipo: sql\nsql: [a.sql]",
            "id: c\ntipo: sql\nsql: [a.sql]\ndepende_de: []",
        ),
    )
    assert [s.depende_de for s in p.etapas] == [[], ["a"], []]


def test_ordered_steps_is_stable_topological_order(tmp_path: Path) -> None:
    p = load_ok(
        tmp_path,
        sql_steps(
            "id: final\ntipo: sql\nsql: [a.sql]\ndepende_de: [z, y]",
            "id: z\ntipo: sql\nsql: [a.sql]\ndepende_de: []",
            "id: y\ntipo: sql\nsql: [a.sql]\ndepende_de: []",
            "id: x\ntipo: sql\nsql: [a.sql]\ndepende_de: [z]",
        ),
    )
    assert [s.id for s in p.ordered_steps()] == ["z", "y", "final", "x"]


def test_get_unknown_step_lists_ids(tmp_path: Path) -> None:
    p = load_ok(tmp_path, sql_steps("id: a\ntipo: sql\nsql: [a.sql]"))
    assert p.get("a").id == "a"
    with pytest.raises(ConfigError, match="'nada'.*a"):
        p.get("nada")


def test_duplicate_ids_rejected(tmp_path: Path) -> None:
    msg = load_err(
        tmp_path,
        sql_steps("id: a\ntipo: sql\nsql: [a.sql]", "id: a\ntipo: sql\nsql: [a.sql]"),
    )
    assert "pipeline.yaml" in msg
    assert "repetid" in msg and "'a'" in msg


def test_cycle_rejected_and_names_the_cycle(tmp_path: Path) -> None:
    msg = load_err(
        tmp_path,
        sql_steps(
            "id: a\ntipo: sql\nsql: [a.sql]\ndepende_de: [c]",
            "id: b\ntipo: sql\nsql: [a.sql]",
            "id: c\ntipo: sql\nsql: [a.sql]",
        ),
    )
    assert "ciclo" in msg
    assert "a -> c -> b -> a" in msg


def test_unknown_dependency_rejected(tmp_path: Path) -> None:
    msg = load_err(tmp_path, sql_steps("id: a\ntipo: sql\nsql: [a.sql]\ndepende_de: [fantasma]"))
    assert "'a'" in msg and "'fantasma'" in msg


def test_motor_incompatible_with_type_rejected(tmp_path: Path) -> None:
    msg = load_err(tmp_path, sql_steps("id: minha_view\ntipo: view\nsql: a.sql\nmotor: cloud_run"))
    assert "pipeline.yaml" in msg
    assert "minha_view" in msg
    assert "motor" in msg


def test_spark_only_runs_on_dataproc(tmp_path: Path) -> None:
    msg = load_err(tmp_path, sql_steps("id: s\ntipo: spark\nscript: a.sql\nmotor: cloud_run"))
    assert "motor" in msg and "dataproc" in msg


def test_merge_requires_chaves_merge(tmp_path: Path) -> None:
    yaml_text = sql_steps(
        """
        id: ing
        tipo: ingestao
        origem: {tipo: api, endpoint: "https://api.exemplo/v1/itens"}
        destino:
          bronze: "gs://${bucket_bronze}/bronze/itens/"
          tabela: "${dataset_silver}.itens"
          schema: a.sql
          modo_carga: merge
        """
    )
    msg = load_err(tmp_path, yaml_text)
    assert "ing" in msg and "chaves_merge" in msg


def test_missing_sql_file_rejected(tmp_path: Path) -> None:
    msg = load_err(
        tmp_path,
        sql_steps("id: a\ntipo: sql\nsql: [a.sql, sql/nao_existe.sql]", "id: v\ntipo: view\nsql: sql/v.sql"),
    )
    assert "inexistente" in msg
    assert "sql/nao_existe.sql" in msg and "sql/v.sql" in msg
    assert "'a'" in msg and "'v'" in msg


def test_unknown_step_type_lists_valid_types(tmp_path: Path) -> None:
    msg = load_err(tmp_path, sql_steps("id: a\ntipo: notebook"))
    assert "notebook" in msg
    for tipo in ("ingestao", "sql", "view", "python", "spark", "qualidade"):
        assert tipo in msg


def test_unsupported_schema_version_fails_with_clear_message(tmp_path: Path) -> None:
    yaml_text = sql_steps("id: a\ntipo: sql\nsql: [a.sql]").replace("schema_version: 1", "schema_version: 2")
    msg = load_err(tmp_path, yaml_text)
    assert "schema_version" in msg
    assert "2" in msg and "suportada" in msg and "1" in msg


def test_undefined_param_names_param_and_file(tmp_path: Path) -> None:
    msg = load_err(tmp_path, sql_steps("id: a\ntipo: sql\nsql: ['${pasta}/a.sql']"))
    assert "pasta" in msg and "pipeline.yaml" in msg


def test_extra_params_override_environment_and_pipeline(tmp_path: Path) -> None:
    yaml_text = sql_steps("id: a\ntipo: sql\nsql: ['${pasta}/a.sql']").replace(
        "etapas:", "parametros:\n  pasta: errada\n  dataset_gold: gold\netapas:"
    )
    config = make_project(tmp_path, yaml_text, ["certa/a.sql"])
    p = load_pipeline(config, extra_params={"pasta": "certa"})
    step = p.get("a")
    assert isinstance(step, SqlStep)
    assert step.sql == [Path("certa/a.sql")]
    assert p.parametros["dataset_gold"] == "gold_local"


def test_missing_environment_lists_available(tmp_path: Path) -> None:
    config = make_project(tmp_path, sql_steps("id: a\ntipo: sql\nsql: [a.sql]"), ["a.sql"])
    with pytest.raises(ConfigError, match="'prod'.*local"):
        load_pipeline(config, ambiente="prod")


def test_quality_short_forms_are_normalized(tmp_path: Path) -> None:
    yaml_text = sql_steps(
        """
        id: q
        tipo: qualidade
        depende_de: []
        verificacoes:
          - unico: {tabela: t, colunas: [[a, b], [c]], bloqueante: true}
          - conjunto: {tabela: t, coluna: uf, valores: [SP, RJ]}
          - {tipo: formato, tabela: t, colunas: [cnpj], padrao: '^\\d{14}$'}
        """
    )
    p = load_ok(tmp_path, yaml_text)
    step = p.get("q")
    assert isinstance(step, QualidadeStep)
    checks = [(c.tipo, c.colunas, c.bloqueante) for c in step.verificacoes]
    assert checks == [
        ("unico", ["a", "b"], True),
        ("unico", ["c"], True),
        ("conjunto", ["uf"], False),
        ("formato", ["cnpj"], False),
    ]
    assert step.verificacoes[2].valores == ["SP", "RJ"]


def test_qualidade_step_requires_tabela(tmp_path: Path) -> None:
    msg = load_err(tmp_path, sql_steps("id: q\ntipo: qualidade\nverificacoes:\n  - nao_nulo: [a]"))
    assert "id=q" in msg and "tabela" in msg


def test_api_origin_is_parsed(tmp_path: Path) -> None:
    yaml_text = sql_steps(
        """
        id: ing
        tipo: ingestao
        origem:
          tipo: api
          endpoint: "https://api.exemplo/v1/itens"
          autenticacao: {tipo: bearer, segredo_env: TOKEN_API}
          paginacao: {tipo: cursor, caminho_proximo: next, caminho_dados: data}
          limite_taxa: 2
        destino:
          bronze: "gs://${bucket_bronze}/bronze/itens/"
          tabela: "${dataset_silver}.itens"
          schema: a.sql
          modo_carga: merge
          chaves_merge: [id]
        """
    )
    step = load_ok(tmp_path, yaml_text).get("ing")
    assert isinstance(step, IngestaoStep)
    assert isinstance(step.origem, ApiOrigem)
    assert step.origem.autenticacao.segredo_env == "TOKEN_API"
    assert step.origem.paginacao.tipo == "cursor"
    assert step.motor == "cloud_run"  # padrão da ingestão


def test_invalid_value_is_not_echoed_in_error(tmp_path: Path) -> None:
    yaml_text = sql_steps("id: a\ntipo: sql\nsql: [a.sql]\nmotor: S3CR3T-valor")
    msg = load_err(tmp_path, yaml_text)
    assert "motor" in msg
    assert "S3CR3T" not in msg


def test_json_schema_is_generated_from_model() -> None:
    schema = Pipeline.model_json_schema()
    assert schema["title"] == "Pipeline"
    assert set(schema["required"]) >= {"schema_version", "nome", "agenda", "etapas"}
    etapas = schema["properties"]["etapas"]["items"]
    assert etapas["discriminator"]["propertyName"] == "tipo"
    assert set(etapas["discriminator"]["mapping"]) == {"ingestao", "sql", "view", "python", "spark", "qualidade"}
