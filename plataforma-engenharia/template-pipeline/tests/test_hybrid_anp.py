"""Aceite híbrido da ANP: um projeto `hibrido` gerado pelo template, com o pipeline da ANP no lugar do exemplo.

A amostra (`tests/fixtures/anp/data/amostra.csv`) é 100% sintética (CNPJs fictícios `99999900...`) e tem a mesma
estrutura da amostra do aceite da biblioteca, com uma coluna `uf` a mais (necessária para a view por UF). Tem 49
linhas de dados: 44 válidas e únicas (11 postos x 4 produtos), 2 duplicatas (uma exata e uma que só difere pelo
espaço no início do CNPJ), 2 linhas com `;` no endereço (colunas deslocadas) e 1 com preço negativo; além de BOM,
`;`, vírgula decimal, datas dd/mm/aaaa e 4 CNPJs com espaço no início.

Todos os números esperados abaixo foram escritos à mão a partir da amostra (calculados com um script descartável
com a stdlib, fora do código testado): nada é derivado do código sob teste.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest
from dataeng_core import cli

Bake = Callable[..., Path]
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "anp"
RUN_ID = "20260301T060000-aaaaaa"
SILVER = "silver.precos_combustiveis"
QUARANTINE = "silver.precos_combustiveis_rejeitados"
CONTROL = "silver._controle_execucoes"
STEPS = ["ingere_precos", "view_preco_medio_uf", "agrega_uf_produto", "checagem_final"]

# view gold.preco_medio_uf: (uf, nº de preços, média com 4 casas) sobre as 44 linhas válidas
VIEW_ROWS = [
    ("BA", 4, Decimal("3.8950")),
    ("MG", 8, Decimal("3.7900")),
    ("PR", 4, Decimal("3.9650")),
    ("RJ", 12, Decimal("3.6150")),
    ("SP", 16, Decimal("3.7719")),  # 60,35 / 16 = 3,771875
]
# tabela gold.agregado_uf_produto (MERGE): (uf, produto, nº de preços, média com 4 casas)
AGG_ROWS = [
    ("BA", "DIESEL", 1, Decimal("3.9600")),
    ("BA", "ETANOL", 1, Decimal("3.8300")),
    ("BA", "GASOLINA", 1, Decimal("3.7000")),
    ("BA", "GNV", 1, Decimal("4.0900")),
    ("MG", "DIESEL", 2, Decimal("3.8550")),
    ("MG", "ETANOL", 2, Decimal("3.7250")),
    ("MG", "GASOLINA", 2, Decimal("3.5950")),
    ("MG", "GNV", 2, Decimal("3.9850")),
    ("PR", "DIESEL", 1, Decimal("4.0300")),
    ("PR", "ETANOL", 1, Decimal("3.9000")),
    ("PR", "GASOLINA", 1, Decimal("3.7700")),
    ("PR", "GNV", 1, Decimal("4.1600")),
    ("RJ", "DIESEL", 3, Decimal("3.6800")),
    ("RJ", "ETANOL", 3, Decimal("3.5500")),
    ("RJ", "GASOLINA", 3, Decimal("3.4200")),
    ("RJ", "GNV", 3, Decimal("3.8100")),
    ("SP", "DIESEL", 4, Decimal("4.1400")),
    ("SP", "ETANOL", 4, Decimal("3.5025")),
    ("SP", "GASOLINA", 4, Decimal("3.8800")),
    ("SP", "GNV", 4, Decimal("3.5650")),
]
SHIFTED_LINES = [
    "99999900009001;POSTO FICTICIO 90;RUA X; QUADRA 5;SP;GASOLINA;10/03/2026;5,10",
    "99999900009002;POSTO FICTICIO 91;AV Y; LOTE 7;RJ;ETANOL;11/03/2026;4,20",
]


@pytest.fixture
def anp(bake: Bake) -> Path:
    """Projeto `hibrido` gerado; pipeline, ambiente, schemas, SQL e dados do exemplo são trocados pelos da ANP."""
    root = bake(
        nome_projeto="anp",  # curto: o caminho do bronze local estoura o limite de 260 caracteres do Windows
        tipo_projeto="hibrido",
        origens="arquivo",
        motor_ingestao="cloud_run",
        modo_carga="overwrite_particao",
    )
    for example in ("sql", "schemas", "data"):  # nada do exemplo genérico sobra: o projeto da ANP é autossuficiente
        shutil.rmtree(root / example)
    shutil.copytree(FIXTURE, root, dirs_exist_ok=True)
    return root


def _args(root: Path, command: str, *extra: str) -> list[str]:
    return [command, "--config", str(root / "config/pipeline.yaml"), *extra]


def _run(root: Path, run_id: str = RUN_ID) -> int:
    return cli.main(["run", "local", "--config", str(root / "config/pipeline.yaml"), "--run-id", run_id])


def _query(root: Path, sql: str) -> list[tuple[Any, ...]]:
    con = duckdb.connect(str(next((root / ".local").rglob("*.duckdb"))), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _snapshot(root: Path) -> dict[str, list[tuple[Any, ...]]]:
    """Conteúdo completo (sem colunas de data de carga) das tabelas e da view, para comparar entre rodadas."""
    return {
        "silver": _query(
            root,
            "SELECT cnpj_revenda, razao_social, endereco, uf, produto, data_coleta, valor_venda, _run_id, "
            f"_source_file, _ingestion_date FROM {SILVER} ORDER BY cnpj_revenda, produto, data_coleta",
        ),
        "quarentena": _query(root, f"SELECT _linha_original, _motivo FROM {QUARANTINE} ORDER BY _linha_original"),
        "view": _query(root, "SELECT uf, qtd, preco_medio FROM gold.preco_medio_uf ORDER BY uf"),
        "agregado": _query(root, "SELECT uf, produto, qtd, preco_medio FROM gold.agregado_uf_produto ORDER BY uf, produto"),
    }


def test_sample_has_the_documented_defects() -> None:
    """Protege a fixture: se alguém a editar, os números esperados deixam de valer."""
    raw = (FIXTURE / "data/amostra.csv").read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")  # BOM
    data = raw.decode("utf-8-sig").splitlines()[1:]
    assert len(data) == 49
    assert sum(1 for ln in data if ln.startswith(" ")) == 4  # CNPJ com espaço no início
    assert sorted(ln for ln in data if ln.count(";") == 7) == SHIFTED_LINES  # endereço com ';'
    assert sum(1 for ln in data if ln.endswith(";-4,50")) == 1
    assert all(ln.startswith(("9999990000", " 9999990000")) for ln in data)  # só CNPJs fictícios


def test_generated_project_is_replaced_by_the_anp_one(anp: Path) -> None:
    files = {p.relative_to(anp).as_posix() for p in anp.rglob("*") if p.is_file()}
    assert "sql/ddl/silver_registros_arquivo.sql" not in files and "sql/views/resumo_por_nome.sql" not in files
    assert {"config/pipeline.yaml", "schemas/destino_precos.yaml", "sql/views/preco_medio_uf.sql"} <= files
    assert {"dags/pipeline.py", "Dockerfile", "tests/test_config.py"} <= files  # o resto do template continua


def test_plan_shows_step_order_and_rendered_sql(anp: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(_args(anp, "validate-config")) == 0
    capsys.readouterr()
    assert cli.main(_args(anp, "plan", "--run-id", RUN_ID)) == 0
    out = capsys.readouterr().out

    assert re.findall(r"^\d+\. (\S+)  tipo=(\S+)", out, flags=re.MULTILINE) == [
        ("ingere_precos", "ingestao"),
        ("view_preco_medio_uf", "view"),
        ("agrega_uf_produto", "sql"),
        ("checagem_final", "qualidade"),
    ]
    assert "2. view_preco_medio_uf  tipo=view  motor=bigquery  depende_de=ingere_precos" in out
    assert "3. agrega_uf_produto  tipo=sql  motor=bigquery  depende_de=ingere_precos" in out
    assert "4. checagem_final  tipo=qualidade  motor=cloud_run" in out and "depende_de=view_preco_medio_uf,agrega_uf_produto" in out
    # SQL já renderizado: parâmetros trocados pelos valores do ambiente local, nenhum `${` sobrando
    assert "CREATE TABLE IF NOT EXISTS silver.precos_combustiveis (" in out
    assert "     CREATE OR REPLACE VIEW gold.preco_medio_uf AS" in out
    assert "     FROM silver.precos_combustiveis\n     GROUP BY uf;" in out
    assert "     MERGE INTO gold.agregado_uf_produto AS t" in out
    assert "     ON t.uf = s.uf AND t.produto = s.produto" in out
    assert "${" not in out
    # a ordem do SQL no plano segue a das etapas
    assert out.index("silver.precos_combustiveis (") < out.index("gold.preco_medio_uf") < out.index("MERGE INTO")


def test_hybrid_anp_runs_local_twice_without_duplicates(anp: Path) -> None:
    assert _run(anp) == 0
    first = _snapshot(anp)

    # silver: exatamente 44 linhas, sem duplicatas, CNPJ só com dígitos (inclusive os 4 que vieram com espaço)
    silver = first["silver"]
    assert len(silver) == 44  # 49 - 2 deslocadas - 1 negativa - 2 duplicadas
    assert len({(r[0], r[4], r[5]) for r in silver}) == 44
    assert {r[0] for r in silver} == {f"9999990000{i:04d}" for i in range(1, 12)}
    assert all(len(r[0]) == 14 and r[0].isdigit() for r in silver)
    assert {(r[7], r[9]) for r in silver} == {(RUN_ID, date(2026, 3, 1))}
    by_key = {(r[0], r[4]): r for r in silver}
    assert by_key[("99999900000001", "GASOLINA")][3:7] == ("SP", "GASOLINA", date(2026, 3, 2), Decimal("5.89"))
    assert ("99999900000002", "ETANOL") in by_key and ("99999900000005", "ETANOL") in by_key  # vieram com espaço

    # quarentena: as 2 deslocadas (linha crua) e a de preço negativo (linha como dicionário), com o motivo
    quarantine = first["quarentena"]
    assert len(quarantine) == 3
    shifted = sorted(q for q in quarantine if q[1] == "numero_de_colunas_inesperado")
    assert [q[0] for q in shifted] == SHIFTED_LINES
    negative = [q for q in quarantine if q[1] != "numero_de_colunas_inesperado"]
    assert len(negative) == 1
    original = json.loads(negative[0][0])
    assert original["cnpj_revenda"] == "99999900009003" and original["valor_venda"] == "-4,50"
    assert original["uf"] == "MG"
    assert "valor_venda" in negative[0][1] and "faixa" in negative[0][1]

    # view de preço médio por UF e agregado por MERGE: valores exatos, uma linha por UF / por UF x produto
    assert first["view"] == VIEW_ROWS
    assert first["agregado"] == AGG_ROWS
    assert sum(r[1] for r in first["view"]) == 44 and sum(r[2] for r in first["agregado"]) == 44

    # controle: uma linha por etapa, na ordem de execução
    control = _query(anp, f"SELECT etapa, status, run_id FROM {CONTROL} ORDER BY registrado_em")
    assert control == [(step, "ok", RUN_ID) for step in STEPS]
    ingestion = _query(
        anp,
        "SELECT linhas_lidas, linhas_gravadas, linhas_rejeitadas FROM "
        f"{CONTROL} WHERE etapa = 'ingere_precos'",
    )
    assert ingestion == [(49, 44, 3)]

    # 2ª rodada, mesmo run_id: nada muda (silver, quarentena, view, MERGE) e o controle ganha uma linha por etapa
    assert _run(anp) == 0
    assert _snapshot(anp) == first
    assert _query(anp, f"SELECT COUNT(*) FROM {SILVER}") == [(44,)]
    assert _query(anp, "SELECT COUNT(*) FROM gold.agregado_uf_produto") == [(20,)]
    assert _query(anp, f"SELECT etapa, COUNT(*) FROM {CONTROL} GROUP BY etapa ORDER BY etapa") == [
        ("agrega_uf_produto", 2),
        ("checagem_final", 2),
        ("ingere_precos", 2),
        ("view_preco_medio_uf", 2),
    ]
    assert _query(anp, f"SELECT COUNT(*) FROM {CONTROL} WHERE status <> 'ok'") == [(0,)]


def test_merge_updates_changed_aggregates_in_place(anp: Path) -> None:
    """Nova carga do mesmo dia com um preço alterado: o MERGE atualiza a linha existente, sem duplicar."""
    assert _run(anp) == 0
    csv = anp / "data/amostra.csv"
    text = csv.read_text(encoding="utf-8")
    assert text.count("GASOLINA;02/03/2026;5,89") == 1
    csv.write_text(text.replace("GASOLINA;02/03/2026;5,89", "GASOLINA;02/03/2026;6,89"), encoding="utf-8")

    other = "20260301T070000-bbbbbb"  # mesmo dia (2026-03-01): a partição é regravada, não somada
    assert _run(anp, other) == 0
    assert _query(anp, f"SELECT COUNT(*), COUNT(DISTINCT _run_id) FROM {SILVER}") == [(44, 1)]
    changed = [
        ("SP", 16, Decimal("3.8344")) if row[0] == "SP" else row for row in VIEW_ROWS
    ]  # SP: 61,35 / 16 = 3,834375
    assert _snapshot(anp)["view"] == changed
    assert _snapshot(anp)["agregado"] == [
        ("SP", "GASOLINA", 4, Decimal("4.1300")) if row[:2] == ("SP", "GASOLINA") else row for row in AGG_ROWS
    ]  # (6,89 + 3,14 + 3,21 + 3,28) / 4 = 4,13 (antes: 3,88)
    assert _query(anp, "SELECT COUNT(*) FROM gold.agregado_uf_produto") == [(20,)]


def test_quality_step_checks_the_merged_table(anp: Path) -> None:
    assert _run(anp) == 0
    (quality,) = _query(anp, f"SELECT qualidade FROM {CONTROL} WHERE etapa = 'checagem_final'")
    results = json.loads(quality[0])
    assert [(r["nome"], r["coluna"], r["aprovado"], r["taxa"]) for r in results] == [
        ("nao_nulo", "uf", True, 1.0),
        ("nao_nulo", "produto", True, 1.0),
        ("unico", "uf, produto", True, 1.0),
    ]


def test_generated_tests_of_the_replaced_project_still_pass(anp: Path) -> None:
    """Os testes do próprio projeto (validate-config, plan, planejamento da DAG) valem para o pipeline da ANP."""
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests/test_config.py"],
        cwd=anp,
        check=False,
        capture_output=True,
        text=True,
        timeout=240,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
