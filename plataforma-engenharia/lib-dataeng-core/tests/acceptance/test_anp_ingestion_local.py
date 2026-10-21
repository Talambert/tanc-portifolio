"""Teste de aceite: amostra sintética no estilo da ANP, ingerida pelo CLI local de ponta a ponta.

A amostra (`tests/fixtures/anp/amostra.csv`) tem 49 linhas de dados: 44 válidas e únicas (11 postos x 4
produtos), 2 duplicatas (uma exata e uma que só difere pelo espaço no início do CNPJ), 2 linhas com `;` no
endereço (colunas deslocadas) e 1 com preço negativo. Além disso: BOM, `;`, vírgula decimal, datas dd/mm/aaaa
e 4 CNPJs com espaço no início. O número esperado de linhas é escrito à mão, não derivado do código.
"""

from __future__ import annotations

import json
import shutil
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dataeng_core.cli import main

FIXTURE = Path(__file__).parent.parent / "fixtures" / "anp"
RUN_ID = "20260301T060000-aaaaaa"
OTHER_RUN_ID = "20260302T060000-bbbbbb"
VALID_ROWS = 44
SILVER = "silver.precos_combustiveis"
QUARANTINE = "silver.precos_combustiveis_rejeitados"
SILVER_ROWS = (
    "SELECT cnpj_revenda, razao_social, endereco, produto, data_coleta, valor_venda, _run_id, _ingestion_date "
    f"FROM {SILVER} ORDER BY cnpj_revenda, produto"
)
CONTROL = "silver._controle_execucoes"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Cópia da fixture em `tmp_path`, para o `.local` (DuckDB) não sujar o repositório."""
    root = tmp_path / "anp"
    shutil.copytree(FIXTURE, root, ignore=shutil.ignore_patterns(".local"))
    return root


def _run(project: Path, run_id: str = RUN_ID) -> int:
    return main(["run", "local", "--config", str(project / "pipeline.yaml"), "--run-id", run_id])


def _query(project: Path, sql: str) -> list[tuple[Any, ...]]:
    db = next((project / ".local").rglob("*.duckdb"))
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_sample_has_the_documented_defects() -> None:
    """Protege a fixture: se alguém a editar, o N esperado deixa de valer."""
    raw = (FIXTURE / "amostra.csv").read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")  # BOM
    lines = raw.decode("utf-8-sig").splitlines()
    data = lines[1:]
    assert len(data) == 49
    assert sum(1 for ln in data if ln.startswith(" ")) == 4  # CNPJ com espaço no início
    assert sum(1 for ln in data if ln.count(";") == 6) == 2  # endereço com ';'
    assert sum(1 for ln in data if ln.endswith(";-4,50")) == 1
    assert sum(1 for ln in data if ln.count(";") == 5) == 47  # 44 válidas + 2 duplicadas + 1 negativa


def test_anp_sample_ingests_with_quarantine_and_is_idempotent(project: Path) -> None:
    assert _run(project) == 0

    silver = _query(
        project,
        "SELECT cnpj_revenda, produto, data_coleta, valor_venda, _run_id, _ingestion_date "
        f"FROM {SILVER} ORDER BY cnpj_revenda, produto",
    )
    assert len(silver) == VALID_ROWS  # 49 - 2 deslocadas - 1 negativa - 2 duplicadas
    assert len({(r[0], r[1], r[2]) for r in silver}) == VALID_ROWS  # duplicadas removidas de fato

    # CNPJ só com dígitos em toda a silver (inclusive nas 4 linhas que vieram com espaço)
    assert all(len(r[0]) == 14 and r[0].isdigit() for r in silver)
    assert {r[0] for r in silver} == {f"9999990000{i:04d}" for i in range(1, 12)}

    # tipos e valores exatos
    by_key = {(r[0], r[1]): r for r in silver}
    gasolina = by_key[("99999900000001", "GASOLINA")]
    assert gasolina[2] == date(2026, 3, 2) and gasolina[3] == Decimal("5.89")
    etanol = by_key[("99999900000001", "ETANOL")]
    assert etanol[2] == date(2026, 3, 3) and etanol[3] == Decimal("3.99")
    diesel = by_key[("99999900000001", "DIESEL")]
    assert diesel[2] == date(2026, 3, 4) and diesel[3] == Decimal("6.15")
    assert all(isinstance(r[2], date) and r[3] > 0 for r in silver)
    assert {(r[4], r[5]) for r in silver} == {(RUN_ID, date(2026, 3, 1))}
    # linha cujo CNPJ veio com espaço no início (posto 02, ETANOL) está na silver já normalizada
    assert ("99999900000002", "ETANOL") in by_key

    # quarentena: as 2 deslocadas (linha crua) e a de preço negativo (linha como dicionário)
    quarantine = _query(project, f"SELECT _linha_original, _motivo FROM {QUARANTINE} ORDER BY _linha_original")
    assert len(quarantine) == 3
    shifted = sorted(q for q in quarantine if q[1] == "numero_de_colunas_inesperado")
    assert [q[0] for q in shifted] == [
        "99999900009001;POSTO FICTICIO 90;RUA X; QUADRA 5;GASOLINA;10/03/2026;5,10",
        "99999900009002;POSTO FICTICIO 91;AV Y; LOTE 7;ETANOL;11/03/2026;4,20",
    ]
    negative = [q for q in quarantine if q[1] != "numero_de_colunas_inesperado"]
    assert len(negative) == 1
    original = json.loads(negative[0][0])
    assert original["cnpj_revenda"] == "99999900009003" and original["valor_venda"] == "-4,50"
    assert "valor_venda" in negative[0][1] and "faixa" in negative[0][1]

    first_control = _query(
        project, f"SELECT etapa, status, linhas_lidas, linhas_gravadas, linhas_rejeitadas FROM {CONTROL}"
    )
    assert first_control == [("ingere_precos", "ok", 49, VALID_ROWS, 3)]

    # 2ª rodada, mesmo run_id: nada a mais na silver nem na quarentena; uma linha nova de controle
    assert _run(project) == 0
    assert _query(project, f"SELECT COUNT(*) FROM {SILVER}") == [(VALID_ROWS,)]
    assert _query(project, f"SELECT COUNT(*) FROM {QUARANTINE}") == [(3,)]
    control = _query(project, f"SELECT etapa, status, run_id FROM {CONTROL} ORDER BY registrado_em")
    assert control == [("ingere_precos", "ok", RUN_ID)] * 2


def test_anp_sample_with_a_new_run_id_does_not_duplicate_the_same_data_day(project: Path) -> None:
    """Outro run_id é outra carga: a silver tem as linhas de cada execução, cada uma com N linhas."""
    assert _run(project) == 0
    assert _run(project, OTHER_RUN_ID) == 0
    per_run = _query(project, f"SELECT _run_id, COUNT(*) FROM {SILVER} GROUP BY _run_id ORDER BY _run_id")
    assert per_run == [(RUN_ID, VALID_ROWS), (OTHER_RUN_ID, VALID_ROWS)]
    assert _query(project, f"SELECT COUNT(*) FROM {CONTROL}") == [(2,)]


@pytest.mark.parametrize(
    "content", [b"\xef\xbb\xbfcnpj_revenda;razao_social;endereco;produto;data_coleta;valor_venda\n", b""]
)
def test_header_only_and_empty_files_end_defined_and_keep_existing_silver(
    project: Path, content: bytes, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run(project) == 0
    before = _query(project, SILVER_ROWS)
    capsys.readouterr()

    (project / "amostra.csv").write_bytes(content)
    code = _run(project, OTHER_RUN_ID)
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err and "Traceback" not in captured.out
    assert code == 0  # contrato da T12: entrada vazia termina ok com 0 linhas
    assert _query(project, SILVER_ROWS) == before
    assert _query(project, f"SELECT COUNT(*) FROM {QUARANTINE}") == [(3,)]
    last = _query(
        project, f"SELECT status, linhas_lidas, linhas_gravadas FROM {CONTROL} ORDER BY registrado_em DESC LIMIT 1"
    )
    assert last == [("ok", 0, 0)]
