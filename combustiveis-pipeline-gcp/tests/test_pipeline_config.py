"""Testes do pipeline da ANP gerado a partir do template: configuração, plano e uma rodada local sintética.

Sem rede e sem dados reais: a amostra é escrita aqui, com CNPJs fictícios (`99999900...`). A rodada local copia o
projeto para uma pasta temporária (o DuckDB e o bronze local ficam lá, nunca no repositório).
Requer `dataeng-core` instalado: `pip install -e "lib-dataeng-core[local]" pytest` a partir da raiz do repositório.
"""

from __future__ import annotations

import shutil
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
cli = pytest.importorskip("dataeng_core.cli")

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config" / "pipeline.yaml"
RUN_ID = "20260301T060000-aaaaaa"
HEADER = (
    "Regiao - Sigla;Estado - Sigla;Municipio;Revenda;CNPJ da Revenda;Nome da Rua;Numero Rua;Complemento;"
    "Bairro;Cep;Produto;Data da Coleta;Valor de Venda;Valor de Compra;Unidade de Medida;Bandeira"
)
# 7 linhas de dados: 3 válidas e únicas, 1 duplicata, 1 com ';' no complemento (17 campos), 1 com preço negativo
# e 1 com CNPJ curto demais.
ROWS = [
    "S;sp;sao paulo ;POSTO  FICTICIO 01;99.999.900/0001-01;rua inventada;10;sala 2;centro;01000-000;Gasolina ;02/03/2026;5,89;;R$/l;BANDEIRA X",
    "NE;BA;SALVADOR;POSTO FICTICIO 02; 99999900000202;AV TESTE;;;ORLA;40000-000;ETANOL;03/03/2026;3,99;3,50;R$/l;BRANCA",
    "S;SP;SAO PAULO;POSTO FICTICIO 01;99999900000101;RUA INVENTADA;10;SALA 2;CENTRO;01000-000;GASOLINA;02/03/2026;5,95;;R$/l;BANDEIRA X",
    "NE;BA;SALVADOR;POSTO FICTICIO 03;99999900000303;RUA Y;5;LOTE 7; QUADRA 1;ORLA;40000-000;GNV;04/03/2026;4,10;;R$/m3;BRANCA",
    "N;PA;BELEM;POSTO FICTICIO 04;99999900000404;RUA Z;1;;CENTRO;66000-000;DIESEL S10;05/03/2026;-1,00;;R$/l;BRANCA",
    "N;PA;BELEM;POSTO FICTICIO 05;123;RUA W;2;;CENTRO;66000-000;DIESEL S10;05/03/2026;6,10;;R$/l;BRANCA",
    "N;PA;BELEM;POSTO FICTICIO 06;99999900000606;RUA V;3;;CENTRO;66000-000;DIESEL S10;06/03/2026;6,20;6,00;R$/l;BRANCA",
]


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Cópia do projeto (config, schemas, sql, custom) com a amostra sintética em data/amostra.csv."""
    dest = tmp_path / "anp"
    for pasta in ("config", "schemas", "sql", "custom"):
        shutil.copytree(ROOT / pasta, dest / pasta, ignore=shutil.ignore_patterns("__pycache__"))
    (dest / "data").mkdir()
    (dest / "data" / "amostra.csv").write_text("\n".join([HEADER, *ROWS]) + "\n", encoding="utf-8-sig")
    return dest


def _query(root: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(str(next((root / ".local").rglob("*.duckdb"))), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


@pytest.mark.parametrize("ambiente", ["local", "dev", "prod"])
@pytest.mark.parametrize("comando", ["validate-config", "plan"])
def test_config_validates_and_plans_in_every_environment(comando: str, ambiente: str) -> None:
    assert cli.main([comando, "--config", str(CONFIG), "--ambiente", ambiente]) == 0


def test_run_local_with_synthetic_sample(project: Path) -> None:
    config = project / "config" / "pipeline.yaml"
    argv = ["run", "local", "--config", str(config), "--run-id", RUN_ID, "--param", "arquivo_anp=data/amostra.csv"]
    assert cli.main(argv) == 0

    silver = _query(
        project,
        "SELECT cnpj_revenda, uf, municipio, endereco, produto, data_coleta, valor_venda, valor_compra, cep "
        "FROM silver.precos_combustiveis ORDER BY cnpj_revenda",
    )
    assert silver == [
        ("99999900000101", "SP", "SAO PAULO", "RUA INVENTADA 10 SALA 2", "GASOLINA", date(2026, 3, 2), Decimal("5.89"), None, "01000-000"),
        ("99999900000202", "BA", "SALVADOR", "AV TESTE", "ETANOL", date(2026, 3, 3), Decimal("3.99"), Decimal("3.50"), "40000-000"),
        ("99999900000606", "PA", "BELEM", "RUA V 3", "DIESEL S10", date(2026, 3, 6), Decimal("6.20"), Decimal("6.00"), "66000-000"),
    ]
    motivos = _query(project, "SELECT _motivo FROM silver.precos_combustiveis_rejeitados ORDER BY _motivo")
    assert len(motivos) == 3
    assert sum(1 for (m,) in motivos if m == "numero_de_colunas_inesperado") == 1
    assert sum(1 for (m,) in motivos if "valor_venda" in m) == 1
    assert sum(1 for (m,) in motivos if "cnpj_revenda" in m) == 1
    assert _query(project, "SELECT etapa, status FROM silver._controle_execucoes ORDER BY registrado_em") == [
        ("ingere_precos", "ok"),
        ("checagem_silver", "ok"),
    ]
