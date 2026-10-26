"""`jobs.ingest_spark`: sem Spark, com sessão falsa e conversões falsas (o motor real roda no CI, marca `spark`)."""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest
from tests.cli.project import make_project

from dataeng_core.base.pipeline import TransformOp
from dataeng_core.jobs import ingest, ingest_spark
from dataeng_core.transform.engine_pandas import apply_transform
from dataeng_core.transform.engine_spark import SparkTransformResult

RID = "20260101T060000-aaaaaa"


def _args(config: Path, etapa: str = "ingere") -> list[str]:
    return ["--config", str(config), "--ambiente", "local", "--etapa", etapa, "--run-id", RID]


def _query(root: Path, sql: str) -> list[tuple[Any, ...]]:
    db = next((root / ".local").rglob("*.duckdb"))
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


SILVER = "SELECT id, produto, valor FROM silver.vendas ORDER BY id"
QUARANTINE = "SELECT _motivo FROM silver.vendas_rejeitados ORDER BY _motivo"


class FakeSpark:
    def __init__(self) -> None:
        self.stopped = 0

    def stop(self) -> None:
        self.stopped += 1


class FakeFrame:
    def __init__(self, pdf: pd.DataFrame) -> None:
        self.pdf = pdf


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Any]]:
    """Troca as conversões e o motor Spark por equivalentes em pandas, registrando as chamadas."""
    log: list[tuple[str, Any]] = []

    def to_spark(spark: Any, df: pd.DataFrame) -> FakeFrame:
        log.append(("pandas_to_spark", spark))
        return FakeFrame(df)

    def apply(sdf: FakeFrame, ops: list[TransformOp], *, project_root: Path) -> SparkTransformResult:
        log.append(("apply_transform_spark", [op.nome for op in ops]))
        r = apply_transform(sdf.pdf, ops, project_root=project_root)
        return SparkTransformResult(FakeFrame(r.rows), FakeFrame(r.rejected), r.duplicates_removed)

    def to_pandas(sdf: FakeFrame) -> pd.DataFrame:
        log.append(("spark_to_pandas", len(sdf.pdf)))
        return sdf.pdf

    monkeypatch.setattr(ingest_spark, "pandas_to_spark", to_spark)
    monkeypatch.setattr(ingest_spark, "apply_transform_spark", apply)
    monkeypatch.setattr(ingest_spark, "spark_to_pandas", to_pandas)
    return log


def _no_session() -> Any:
    raise AssertionError("a sessão Spark não deveria ser criada")


def test_ingest_spark_runs_ingestion_with_the_spark_engine(
    tmp_path: Path, calls: list[tuple[str, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    config = make_project(tmp_path / "spark")
    fake = FakeSpark()
    assert ingest_spark.main(_args(config), spark=fake) == 0
    assert "[ok] ingere: lidas=" in capsys.readouterr().out
    assert calls == [
        ("pandas_to_spark", fake),
        ("apply_transform_spark", ["tipar", "rejeitar_se", "deduplicar"]),
        ("spark_to_pandas", 2),
        ("spark_to_pandas", 2),
    ]
    assert fake.stopped == 0  # sessão injetada: quem a criou é quem a encerra

    # mesmo resultado que o ponto de entrada pandas (jobs.ingest) num projeto idêntico
    other = make_project(tmp_path / "pandas")
    assert ingest.main(_args(other)) == 0
    silver = _query(tmp_path / "spark", SILVER)
    assert silver == _query(tmp_path / "pandas", SILVER) == [("1", "gasolina", 5.5), ("2", "etanol", 3.99)]
    assert _query(tmp_path / "spark", QUARANTINE) == _query(tmp_path / "pandas", QUARANTINE)


def test_ingest_spark_creates_and_stops_its_own_session(
    tmp_path: Path, calls: list[tuple[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeSpark()
    monkeypatch.setattr(ingest_spark, "new_session", lambda: fake)
    assert ingest_spark.main(_args(make_project(tmp_path))) == 0
    assert calls[0] == ("pandas_to_spark", fake)
    assert fake.stopped == 1


def test_ingest_spark_stops_its_session_even_when_the_step_fails(
    tmp_path: Path, calls: list[tuple[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeSpark()
    monkeypatch.setattr(ingest_spark, "new_session", lambda: fake)
    config = make_project(tmp_path, sql="INSERT INTO silver.nada SELECT 1;")  # dml_pos_carga falha
    assert ingest_spark.main(_args(config)) == 1
    assert fake.stopped == 1


def test_ingest_spark_refuses_other_step_types_before_starting_spark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(ingest_spark, "new_session", _no_session)
    config = make_project(tmp_path, with_sql_step=True)
    assert ingest_spark.main(_args(config, "resumo")) == 1
    err = capsys.readouterr().err
    assert "a etapa 'resumo' é do tipo 'sql'" in err and "'ingestao'" in err and "jobs.run_step" in err


def test_ingest_spark_without_pyspark_is_actionable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql", None)
    assert ingest_spark.main(_args(make_project(tmp_path))) == 1
    err = capsys.readouterr().err
    assert "instale o extra [spark]" in err and "Como corrigir" in err and "Traceback" not in err


def _fake_pyspark(monkeypatch: pytest.MonkeyPatch, fail: bool) -> list[tuple[str, Any]]:
    """Pacote `pyspark` falso só com o `SparkSession.builder` (registra a configuração)."""
    seen: list[tuple[str, Any]] = []

    class Builder:
        def appName(self, name: str) -> Builder:  # noqa: N802 (API do PySpark)
            seen.append(("appName", name))
            return self

        def config(self, key: str, value: str) -> Builder:
            seen.append(("config", (key, value)))
            return self

        def getOrCreate(self) -> Any:  # noqa: N802
            if fail:
                raise RuntimeError("JAVA_GATEWAY_EXITED em /segredo/caminho")
            return FakeSpark()

    session_cls = type("SparkSession", (), {"builder": Builder()})
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = session_cls  # type: ignore[attr-defined]
    pkg = types.ModuleType("pyspark")
    pkg.sql = sql  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", pkg)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    return seen


def test_new_session_uses_utc_and_the_app_name(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _fake_pyspark(monkeypatch, fail=False)
    assert isinstance(ingest_spark.new_session(), FakeSpark)
    assert seen == [("appName", "dataeng-ingest-spark"), ("config", ("spark.sql.session.timeZone", "UTC"))]


def test_session_start_failure_is_actionable_without_raw_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _fake_pyspark(monkeypatch, fail=True)
    assert ingest_spark.main(_args(make_project(tmp_path))) == 1
    err = capsys.readouterr().err
    assert "não foi possível iniciar a sessão Spark (RuntimeError)" in err and "Java" in err
    assert "/segredo/caminho" not in err


def test_ingest_spark_usage_error() -> None:
    assert ingest_spark.main([]) == 2


@pytest.mark.spark
def test_ingest_spark_real_session_matches_pandas_entrypoint(tmp_path: Path, spark: Any) -> None:
    config = make_project(tmp_path / "spark")
    assert ingest_spark.main(_args(config), spark=spark) == 0
    other = make_project(tmp_path / "pandas")
    assert ingest.main(_args(other)) == 0
    assert _query(tmp_path / "spark", SILVER) == _query(tmp_path / "pandas", SILVER)
    assert _query(tmp_path / "spark", QUARANTINE) == _query(tmp_path / "pandas", QUARANTINE)
