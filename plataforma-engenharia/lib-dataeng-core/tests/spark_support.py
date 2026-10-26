"""Fixture `spark`: sessão local do PySpark, pulada sem o PySpark (extra [spark]) ou sem Java.

Exposta pelos `conftest.py` de `tests/transform` e `tests/jobs` (importam a MESMA função). A sessão é criada uma
vez e guardada aqui; `stop()` é registrado uma única vez no fim da sessão de testes. Não há `tests/conftest.py`
de propósito: ele poria `tests/` no `sys.path`, e `tests/airflow/` passaria a ser importável como `airflow`.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest


def _has_pyspark() -> bool:
    try:
        return importlib.util.find_spec("pyspark") is not None
    except (ImportError, ValueError):
        return False


def _has_java() -> bool:
    home = os.environ.get("JAVA_HOME")
    if home and any((Path(home) / "bin" / name).is_file() for name in ("java", "java.exe")):
        return True
    return shutil.which("java") is not None


SPARK_MISSING = [
    reason
    for ok, reason in ((_has_pyspark(), "PySpark (extra [spark])"), (_has_java(), "Java (JAVA_HOME ou java no PATH)"))
    if not ok
]

_SESSION: list[Any] = []


def _stop() -> None:
    while _SESSION:
        _SESSION.pop().stop()


@pytest.fixture(scope="session")
def spark(request: pytest.FixtureRequest) -> Any:
    if SPARK_MISSING:
        pytest.skip("sem " + " e ".join(SPARK_MISSING) + "; os testes de Spark rodam no CI (Linux)")
    if not _SESSION:
        # os workers Python do Spark usam o mesmo interpretador dos testes (com dataeng_core instalado)
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        from pyspark.sql import SparkSession

        _SESSION.append(
            SparkSession.builder.master("local[1]")
            .appName("dataeng-core-testes")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        request.config.add_cleanup(_stop)
    return _SESSION[0]
