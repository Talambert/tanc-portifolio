"""Warehouse no BigQuery: `dataset.tabela` vira `` `projeto.dataset.tabela` ``.

As importações de `google.cloud` são tardias (construtor sem `client=` e montagem do `LoadJobConfig`), para o
pacote base continuar funcionando sem o extra `[gcp]`. O parâmetro `client` existe para injeção em testes.

Cargas (`load`): os valores NUNCA entram no texto SQL. O DataFrame vai para uma tabela de passagem
(`<dataset>._stg_<uuid>`) via `load_table_from_dataframe`, e o SQL só referencia identificadores validados
(`validate_identifier`) entre crases:

- `append`: um único `INSERT ... SELECT` (atômico por ser uma instrução só).
- `overwrite_partition` (por QUALQUER coluna presente nos dados, inclusive `_run_id`): um script
  `BEGIN BEGIN TRANSACTION; DELETE ...; INSERT ...; COMMIT TRANSACTION; EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION; RAISE; END;` enviado como UM job de script: ou apaga e insere, ou nada muda.
  O `DELETE` apaga as linhas cujo valor da coluna aparece nos dados (`IS NOT DISTINCT FROM`, nulos incluídos).
- `merge`: um único `MERGE` (atômico), chaves comparadas com `IS NOT DISTINCT FROM`.

A tabela de destino é criada (`CREATE TABLE IF NOT EXISTS ... AS SELECT * ... WHERE FALSE`) ANTES do script,
porque DDL permanente não é permitido dentro de transação. A tabela de passagem é apagada ao final (melhor
esforço: se o processo morrer no meio, sobra uma `_stg_*` pequena que pode ser apagada à mão).
"""

from __future__ import annotations

import datetime as dt
import re
import uuid
from collections.abc import Sequence
from typing import Any

import pandas as pd

from dataeng_core.base.errors import ConfigError, DataEngError, LoadError, SqlError
from dataeng_core.base.runtime import get_logger
from dataeng_core.warehouse.base import LoadMode, check_load_request, split_table

_EXTRA_HINT = 'Instale o extra de GCP: pip install "dataeng-core[gcp]".'
_PROJECT = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]")
_LOCATION = re.compile(r"[A-Za-z][A-Za-z0-9-]*")
_STAGING_TTL = dt.timedelta(days=1)
_log = get_logger("dataeng_core.warehouse.bigquery")


def _q(name: str) -> str:
    """Cita um identificador já validado."""
    return f"`{name}`"


def _api_failure(exc: Exception, what: str) -> str:
    """Texto seguro de uma falha da API: só o tipo e o código HTTP, nunca a mensagem (pode citar dados)."""
    name = type(exc).__name__
    code = getattr(exc, "code", None)
    shown = f"{name} (HTTP {code})" if isinstance(code, int) else name
    if code in (401, 403):
        hint = "sem permissão ou credenciais inválidas: confira a conta de serviço e seus papéis no BigQuery"
    elif code == 404:
        hint = "dataset ou tabela não encontrado: confira o nome, o projeto e a localização"
    elif code == 400:
        hint = "a consulta ou os tipos dos dados foram recusados: confira o esquema da tabela de destino"
    else:
        hint = "verifique a conectividade, a cota e a localização, e tente novamente"
    return f"{what}: {shown}; {hint}."


def _first_line(exc: Exception) -> str:
    return (str(getattr(exc, "message", None) or exc).splitlines() or [""])[0][:200]


class BigQueryWarehouse:
    def __init__(self, project: str, location: str, client: Any = None, timeout: float = 600) -> None:
        if not isinstance(project, str) or not _PROJECT.fullmatch(project):
            raise ConfigError(
                "projeto GCP inválido: use o ID do projeto (minúsculas, dígitos e '-', de 6 a 30 caracteres).",
                operation="BigQueryWarehouse",
            )
        if not isinstance(location, str) or not _LOCATION.fullmatch(location):
            raise ConfigError(
                "localização do BigQuery inválida: use, por exemplo, 'US' ou 'southamerica-east1'.",
                operation="BigQueryWarehouse",
            )
        self._project = project
        self._location = location
        self._timeout = timeout
        if client is None:
            try:
                from google.cloud import bigquery
            except ImportError as exc:
                raise ConfigError(
                    f"o pacote google-cloud-bigquery não está instalado. {_EXTRA_HINT}",
                    operation="BigQueryWarehouse",
                ) from exc
            try:
                client = bigquery.Client(project=project, location=location)
            except Exception as exc:  # credenciais ausentes etc.; a mensagem original pode citar caminhos
                raise ConfigError(
                    f"não foi possível criar o cliente do BigQuery ({type(exc).__name__}). "
                    "Confira GOOGLE_APPLICATION_CREDENTIALS ou rode `gcloud auth application-default login`.",
                    operation="BigQueryWarehouse",
                ) from exc
        self._client: Any = client

    # ---- nomes -----------------------------------------------------------------------------

    def _ref(self, table: str, operation: str) -> tuple[str, str, str]:
        """(dataset, tabela, `projeto.dataset.tabela`) validados; o dataset é obrigatório."""
        dataset, name = split_table(table, operation=operation)
        if dataset is None:
            raise ConfigError(
                f"nome de tabela sem dataset: '{table}'. No BigQuery use 'dataset.tabela'.", operation=operation
            )
        return dataset, name, f"{self._project}.{dataset}.{name}"

    # ---- SQL livre -------------------------------------------------------------------------

    def _run(self, sql: str, operation: str) -> Any:
        """Envia um comando e espera terminar.

        Atenção: em SQL livre, a mensagem de erro (HTTP 400) inclui a 1ª linha (até 200 caracteres) do erro do
        BigQuery, que pode citar trechos do SQL ou de valores. Nunca inclui o corpo completo da resposta.
        """
        if not isinstance(sql, str) or not sql.strip():
            raise SqlError("o SQL está vazio.", operation=operation)
        try:
            return self._client.query(sql, location=self._location).result(timeout=self._timeout)
        except DataEngError:
            raise
        except Exception as exc:
            if getattr(exc, "code", None) == 400:
                raise SqlError(f"{type(exc).__name__}: {_first_line(exc)}", operation=operation) from exc
            raise SqlError(_api_failure(exc, "falha ao executar o SQL"), operation=operation) from exc

    def execute_ddl(self, sql: str) -> None:
        self._run(sql, "warehouse.execute_ddl")

    def run_sql(self, statements: Sequence[str]) -> None:
        for statement in statements:
            self._run(statement, "warehouse.run_sql")

    def create_or_replace_view(self, name: str, select_sql: str) -> None:
        op = "warehouse.create_or_replace_view"
        _, _, ref = self._ref(name, op)
        body = select_sql.strip().rstrip(";").strip() if isinstance(select_sql, str) else ""
        if not body:
            raise SqlError("o SELECT da view está vazio.", operation=op)
        self._run(f"CREATE OR REPLACE VIEW {_q(ref)} AS {body}", op)

    def query(self, sql: str) -> pd.DataFrame:
        result = self._run(sql, "warehouse.query")
        try:
            return result.to_dataframe()  # type: ignore[no-any-return]
        except Exception as exc:
            raise SqlError(
                _api_failure(exc, "falha ao ler o resultado da consulta"), operation="warehouse.query"
            ) from exc

    # ---- catálogo --------------------------------------------------------------------------

    def _get_table(self, ref: str, operation: str) -> Any | None:
        try:
            return self._client.get_table(ref)
        except Exception as exc:
            if getattr(exc, "code", None) == 404:
                return None
            raise LoadError(_api_failure(exc, "falha ao consultar a tabela"), operation=operation) from exc

    def table_exists(self, table: str) -> bool:
        op = "warehouse.table_exists"
        _, _, ref = self._ref(table, op)
        return self._get_table(ref, op) is not None

    def row_count(self, table: str) -> int:
        op = "warehouse.row_count"
        _, _, ref = self._ref(table, op)
        if self._get_table(ref, op) is None:
            raise LoadError(f"tabela inexistente: {table}.", operation=op)
        try:
            for row in self._client.query(f"SELECT COUNT(*) FROM {_q(ref)}", location=self._location).result(
                timeout=self._timeout
            ):
                return int(row[0])
        except Exception as exc:
            raise LoadError(_api_failure(exc, f"falha ao contar linhas de {table}"), operation=op) from exc
        raise LoadError(f"a contagem de {table} não devolveu resultado.", operation=op)

    # ---- carga -----------------------------------------------------------------------------

    def load(
        self,
        table: str,
        rows: pd.DataFrame,
        *,
        mode: LoadMode,
        partition_column: str | None = None,
        merge_keys: Sequence[str] = (),
    ) -> int:
        op = "warehouse.load"
        request = check_load_request(
            table, rows, mode=mode, partition_column=partition_column, merge_keys=merge_keys, operation=op
        )
        columns, keys, part = request.columns, request.keys, request.partition
        _, _, target_ref = self._ref(table, op)
        existing = self._get_table(target_ref, op)
        if existing is not None:
            known = {f.name.lower() for f in existing.schema}
            unknown = [c for c in columns if c.lower() not in known]
            if unknown:
                raise LoadError(f"coluna(s) inexistente(s) em {table}: {', '.join(unknown)}.", operation=op)
            # nomes exatos do esquema (sem diferenciar caixa) e índice descartado: não vira coluna extra
            exact = {f.name.lower(): f.name for f in existing.schema}
            columns = [exact[c.lower()] for c in columns]
            keys = [exact.get(k.lower(), k) for k in keys]
            part = exact.get(part.lower(), part) if part else None
            rows = rows.set_axis(columns, axis="columns")
        rows = rows.reset_index(drop=True)
        if len(rows) == 0:
            return 0  # no-op: não apaga nada (e não cria tabela sem dados para inferir tipos)

        dataset = target_ref.split(".")[1]
        staging_ref = f"{self._project}.{dataset}._stg_{uuid.uuid4().hex}"
        target, staging = _q(target_ref), _q(staging_ref)
        try:
            self._load_staging(rows, staging_ref, existing, columns)
            if existing is None:
                self._client.query(
                    f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM {staging} WHERE FALSE",
                    location=self._location,
                ).result(timeout=self._timeout)
            self._client.query(
                self._load_sql(mode, target, staging, columns, part, keys), location=self._location
            ).result(timeout=self._timeout)
        except DataEngError:
            raise
        except Exception as exc:
            raise LoadError(_api_failure(exc, f"falha ao gravar em {table}"), operation=op) from exc
        finally:
            self._drop_staging(staging_ref)
        return len(rows)

    def _load_staging(self, rows: pd.DataFrame, staging_ref: str, existing: Any | None, columns: list[str]) -> None:
        from google.cloud import bigquery

        schema = None
        if existing is not None:  # usa os tipos da tabela de destino (evita inferir tipo de coluna toda nula)
            by_name = {f.name.lower(): f for f in existing.schema}
            schema = [by_name[c.lower()] for c in columns]
        config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
        self._client.load_table_from_dataframe(rows, staging_ref, job_config=config, location=self._location).result(
            timeout=self._timeout
        )
        # expiração ANTES de qualquer INSERT/MERGE: se o processo morrer, o BigQuery apaga a tabela sozinho
        staging = self._client.get_table(staging_ref)
        staging.expires = dt.datetime.now(dt.UTC) + _STAGING_TTL
        self._client.update_table(staging, ["expires"])

    def _drop_staging(self, staging_ref: str) -> None:
        """Limpeza de melhor esforço: não mascara resultado nem erro original, mas deixa rastro no log."""
        try:
            self._client.query(f"DROP TABLE IF EXISTS `{staging_ref}`", location=self._location).result(
                timeout=self._timeout
            )
        except Exception as exc:
            _log.warning(
                "não foi possível apagar a tabela de passagem %s (%s); ela expira sozinha em %s.",
                staging_ref,
                type(exc).__name__,
                _STAGING_TTL,
            )

    @staticmethod
    def _load_sql(mode: str, target: str, staging: str, columns: list[str], part: str | None, keys: list[str]) -> str:
        cols = ", ".join(_q(c) for c in columns)
        insert = f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging}"
        if mode == "append":
            return insert
        if mode == "overwrite_partition" and part:
            delete = (
                f"DELETE FROM {target} AS t WHERE EXISTS "
                f"(SELECT 1 FROM {staging} AS d WHERE d.{_q(part)} IS NOT DISTINCT FROM t.{_q(part)})"
            )
            return (
                "BEGIN\n"
                "BEGIN TRANSACTION;\n"
                f"{delete};\n"
                f"{insert};\n"
                "COMMIT TRANSACTION;\n"
                "EXCEPTION WHEN ERROR THEN\n"
                "ROLLBACK TRANSACTION;\n"
                "RAISE;\n"
                "END;"
            )
        match = " AND ".join(f"t.{_q(k)} IS NOT DISTINCT FROM d.{_q(k)}" for k in keys)
        updates = [c for c in columns if c not in keys]
        matched = (
            "WHEN MATCHED THEN UPDATE SET " + ", ".join(f"{_q(c)} = d.{_q(c)}" for c in updates) + " "
            if updates
            else ""
        )
        values = ", ".join(f"d.{_q(c)}" for c in columns)
        return (
            f"MERGE {target} AS t USING {staging} AS d ON {match} "
            f"{matched}WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({values})"
        )
