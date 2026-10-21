"""Motor de transformação declarativa em PySpark: a MESMA semântica do motor pandas (`engine_pandas`).

O PySpark é importado só dentro das funções (extra `[spark]`): este módulo importa sem ele.
Versão mínima: PySpark 3.5 (`make_date`, `unix_micros`, `timestamp_micros`, `try_element_at`, `withColumns`,
`unionByName(allowMissingColumns)`), com Java compatível. Projetado para funcionar com `spark.sql.ansi.enabled`
ligado ou desligado (nenhum `cast` é feito sem validar o texto antes); validado só no CI Linux, não executado no
ambiente de desenvolvimento.

Contrato (igual ao do pandas): `rows` com os valores convertidos e `rejected` com as colunas ORIGINAIS da linha
mais `_motivo` (sem eco do valor da célula). "Primeira" ocorrência (`deduplicar`) e a ordem de saída seguem a
ordem de ENTRADA, capturada por `monotonically_increasing_id` + `localCheckpoint` antes de qualquer operação.
Tipos de saída: `decimal` → double, `inteiro` → long, `booleano` → boolean, `texto` → string, `data` → date,
`timestamp` → timestamp (instante UTC; anos fora de 1678-2261 são rejeitados, como no pandas).
`spark_to_pandas` devolve os mesmos dtypes do motor pandas (`Float64`, `Int64`, `boolean`, `string`, `date` em
`object` e `datetime64[us, UTC]`) sem depender do fuso da sessão.

Tamanho do plano: `tipar` converte em ETAPAS materializadas (`withColumns` sucessivos com colunas temporárias
`_dataeng_*`, removidas no fim), para que nenhuma expressão repita subárvores. Datas e timestamps fazem UMA
`regexp_replace` por formato candidato (veja `_date_patterns`) e escolhem o primeiro que converte com um
`coalesce` sobre colunas já calculadas. Tetos medidos sem Spark em `tests/transform/test_engine_spark_plan.py`.
Premissa: o `CollapseProject` do Catalyst (Spark ≥ 3.3) não junta projeções quando isso duplicaria uma
expressão não trivial usada mais de uma vez.

`localCheckpoint`: guarda os dados nos executores, sem tolerância a falhas. Se um executor se perde (comum no
Dataproc Serverless com autoescalonamento), o job falha em vez de recomputar. Para lotes grandes ou longos,
prefira `spark.sparkContext.setCheckpointDir(<gs://...>)` e troque por `checkpoint()` (mais lento, confiável).

Nomes reservados: colunas com o prefixo `_dataeng_` são do motor. Entrada, `renomear` ou gancho que as produza é
`ConfigError`. `renomear` também recusa nomes que só diferem em maiúsculas/minúsculas (o Spark não os distingue).

Ganchos (`hook`): recebem e devolvem um DataFrame do PySpark (clássico ou Spark Connect; não pandas). Mantendo
a coluna `_dataeng_row_id` (sem nulos, sem repetição e só com ids existentes), a "linha original" continua sendo a
da entrada; senão passa a ser a saída do gancho, como no pandas.

Diferenças conhecidas em relação ao pandas (casos raros):
- capacidade: `formato` aceita só `%Y %y %m %d %H %M %S %f %%`. Outras diretivas (`%b`, `%z`, `%j`, `%p`...)
  funcionam no pandas, mas aqui são `ConfigError` ao montar a transformação, antes de tocar nos dados;
- dígitos não ASCII (ex.: árabe-índicos) são aceitos pelo Python e rejeitados aqui;
- `timestamp` sem `formato`: do ISO 8601 só o subconjunto de `_date_patterns.ISO_PATTERN` (sem semanas ISO nem
  fração no fuso, que o Python aceita);
- as diretivas do `formato` vêm do `_strptime` do próprio interpretador (ex.: `%H` aceita ` 9` no 3.13);
- após um gancho que REORDENA mantendo `_dataeng_row_id`, "primeira" continua sendo a ordem de entrada;
- `texto`/`normalizar` de double fora do intervalo de long ou com expoente formata como o Java (`1.0E20`;
  JDK < 19 pode diferir também em outros doubles); de timestamp usa o fuso da sessão;
- `data` de coluna timestamp usa `to_date` no fuso da sessão (use `spark.sql.session.timeZone=UTC`);
- maiúsculas/minúsculas usam as regras da JVM (locale) para não ASCII; padrões `regex` são compilados pelo Java;
- colunas com prefixo `_dataeng_` e nomes que só diferem na caixa são recusados aqui (o pandas aceita).
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from functools import reduce
from pathlib import Path
from typing import Any

import pandas as pd

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import RegraRejeicao, TipoSpec, TransformOp
from dataeng_core.transform._date_patterns import (
    ISO_PATTERN,
    MARK,
    SEP,
    WS_JAVA,
    DatePattern,
    strptime_pattern,
)
from dataeng_core.transform.engine_pandas import (
    _DATE_FORMATS,
    _FALSE,
    _TRUE,
    _TS_FORMATS,
    _TS_MAX_YEAR,
    _TS_MIN_YEAR,
    MOTIVO,
    ROW_ID,
    _is_null,
    _to_text,
)
from dataeng_core.transform.hooks import resolve_hook

RESERVED_PREFIX = "_dataeng_"
_PART = "_dataeng_parte"
_TMP = "_dataeng_motivo_tmp"
_RANK = "_dataeng_ordem"
_EPOCH = dt.date(1970, 1, 1)
_US_PER_DAY = 86_400_000_000
_UTC_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
_TS_LO = (dt.datetime(_TS_MIN_YEAR, 1, 1, tzinfo=dt.UTC) - _UTC_EPOCH) // dt.timedelta(microseconds=1)
_TS_HI = (dt.datetime(_TS_MAX_YEAR + 1, 1, 1, tzinfo=dt.UTC) - _UTC_EPOCH) // dt.timedelta(microseconds=1)
# sufixos das temporárias por componente: o Spark não diferencia maiúsculas nos nomes (m/M, y/Y colidiriam)
_SUFFIX = {
    "Y": "ano",
    "y": "ano2",
    "m": "mes",
    "d": "dia",
    "H": "hora",
    "M": "minuto",
    "S": "seg",
    "f": "frac",
    "tzs": "tzsinal",
    "tzh": "tzhora",
    "tzm": "tzmin",
    "tzsec": "tzseg",
}
_TARGET = {"decimal": "double", "inteiro": "long", "booleano": "boolean", "data": "date", "timestamp": "timestamp"}


@dataclass
class SparkTransformResult:
    rows: Any  # pyspark.sql.DataFrame
    rejected: Any  # pyspark.sql.DataFrame
    duplicates_removed: int = field(default=0)  # removidas por `deduplicar` (não são rejeições)


def _pyspark() -> tuple[Any, Any, Any]:
    """`functions`, `types` e `Window` do PySpark, com erro acionável se o extra faltar."""
    try:
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        from pyspark.sql import types as T
    except ImportError as exc:
        raise ConfigError(
            "o motor Spark exige o PySpark: instale o extra [spark] (pip install 'dataeng-core[spark]') "
            "e um Java compatível (JAVA_HOME ou java no PATH)",
            operation="transform.spark",
        ) from exc
    return F, T, Window


def _is_dataframe(obj: Any) -> bool:
    """DataFrame do PySpark clássico ou do Spark Connect (duck typing: as duas classes têm esta interface)."""
    return isinstance(getattr(obj, "columns", None), list) and callable(getattr(obj, "toPandas", None))


def check_reserved(columns: Sequence[str], where: str) -> None:
    """Colunas com o prefixo `_dataeng_` são do motor (exceto o id de linha mantido por um gancho)."""
    reserved = [c for c in columns if c.casefold().startswith(RESERVED_PREFIX) and c != ROW_ID]
    if reserved:
        raise ConfigError(
            f"{where}: as colunas {', '.join(repr(c) for c in reserved)} usam o prefixo reservado "
            f"'{RESERVED_PREFIX}' do motor de transformação; renomeie-as",
            operation="transform.spark",
        )


def check_rename(columns: Sequence[str], mapping: Mapping[str, str]) -> None:
    """Resultado de `renomear` sem duplicadas, também sem diferenciar maiúsculas (o Spark não diferencia)."""
    names = [mapping.get(c, c) for c in columns]
    dup = sorted({n for n in names if names.count(n) > 1})
    if dup:
        raise ConfigError(
            f"operação 'renomear' geraria colunas duplicadas: {', '.join(repr(d) for d in dup)}",
            operation="transform.renomear",
        )
    folded = [n.casefold() for n in names]
    clash = sorted({n for n in names if folded.count(n.casefold()) > 1})
    if clash:
        raise ConfigError(
            f"operação 'renomear' geraria colunas que só diferem em maiúsculas/minúsculas: "
            f"{', '.join(repr(d) for d in clash)} (o Spark não as distingue)",
            operation="transform.renomear",
        )
    check_reserved(names, "operação 'renomear'")


def validate_ops(ops: Sequence[TransformOp]) -> None:
    """Erros de configuração detectáveis sem dados, antes de tocar no DataFrame (formatos, separadores, regex)."""
    for op in ops:
        for spec in (op.tipar or {}).values():
            if spec.separador_decimal not in (None, ".", ","):
                raise ConfigError(
                    "separador_decimal deve ser '.' ou ',' (o outro é usado como separador de milhar)",
                    operation="transform.tipar",
                )
            if spec.tipo in ("data", "timestamp") and spec.formato:
                strptime_pattern(spec.formato)
        for regra in op.rejeitar_se or []:
            if regra.regra == "regex":
                try:
                    re.compile(regra.padrao or "")
                except re.error as exc:
                    raise ConfigError(
                        f"regra regex da coluna '{regra.coluna}': padrão inválido ({exc.msg})",
                        operation="transform.rejeitar_se",
                    ) from exc


# ------------------------------------------------------------------------------------------- motor


class _Engine:
    def __init__(self, df: Any, project_root: Path) -> None:
        self.F, self.T, self.Window = _pyspark()
        if ROW_ID in df.columns:
            raise ConfigError(
                f"a coluna '{ROW_ID}' é reservada ao motor de transformação; renomeie-a na origem",
                operation="transform.spark",
            )
        check_reserved(df.columns, "entrada")
        self.root = project_root
        self.cur = self._with_ids(df)
        self.orig = self.cur
        self.parts: list[Any] = []
        self.duplicates = 0

    # ---------------------------------------------------------------- utilitários

    def _with_ids(self, df: Any) -> Any:
        # ids estáveis: materializados antes de qualquer operação (recomputar poderia renumerar)
        return df.withColumn(ROW_ID, self.F.monotonically_increasing_id()).localCheckpoint()

    def _col(self, name: str) -> Any:
        return self.F.col("`" + name.replace("`", "``") + "`")

    def _user_columns(self, df: Any) -> list[str]:
        return [c for c in df.columns if not c.casefold().startswith(RESERVED_PREFIX)]

    def _dtype(self, name: str) -> Any:
        return self.cur.schema[name].dataType

    def _has_rows(self) -> bool:
        return len(self.cur.head(1)) > 0

    def _check_columns(self, nomes: Sequence[str], op: str) -> list[str]:
        cols = self._user_columns(self.cur)
        faltam = [n for n in nomes if n not in cols]
        if faltam and self._has_rows():
            raise ConfigError(
                f"operação '{op}': coluna {', '.join(repr(n) for n in faltam)} não existe; "
                f"colunas disponíveis: {', '.join(cols) or 'nenhuma'}",
                operation=f"transform.{op}",
            )
        return [n for n in nomes if n in cols]

    def _null(self, typ: str) -> Any:
        return self.F.lit(None).cast(typ)

    def _strip(self, c: Any) -> Any:
        return self.F.regexp_replace(c, f"^{WS_JAVA}+|{WS_JAVA}+$", "")

    def _kind(self, dtype: Any) -> str:
        T = self.T
        if isinstance(dtype, T.StringType | T.NullType):
            return "string"
        if isinstance(dtype, T.BooleanType):
            return "bool"
        if isinstance(dtype, T.ByteType | T.ShortType | T.IntegerType | T.LongType):
            return "int"
        if isinstance(dtype, T.FloatType | T.DoubleType):
            return "float"
        if isinstance(dtype, T.DecimalType):
            return "decimal"
        if isinstance(dtype, T.DateType):
            return "date"
        if isinstance(dtype, T.TimestampType):
            return "timestamp"
        return "other"

    def _blank(self, c: Any, kind: str) -> Any:
        F = self.F
        if kind == "string":
            return c.isNull() | c.cast("string").rlike(f"^{WS_JAVA}*$")
        if kind == "float":
            return c.isNull() | F.isnan(c)
        return c.isNull()

    def _text(self, c: Any, kind: str, name: str) -> Any:
        """Igual a `_to_text` do pandas (nulo e NaN viram nulo)."""
        F = self.F
        if kind == "string":
            return c.cast("string")
        if kind == "bool":
            return F.when(c.isNull(), self._null("string")).when(c, F.lit("true")).otherwise(F.lit("false"))
        if kind in ("int", "decimal", "date"):
            return c.cast("string")
        if kind == "float":
            d = c.cast("double")
            whole = (F.abs(d) < F.lit(2.0**63)) & ((d % F.lit(1.0)) == F.lit(0.0))
            return (
                F.when(c.isNull() | F.isnan(d), self._null("string"))
                .when(whole, d.cast("long").cast("string"))
                .otherwise(d.cast("string"))
            )
        if kind == "timestamp":
            micro = F.unix_micros(c) % F.lit(1_000_000)
            return F.when(micro == 0, F.date_format(c, "yyyy-MM-dd'T'HH:mm:ssxxx")).otherwise(
                F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSxxx")
            )
        raise ConfigError(
            f"coluna '{name}': tipo {self._dtype(name).simpleString()} não suportado pelo motor Spark nesta operação",
            operation="transform.spark",
        )

    # ------------------------------------------------------------------ conversões

    def _decimal_text(self, t: Any, sep: str) -> tuple[Any, Any]:
        """`t` já sem espaços nas pontas."""
        F = self.F
        thou = "," if sep == "." else "."
        t = F.translate(t, " ", "")
        pattern = rf"^[+-]?(?:\d{{1,3}}(?:{re.escape(thou)}\d{{3}})+|\d+)(?:{re.escape(sep)}\d+)?$"
        valid = t.rlike(pattern)
        number = F.translate(F.translate(t, thou, ""), sep, ".")
        value = F.when(valid, number.cast("double"))
        ok = F.coalesce(valid & (F.abs(value) != F.lit(float("inf"))), F.lit(False))
        return value, ok

    def _decimal(self, c: Any, kind: str, sep: str, stripped: Any = None) -> tuple[Any, Any]:
        F = self.F
        if kind == "string":
            return self._decimal_text(self._strip(c) if stripped is None else stripped, sep)
        if kind in ("int", "decimal"):
            return c.cast("double"), F.lit(True)
        if kind == "float":
            d = c.cast("double")
            return d, F.abs(d) != F.lit(float("inf"))
        return self._null("double"), F.lit(False)  # booleano, data etc.: não é número

    def _whole(self, d: Any, ok: Any) -> tuple[Any, Any]:
        F = self.F
        good = F.coalesce(
            ok & ((d % F.lit(1.0)) == F.lit(0.0)) & (d >= F.lit(-(2.0**63))) & (d < F.lit(2.0**63)), F.lit(False)
        )
        return F.when(good, d.cast("long")), good

    def _integer(self, c: Any, kind: str, spec: TipoSpec, t: Any, p: str) -> tuple[Any, Any]:
        F = self.F
        if kind == "string" and spec.separador_decimal is None:
            valid = t.rlike(r"^[+-]?\d+(?:\.0+)?$")
            neg = t.startswith("-")
            digits = F.split(F.ltrim(F.translate(t, "+-", "")), "\\.", 2).getItem(0)
            mag = F.ltrim(F.translate(digits, "0", " "))  # sem zeros à esquerda (não há espaços no texto)
            mag = F.translate(mag, " ", "0")
            limit = F.when(neg, F.lit("9223372036854775808")).otherwise(F.lit("9223372036854775807"))
            fits = (F.length(mag) < 19) | ((F.length(mag) == 19) & (mag <= limit))
            ok = F.coalesce(valid & fits, F.lit(False))
            signed = F.concat(
                F.when(neg, F.lit("-")).otherwise(F.lit("")), F.when(mag == "", F.lit("0")).otherwise(mag)
            )
            return F.when(ok, signed.cast("long")), ok
        if kind == "int":
            return c.cast("long"), F.lit(True)
        d, ok = self._decimal(c, kind, spec.separador_decimal or ".", stripped=t)
        # materializa o número antes de testar se é inteiro (senão a subárvore se repetiria)
        self.cur = self.cur.withColumns({f"{p}num": d, f"{p}numok": F.coalesce(ok, F.lit(False))})
        return self._whole(F.col(f"{p}num"), F.col(f"{p}numok"))

    def _boolean(self, c: Any, kind: str, t: Any) -> tuple[Any, Any]:
        F = self.F
        if kind == "bool":
            return c, F.lit(True)
        if kind == "string":
            low = F.lower(t)
            value = F.when(low.isin(*sorted(_TRUE)), F.lit(True)).when(low.isin(*sorted(_FALSE)), F.lit(False))
        elif kind in ("int", "float", "decimal"):
            d = c.cast("double")
            value = F.when(d == 1, F.lit(True)).when(d == 0, F.lit(False))
        else:
            return self._null("boolean"), F.lit(False)
        return value, value.isNotNull()

    def _stage_dates(self, s_name: str, patterns: Sequence[DatePattern], p: str, want_ts: bool) -> tuple[Any, Any]:
        """Converte o texto `s_name` (sem espaços nas pontas) em etapas; devolve (valor, ok) curtos.

        Por padrão j: `r` = uma `regexp_replace`; `w` = casou o texto todo; `a` = grupos; strings e inteiros dos
        componentes; `ok`, data e microssegundos UTC. Por fim, o primeiro padrão que converte (`coalesce`).
        """
        F = self.F
        s = F.col(s_name)
        n = len(patterns)
        self.cur = self.cur.withColumns(
            {f"{p}r{j}": F.regexp_replace(s, pat.regex, pat.replacement) for j, pat in enumerate(patterns)}
        )
        stage: dict[str, Any] = {}
        for j in range(n):
            r = F.col(f"{p}r{j}")
            stage[f"{p}w{j}"] = F.coalesce(r.startswith(MARK) & r.endswith(MARK) & ~s.contains(MARK), F.lit(False))
            stage[f"{p}a{j}"] = F.split(F.translate(r, MARK, ""), SEP, -1)
        self.cur = self.cur.withColumns(stage)
        stage = {}
        for j, pat in enumerate(patterns):
            for comp, group in pat.groups.items():
                item = F.try_element_at(F.col(f"{p}a{j}"), F.lit(group))
                stage[f"{p}{j}_t{_SUFFIX[comp]}"] = F.when(F.col(f"{p}w{j}"), item)
        self.cur = self.cur.withColumns(stage)

        def text(j: int, comp: str) -> Any:
            return F.col(f"{p}{j}_t{_SUFFIX[comp]}") if comp in patterns[j].groups else None

        def number(j: int, comp: str, default: int) -> Any:
            g = text(j, comp)
            if g is None:
                return F.lit(default)
            # componentes de uma correspondência são só dígitos (às vezes com espaço à esquerda, ex.: `%d`)
            return F.coalesce(F.when(F.length(g) > 0, F.trim(g).cast("int")), F.lit(default))

        stage = {}
        for j, pat in enumerate(patterns):
            if "Y" in pat.groups:
                stage[f"{p}{j}_ano"] = number(j, "Y", 1900)
            elif "y" in pat.groups:
                yy = number(j, "y", 0)
                stage[f"{p}{j}_ano"] = F.when(yy <= 68, yy + 2000).otherwise(yy + 1900)
            else:
                stage[f"{p}{j}_ano"] = F.lit(1900)
            defaults = (("m", 1), ("d", 1), ("H", 0), ("M", 0), ("S", 0), ("tzh", 0), ("tzm", 0), ("tzsec", 0))
            for comp, default in defaults:
                stage[f"{p}{j}_{_SUFFIX[comp]}"] = number(j, comp, default)
            frac = text(j, "f")
            stage[f"{p}{j}_us"] = (
                F.lit(0)
                if frac is None
                else F.coalesce(
                    F.when(F.length(frac) > 0, F.rpad(F.substring(frac, 1, 6), 6, "0").cast("long")), F.lit(0)
                )
            )
            sign = text(j, "tzs")
            stage[f"{p}{j}_neg"] = F.lit(False) if sign is None else F.coalesce(sign == "-", F.lit(False))
        self.cur = self.cur.withColumns(stage)

        def c(j: int, comp: str) -> Any:
            return F.col(f"{p}{j}_{_SUFFIX.get(comp, comp)}")

        stage = {}
        for j in range(n):
            year, month, day = c(j, "Y"), c(j, "m"), c(j, "d")
            leap = ((year % 4 == 0) & (year % 100 != 0)) | (year % 400 == 0)
            days_in_month = (
                F.when(month == 2, F.when(leap, 29).otherwise(28)).when(month.isin(4, 6, 9, 11), 30).otherwise(31)
            )
            valid = (
                F.col(f"{p}w{j}")
                & (year >= 1)
                & (month >= 1)
                & (month <= 12)
                & (day >= 1)
                & (day <= days_in_month)
                & (c(j, "H") <= 23)
                & (c(j, "M") <= 59)
                & (c(j, "S") <= 59)
                & (c(j, "tzh") <= 23)
                & (c(j, "tzm") <= 59)
                & (c(j, "tzsec") <= 59)
            )
            # componentes só são comparados se casou (senão podem ser nulos)
            stage[f"{p}ok{j}"] = F.coalesce(F.when(F.col(f"{p}w{j}"), valid), F.lit(False))
        self.cur = self.cur.withColumns(stage)
        self.cur = self.cur.withColumns(
            {f"{p}d{j}": F.when(F.col(f"{p}ok{j}"), F.make_date(c(j, "Y"), c(j, "m"), c(j, "d"))) for j in range(n)}
        )
        ok_any = reduce(lambda a, b: a | b, [F.col(f"{p}ok{j}") for j in range(n)])
        if not want_ts:
            self.cur = self.cur.withColumns(
                {
                    f"{p}data": F.coalesce(*[F.when(F.col(f"{p}ok{j}"), F.col(f"{p}d{j}")) for j in range(n)]),
                    f"{p}okany": ok_any,
                }
            )
            return F.col(f"{p}data"), F.col(f"{p}okany")
        stage = {}
        for j in range(n):
            tz = c(j, "tzh") * 3600 + c(j, "tzm") * 60 + c(j, "tzsec")
            offset = F.when(c(j, "neg"), -tz).otherwise(tz)
            seconds = c(j, "H") * 3600 + c(j, "M") * 60 + c(j, "S") - offset
            micros = (
                F.datediff(F.col(f"{p}d{j}"), F.lit(_EPOCH)).cast("long") * F.lit(_US_PER_DAY)
                + seconds.cast("long") * F.lit(1_000_000)
                + c(j, "us")
            )
            stage[f"{p}u{j}"] = F.when(F.col(f"{p}ok{j}"), micros)
        self.cur = self.cur.withColumns(stage)
        self.cur = self.cur.withColumns(
            {
                f"{p}micros": F.coalesce(*[F.when(F.col(f"{p}ok{j}"), F.col(f"{p}u{j}")) for j in range(n)]),
                f"{p}okany": ok_any,
            }
        )
        micros_col = F.col(f"{p}micros")
        good = F.coalesce(
            F.col(f"{p}okany") & (micros_col >= F.lit(_TS_LO)) & (micros_col < F.lit(_TS_HI)), F.lit(False)
        )
        self.cur = self.cur.withColumn(f"{p}bom", good)
        return F.when(F.col(f"{p}bom"), F.timestamp_micros(micros_col)), F.col(f"{p}bom")

    def _date(self, c: Any, kind: str, spec: TipoSpec, p: str) -> tuple[Any, Any]:
        F = self.F
        if kind == "date":
            return c, F.lit(True)
        if kind == "timestamp":
            return F.to_date(c), F.lit(True)
        if kind != "string":
            return self._null("date"), F.lit(False)
        formats = [spec.formato] if spec.formato else list(_DATE_FORMATS)
        return self._stage_dates(f"{p}s", [strptime_pattern(f) for f in formats], p, want_ts=False)

    def _timestamp(self, c: Any, kind: str, spec: TipoSpec, p: str) -> tuple[Any, Any]:
        F = self.F
        if kind == "string":
            if spec.formato:
                patterns = [strptime_pattern(spec.formato)]
            else:
                patterns = [ISO_PATTERN, *(strptime_pattern(f) for f in _TS_FORMATS)]
            return self._stage_dates(f"{p}s", patterns, p, want_ts=True)
        if kind == "timestamp":
            micros, ok = F.unix_micros(c), F.lit(True)
        elif kind == "date":
            micros, ok = F.datediff(c, F.lit(_EPOCH)).cast("long") * F.lit(_US_PER_DAY), F.lit(True)
        else:
            return self._null("timestamp"), F.lit(False)
        good = F.coalesce(ok & (micros >= F.lit(_TS_LO)) & (micros < F.lit(_TS_HI)), F.lit(False))
        return F.when(good, F.timestamp_micros(micros)), good

    def _convert(self, name: str, spec: TipoSpec, kind: str, p: str) -> tuple[Any, Any]:
        """(valor, ok) a partir das temporárias da etapa 1 (`{p}s`: texto sem espaços nas pontas)."""
        F = self.F
        c = self._col(name)
        t = F.col(f"{p}s") if kind == "string" else None
        if spec.tipo == "texto":
            return self._text(c, kind, name), F.lit(True)
        if spec.tipo == "decimal":
            return self._decimal(c, kind, spec.separador_decimal or ".", stripped=t)
        if spec.tipo == "inteiro":
            return self._integer(c, kind, spec, t, p)
        if spec.tipo == "booleano":
            return self._boolean(c, kind, t)
        if spec.tipo == "data":
            return self._date(c, kind, spec, p)
        return self._timestamp(c, kind, spec, p)

    # ---------------------------------------------------------------- rejeição

    def _reject(self, reason: Any) -> None:
        """Linhas com `reason` não nulo vão para a quarentena (linha original + motivo); as demais seguem."""
        F = self.F
        flagged = self.cur.withColumn(_TMP, reason)
        part = flagged.filter(F.col(_TMP).isNotNull()).select(ROW_ID, F.col(_TMP).alias(MOTIVO))
        originals = self._user_columns(self.orig)
        self.parts.append(
            self.orig.join(part, on=ROW_ID, how="inner").select(
                *[self._col(c) for c in originals],
                F.col(MOTIVO).cast("string").alias(MOTIVO),
                F.col(ROW_ID),
                F.lit(len(self.parts)).alias(_PART),
            )
        )
        self.cur = flagged.filter(F.col(_TMP).isNull()).drop(_TMP)

    def _rule_reason(self, regra: RegraRejeicao) -> Any:
        F = self.F
        col = regra.coluna
        c, kind = self._col(col), self._kind(self._dtype(col))
        blank = F.coalesce(self._blank(c, kind), F.lit(False))
        if regra.regra == "nao_nulo":
            return F.when(blank, F.lit(f"coluna '{col}': valor nulo (regra nao_nulo)"))
        if regra.regra == "regex":
            passes = self._text(c, kind, col).rlike(regra.padrao or "")
            return F.when(blank, self._null("string")).when(
                ~passes, F.lit(f"coluna '{col}': valor fora do padrão (regra regex)")
            )
        number, ok = self._decimal(c, kind, ".")
        out = F.lit(False)
        if regra.minimo is not None:
            out = out | (number < F.lit(float(regra.minimo)))
        if regra.maximo is not None:
            out = out | (number > F.lit(float(regra.maximo)))
        return (
            F.when(blank, self._null("string"))
            .when(~ok, F.lit(f"coluna '{col}': valor não numérico (regra faixa)"))
            .when(out, F.lit(f"coluna '{col}': valor fora da faixa (regra faixa)"))
        )

    # --------------------------------------------------------------- operações

    def renomear(self, mapping: dict[str, str]) -> None:
        present = {old: new for old, new in mapping.items() if old in self._check_columns([old], "renomear")}
        check_rename(self._user_columns(self.cur), present)
        self.cur = self.cur.select(
            *[self._col(c).alias(present.get(c, c)) for c in self._user_columns(self.cur)], self.F.col(ROW_ID)
        )

    def tipar(self, specs: dict[str, TipoSpec]) -> None:
        F = self.F
        cols = self._check_columns(list(specs), "tipar")
        if not cols:
            return
        prefixes = {c: f"{RESERVED_PREFIX}c{i}_" for i, c in enumerate(cols)}
        kinds = {c: self._kind(self._dtype(c)) for c in cols}
        # etapa 1: branco e texto sem espaços nas pontas (uma regex cada)
        stage: dict[str, Any] = {}
        for c in cols:
            stage[prefixes[c] + "branco"] = F.coalesce(self._blank(self._col(c), kinds[c]), F.lit(False))
            if kinds[c] == "string":
                stage[prefixes[c] + "s"] = self._strip(self._col(c).cast("string"))
        self.cur = self.cur.withColumns(stage)
        # etapa 2 (datas: várias): valor e ok de cada coluna
        for c in cols:
            p = prefixes[c]
            value, ok = self._convert(c, specs[c], kinds[c], p)
            self.cur = self.cur.withColumns({p + "v": value, p + "ok": F.coalesce(ok, F.lit(False))})
        # etapa final: colunas tipadas e marcas de falha (expressões curtas sobre as temporárias)
        typed: dict[str, Any] = {}
        bad: dict[str, Any] = {}
        for c in cols:
            p = prefixes[c]
            if specs[c].tipo == "texto":
                typed[c], bad[c] = F.col(p + "v"), F.lit(False)
                continue
            target = _TARGET[specs[c].tipo]
            typed[c] = (
                F.when(F.col(p + "branco"), self._null(target)).otherwise(F.when(F.col(p + "ok"), F.col(p + "v")))
            ).cast(target)
            bad[c] = F.when(F.col(p + "branco"), F.lit(False)).otherwise(~F.col(p + "ok"))
        bad_names = {c: prefixes[c] + "falha" for c in cols}
        self.cur = self.cur.select(
            *[typed[c].alias(c) if c in typed else self._col(c) for c in self._user_columns(self.cur)],
            F.col(ROW_ID),
            *[bad[c].alias(bad_names[c]) for c in cols],
        )
        motivos = [
            F.when(F.col(bad_names[c]), F.lit(f"coluna '{c}': valor não convertível para {specs[c].tipo}"))
            for c in cols
        ]
        any_bad = reduce(lambda a, b: a | b, [F.col(bad_names[c]) for c in cols])
        self._reject(F.when(any_bad, F.concat_ws("; ", *motivos)))
        self.cur = self.cur.drop(*bad_names.values())

    def normalizar(self, spec: Mapping[str, Sequence[str]]) -> None:
        F = self.F
        cols = self._check_columns(list(spec), "normalizar")
        exprs: dict[str, Any] = {}
        for col in cols:
            text = self._text(self._col(col), self._kind(self._dtype(col)), col)
            for name in spec[col]:
                if name == "apenas_digitos":
                    text = F.regexp_replace(text, "[^0-9]", "")
                elif name == "trim":
                    text = self._strip(text)
                elif name == "maiusculas":
                    text = F.upper(text)
                elif name == "minusculas":
                    text = F.lower(text)
                elif name == "colapsar_espacos":
                    text = F.regexp_replace(text, f"{WS_JAVA}+", " ")
            exprs[col] = text.cast("string")
        if exprs:
            self.cur = self.cur.select(
                *[exprs[c].alias(c) if c in exprs else self._col(c) for c in self._user_columns(self.cur)],
                F.col(ROW_ID),
            )

    def deduplicar(self, keys: list[str]) -> None:
        F = self.F
        if not keys:
            raise ConfigError("operação 'deduplicar' exige ao menos uma coluna", operation="transform.deduplicar")
        cols = self._check_columns(keys, "deduplicar")
        if not cols or not self._has_rows():
            return
        window = self.Window.partitionBy(*[self._col(c) for c in cols]).orderBy(F.col(ROW_ID))
        ranked = self.cur.withColumn(_RANK, F.row_number().over(window)).localCheckpoint()
        self.duplicates += ranked.filter(F.col(_RANK) > 1).count()
        self.cur = ranked.filter(F.col(_RANK) == 1).drop(_RANK)

    def rejeitar_se(self, regras: list[RegraRejeicao]) -> None:
        for regra in regras:
            if not self._check_columns([regra.coluna], "rejeitar_se"):
                continue
            self._reject(self._rule_reason(regra))

    def hook(self, ref: str) -> None:
        F = self.F
        fn = resolve_hook(ref, project_root=self.root)
        try:
            out: Any = fn(self.cur)  # DataFrame do PySpark (o tipo `Hook` descreve o pandas)
        except Exception as exc:
            raise DataEngError(
                f"o gancho '{ref}' falhou ({type(exc).__name__}); veja o traceback e corrija a função",
                operation="transform.hook",
            ) from exc
        if not _is_dataframe(out):
            raise DataEngError(
                f"o gancho '{ref}' deve devolver um DataFrame do PySpark, mas devolveu {type(out).__name__}",
                operation="transform.hook",
            )
        check_reserved(out.columns, f"saída do gancho '{ref}'")
        try:  # o Spark é preguiçoso: o código do gancho só roda aqui
            out = out.localCheckpoint()
            stable = False
            if ROW_ID in out.columns:
                stats = out.agg(
                    F.count(F.lit(1)).alias("n"), F.count(F.col(ROW_ID)).alias("nn"), F.countDistinct(ROW_ID)
                ).first()
                unknown = out.select(ROW_ID).join(self.cur.select(ROW_ID), on=ROW_ID, how="left_anti").head(1)
                stable = stats is not None and stats[0] == stats[1] == stats[2] and not unknown
        except Exception as exc:
            raise DataEngError(
                f"o gancho '{ref}' falhou ao ser executado ({type(exc).__name__}); veja o traceback e corrija a função",
                operation="transform.hook",
            ) from exc
        if stable:
            self.cur = out.select(
                *[self._col(c) for c in self._user_columns(out)], F.col(ROW_ID).cast("long").alias(ROW_ID)
            )
        else:  # sem identificador confiável: a "linha original" passa a ser a saída do gancho
            self.cur = self._with_ids(out.drop(ROW_ID))
            self.orig = self.cur

    def result(self) -> SparkTransformResult:
        F = self.F
        rows = self.cur.orderBy(F.col(ROW_ID)).drop(ROW_ID)
        if self.parts:
            union = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), self.parts)
            rejected = union.orderBy(F.col(_PART), F.col(ROW_ID)).drop(_PART, ROW_ID)
        else:
            rejected = self.orig.limit(0).drop(ROW_ID).withColumn(MOTIVO, self._null("string"))
        return SparkTransformResult(rows=rows, rejected=rejected, duplicates_removed=self.duplicates)


def apply_transform_spark(df: Any, ops: Sequence[TransformOp], *, project_root: Path) -> SparkTransformResult:
    """Aplica `ops` na ordem declarada sobre um DataFrame do PySpark; linhas ruins vão para `rejected`.

    Erros de configuração que não dependem dos dados (formato de data não suportado, separador, regex inválida)
    são levantados antes de qualquer ação sobre o DataFrame.
    """
    _pyspark()
    validate_ops(ops)
    engine = _Engine(df, Path(project_root))
    for op in ops:
        if op.renomear is not None:
            engine.renomear(op.renomear)
        elif op.tipar is not None:
            engine.tipar(op.tipar)
        elif op.normalizar is not None:
            engine.normalizar(op.normalizar)
        elif op.deduplicar is not None:
            engine.deduplicar(op.deduplicar)
        elif op.rejeitar_se is not None:
            engine.rejeitar_se(op.rejeitar_se)
        elif op.hook is not None:
            engine.hook(op.hook)
    return engine.result()


# ----------------------------------------------------------------------------- conversões pandas <-> Spark


def pandas_to_spark(spark: Any, df: pd.DataFrame) -> Any:
    """DataFrame pandas → Spark com TODAS as colunas texto (como a ingestão as entrega): nulo vira nulo."""
    _, T, _ = _pyspark()
    schema = T.StructType([T.StructField(str(c), T.StringType(), True) for c in df.columns])
    data = [tuple(None if _is_null(v) else _to_text(v) for v in row) for row in df.itertuples(index=False, name=None)]
    return spark.createDataFrame(data, schema=schema)


def spark_to_pandas(df: Any) -> pd.DataFrame:
    """DataFrame Spark → pandas com os dtypes do motor pandas; timestamps em UTC, sem depender do fuso da sessão."""
    F, T, _ = _pyspark()
    fields = list(df.schema.fields)
    exprs = [
        F.unix_micros(F.col("`" + f.name.replace("`", "``") + "`")).alias(f.name)
        if isinstance(f.dataType, T.TimestampType)
        else F.col("`" + f.name.replace("`", "``") + "`")
        for f in fields
    ]
    collected = df.select(*exprs).collect() if fields else []
    out: dict[str, pd.Series] = {}
    for i, f in enumerate(fields):
        values = [r[i] for r in collected]
        typ = f.dataType
        if isinstance(typ, T.TimestampType):
            stamps = [pd.NaT if v is None else pd.Timestamp(v, unit="us", tz="UTC") for v in values]
            out[f.name] = pd.Series(stamps, dtype="datetime64[us, UTC]")
        elif isinstance(typ, T.ByteType | T.ShortType | T.IntegerType | T.LongType):
            out[f.name] = pd.Series(values, dtype="Int64")
        elif isinstance(typ, T.FloatType | T.DoubleType):
            out[f.name] = pd.Series(values, dtype="Float64")
        elif isinstance(typ, T.BooleanType):
            out[f.name] = pd.Series(values, dtype="boolean")
        elif isinstance(typ, T.StringType):
            out[f.name] = pd.Series(values, dtype="string")
        else:
            out[f.name] = pd.Series(values, dtype=object)
    return pd.DataFrame(out, index=pd.RangeIndex(len(collected)))
