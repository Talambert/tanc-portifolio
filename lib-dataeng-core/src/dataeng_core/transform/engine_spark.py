"""Motor de transformação declarativa em PySpark: a MESMA semântica do motor pandas (`engine_pandas`).

O PySpark é importado só dentro das funções (extra `[spark]`): este módulo importa sem ele.
Versão mínima: PySpark 3.5 (`make_date`, `unix_micros`, `timestamp_micros`, `unionByName(allowMissingColumns)`),
com Java compatível. Funciona com `spark.sql.ansi.enabled` ligado ou desligado: nenhuma conversão é feita sem
antes validar o texto por expressão regular (um `cast` inválido com ANSI levantaria erro em vez de devolver nulo).

Contrato (igual ao do pandas): `rows` com os valores convertidos e `rejected` com as colunas ORIGINAIS da linha
mais `_motivo` (sem eco do valor da célula). "Primeira" ocorrência (`deduplicar`) e a ordem de saída seguem a
ordem de ENTRADA, capturada por `monotonically_increasing_id` + `localCheckpoint` antes de qualquer operação.
Tipos de saída: `decimal` → double, `inteiro` → long, `booleano` → boolean, `texto` → string, `data` → date,
`timestamp` → timestamp (instante UTC; anos fora de 1678-2261 são rejeitados, como no pandas).
`spark_to_pandas` devolve os mesmos dtypes do motor pandas (`Float64`, `Int64`, `boolean`, `string`, `date` em
`object` e `datetime64[us, UTC]`) sem depender do fuso da sessão.

Ganchos (`hook`): recebem e devolvem um `pyspark.sql.DataFrame` (não pandas). Mantendo a coluna
`_dataeng_row_id` (sem nulos, sem repetição e só com ids existentes), a "linha original" continua sendo a da
entrada; senão passa a ser a saída do gancho, como no pandas.

Diferenças conhecidas em relação ao pandas (casos raros, documentados):
- dígitos não ASCII (ex.: árabe-índicos) são aceitos pelo Python e rejeitados aqui;
- `formato` aceita só as diretivas `%Y %y %m %d %H %M %S %f %%` (outras são `ConfigError` aqui);
- `timestamp` sem `formato`: do ISO 8601 aceita `AAAA-MM-DD` ou `AAAAMMDD`, opcionalmente seguido de um
  separador qualquer, `HH[:MM[:SS[.f]]]` ou `HH[MM[SS[.f]]]` e fuso `Z` ou `±HH[[:]MM[[:]SS]]` (sem semanas ISO
  nem fração no fuso, que o Python aceita);
- as diretivas do `formato` vêm do `_strptime` do próprio interpretador (ex.: `%H` aceita ` 9` no 3.13);
- após um gancho que REORDENA mantendo `_dataeng_row_id`, "primeira" continua sendo a ordem de entrada;
- `texto`/`normalizar` de double fora do intervalo de long ou com expoente formata como Java (`1.0E20`);
  de timestamp usa o fuso da sessão (use `spark.sql.session.timeZone=UTC`);
- maiúsculas/minúsculas usam as regras do Java; padrões `regex` são compilados pelo Java (sintaxe quase igual).
"""

from __future__ import annotations

import datetime as dt
import functools
import importlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from functools import reduce
from pathlib import Path
from typing import Any

import pandas as pd

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import RegraRejeicao, TipoSpec, TransformOp
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

# Espaço em branco do `str.isspace()` do Python, em regex Java.
WS_JAVA = r"[\p{IsWhite_Space}\x1c-\x1f]"

_PART = "_dataeng_parte"
_TMP = "_dataeng_motivo_tmp"
_RANK = "_dataeng_ordem"
_EPOCH = dt.date(1970, 1, 1)
_US_PER_DAY = 86_400_000_000
_TS_LO = (dt.datetime(_TS_MIN_YEAR, 1, 1, tzinfo=dt.UTC) - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)) // dt.timedelta(
    microseconds=1
)
_TS_HI = (dt.datetime(_TS_MAX_YEAR + 1, 1, 1, tzinfo=dt.UTC) - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)) // dt.timedelta(
    microseconds=1
)


@dataclass
class SparkTransformResult:
    rows: Any  # pyspark.sql.DataFrame
    rejected: Any  # pyspark.sql.DataFrame
    duplicates_removed: int = field(default=0)  # removidas por `deduplicar` (não são rejeições)


def _pyspark() -> tuple[Any, Any, Any, Any]:
    """`functions`, `types`, `Window` e `DataFrame` do PySpark, com erro acionável se o extra faltar."""
    try:
        from pyspark.sql import DataFrame, Window
        from pyspark.sql import functions as F
        from pyspark.sql import types as T
    except ImportError as exc:
        raise ConfigError(
            "o motor Spark exige o PySpark: instale o extra [spark] (pip install 'dataeng-core[spark]') "
            "e um Java compatível (JAVA_HOME ou java no PATH)",
            operation="transform.spark",
        ) from exc
    return F, T, Window, DataFrame


# ------------------------------------------------------------- padrões de data (Python puro, testável sem Spark)

_SUPPORTED_DIRECTIVES = "dmYyHMSf"


@functools.cache
def _directive(directive: str) -> str:
    """Fragmento do `_strptime` do PRÓPRIO interpretador (muda entre versões), com grupo numerado.

    Ex.: `(?P<d>3[0-1]|[1-2]\\d|0[1-9]|[1-9]| [1-9])` vira `(3[0-1]|...)`: válido em Python e em Java.
    """
    fragment = str(importlib.import_module("_strptime").TimeRE()[directive])
    return re.sub(r"^\(\?P<\w+>", "(", fragment)


@dataclass(frozen=True)
class DatePattern:
    """Regex ancorada no início; a correspondência (a primeira, como `re.match`) deve cobrir o texto todo.

    `groups`: componente (`Y y m d H M S f Z tzs tzh tzm`) → índices dos grupos; mais de um índice são
    alternativas exclusivas (só uma participa), e o valor é a concatenação deles.
    """

    regex: str
    groups: Mapping[str, tuple[int, ...]]


def strptime_pattern(fmt: str, ws: str = WS_JAVA) -> DatePattern:
    """Traduz um formato do `strptime` (subconjunto) para regex, como o próprio `_strptime` faz."""
    parts: list[str] = []
    groups: dict[str, tuple[int, ...]] = {}
    i = 0
    while i < len(fmt):
        ch = fmt[i]
        if ch == "%":
            directive = fmt[i + 1] if i + 1 < len(fmt) else ""
            i += 2
            if directive == "%":
                parts.append("%")
                continue
            if not directive or directive not in _SUPPORTED_DIRECTIVES:
                raise ConfigError(
                    f"formato de data '{fmt}': a diretiva '%{directive}' não é suportada pelo motor Spark; "
                    "use só %Y %y %m %d %H %M %S %f",
                    operation="transform.tipar",
                )
            if directive in groups or (directive in "Yy" and ("Y" in groups or "y" in groups)):
                raise ConfigError(
                    f"formato de data '{fmt}': diretiva de ano/campo repetida ('%{directive}')",
                    operation="transform.tipar",
                )
            groups[directive] = (len(groups) + 1,)
            parts.append(_directive(directive))
        elif ch.isspace():
            while i < len(fmt) and fmt[i].isspace():
                i += 1
            parts.append(ws + "+")
        else:
            parts.append(ch if ch.isalnum() else re.escape(ch))
            i += 1
    return DatePattern("(?i)^(?:" + "".join(parts) + ")", groups)


_TIME = r"(?:(\d{2})(?::(\d{2})(?::(\d{2})(?:[.,](\d+))?)?)?|(\d{2})(?:(\d{2})(?:(\d{2})(?:[.,](\d+))?)?)?)"
_TZ = r"(?:(Z)|([+-])(\d{2})(?::?(\d{2})(?::?(\d{2}))?)?)?"


def _iso(date: str) -> DatePattern:
    groups: dict[str, tuple[int, ...]] = {"Y": (1,), "m": (2,), "d": (3,)}
    groups.update({"H": (4, 8), "M": (5, 9), "S": (6, 10), "f": (7, 11)})
    groups.update({"Z": (12,), "tzs": (13,), "tzh": (14,), "tzm": (15,), "tzsec": (16,)})
    return DatePattern(f"^{date}(?:(?s:.){_TIME}{_TZ})?$", groups)


# Subconjunto do `datetime.fromisoformat` (Python 3.11+): data estendida ou básica, separador de um caractere
# qualquer, hora estendida ou básica, fuso `Z` ou `±HH[[:]MM[[:]SS]]`.
ISO_PATTERNS = (_iso(r"(\d{4})-(\d{2})-(\d{2})"), _iso(r"(\d{4})(\d{2})(\d{2})"))


# ------------------------------------------------------------------------------------------- motor


class _Engine:
    def __init__(self, df: Any, project_root: Path) -> None:
        self.F, self.T, self.Window, self.DataFrame = _pyspark()
        if ROW_ID in df.columns:
            raise ConfigError(
                f"a coluna '{ROW_ID}' é reservada ao motor de transformação; renomeie-a na origem",
                operation="transform.spark",
            )
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
        return [c for c in df.columns if c != ROW_ID]

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

    def _decimal_text(self, s: Any, sep: str) -> tuple[Any, Any]:
        F = self.F
        thou = "," if sep == "." else "."
        t = F.regexp_replace(self._strip(s), " ", "")
        pattern = rf"^[+-]?(?:\d{{1,3}}(?:{re.escape(thou)}\d{{3}})+|\d+)(?:{re.escape(sep)}\d+)?$"
        valid = t.rlike(pattern)
        number = F.regexp_replace(F.regexp_replace(t, re.escape(thou), ""), re.escape(sep), ".")
        value = F.when(valid, number.cast("double"))
        ok = F.coalesce(valid & (F.abs(value) != F.lit(float("inf"))), F.lit(False))
        return value, ok

    def _decimal(self, c: Any, kind: str, sep: str) -> tuple[Any, Any]:
        F = self.F
        if kind == "string":
            return self._decimal_text(c, sep)
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

    def _integer(self, c: Any, kind: str, spec: TipoSpec) -> tuple[Any, Any]:
        F = self.F
        if kind == "string" and spec.separador_decimal is None:
            t = self._strip(c)
            valid = t.rlike(r"^[+-]?\d+(?:\.0+)?$")
            neg = t.startswith("-")
            mag = F.regexp_replace(F.regexp_extract(t, r"^[+-]?(\d+)", 1), "^0+", "")
            limit = F.when(neg, F.lit("9223372036854775808")).otherwise(F.lit("9223372036854775807"))
            fits = (F.length(mag) < 19) | ((F.length(mag) == 19) & (mag <= limit))
            ok = F.coalesce(valid & fits, F.lit(False))
            signed = F.concat(
                F.when(neg, F.lit("-")).otherwise(F.lit("")), F.when(mag == "", F.lit("0")).otherwise(mag)
            )
            return F.when(ok, signed.cast("long")), ok
        if kind == "int":
            return c.cast("long"), F.lit(True)
        d, ok = self._decimal(c, kind, spec.separador_decimal or ".")
        return self._whole(d, ok)

    def _boolean(self, c: Any, kind: str) -> tuple[Any, Any]:
        F = self.F
        if kind == "bool":
            return c, F.lit(True)
        if kind == "string":
            t = F.lower(self._strip(c))
            value = F.when(t.isin(*sorted(_TRUE)), F.lit(True)).when(t.isin(*sorted(_FALSE)), F.lit(False))
        elif kind in ("int", "float", "decimal"):
            d = c.cast("double")
            value = F.when(d == 1, F.lit(True)).when(d == 0, F.lit(False))
        else:
            return self._null("boolean"), F.lit(False)
        return value, value.isNotNull()

    def _int_of(self, text: Any) -> Any:
        F = self.F
        digits = F.regexp_replace(text, " ", "")
        return F.when(digits.rlike(r"^[0-9]{1,9}$"), digits.cast("int"))

    def _parse(self, t: Any, pattern: DatePattern) -> tuple[Any, Any, Any]:
        """(data, microssegundos UTC desde a época, ok) de um texto já sem espaços nas pontas."""
        F = self.F

        def grp(name: str) -> Any:
            idx = pattern.groups.get(name)
            if not idx:
                return None
            pieces = [F.regexp_extract(t, pattern.regex, i) for i in idx]
            return pieces[0] if len(pieces) == 1 else F.concat(*pieces)

        def num(name: str, default: int) -> Any:
            g = grp(name)
            return F.lit(default) if g is None else F.coalesce(self._int_of(g), F.lit(default))

        matched = F.regexp_extract(t, pattern.regex, 0) == t
        if "Y" in pattern.groups:
            year = num("Y", 1900)
        elif "y" in pattern.groups:
            yy = num("y", 0)
            year = F.when(yy <= 68, yy + 2000).otherwise(yy + 1900)
        else:
            year = F.lit(1900)
        month, day = num("m", 1), num("d", 1)
        hour, minute, second = num("H", 0), num("M", 0), num("S", 0)
        frac = grp("f")
        micro = (
            F.lit(0)
            if frac is None
            else F.when(frac.rlike(r"^[0-9]+$"), F.rpad(F.substring(frac, 1, 6), 6, "0").cast("long")).otherwise(0)
        )
        sign = grp("tzs")
        tz_h, tz_m, tz_s = num("tzh", 0), num("tzm", 0), num("tzsec", 0)
        leap = ((year % 4 == 0) & (year % 100 != 0)) | (year % 400 == 0)
        days_in_month = (
            F.when(month == 2, F.when(leap, 29).otherwise(28)).when(month.isin(4, 6, 9, 11), 30).otherwise(31)
        )
        valid = (
            matched
            & (year >= 1)
            & (month >= 1)
            & (month <= 12)
            & (day >= 1)
            & (day <= days_in_month)
            & (hour <= 23)
            & (minute <= 59)
            & (second <= 59)
            & (tz_h <= 23)
            & (tz_m <= 59)
            & (tz_s <= 59)
        )
        ok = F.coalesce(F.when(matched, valid), F.lit(False))  # componentes só são avaliados se casou
        date = F.when(ok, F.make_date(year, month, day))
        tz_total = tz_h * 3600 + tz_m * 60 + tz_s
        offset = F.lit(0) if sign is None else F.when(sign == "-", -tz_total).otherwise(tz_total)
        micros = (
            F.datediff(date, F.lit(_EPOCH)).cast("long") * F.lit(_US_PER_DAY)
            + (hour * 3600 + minute * 60 + second - offset).cast("long") * F.lit(1_000_000)
            + micro
        )
        return date, F.when(ok, micros), ok

    def _first(self, t: Any, patterns: Sequence[DatePattern]) -> tuple[Any, Any, Any]:
        """Primeiro formato que converte (casa E é data válida), como o laço do motor pandas."""
        F = self.F
        parsed = [self._parse(t, p) for p in patterns]
        ok = reduce(lambda a, b: a | b, [p[2] for p in parsed])
        date = F.coalesce(*[F.when(p[2], p[0]) for p in parsed])
        micros = F.coalesce(*[F.when(p[2], p[1]) for p in parsed])
        return date, micros, ok

    def _date(self, c: Any, kind: str, spec: TipoSpec) -> tuple[Any, Any]:
        F = self.F
        if kind == "date":
            return c, F.lit(True)
        if kind == "timestamp":
            return F.to_date(c), F.lit(True)
        if kind != "string":
            return self._null("date"), F.lit(False)
        formats = [spec.formato] if spec.formato else list(_DATE_FORMATS)
        date, _, ok = self._first(self._strip(c), [strptime_pattern(f) for f in formats])
        return date, ok

    def _timestamp(self, c: Any, kind: str, spec: TipoSpec) -> tuple[Any, Any]:
        F = self.F
        if kind == "timestamp":
            micros, ok = F.unix_micros(c), F.lit(True)
        elif kind == "date":
            micros, ok = F.datediff(c, F.lit(_EPOCH)).cast("long") * F.lit(_US_PER_DAY), F.lit(True)
        elif kind == "string":
            if spec.formato:
                patterns = [strptime_pattern(spec.formato)]
            else:
                patterns = [*ISO_PATTERNS, *(strptime_pattern(f) for f in _TS_FORMATS)]
            _, micros, ok = self._first(self._strip(c), patterns)
        else:
            return self._null("timestamp"), F.lit(False)
        good = F.coalesce(ok & (micros >= F.lit(_TS_LO)) & (micros < F.lit(_TS_HI)), F.lit(False))
        return F.when(good, F.timestamp_micros(micros)), good

    _TARGET = {"decimal": "double", "inteiro": "long", "booleano": "boolean", "data": "date", "timestamp": "timestamp"}

    def _cast(self, name: str, spec: TipoSpec) -> tuple[Any, Any]:
        """(coluna convertida, falhou?) — nulo/branco vira nulo sem falha (exceto em `texto`)."""
        F = self.F
        c, kind = self._col(name), self._kind(self._dtype(name))
        if spec.tipo == "texto":
            return self._text(c, kind, name), F.lit(False)
        if spec.tipo == "decimal":
            value, ok = self._decimal(c, kind, spec.separador_decimal or ".")
        elif spec.tipo == "inteiro":
            value, ok = self._integer(c, kind, spec)
        elif spec.tipo == "booleano":
            value, ok = self._boolean(c, kind)
        elif spec.tipo == "data":
            value, ok = self._date(c, kind, spec)
        else:
            value, ok = self._timestamp(c, kind, spec)
        target = self._TARGET[spec.tipo]
        blank = F.coalesce(self._blank(c, kind), F.lit(False))
        typed = F.when(blank, self._null(target)).otherwise(F.when(ok, value)).cast(target)
        bad = F.when(blank, F.lit(False)).otherwise(~F.coalesce(ok, F.lit(False)))
        return typed, bad

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

    def _numeric(self, c: Any, kind: str) -> tuple[Any, Any]:
        return self._decimal(c, kind, ".")

    def _rule_reason(self, regra: RegraRejeicao) -> Any:
        F = self.F
        col = regra.coluna
        c, kind = self._col(col), self._kind(self._dtype(col))
        blank = F.coalesce(self._blank(c, kind), F.lit(False))
        if regra.regra == "nao_nulo":
            return F.when(blank, F.lit(f"coluna '{col}': valor nulo (regra nao_nulo)"))
        if regra.regra == "regex":
            try:
                re.compile(regra.padrao or "")
            except re.error as exc:
                raise ConfigError(
                    f"regra regex da coluna '{col}': padrão inválido ({exc.msg})", operation="transform.rejeitar_se"
                ) from exc
            passes = self._text(c, kind, col).rlike(regra.padrao or "")
            return F.when(blank, self._null("string")).when(
                ~passes, F.lit(f"coluna '{col}': valor fora do padrão (regra regex)")
            )
        number, ok = self._numeric(c, kind)
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
        names = [present.get(c, c) for c in self._user_columns(self.cur)]
        dup = sorted({n for n in names if names.count(n) > 1})
        if dup:
            raise ConfigError(
                f"operação 'renomear' geraria colunas duplicadas: {', '.join(repr(d) for d in dup)}",
                operation="transform.renomear",
            )
        self.cur = self.cur.select(
            *[self._col(c).alias(present.get(c, c)) for c in self._user_columns(self.cur)], self.F.col(ROW_ID)
        )

    def tipar(self, specs: dict[str, TipoSpec]) -> None:
        F = self.F
        for spec in specs.values():
            if spec.separador_decimal not in (None, ".", ","):
                raise ConfigError(
                    "separador_decimal deve ser '.' ou ',' (o outro é usado como separador de milhar)",
                    operation="transform.tipar",
                )
        cols = self._check_columns(list(specs), "tipar")
        if not cols:
            return
        casts = {c: self._cast(c, specs[c]) for c in cols}
        bad_names = {c: f"_dataeng_falha_{i}" for i, c in enumerate(cols)}
        self.cur = self.cur.select(
            *[casts[c][0].alias(c) if c in casts else self._col(c) for c in self._user_columns(self.cur)],
            F.col(ROW_ID),
            *[casts[c][1].alias(bad_names[c]) for c in cols],
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
            out = fn(self.cur)
        except Exception as exc:
            raise DataEngError(
                f"o gancho '{ref}' falhou ({type(exc).__name__}); veja o traceback e corrija a função",
                operation="transform.hook",
            ) from exc
        if not isinstance(out, self.DataFrame):
            raise DataEngError(
                f"o gancho '{ref}' deve devolver um DataFrame do PySpark, mas devolveu {type(out).__name__}",
                operation="transform.hook",
            )
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
    """Aplica `ops` na ordem declarada sobre um `pyspark.sql.DataFrame`; linhas ruins vão para `rejected`."""
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
    _, T, _, _ = _pyspark()
    schema = T.StructType([T.StructField(str(c), T.StringType(), True) for c in df.columns])
    data = [tuple(None if _is_null(v) else _to_text(v) for v in row) for row in df.itertuples(index=False, name=None)]
    return spark.createDataFrame(data, schema=schema)


def spark_to_pandas(df: Any) -> pd.DataFrame:
    """DataFrame Spark → pandas com os dtypes do motor pandas; timestamps em UTC, sem depender do fuso da sessão."""
    F, T, _, _ = _pyspark()
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
