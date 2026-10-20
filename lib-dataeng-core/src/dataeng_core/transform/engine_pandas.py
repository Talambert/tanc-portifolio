"""Motor de transformação declarativa em pandas, com quarentena das linhas ruins.

Contrato de entrada: célula nula é `None`/`NaN`/`pd.NA`; valores chegam como texto (arquivos) ou crus (API).
Contrato de saída: `rows` com dtypes anuláveis (`Int64`, `Float64`, `boolean`, `string`, `date` como
`datetime.date` em coluna `object`, `timestamp` em UTC) e `rejected` com as colunas ORIGINAIS da linha
mais `_motivo` (o motivo nunca contém o valor da célula).
"""

from __future__ import annotations

import datetime as dt
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import RegraRejeicao, TipoSpec, TransformOp
from dataeng_core.transform.hooks import resolve_hook

MOTIVO = "_motivo"
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


@dataclass
class TransformResult:
    rows: pd.DataFrame
    rejected: pd.DataFrame
    duplicates_removed: int = field(default=0)  # removidas por `deduplicar` (não são rejeições)


class _Bad(ValueError):
    """Valor que não converte para o tipo pedido (sem eco do valor)."""


# ------------------------------------------------------------------ utilitários


def _is_null(v: Any) -> bool:
    if v is None or v is pd.NA or v is pd.NaT:
        return True
    if isinstance(v, list | tuple | dict | set):
        return False
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def _is_blank(v: Any) -> bool:
    return _is_null(v) or (isinstance(v, str) and not v.strip())


def _to_text(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, dt.datetime | dt.date):
        return v.isoformat()
    return str(v)


def _check_columns(frame: pd.DataFrame, nomes: Sequence[str], op: str) -> list[str]:
    """Colunas pedidas que existem. Ausente é erro, exceto num quadro sem linhas (arquivo vazio)."""
    faltam = [n for n in nomes if n not in frame.columns]
    if faltam and len(frame) > 0:
        raise ConfigError(
            f"operação '{op}': coluna {', '.join(repr(n) for n in faltam)} não existe; "
            f"colunas disponíveis: {', '.join(map(str, frame.columns)) or 'nenhuma'}",
            operation=f"transform.{op}",
        )
    return [n for n in nomes if n in frame.columns]


# ------------------------------------------------------------------ conversões


def _decimal_text(s: str, sep: str) -> float:
    thou = "," if sep == "." else "."
    s = s.strip().replace(" ", "")
    pattern = rf"[+-]?(?:\d{{1,3}}(?:{re.escape(thou)}\d{{3}})+|\d+)(?:{re.escape(sep)}\d+)?"
    if not re.fullmatch(pattern, s):
        raise _Bad
    out = float(s.replace(thou, "").replace(sep, "."))
    if not math.isfinite(out):
        raise _Bad
    return out


def _to_decimal(v: Any, spec: TipoSpec) -> float:
    if isinstance(v, bool):
        raise _Bad
    if isinstance(v, str):
        return _decimal_text(v, spec.separador_decimal or ".")
    try:
        out = float(v)
    except (TypeError, ValueError, OverflowError) as exc:
        raise _Bad from exc
    if not math.isfinite(out):
        raise _Bad
    return out


def _to_int(v: Any, spec: TipoSpec) -> int:
    if isinstance(v, str) and spec.separador_decimal is None:
        s = v.strip()
        if not re.fullmatch(r"[+-]?\d+(?:\.0+)?", s):
            raise _Bad
        out = int(s.split(".")[0])
    else:
        number = _to_decimal(v, spec)
        if not number.is_integer():
            raise _Bad
        out = int(v) if isinstance(v, int) else int(number)
    if not _INT64_MIN <= out <= _INT64_MAX:
        raise _Bad
    return out


_TRUE = {"true", "t", "1", "sim", "s", "yes", "y", "verdadeiro", "v"}
_FALSE = {"false", "f", "0", "nao", "não", "n", "no", "falso"}


def _to_bool(v: Any, spec: TipoSpec) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int | float) and v in (0, 1):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in _TRUE:
            return True
        if s in _FALSE:
            return False
    raise _Bad


_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y")
_TS_FORMATS = (
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
    "%d-%m-%Y",
)


def _strptime(s: str, formats: Sequence[str]) -> dt.datetime:
    for fmt in formats:
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise _Bad


def _to_date(v: Any, spec: TipoSpec) -> dt.date:
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    if isinstance(v, str):
        return _strptime(v.strip(), [spec.formato] if spec.formato else _DATE_FORMATS).date()
    raise _Bad


def _to_timestamp(v: Any, spec: TipoSpec) -> dt.datetime:
    if isinstance(v, dt.datetime):
        parsed = v
    elif isinstance(v, dt.date):
        parsed = dt.datetime(v.year, v.month, v.day)
    elif isinstance(v, str):
        s = v.strip()
        if spec.formato:
            parsed = _strptime(s, [spec.formato])
        else:
            try:
                parsed = dt.datetime.fromisoformat(s)
            except ValueError:
                parsed = _strptime(s, _TS_FORMATS)
    else:
        raise _Bad
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _to_text_value(v: Any, spec: TipoSpec) -> str:
    return _to_text(v)


_CONVERTERS: dict[str, Callable[[Any, TipoSpec], Any]] = {
    "decimal": _to_decimal,
    "inteiro": _to_int,
    "booleano": _to_bool,
    "data": _to_date,
    "timestamp": _to_timestamp,
    "texto": _to_text_value,
}
_PANDAS_DTYPE = {"decimal": "Float64", "inteiro": "Int64", "booleano": "boolean", "texto": "string"}


def _cast(series: pd.Series, spec: TipoSpec) -> tuple[pd.Series, list[bool]]:
    """Converte a coluna; devolve a série tipada (falhas viram nulo) e a máscara das falhas."""
    convert = _CONVERTERS[spec.tipo]
    blanks_are_null = spec.tipo != "texto"
    values: list[Any] = []
    bad: list[bool] = []
    for v in series.tolist():
        if _is_null(v) or (blanks_are_null and _is_blank(v)):
            values.append(None)
            bad.append(False)
            continue
        try:
            values.append(convert(v, spec))
            bad.append(False)
        except _Bad:
            values.append(None)
            bad.append(True)
    typed: pd.Series
    if spec.tipo == "timestamp":
        typed = pd.Series(pd.to_datetime(pd.Series(values, dtype=object), utc=True), dtype="datetime64[us, UTC]")
    elif spec.tipo == "data":
        typed = pd.Series(values, dtype=object)
    else:
        typed = pd.Series(values, dtype=_PANDAS_DTYPE[spec.tipo])
    typed.index = series.index
    return typed, bad


# ----------------------------------------------------------------- normalização


_NORMALIZERS: dict[str, Callable[[str], str]] = {
    "apenas_digitos": lambda s: re.sub(r"[^0-9]", "", s),
    "trim": lambda s: s.strip(),
    "maiusculas": lambda s: s.upper(),
    "minusculas": lambda s: s.lower(),
    "colapsar_espacos": lambda s: re.sub(r"\s+", " ", s),
}


def _normalize(series: pd.Series, funcs: Sequence[str]) -> pd.Series:
    out: list[str | None] = []
    for v in series.tolist():
        if _is_null(v):
            out.append(None)
            continue
        text = _to_text(v)
        for name in funcs:
            text = _NORMALIZERS[name](text)
        out.append(text)
    result = pd.Series(out, dtype="string")
    result.index = series.index
    return result


# ---------------------------------------------------------------- rejeição


def _numeric(v: Any) -> float | None:
    try:
        return _to_decimal(v, TipoSpec(tipo="decimal"))
    except _Bad:
        return None


def _rule_reasons(series: pd.Series, regra: RegraRejeicao) -> list[str | None]:
    """Motivo por linha (None = passa). Sem eco do valor."""
    col = regra.coluna
    pattern: re.Pattern[str] | None = None
    if regra.regra == "regex":
        try:
            pattern = re.compile(regra.padrao or "")
        except re.error as exc:
            raise ConfigError(
                f"regra regex da coluna '{col}': padrão inválido ({exc.msg})", operation="transform.rejeitar_se"
            ) from exc
    out: list[str | None] = []
    for v in series.tolist():
        if regra.regra == "nao_nulo":
            out.append(f"coluna '{col}': valor nulo (regra nao_nulo)" if _is_blank(v) else None)
        elif _is_blank(v):
            out.append(None)  # regex e faixa ignoram nulos; use nao_nulo para exigir valor
        elif pattern is not None:
            ok = pattern.search(_to_text(v)) is not None
            out.append(None if ok else f"coluna '{col}': valor fora do padrão (regra regex)")
        else:
            number = _numeric(v)
            if number is None:
                out.append(f"coluna '{col}': valor não numérico (regra faixa)")
            elif (regra.minimo is not None and number < regra.minimo) or (
                regra.maximo is not None and number > regra.maximo
            ):
                out.append(f"coluna '{col}': valor fora da faixa (regra faixa)")
            else:
                out.append(None)
    return out


# ------------------------------------------------------------------- motor


class _State:
    """Quadro corrente (índice = id da linha original) e quarentena acumulada."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.cur = df.reset_index(drop=True).copy()
        self.orig = self.cur.copy()
        self.rejected: list[pd.DataFrame] = []
        self.duplicates = 0

    def reject(self, mask: Sequence[bool], reasons: Sequence[str | None]) -> None:
        if not any(mask):
            return
        ids = [i for i, m in zip(self.cur.index, mask, strict=True) if m]
        motivos = [r for r, m in zip(reasons, mask, strict=True) if m]
        frame = self.orig.loc[ids].copy()
        frame[MOTIVO] = pd.Series(motivos, dtype="string", index=frame.index)
        self.rejected.append(frame)
        self.cur = self.cur.loc[[not m for m in mask]]

    def result(self) -> TransformResult:
        if self.rejected:
            rejected = pd.concat(self.rejected, ignore_index=True)
        else:
            rejected = self.orig.iloc[0:0].copy()
            rejected[MOTIVO] = pd.Series([], dtype="string")
        return TransformResult(
            rows=self.cur.reset_index(drop=True), rejected=rejected, duplicates_removed=self.duplicates
        )


def _op_renomear(st: _State, mapping: dict[str, str]) -> None:
    present = {old: new for old, new in mapping.items() if old in _check_columns(st.cur, [old], "renomear")}
    renamed = st.cur.rename(columns=present)
    dup = sorted({str(c) for c in renamed.columns[renamed.columns.duplicated()]})
    if dup:
        raise ConfigError(
            f"operação 'renomear' geraria colunas duplicadas: {', '.join(repr(d) for d in dup)}",
            operation="transform.renomear",
        )
    st.cur = renamed


def _op_tipar(st: _State, specs: dict[str, TipoSpec]) -> None:
    for spec in specs.values():
        if spec.separador_decimal not in (None, ".", ","):
            raise ConfigError(
                "separador_decimal deve ser '.' ou ',' (o outro é usado como separador de milhar)",
                operation="transform.tipar",
            )
    reasons: dict[Any, list[str]] = {}
    for col in _check_columns(st.cur, list(specs), "tipar"):
        spec = specs[col]
        typed, bad = _cast(st.cur[col], spec)
        st.cur[col] = typed
        for idx, is_bad in zip(st.cur.index, bad, strict=True):
            if is_bad:
                reasons.setdefault(idx, []).append(f"coluna '{col}': valor não convertível para {spec.tipo}")
    mask = [i in reasons for i in st.cur.index]
    st.reject(mask, ["; ".join(reasons.get(i, [])) for i in st.cur.index])


def _op_normalizar(st: _State, spec: Mapping[str, Sequence[str]]) -> None:
    for col in _check_columns(st.cur, list(spec), "normalizar"):
        st.cur[col] = _normalize(st.cur[col], spec[col])


def _op_deduplicar(st: _State, keys: list[str]) -> None:
    if not keys:
        raise ConfigError("operação 'deduplicar' exige ao menos uma coluna", operation="transform.deduplicar")
    cols = _check_columns(st.cur, keys, "deduplicar")
    if not cols or st.cur.empty:
        return
    frame = st.cur[cols].astype(object).map(lambda v: repr(v) if isinstance(v, list | dict | set) else v)
    dup = frame.duplicated(keep="first")
    st.duplicates += int(dup.sum())
    st.cur = st.cur.loc[~dup.to_numpy()]


def _op_rejeitar(st: _State, regras: list[RegraRejeicao]) -> None:
    for regra in regras:
        if not _check_columns(st.cur, [regra.coluna], "rejeitar_se"):
            continue
        reasons = _rule_reasons(st.cur[regra.coluna], regra)
        st.reject([r is not None for r in reasons], reasons)


def _op_hook(st: _State, ref: str, project_root: Path) -> None:
    fn = resolve_hook(ref, project_root=project_root)
    try:
        out = fn(st.cur.copy())
    except Exception as exc:
        raise DataEngError(
            f"o gancho '{ref}' falhou ({type(exc).__name__}); veja o traceback e corrija a função",
            operation="transform.hook",
        ) from exc
    if not isinstance(out, pd.DataFrame):
        raise DataEngError(
            f"o gancho '{ref}' deve devolver um DataFrame, mas devolveu {type(out).__name__}",
            operation="transform.hook",
        )
    before = st.cur.index
    kept = out.index.is_unique and (
        out.index.equals(before) or (not isinstance(out.index, pd.RangeIndex) and bool(out.index.isin(before).all()))
    )
    if kept:
        st.cur = out
    else:  # o gancho renumerou as linhas: a "linha original" passa a ser a saída do gancho
        st.cur = out.reset_index(drop=True)
        st.orig = st.cur.copy()


def apply_transform(df: pd.DataFrame, ops: Sequence[TransformOp], *, project_root: Path) -> TransformResult:
    """Aplica `ops` na ordem declarada; linhas que falham vão para `rejected`, sem derrubar o lote."""
    st = _State(df)
    for op in ops:
        if op.renomear is not None:
            _op_renomear(st, op.renomear)
        elif op.tipar is not None:
            _op_tipar(st, op.tipar)
        elif op.normalizar is not None:
            _op_normalizar(st, op.normalizar)
        elif op.deduplicar is not None:
            _op_deduplicar(st, op.deduplicar)
        elif op.rejeitar_se is not None:
            _op_rejeitar(st, op.rejeitar_se)
        elif op.hook is not None:
            _op_hook(st, op.hook, Path(project_root))
    return st.result()
