"""PySpark FALSO que só monta o plano: cada `Column` é um nó de árvore e cada expressão passada ao DataFrame é
registrada. Serve para medir, sem Java, o tamanho das expressões que o motor Spark gera (como o Catalyst as
veria: uma subárvore reaproveitada conta de novo a cada uso) e para testar validações que não tocam nos dados.

Instale com `install(monkeypatch)`; nenhuma ação devolve dados reais (`head` devolve uma linha fictícia, `count` 0).
"""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest

REGEX_OPS = ("regexp_extract", "regexp_replace", "rlike")


class Expr:
    __hash__ = object.__hash__

    def __init__(self, op: str, *children: Any, name: str | None = None, dtype: str | None = None) -> None:
        self.op = op
        self.children = [c if isinstance(c, Expr) else Expr("lit") for c in children]
        self.name = name
        self.dtype = dtype

    def __bool__(self) -> bool:
        raise TypeError("Column não pode virar bool (use & | ~)")

    def _b(self, op: str, other: Any) -> Expr:
        return Expr(op, self, other)

    def __and__(self, o: Any) -> Expr:
        return self._b("and", o)

    def __rand__(self, o: Any) -> Expr:
        return self._b("and", o)

    def __or__(self, o: Any) -> Expr:
        return self._b("or", o)

    def __ror__(self, o: Any) -> Expr:
        return self._b("or", o)

    def __invert__(self) -> Expr:
        return Expr("not", self)

    def __neg__(self) -> Expr:
        return Expr("neg", self)

    def __eq__(self, o: Any) -> Expr:  # type: ignore[override]
        return self._b("eq", o)

    def __ne__(self, o: Any) -> Expr:  # type: ignore[override]
        return self._b("ne", o)

    def __lt__(self, o: Any) -> Expr:
        return self._b("lt", o)

    def __le__(self, o: Any) -> Expr:
        return self._b("le", o)

    def __gt__(self, o: Any) -> Expr:
        return self._b("gt", o)

    def __ge__(self, o: Any) -> Expr:
        return self._b("ge", o)

    def __add__(self, o: Any) -> Expr:
        return self._b("add", o)

    __radd__ = __add__

    def __sub__(self, o: Any) -> Expr:
        return self._b("sub", o)

    def __rsub__(self, o: Any) -> Expr:
        return self._b("sub", o)

    def __mul__(self, o: Any) -> Expr:
        return self._b("mul", o)

    __rmul__ = __mul__

    def __mod__(self, o: Any) -> Expr:
        return self._b("mod", o)

    def cast(self, t: str) -> Expr:
        return Expr("cast", self, dtype=t)

    def alias(self, n: str) -> Expr:
        return Expr("alias", self, name=n, dtype=self.dtype)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        # isNull, isNotNull, rlike, startswith, endswith, contains, isin, when, otherwise, over, getItem...
        return lambda *args: Expr(name, self, *args)


def size(e: Expr, memo: dict[int, int] | None = None) -> int:
    """Nós da árvore, contando de novo cada reuso (o Catalyst copia a subárvore)."""
    memo = {} if memo is None else memo
    if id(e) not in memo:
        memo[id(e)] = 1 + sum(size(c, memo) for c in e.children)
    return memo[id(e)]


def count_ops(e: Expr, ops: tuple[str, ...], memo: dict[int, int] | None = None) -> int:
    memo = {} if memo is None else memo
    if id(e) not in memo:
        memo[id(e)] = int(e.op in ops) + sum(count_ops(c, ops, memo) for c in e.children)
    return memo[id(e)]


# ------------------------------------------------------------------------------------- tipos


class DataType:
    def simpleString(self) -> str:  # noqa: N802
        return type(self).__name__.removesuffix("Type").lower()

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other)

    __hash__ = object.__hash__


def _types_module() -> types.ModuleType:
    mod = types.ModuleType("pyspark.sql.types")
    for name in (
        "StringType",
        "NullType",
        "BooleanType",
        "ByteType",
        "ShortType",
        "IntegerType",
        "LongType",
        "FloatType",
        "DoubleType",
        "DecimalType",
        "DateType",
        "TimestampType",
    ):
        setattr(mod, name, type(name, (DataType,), {}))

    class StructField:
        def __init__(self, name: str, dataType: DataType, nullable: bool = True) -> None:  # noqa: N803
            self.name, self.dataType, self.nullable = name, dataType, nullable

    class StructType:
        def __init__(self, fields: list[StructField]) -> None:
            self.fields = list(fields)

        def __getitem__(self, name: str) -> StructField:
            return next(f for f in self.fields if f.name == name)

    mod.StructField = StructField  # type: ignore[attr-defined]
    mod.StructType = StructType  # type: ignore[attr-defined]
    return mod


_CAST = {
    "string": "StringType",
    "double": "DoubleType",
    "long": "LongType",
    "int": "IntegerType",
    "boolean": "BooleanType",
    "date": "DateType",
    "timestamp": "TimestampType",
}


# ------------------------------------------------------------------------------- DataFrame


class FakeDataFrame:
    def __init__(self, schema: dict[str, Any], log: list[tuple[str, Expr]], T: types.ModuleType) -> None:
        self._schema = dict(schema)
        self.log = log
        self.T = T
        self.actions: list[str] = []

    # -- inspeção
    @property
    def columns(self) -> list[str]:
        return list(self._schema)

    @property
    def schema(self) -> Any:
        return self.T.StructType([self.T.StructField(n, t) for n, t in self._schema.items()])

    def toPandas(self) -> Any:  # noqa: N802
        raise NotImplementedError

    def _type(self, e: Expr) -> Any:
        if e.op == "col" and e.name in self._schema:
            return self._schema[e.name]
        if e.dtype in _CAST:
            return getattr(self.T, _CAST[e.dtype])()
        if e.op == "alias":
            return self._type(e.children[0])
        return self.T.StringType()

    def _new(self, schema: dict[str, Any]) -> FakeDataFrame:
        out = FakeDataFrame(schema, self.log, self.T)
        out.actions = self.actions
        return out

    # -- transformações
    def withColumn(self, name: str, e: Expr) -> FakeDataFrame:  # noqa: N802
        clash = [k for k in self._schema if k != name and k.casefold() == name.casefold()]
        if clash:  # o Spark não diferencia maiúsculas: seria referência ambígua
            raise AssertionError(f"coluna {name!r} colide com {clash[0]!r} sem diferenciar maiúsculas")
        self.log.append((name, e))
        return self._new({**self._schema, name: self._type(e)})

    def withColumns(self, cols: dict[str, Expr]) -> FakeDataFrame:  # noqa: N802
        out = self
        for name, e in cols.items():
            out = out.withColumn(name, e)
        return out

    def select(self, *exprs: Any) -> FakeDataFrame:
        schema: dict[str, Any] = {}
        for e in exprs:
            e = Expr("col", name=e) if isinstance(e, str) else e
            name = e.name or e.op
            self.log.append((name, e))
            schema[name] = self._type(e)
        return self._new(schema)

    def filter(self, e: Expr) -> FakeDataFrame:
        self.log.append(("<filtro>", e))
        return self._new(self._schema)

    def drop(self, *names: str) -> FakeDataFrame:
        return self._new({k: v for k, v in self._schema.items() if k not in names})

    def join(self, other: FakeDataFrame, on: str, how: str = "inner") -> FakeDataFrame:
        return self._new({**self._schema, **other._schema})

    def unionByName(self, other: FakeDataFrame, allowMissingColumns: bool = False) -> FakeDataFrame:  # noqa: N803
        return self._new({**self._schema, **other._schema})

    def orderBy(self, *a: Any) -> FakeDataFrame:  # noqa: N802
        return self

    def limit(self, n: int) -> FakeDataFrame:
        return self

    def agg(self, *exprs: Any) -> FakeDataFrame:
        return self

    def first(self) -> tuple[int, int, int]:
        self.actions.append("first")
        return (1, 1, 1)

    # -- ações (registradas, sem dados)
    def localCheckpoint(self, eager: bool = True) -> FakeDataFrame:  # noqa: N802
        self.actions.append("localCheckpoint")
        return self

    def head(self, n: int = 1) -> list[Any]:
        self.actions.append("head")
        return [object()]

    def count(self) -> int:
        self.actions.append("count")
        return 0


class FakeSpark:
    """Tudo o que o teste precisa: o módulo falso instalado e um `createDataFrame` com o log de expressões."""

    def __init__(self) -> None:
        self.log: list[tuple[str, Expr]] = []
        self.T = _types_module()

    def frame(self, columns: dict[str, str]) -> FakeDataFrame:
        return FakeDataFrame({n: getattr(self.T, t)() for n, t in columns.items()}, self.log, self.T)

    def expressions_since(self, start: int) -> list[tuple[str, Expr]]:
        return self.log[start:]


def install(monkeypatch: pytest.MonkeyPatch) -> FakeSpark:
    fake = FakeSpark()

    functions = types.ModuleType("pyspark.sql.functions")

    def col(name: str) -> Expr:
        plain = name[1:-1].replace("``", "`") if name.startswith("`") and name.endswith("`") else name
        return Expr("col", name=plain)

    def lit(value: Any) -> Expr:
        return Expr("lit")

    def generic(op: str) -> Any:
        if op.startswith("__"):
            raise AttributeError(op)
        return lambda *args, **kw: Expr(op, *args)

    functions.col = col  # type: ignore[attr-defined]
    functions.lit = lit  # type: ignore[attr-defined]
    functions.__getattr__ = generic  # type: ignore[attr-defined]

    class _Spec:
        def partitionBy(self, *cols: Any) -> _Spec:  # noqa: N802
            return self

        def orderBy(self, *cols: Any) -> _Spec:  # noqa: N802
            return self

    window = type("Window", (), {"partitionBy": staticmethod(lambda *c: _Spec())})

    sql = types.ModuleType("pyspark.sql")
    sql.functions = functions  # type: ignore[attr-defined]
    sql.types = fake.T  # type: ignore[attr-defined]
    sql.Window = window  # type: ignore[attr-defined]
    sql.DataFrame = FakeDataFrame  # type: ignore[attr-defined]
    pkg = types.ModuleType("pyspark")
    pkg.sql = sql  # type: ignore[attr-defined]
    for name, mod in (
        ("pyspark", pkg),
        ("pyspark.sql", sql),
        ("pyspark.sql.functions", functions),
        ("pyspark.sql.types", fake.T),
    ):
        monkeypatch.setitem(sys.modules, name, mod)
    return fake


def walk(e: Expr) -> Iterator[Expr]:
    yield e
    for c in e.children:
        yield from walk(c)
