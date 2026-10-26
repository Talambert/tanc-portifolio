"""Tradução dos formatos de data do motor pandas para regex (Python puro; usada pelo motor Spark).

Cada `DatePattern` é avaliado com UMA operação de regex por célula: `regexp_replace(texto, regex, replacement)`
troca a correspondência (ancorada no início) por `MARK + g1 SEP g2 SEP ... gN + MARK`. Se o resultado começa e
termina com `MARK` (e o texto original não contém `MARK`), a correspondência cobriu o texto todo, que é a
semântica do `_strptime` (`re.match` + "unconverted data remains"); os grupos saem de `split(resultado, SEP)`.
Grupo que não participou vira texto vazio (Java: `$n` de grupo nulo não acrescenta nada; Python: `\\g<n>` idem).

As regex usam só sintaxe comum a Python e Java (grupos numerados, `(?i)`, `(?s:.)`, retrorreferência `\\n`).
"""

from __future__ import annotations

import functools
import importlib
import re
from collections.abc import Mapping
from dataclasses import dataclass

from dataeng_core.base.errors import ConfigError

# Espaço em branco do `str.isspace()` do Python, em regex Java.
WS_JAVA = r"[\p{IsWhite_Space}\x1c-\x1f]"
MARK = ""  # uso privado do Unicode: não aparece em datas e passa intacto pelo Py4J e pelo Spark Connect
SEP = ""

SUPPORTED_DIRECTIVES = "dmYyHMSf"
SUPPORTED_TEXT = "%Y %y %m %d %H %M %S %f"


@dataclass(frozen=True)
class DatePattern:
    """`groups`: componente (`Y y m d H M S f tzs tzh tzm tzsec`) → número do grupo na regex."""

    regex: str
    groups: Mapping[str, int]
    n_groups: int

    @property
    def replacement(self) -> str:
        """Substituição no formato do Java (`$n`); todos os grupos, na ordem, entre marcadores."""
        return MARK + SEP.join(f"${i}" for i in range(1, self.n_groups + 1)) + MARK


@functools.cache
def _directive(directive: str) -> str:
    """Fragmento do `_strptime` do PRÓPRIO interpretador (muda entre versões), com grupo numerado.

    Ex.: `(?P<d>3[0-1]|[1-2]\\d|0[1-9]|[1-9]| [1-9])` vira `(3[0-1]|...)`: válido em Python e em Java.
    """
    fragment = str(importlib.import_module("_strptime").TimeRE()[directive])
    return re.sub(r"^\(\?P<\w+>", "(", fragment)


def strptime_pattern(fmt: str, ws: str = WS_JAVA) -> DatePattern:
    """Traduz um formato do `strptime` (subconjunto) para regex, como o próprio `_strptime` faz.

    Diretiva fora de `SUPPORTED_DIRECTIVES` (ex.: `%b`, `%z`, `%j`, `%p`) levanta `ConfigError`.
    """
    parts: list[str] = []
    groups: dict[str, int] = {}
    i = 0
    while i < len(fmt):
        ch = fmt[i]
        if ch == "%":
            directive = fmt[i + 1] if i + 1 < len(fmt) else ""
            i += 2
            if directive == "%":
                parts.append("%")
                continue
            if not directive or directive not in SUPPORTED_DIRECTIVES:
                raise ConfigError(
                    f"formato de data '{fmt}': a diretiva '%{directive}' não é suportada pelo motor Spark; "
                    f"use só {SUPPORTED_TEXT} (o motor pandas aceita todas as do strptime)",
                    operation="transform.tipar",
                )
            if directive in groups or (directive in "Yy" and ("Y" in groups or "y" in groups)):
                raise ConfigError(
                    f"formato de data '{fmt}': diretiva de ano/campo repetida ('%{directive}')",
                    operation="transform.tipar",
                )
            groups[directive] = len(groups) + 1
            parts.append(_directive(directive))
        elif ch.isspace():
            while i < len(fmt) and fmt[i].isspace():
                i += 1
            parts.append(ws + "+")
        else:
            parts.append(ch if ch.isalnum() else re.escape(ch))
            i += 1
    return DatePattern("(?i)^(?:" + "".join(parts) + ")", groups, len(groups))


# Subconjunto do `datetime.fromisoformat` (Python 3.11+): data `AAAA-MM-DD` ou `AAAAMMDD` (o separador da data é
# o mesmo nos dois lugares: retrorreferência \2), um separador de um caractere qualquer, hora `HH[:MM[:SS[.f]]]`
# ou `HH[MM[SS[.f]]]` (separador consistente: \6), fuso `Z` ou `±HH[[:]MM[[:]SS]]`.
ISO_PATTERN = DatePattern(
    r"^(\d{4})(-?)(\d{2})\2(\d{2})"
    r"(?:(?s:.)(\d{2})(?:(:?)(\d{2})(?:\6(\d{2})(?:[.,](\d+))?)?)?"
    r"(?:Z|([+-])(\d{2})(?::?(\d{2})(?::?(\d{2}))?)?)?)?$",
    {"Y": 1, "m": 3, "d": 4, "H": 5, "M": 7, "S": 8, "f": 9, "tzs": 10, "tzh": 11, "tzm": 12, "tzsec": 13},
    13,
)
