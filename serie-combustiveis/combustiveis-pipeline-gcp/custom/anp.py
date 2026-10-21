"""Gancho de transformação da ANP: monta `endereco` a partir de rua, número e complemento.

Contrato do gancho `hook` do motor pandas: recebe e devolve um DataFrame. A biblioteca não tem operação de
concatenação, por isso este passo mora aqui. O motor Spark (Fase 3) precisará de uma versão própria do gancho.
"""

from __future__ import annotations

import re

import pandas as pd

PARTES = ("rua", "numero", "complemento")


def montar_endereco(df: pd.DataFrame) -> pd.DataFrame:
    """`endereco` = rua + numero + complemento, sem partes vazias e com espaços colapsados; remove as partes."""
    out = df.copy()
    colunas = [c for c in PARTES if c in out.columns]
    textos = [out[c].fillna("").astype(str).str.strip() for c in colunas]
    out["endereco"] = [
        re.sub(r"\s+", " ", " ".join(p for p in partes if p)).strip().upper() or None for partes in zip(*textos)
    ]
    return out.drop(columns=colunas)
