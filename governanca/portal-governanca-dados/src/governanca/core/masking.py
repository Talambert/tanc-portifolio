"""Mascaramento de dados sensíveis antes de qualquer envio ao modelo."""
import re

import pandas as pd

MASK = "***"

_PATTERNS = [
    re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"),                 # e-mail
    re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b"),            # CPF formatado
    re.compile(r"\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b"),      # CNPJ formatado
    re.compile(r"\(?\b\d{2}\)?\s?9?\d{4}-?\d{4}\b"),         # telefone
]

# Colunas cujo conteúdo é mascarado por inteiro, pelo nome
SENSITIVE_NAME_HINTS = ("cpf", "email", "e_mail", "telefone", "celular", "nome")


def mask_text(text: str) -> str:
    for pattern in _PATTERNS:
        text = pattern.sub(MASK, text)
    return text


def is_sensitive_column(name: str) -> bool:
    lowered = name.lower()
    return any(hint in lowered for hint in SENSITIVE_NAME_HINTS)


def mask_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Devolve uma cópia em que colunas sensíveis são ocultadas e padrões soltos, mascarados."""
    masked = df.copy()
    for column in masked.columns:
        if is_sensitive_column(column):
            masked[column] = MASK
        elif pd.api.types.is_string_dtype(masked[column]):
            masked[column] = masked[column].map(lambda v: mask_text(v) if isinstance(v, str) else v)
    return masked
