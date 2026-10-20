"""Leitura de arquivos de origem (csv, json, jsonl, xlsx e zip) com quarentena de linhas ruins.

Todos os valores são lidos como texto: a tipagem é uma etapa posterior (`transform`).
Mensagens de erro nunca incluem o conteúdo das linhas, só posições e nomes de colunas.
"""

from __future__ import annotations

import codecs
import csv
import io
import json
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

import pandas as pd

from dataeng_core.base.errors import ConfigError, SourceError
from dataeng_core.base.pipeline import ArquivoOrigem
from dataeng_core.base.schema import SchemaSpec

REJECTED_COLUMNS = ["_linha_original", "_motivo"]
MOTIVO_COLUNAS = "numero_de_colunas_inesperado"
MOTIVO_JSON_INVALIDO = "json_invalido"
MOTIVO_REGISTRO_INVALIDO = "registro_json_invalido"

_UTF8_FLAG = 0x800
_ENCRYPTED_FLAG = 0x1


@dataclass
class ReadResult:
    rows: pd.DataFrame
    rejected: pd.DataFrame


def _frame(rows: list[list[Any]], columns: list[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame({c: pd.Series([], dtype="str") for c in columns})
    return pd.DataFrame(rows, columns=columns, dtype="str")


def _rejected_frame(items: list[tuple[str, str]]) -> pd.DataFrame:
    if not items:
        return _frame([], REJECTED_COLUMNS)
    return _frame([[linha, motivo] for linha, motivo in items], REJECTED_COLUMNS)


def _decode(data: bytes, encoding: str) -> str:
    try:
        nome = codecs.lookup(encoding).name
    except LookupError as exc:
        raise ConfigError(
            f"encoding '{encoding}' desconhecido; use um nome como utf-8 ou latin-1", operation="read_file"
        ) from exc
    try:
        texto = data.decode("utf-8-sig" if nome == "utf-8" else encoding)
    except UnicodeDecodeError as exc:
        raise SourceError(
            f"o arquivo não está em encoding '{encoding}' (byte inválido na posição {exc.start}); "
            "ajuste `encoding` na origem",
            operation="read_file",
        ) from exc
    return texto.lstrip("﻿")


def _check_header(names: list[str], schema: SchemaSpec) -> None:
    for pos, nome in enumerate(names, start=1):
        if not nome:
            raise SourceError(f"cabeçalho com nome de coluna vazio (posição {pos})", operation="read_file")
    repetidas = sorted({n for n in names if names.count(n) > 1})
    if repetidas:
        raise SourceError(f"colunas repetidas no cabeçalho: {', '.join(repetidas)}", operation="read_file")
    faltando = [c.nome for c in schema.colunas if c.obrigatoria and c.nome not in names]
    if faltando:
        raise SourceError(
            f"coluna obrigatória ausente no arquivo: {', '.join(faltando)}; colunas encontradas: {', '.join(names)}",
            operation="read_file",
        )


def _empty(schema: SchemaSpec) -> ReadResult:
    return ReadResult(_frame([], [c.nome for c in schema.colunas]), _rejected_frame([]))


# ------------------------------------------------------------------- csv


def _read_csv(text: str, delimiter: str, schema: SchemaSpec) -> ReadResult:
    consumed: list[str] = []

    def lines() -> Iterator[str]:
        for line in io.StringIO(text, newline=""):
            consumed.append(line)
            yield line

    reader = csv.reader(lines(), delimiter=delimiter)
    header: list[str] | None = None
    rows: list[list[str]] = []
    rejected: list[tuple[str, str]] = []
    try:
        for fields in reader:
            original = "".join(consumed).rstrip("\r\n")
            consumed.clear()
            if not fields:  # linha em branco
                continue
            if header is None:
                header = [f.strip() for f in fields]
                _check_header(header, schema)
            elif len(fields) != len(header):
                rejected.append((original, MOTIVO_COLUNAS))
            else:
                rows.append(fields)
    except csv.Error as exc:
        raise SourceError(
            f"CSV malformado perto da linha {reader.line_num}: {exc}; confira `delimitador` e as aspas",
            operation="read_file",
        ) from exc
    if header is None:
        return _empty(schema)
    return ReadResult(_frame(rows, header), _rejected_frame(rejected))


# ------------------------------------------------------------------ json


def _json_cell(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool | dict | list):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _records_to_result(
    records: list[dict[str, Any]], rejected: list[tuple[str, str]], schema: SchemaSpec
) -> ReadResult:
    if not records:
        empty = _empty(schema)
        return ReadResult(empty.rows, _rejected_frame(rejected))
    columns: list[str] = []
    for rec in records:
        columns.extend(k for k in rec if k not in columns)
    _check_header(columns, schema)
    rows = [[_json_cell(rec.get(c)) for c in columns] for rec in records]
    return ReadResult(_frame(rows, columns), _rejected_frame(rejected))


def _read_jsonl(text: str, schema: SchemaSpec) -> ReadResult:
    records: list[dict[str, Any]] = []
    rejected: list[tuple[str, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except ValueError:
            rejected.append((line, MOTIVO_JSON_INVALIDO))
            continue
        if isinstance(value, dict):
            records.append(value)
        else:
            rejected.append((line, MOTIVO_REGISTRO_INVALIDO))
    return _records_to_result(records, rejected, schema)


def _read_json_array(text: str, schema: SchemaSpec) -> ReadResult:
    if not text.strip():
        return _empty(schema)
    try:
        value = json.loads(text)
    except ValueError as exc:
        raise SourceError(
            f"JSON inválido (erro na linha {getattr(exc, 'lineno', '?')}, coluna {getattr(exc, 'colno', '?')})",
            operation="read_file",
        ) from exc
    if not isinstance(value, list):
        raise SourceError(
            "o JSON deve ser uma lista de objetos; para um objeto por linha use formato `jsonl`", operation="read_file"
        )
    records: list[dict[str, Any]] = []
    rejected: list[tuple[str, str]] = []
    for item in value:
        if isinstance(item, dict):
            records.append(item)
        else:
            rejected.append((json.dumps(item, ensure_ascii=False), MOTIVO_REGISTRO_INVALIDO))
    return _records_to_result(records, rejected, schema)


# ------------------------------------------------------------------ xlsx


def _read_xlsx(data: bytes, schema: SchemaSpec) -> ReadResult:
    try:
        raw = pd.read_excel(io.BytesIO(data), sheet_name=0, header=None, dtype=str)
    except Exception as exc:  # o pandas/openpyxl levantam tipos variados para arquivo corrompido
        raise SourceError(
            f"arquivo xlsx inválido ou ilegível ({type(exc).__name__}); confira se é mesmo um .xlsx",
            operation="read_file",
        ) from exc
    raw = raw.dropna(how="all").reset_index(drop=True)
    if raw.empty:
        return _empty(schema)
    header = ["" if pd.isna(v) else str(v).strip() for v in raw.iloc[0].tolist()]
    _check_header(header, schema)
    body = raw.iloc[1:].reset_index(drop=True)
    body.columns = pd.Index(header)
    return ReadResult(body.astype("str"), _rejected_frame([]))


# ---------------------------------------------------------------- público


def read_file(data: bytes, origem: ArquivoOrigem, schema: SchemaSpec) -> ReadResult:
    """Lê o arquivo como texto; linhas inutilizáveis vão para `rejected` (`_linha_original`, `_motivo`).

    Arquivo vazio ou só com cabeçalho dá zero linhas e sucesso: as colunas vêm do cabeçalho
    (ou do schema, se não houver cabeçalho).
    """
    if origem.formato == "xlsx":
        return _read_xlsx(data, schema)
    text = _decode(data, origem.encoding)
    if origem.formato == "csv":
        return _read_csv(text, origem.delimitador, schema)
    if origem.formato == "jsonl":
        return _read_jsonl(text, schema)
    return _read_json_array(text, schema)


def _safe_member_path(name: str) -> str | None:
    """Caminho relativo normalizado, ou `None` se escapar da pasta (zip slip) ou for pasta."""
    normalized = name.replace("\\", "/")
    if normalized.startswith("/") or (len(normalized) > 1 and normalized[1] == ":"):
        return None
    parts = [p for p in PurePosixPath(normalized).parts if p != "."]
    if not parts or ".." in parts:
        return None
    return "/".join(parts)


def _fix_name(info: zipfile.ZipInfo) -> str:
    """O zipfile lê em cp437 quando o flag UTF-8 falta; muitas ferramentas gravam UTF-8 sem o flag."""
    if info.flag_bits & _UTF8_FLAG:
        return info.filename
    try:
        return info.filename.encode("cp437").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return info.filename


def extract_zip_members(data: bytes) -> dict[str, bytes]:
    """Conteúdo dos arquivos do zip por nome (UTF-8 corrigido); ignora pastas e caminhos fora da pasta."""
    members: dict[str, bytes] = {}
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                path = _safe_member_path(_fix_name(info))
                if path is None:
                    continue
                if info.flag_bits & _ENCRYPTED_FLAG:
                    raise SourceError(f"o membro '{path}' do zip é criptografado", operation="extract_zip_members")
                if path in members:
                    raise SourceError(f"o zip tem dois membros com o nome '{path}'", operation="extract_zip_members")
                members[path] = zf.read(info)
    except SourceError:
        raise
    except (zipfile.BadZipFile, NotImplementedError, RuntimeError, EOFError, OSError) as exc:
        raise SourceError(
            f"arquivo zip inválido ou corrompido ({type(exc).__name__})", operation="extract_zip_members"
        ) from exc
    return members
