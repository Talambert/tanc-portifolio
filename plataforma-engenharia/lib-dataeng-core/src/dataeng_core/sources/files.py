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
_CHUNK = 1024 * 1024


@dataclass
class ReadResult:
    rows: pd.DataFrame
    rejected: pd.DataFrame


def _cell(value: Any) -> str | None:
    """Contrato de nulo: célula ausente, `NaN` ou vazia vira `None`; o resto vira texto."""
    if value is None or value == "" or (not isinstance(value, str) and pd.isna(value)):
        return None
    return str(value)


def _frame(rows: list[list[Any]], columns: list[str]) -> pd.DataFrame:
    """DataFrame de texto com `None` nos nulos; `object` dá o mesmo resultado no pandas 2 e no 3."""
    if not rows:
        return pd.DataFrame({c: pd.Series([], dtype=object) for c in columns})
    return pd.DataFrame(rows, columns=columns, dtype=object)


def _finalize(
    header: list[str], rows: list[list[str | None]], rejected: list[tuple[str, str]], schema: SchemaSpec
) -> ReadResult:
    """Descarta colunas finais sem nome e sem dados (ex.: `a;b;`), valida o cabeçalho e monta o resultado."""
    n = len(header)
    while n > 0 and header[n - 1] == "" and all(r[n - 1] is None for r in rows):
        n -= 1
    header = header[:n]
    _check_header(header, schema)
    return ReadResult(_frame([r[:n] for r in rows], header), _rejected_frame(rejected))


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
    rows: list[list[str | None]] = []
    rejected: list[tuple[str, str]] = []
    try:
        for fields in reader:
            original = "".join(consumed).rstrip("\r\n")
            consumed.clear()
            if not fields:  # linha em branco
                continue
            if header is None:
                header = [f.strip() for f in fields]
            elif len(fields) != len(header):
                rejected.append((original, MOTIVO_COLUNAS))
            else:
                rows.append([_cell(f) for f in fields])
    except csv.Error as exc:
        raise SourceError(
            f"CSV malformado perto da linha {reader.line_num}: {exc}; confira `delimitador` e as aspas",
            operation="read_file",
        ) from exc
    if header is None:
        return _empty(schema)
    return _finalize(header, rows, rejected, schema)


# ------------------------------------------------------------------ json


def _json_constant(_name: str) -> None:
    """`NaN`, `Infinity` e `-Infinity` (aceitos pelo Python) são tratados como nulo."""
    return None


def _json_cell(value: Any) -> str | None:
    if isinstance(value, bool | dict | list):
        return json.dumps(value, ensure_ascii=False)
    return _cell(value)


def _records_to_result(
    records: list[dict[str, Any]], rejected: list[tuple[str, str]], schema: SchemaSpec
) -> ReadResult:
    if not records:
        empty = _empty(schema)
        return ReadResult(empty.rows, _rejected_frame(rejected))
    columns: list[str] = []
    for rec in records:
        columns.extend(k for k in rec if k not in columns)
    rows = [[_json_cell(rec.get(c)) for c in columns] for rec in records]
    return _finalize(columns, rows, rejected, schema)


def _read_jsonl(text: str, schema: SchemaSpec) -> ReadResult:
    records: list[dict[str, Any]] = []
    rejected: list[tuple[str, str]] = []
    # só `\n` separa registros: U+2028, \x0b, \x0c, \x1c etc. podem aparecer dentro de strings JSON
    for line in text.split("\n"):
        line = line.removesuffix("\r")
        if not line.strip():
            continue
        try:
            value = json.loads(line, parse_constant=_json_constant)
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
        value = json.loads(text, parse_constant=_json_constant)
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
        raw = pd.read_excel(io.BytesIO(data), sheet_name=0, header=None, dtype=object)
    except Exception as exc:  # o pandas/openpyxl levantam tipos variados para arquivo corrompido
        raise SourceError(
            f"arquivo xlsx inválido ou ilegível ({type(exc).__name__}); confira se é mesmo um .xlsx",
            operation="read_file",
        ) from exc
    cells = [[_cell(v) for v in linha] for linha in raw.to_numpy(dtype=object).tolist()]
    cells = [linha for linha in cells if any(v is not None for v in linha)]
    if not cells:
        return _empty(schema)
    header = [(v or "").strip() for v in cells[0]]
    return _finalize(header, cells[1:], [], schema)


# ---------------------------------------------------------------- público


def read_file(data: bytes, origem: ArquivoOrigem, schema: SchemaSpec) -> ReadResult:
    """Lê o arquivo como texto; linhas inutilizáveis vão para `rejected` (`_linha_original`, `_motivo`).

    Arquivo vazio ou só com cabeçalho dá zero linhas e sucesso: as colunas vêm do cabeçalho
    (ou do schema, se não houver cabeçalho).
    """
    if len(origem.delimitador) != 1:
        raise ConfigError(
            f"`delimitador` deve ter exatamente 1 caractere (recebido: {len(origem.delimitador)} caracteres)",
            operation="read_file",
        )
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


def _read_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo, path: str, limit: int) -> bytes:
    """Lê o membro em pedaços e aborta ao passar de `limit` (o `file_size` do cabeçalho pode mentir)."""
    msg = f"o membro '{path}' do zip excede o limite de {limit} bytes descomprimidos"
    if info.file_size > limit:
        raise SourceError(msg, operation="extract_zip_members")
    out = bytearray()
    with zf.open(info) as fh:
        while chunk := fh.read(min(_CHUNK, limit + 1 - len(out))):
            out += chunk
            if len(out) > limit:
                raise SourceError(msg, operation="extract_zip_members")
    return bytes(out)


def extract_zip_members(
    data: bytes, *, max_member_bytes: int = 512 * 1024**2, max_total_bytes: int = 1024**3
) -> dict[str, bytes]:
    """Conteúdo dos arquivos do zip por nome (UTF-8 corrigido); ignora pastas e caminhos fora da pasta.

    Protege contra zip bomb: limite por membro e total, em bytes descomprimidos.
    """
    members: dict[str, bytes] = {}
    total = 0
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
                if total + info.file_size > max_total_bytes:
                    raise SourceError(
                        f"o zip excede o limite total de {max_total_bytes} bytes descomprimidos (no membro '{path}')",
                        operation="extract_zip_members",
                    )
                content = _read_member(zf, info, path, max_member_bytes)
                total += len(content)
                if total > max_total_bytes:
                    raise SourceError(
                        f"o zip excede o limite total de {max_total_bytes} bytes descomprimidos (no membro '{path}')",
                        operation="extract_zip_members",
                    )
                members[path] = content
    except SourceError:
        raise
    except (zipfile.BadZipFile, NotImplementedError, RuntimeError, EOFError, OSError) as exc:
        raise SourceError(
            f"arquivo zip inválido ou corrompido ({type(exc).__name__})", operation="extract_zip_members"
        ) from exc
    return members
