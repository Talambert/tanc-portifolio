from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from dataeng_core.base.errors import ConfigError, SourceError
from dataeng_core.base.pipeline import ArquivoOrigem
from dataeng_core.base.schema import Coluna, SchemaSpec
from dataeng_core.sources.files import ReadResult, extract_zip_members, read_file

SCHEMA = SchemaSpec(
    colunas=[
        Coluna(nome="id", tipo="inteiro", obrigatoria=True),
        Coluna(nome="nome", tipo="texto"),
        Coluna(nome="valor", tipo="decimal"),
    ]
)


def _origem(formato: str = "csv", encoding: str = "utf-8", delimitador: str = ";") -> ArquivoOrigem:
    return ArquivoOrigem(
        tipo="arquivo",
        localizacao="x",
        formato=formato,  # type: ignore[arg-type]
        encoding=encoding,
        delimitador=delimitador,
        schema=Path("schemas/x.yaml"),
    )


def _rows(res: ReadResult) -> list[list[str]]:
    return res.rows.values.tolist()  # type: ignore[no-any-return]


def test_csv_with_bom_semicolon_and_crlf() -> None:
    data = "\ufeffid;nome;valor\r\n1;Ana;10,5\r\n2;Bia;3\r\n".encode()
    res = read_file(data, _origem(), SCHEMA)
    assert list(res.rows.columns) == ["id", "nome", "valor"]
    assert _rows(res) == [["1", "Ana", "10,5"], ["2", "Bia", "3"]]
    assert res.rejected.empty
    assert list(res.rejected.columns) == ["_linha_original", "_motivo"]


def test_latin1_encoding_when_declared() -> None:
    data = "id;nome;valor\n1;José Conceição;1\n".encode("latin-1")
    res = read_file(data, _origem(encoding="latin-1"), SCHEMA)
    assert _rows(res) == [["1", "José Conceição", "1"]]


def test_invalid_encoding_is_source_error_without_leaking_data() -> None:
    data = "id;nome;valor\n1;SEGREDO-é;1\n".encode("latin-1")
    with pytest.raises(SourceError) as exc:
        read_file(data, _origem(encoding="utf-8"), SCHEMA)
    assert "encoding" in str(exc.value)
    assert "SEGREDO" not in str(exc.value)


def test_unknown_encoding_is_config_error() -> None:
    with pytest.raises(ConfigError, match="encoding"):
        read_file(b"id;nome\n", _origem(encoding="nao-existe"), SCHEMA)


def test_row_with_extra_semicolon_goes_to_rejected_with_reason() -> None:
    data = b"id;nome;valor\n1;Ana;10\n2;Bia;3;extra\n3;Caio;7\n4;Dani\n"
    res = read_file(data, _origem(), SCHEMA)
    assert _rows(res) == [["1", "Ana", "10"], ["3", "Caio", "7"]]
    assert res.rejected["_linha_original"].tolist() == ["2;Bia;3;extra", "4;Dani"]
    assert res.rejected["_motivo"].tolist() == ["numero_de_colunas_inesperado"] * 2


def test_quoted_delimiter_is_not_a_split() -> None:
    data = b'id;nome;valor\n1;"Silva; Ana";10\n2;"linha\nquebrada";5\n'
    res = read_file(data, _origem(), SCHEMA)
    assert _rows(res) == [["1", "Silva; Ana", "10"], ["2", "linha\nquebrada", "5"]]
    assert res.rejected.empty


def test_other_delimiter_is_used() -> None:
    res = read_file(b"id,nome,valor\n1,Ana,2\n", _origem(delimitador=","), SCHEMA)
    assert _rows(res) == [["1", "Ana", "2"]]


def test_header_only_file_returns_zero_rows_and_columns() -> None:
    res = read_file("\ufeffid;nome;valor\r\n".encode(), _origem(), SCHEMA)
    assert len(res.rows) == 0
    assert list(res.rows.columns) == ["id", "nome", "valor"]
    assert len(res.rejected) == 0
    assert list(res.rejected.columns) == ["_linha_original", "_motivo"]


def test_header_only_without_newline() -> None:
    res = read_file(b"id;nome;valor", _origem(), SCHEMA)
    assert len(res.rows) == 0
    assert list(res.rows.columns) == ["id", "nome", "valor"]


@pytest.mark.parametrize("data", [b"", b"\xef\xbb\xbf", b"\r\n\r\n"])
def test_empty_file_returns_zero_rows(data: bytes) -> None:
    res = read_file(data, _origem(), SCHEMA)
    assert len(res.rows) == 0
    assert list(res.rows.columns) == ["id", "nome", "valor"]
    assert len(res.rejected) == 0
    assert list(res.rejected.columns) == ["_linha_original", "_motivo"]


def test_blank_lines_between_rows_are_skipped() -> None:
    res = read_file(b"id;nome;valor\n1;Ana;1\n\n2;Bia;2\n", _origem(), SCHEMA)
    assert _rows(res) == [["1", "Ana", "1"], ["2", "Bia", "2"]]
    assert res.rejected.empty


def test_duplicate_header_columns_is_source_error() -> None:
    with pytest.raises(SourceError, match="repetidas.*nome"):
        read_file(b"id;nome;nome\n1;a;b\n", _origem(), SCHEMA)


def test_missing_required_column_is_source_error() -> None:
    with pytest.raises(SourceError, match="obrigatória.*id"):
        read_file(b"nome;valor\nAna;1\n", _origem(), SCHEMA)


def test_empty_column_name_is_source_error() -> None:
    with pytest.raises(SourceError, match="vazio"):
        read_file(b"id;;valor\n1;a;b\n", _origem(), SCHEMA)


def test_csv_parse_error_does_not_leak_data() -> None:
    with pytest.raises(SourceError) as exc:
        read_file(b"id;nome\n1;SEGREDO" + b"A" * 200_000 + b"\n", _origem(), SCHEMA)
    assert "SEGREDO" not in str(exc.value)


def test_json_lines_and_json_array() -> None:
    jsonl = b'{"id": 1, "nome": "Ana"}\n\n{"id": 2, "valor": 2.5, "extra": {"a": 1}}\n'
    res = read_file(jsonl, _origem("jsonl"), SCHEMA)
    assert list(res.rows.columns) == ["id", "nome", "valor", "extra"]
    assert res.rows["id"].tolist() == ["1", "2"]
    assert res.rows["nome"].tolist()[0] == "Ana"
    assert res.rows["nome"].tolist()[1] is None
    assert res.rows["valor"].tolist()[1] == "2.5"
    assert res.rows["extra"].tolist()[1] == '{"a": 1}'
    arr = json.dumps([{"id": 1, "nome": "Ana"}, {"id": 2, "nome": "Bia"}]).encode()
    res2 = read_file(b"\xef\xbb\xbf" + arr, _origem("json"), SCHEMA)
    assert res2.rows["nome"].tolist() == ["Ana", "Bia"]
    assert res2.rejected.empty


def test_jsonl_bad_lines_go_to_rejected() -> None:
    data = b'{"id": 1}\nnao e json SEGREDO\n[1, 2]\n{"id": 3}\n'
    res = read_file(data, _origem("jsonl"), SCHEMA)
    assert res.rows["id"].tolist() == ["1", "3"]
    assert res.rejected["_linha_original"].tolist() == ["nao e json SEGREDO", "[1, 2]"]
    assert res.rejected["_motivo"].tolist() == ["json_invalido", "registro_json_invalido"]


@pytest.mark.parametrize("formato,data", [("json", b"[]"), ("jsonl", b""), ("json", b"")])
def test_empty_json_returns_zero_rows(formato: str, data: bytes) -> None:
    res = read_file(data, _origem(formato), SCHEMA)
    assert len(res.rows) == 0
    assert list(res.rows.columns) == ["id", "nome", "valor"]
    assert list(res.rejected.columns) == ["_linha_original", "_motivo"]


def test_invalid_json_document_is_source_error() -> None:
    with pytest.raises(SourceError, match="JSON"):
        read_file(b'{"id": 1', _origem("json"), SCHEMA)
    with pytest.raises(SourceError, match="lista"):
        read_file(b'{"id": 1}', _origem("json"), SCHEMA)


def _xlsx(sheets: dict[str, list[list[object]]]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for nome, linhas in sheets.items():
            pd.DataFrame(linhas).to_excel(writer, sheet_name=nome, header=False, index=False)
    return buf.getvalue()


def test_xlsx_first_sheet() -> None:
    data = _xlsx(
        {
            "primeira": [["id", "nome", "valor"], [1, "Ana", 2.5], [2, "Bia", None]],
            "segunda": [["x"], ["ignorada"]],
        }
    )
    res = read_file(data, _origem("xlsx"), SCHEMA)
    assert list(res.rows.columns) == ["id", "nome", "valor"]
    assert res.rows["id"].tolist() == ["1", "2"]
    assert res.rows["nome"].tolist() == ["Ana", "Bia"]
    assert res.rows["valor"].tolist()[0] == "2.5"
    assert res.rows["valor"].tolist()[1] is None


def test_xlsx_header_only_and_invalid() -> None:
    res = read_file(_xlsx({"a": [["id", "nome", "valor"]]}), _origem("xlsx"), SCHEMA)
    assert len(res.rows) == 0
    assert list(res.rows.columns) == ["id", "nome", "valor"]
    with pytest.raises(SourceError, match="xlsx"):
        read_file(b"isto nao e um xlsx", _origem("xlsx"), SCHEMA)


def test_xlsx_duplicate_columns_is_source_error() -> None:
    with pytest.raises(SourceError, match="repetidas"):
        read_file(_xlsx({"a": [["id", "id"], [1, 2]]}), _origem("xlsx"), SCHEMA)


def _zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for nome, conteudo in members.items():
            zf.writestr(nome, conteudo)
    return buf.getvalue()


def test_zip_basic_and_directories() -> None:
    data = _zip({"a.csv": b"1", "pasta/b.csv": b"2", "vazia/": b""})
    assert extract_zip_members(data) == {"a.csv": b"1", "pasta/b.csv": b"2"}


def test_zip_with_accented_member_name() -> None:
    # zip criado por ferramenta que grava UTF-8 sem o flag de UTF-8 (lido como cp437)
    raw = _zip({"RelatXXrio.csv": b"conteudo"}).replace(b"RelatXXrio", "Relatório".encode())
    assert extract_zip_members(raw) == {"Relatório.csv": b"conteudo"}
    # nome com flag UTF-8 correto continua intacto
    assert extract_zip_members(_zip({"Relatório.csv": b"ok"})) == {"Relatório.csv": b"ok"}


def test_zip_slip_member_is_ignored() -> None:
    data = _zip(
        {
            "../fora.csv": b"x",
            "/abs.csv": b"x",
            "a/../../fora2.csv": b"x",
            "C:/win.csv": b"x",
            "..\\fora3.csv": b"x",
            "ok.csv": b"ok",
            "sub\\ok2.csv": b"ok2",
        }
    )
    assert extract_zip_members(data) == {"ok.csv": b"ok", "sub/ok2.csv": b"ok2"}


def test_zip_invalid_is_source_error() -> None:
    with pytest.raises(SourceError, match="zip"):
        extract_zip_members(b"nao e zip")


def test_zip_member_over_limit_is_source_error() -> None:
    data = _zip({"grande.csv": b"x" * 100, "pequeno.csv": b"ok"})
    with pytest.raises(SourceError, match=r"grande\.csv.*limite de 50 bytes") as exc:
        extract_zip_members(data, max_member_bytes=50)
    assert "xxxx" not in str(exc.value)


def test_zip_total_over_limit_is_source_error() -> None:
    data = _zip({"a.csv": b"x" * 60, "b.csv": b"y" * 60})
    with pytest.raises(SourceError, match=r"limite total de 100 bytes"):
        extract_zip_members(data, max_member_bytes=100, max_total_bytes=100)
    assert len(extract_zip_members(data, max_member_bytes=100, max_total_bytes=120)) == 2


def test_zip_lying_header_size_is_source_error() -> None:
    raw = bytearray(_zip({"mentira.csv": b"x" * 1000}))
    # reduz file_size declarado (cabeçalho local e diretório central) para 10 bytes
    size = (1000).to_bytes(4, "little")
    fake = (10).to_bytes(4, "little")
    while (pos := raw.find(size + size)) != -1:
        raw[pos + 4 : pos + 8] = fake
    with pytest.raises(SourceError, match="zip"):
        extract_zip_members(bytes(raw), max_member_bytes=50)


def test_null_contract_is_none_in_every_format() -> None:
    csv_res = read_file(b"id;nome;valor\n1;;3\n", _origem(), SCHEMA)
    assert _rows(csv_res) == [["1", None, "3"]]
    jsonl = read_file(b'{"id": 1, "nome": null, "valor": ""}\n{"id": 2}\n', _origem("jsonl"), SCHEMA)
    assert _rows(jsonl) == [["1", None, None], ["2", None, None]]
    xlsx = read_file(_xlsx({"a": [["id", "nome", "valor"], [1, None, 3]]}), _origem("xlsx"), SCHEMA)
    assert _rows(xlsx) == [["1", None, "3"]]


def test_jsonl_u2028_inside_string_does_not_split_record() -> None:
    # U+2028 e U+0085 são separadores de linha para str.splitlines(), mas válidos dentro de strings JSON
    data = ('{"id": 1, "nome": "a b\u0085c"}\r\n{"id": 2, "nome": "z"}\r\n').encode()
    res = read_file(data, _origem("jsonl"), SCHEMA)
    assert _rows(res) == [["1", "a b\u0085c"], ["2", "z"]]
    assert res.rejected.empty


def test_trailing_empty_header_column_is_dropped() -> None:
    res = read_file(b"id;nome;\n1;Ana;\n2;Bia;\n", _origem(), SCHEMA)
    assert list(res.rows.columns) == ["id", "nome"]
    assert _rows(res) == [["1", "Ana"], ["2", "Bia"]]
    xlsx = read_file(_xlsx({"a": [["id", "nome", None], [1, "Ana", None]]}), _origem("xlsx"), SCHEMA)
    assert list(xlsx.rows.columns) == ["id", "nome"]
    assert _rows(xlsx) == [["1", "Ana"]]


def test_empty_header_name_with_data_or_in_the_middle_is_still_error() -> None:
    with pytest.raises(SourceError, match="vazio"):
        read_file(b"id;nome;\n1;Ana;dado\n", _origem(), SCHEMA)
    with pytest.raises(SourceError, match="vazio"):
        read_file(_xlsx({"a": [["id", None, "valor"], [1, "x", 2]]}), _origem("xlsx"), SCHEMA)


def test_multichar_delimiter_is_config_error() -> None:
    origem = _origem().model_copy(update={"delimitador": ";;"})
    with pytest.raises(ConfigError, match="delimitador"):
        read_file(b"id;;nome\n", origem, SCHEMA)


def test_json_nan_and_infinity_are_null() -> None:
    res = read_file(b'{"id": 1, "valor": NaN, "nome": -Infinity}\n', _origem("jsonl"), SCHEMA)
    assert _rows(res) == [["1", None, None]]
    arr = read_file(b'[{"id": 1, "valor": Infinity}]', _origem("json"), SCHEMA)
    assert arr.rows["valor"].tolist() == [None]


def test_leading_zeros_are_preserved_in_csv_and_xlsx() -> None:
    res = read_file(b"id;nome;valor\n007;00123;0,50\n", _origem(), SCHEMA)
    assert _rows(res) == [["007", "00123", "0,50"]]
    xlsx = read_file(_xlsx({"a": [["id", "nome", "valor"], ["007", "00123", "0,50"]]}), _origem("xlsx"), SCHEMA)
    assert _rows(xlsx) == [["007", "00123", "0,50"]]


def test_zip_deflated_bomb_is_stopped_by_member_limit() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("bomba.csv", b"0" * 5_000_000)
    assert len(buf.getvalue()) < 50_000
    with pytest.raises(SourceError, match=r"bomba\.csv.*limite de 1000000 bytes"):
        extract_zip_members(buf.getvalue(), max_member_bytes=1_000_000)
