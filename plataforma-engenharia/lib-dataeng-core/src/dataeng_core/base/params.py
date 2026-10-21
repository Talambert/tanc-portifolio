"""Parâmetros `${nome}`, leitura de YAML e configuração de ambiente."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from dataeng_core.base.errors import ConfigError

T = TypeVar("T")

_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")


def _sub_str(text: str, params: Mapping[str, str], strict: bool) -> str:
    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in params:
            return params[name]
        if strict:
            raise ConfigError(f"parâmetro '${{{name}}}' não definido", operation="substitute")
        return match.group(0)

    return _PLACEHOLDER.sub(repl, text)


def _walk(value: Any, params: Mapping[str, str], strict: bool) -> Any:
    if isinstance(value, str):
        return _sub_str(value, params, strict)
    if isinstance(value, dict):
        return {k: _walk(v, params, strict) for k, v in value.items()}
    if isinstance(value, list):
        return [_walk(v, params, strict) for v in value]
    return value


def substitute(value: T, params: Mapping[str, str], *, strict: bool = True) -> T:
    """Troca `${nome}` em str, dict e list; com `strict`, nome ausente é erro."""
    return cast(T, _walk(value, params, strict))


def load_yaml(path: Path) -> dict[str, Any]:
    """Lê um YAML; erro de sintaxe vira `ConfigError` com arquivo e linha."""
    try:
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        where = f", linha {mark.line + 1}" if mark is not None else ""
        problem = getattr(exc, "problem", None) or "sintaxe inválida"
        raise ConfigError(f"arquivo {path}{where}: {problem}", operation="load_yaml") from exc
    except UnicodeDecodeError as exc:
        raise ConfigError(
            f"arquivo {path}: o arquivo precisa estar em UTF-8 (byte inválido na posição {exc.start})",
            operation="load_yaml",
            fix="Reabra o arquivo no editor e salve-o com a codificação UTF-8 (sem alterar o conteúdo).",
        ) from exc
    except OSError as exc:
        raise ConfigError(f"não foi possível ler {path}: {exc.strerror}", operation="load_yaml") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"arquivo {path}: o conteúdo deve ser um mapa", operation="load_yaml")
    return cast(dict[str, Any], data)


class Environment(BaseModel):
    """Ambiente de execução (local ou GCP). Campo desconhecido (ex.: erro de digitação) é recusado."""

    model_config = ConfigDict(extra="forbid")

    modo: Literal["local", "gcp"]
    projeto_gcp: str
    regiao: str
    bucket_bronze: str
    dataset_silver: str
    dataset_gold: str
    service_account: str | None = None
    pasta_local: str = ".local"

    def params(self) -> dict[str, str]:
        """Campos do ambiente como parâmetros `${}` (omite os nulos)."""
        return {k: v for k, v in self.model_dump().items() if v is not None}


def load_environment(path: Path) -> Environment:
    """Lê e valida um arquivo de ambiente."""
    data = load_yaml(path)
    try:
        return Environment.model_validate(data)
    except ValidationError as exc:
        valid = ", ".join(Environment.model_fields)
        campos = ", ".join(
            ".".join(str(p) for p in e["loc"])
            + (
                f" (campo desconhecido; campos válidos: {valid})"
                if e["type"] == "extra_forbidden"
                else f" ({e['msg']})"
            )
            for e in exc.errors()
        )
        raise ConfigError(f"arquivo {path}: {campos}", operation="load_environment") from exc
