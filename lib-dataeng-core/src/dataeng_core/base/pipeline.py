"""Modelos do `config/pipeline.yaml` (`schema_version: 1`) e sua validação."""

from __future__ import annotations

import warnings
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal, Self, TypeAlias, get_args

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, ValidationError, model_validator

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import load_environment, load_yaml, substitute
from dataeng_core.base.schema import TipoColuna

SUPPORTED_SCHEMA_VERSIONS = (1,)

Motor = Literal["cloud_run", "dataproc"]
_ID = r"^[A-Za-z0-9_-]+$"
_REF = r"^[A-Za-z_][\w.]*:[A-Za-z_]\w*$"  # "modulo:funcao"


@contextmanager
def _schema_field_allowed() -> Iterator[None]:
    """`schema` é campo do YAML; em pydantic ele esconde o método legado `BaseModel.schema`.

    O acesso `modelo.schema` devolve o campo, como esperado; só o aviso da definição é silenciado.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message='Field name "schema" in ".*" shadows', category=UserWarning)
        yield


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------- origens


class Autenticacao(_Model):
    tipo: Literal["none", "bearer", "header", "basic"] = "none"
    segredo_env: str | None = None
    nome_cabecalho: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.tipo != "none" and not self.segredo_env:
            raise ValueError(f"autenticação '{self.tipo}' exige `segredo_env` (nome da variável com o segredo)")
        if self.tipo == "header" and not self.nome_cabecalho:
            raise ValueError("autenticação 'header' exige `nome_cabecalho`")
        return self


class Paginacao(_Model):
    tipo: Literal["nenhuma", "pagina", "offset", "cursor"] = "nenhuma"
    parametro: str | None = None
    tamanho: int | None = Field(default=None, gt=0)
    caminho_proximo: str | None = None
    caminho_dados: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.tipo == "cursor" and not self.caminho_proximo:
            raise ValueError("paginação 'cursor' exige `caminho_proximo`")
        return self


with _schema_field_allowed():

    class ArquivoOrigem(_Model):
        tipo: Literal["arquivo"]
        localizacao: str = Field(min_length=1)
        formato: Literal["csv", "json", "jsonl", "xlsx"] = "csv"
        encoding: str = "utf-8"
        delimitador: str = Field(default=",", min_length=1, max_length=1)
        schema: Path  # type: ignore[assignment]

    class ApiOrigem(_Model):
        tipo: Literal["api"]
        endpoint: str = Field(min_length=1)
        autenticacao: Autenticacao = Field(default_factory=Autenticacao)
        paginacao: Paginacao = Field(default_factory=Paginacao)
        limite_taxa: float | None = Field(default=None, gt=0)
        schema: Path | None = None  # type: ignore[assignment]

    class PubSubOrigem(_Model):
        tipo: Literal["pubsub"]
        assinatura: str = Field(min_length=1)
        max_mensagens: int = Field(default=1000, gt=0)
        tempo_maximo_s: float = Field(default=60, gt=0)
        formato: Literal["json"] = "json"
        dedupe_por: Literal["message_id"] = "message_id"
        schema: Path  # type: ignore[assignment]

    class Destino(_Model):
        bronze: str = Field(min_length=1)
        tabela: str = Field(min_length=1)
        schema: Path  # type: ignore[assignment]
        modo_carga: Literal["append", "overwrite_particao", "merge"]
        particao: str = "_ingestion_date"
        chaves_merge: list[str] = Field(default_factory=list)
        ddl: list[Path] = Field(default_factory=list)
        dml_pos_carga: list[Path] = Field(default_factory=list)

        @model_validator(mode="after")
        def _check(self) -> Self:
            if self.modo_carga == "merge" and not self.chaves_merge:
                raise ValueError("modo_carga 'merge' exige `chaves_merge` (colunas que identificam a linha)")
            return self


Origem: TypeAlias = Annotated[ArquivoOrigem | ApiOrigem | PubSubOrigem, Field(discriminator="tipo")]


# ---------------------------------------------------------- transformação

Normalizacao = Literal["apenas_digitos", "trim", "maiusculas", "minusculas", "colapsar_espacos"]


class TipoSpec(_Model):
    tipo: TipoColuna
    separador_decimal: str | None = None
    formato: str | None = None


class RegraRejeicao(_Model):
    coluna: str
    regra: Literal["nao_nulo", "regex", "faixa"]
    padrao: str | None = None
    minimo: float | None = None
    maximo: float | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.regra == "regex" and not self.padrao:
            raise ValueError("regra 'regex' exige `padrao`")
        if self.regra == "faixa" and self.minimo is None and self.maximo is None:
            raise ValueError("regra 'faixa' exige `minimo` e/ou `maximo`")
        return self


class TransformOp(_Model):
    """Uma operação de transformação: exatamente uma das chaves abaixo."""

    OPS: ClassVar[tuple[str, ...]] = ("renomear", "tipar", "normalizar", "deduplicar", "rejeitar_se", "hook")

    renomear: dict[str, str] | None = None
    tipar: dict[str, TipoSpec] | None = None
    normalizar: dict[str, list[Normalizacao]] | None = None
    deduplicar: list[str] | None = None
    rejeitar_se: list[RegraRejeicao] | None = None
    hook: str | None = Field(default=None, pattern=_REF)

    @model_validator(mode="after")
    def _exactly_one(self) -> Self:
        if len([op for op in self.OPS if getattr(self, op) is not None]) != 1:
            raise ValueError(f"cada operação deve ter exatamente uma chave entre: {', '.join(self.OPS)}")
        return self

    @property
    def nome(self) -> str:
        """Nome da operação definida (`renomear`, `tipar`, ...)."""
        return next(op for op in self.OPS if getattr(self, op) is not None)


# --------------------------------------------------------------- qualidade

NomeVerificacao = Literal["nao_nulo", "unico", "faixa", "conjunto", "formato"]
_VERIFICACOES: tuple[str, ...] = get_args(NomeVerificacao)


def _normalize_check(data: Any) -> Any:
    """Converte as formas curtas do YAML na forma normalizada (com `tipo`)."""
    if not isinstance(data, dict):
        return data
    campos: dict[str, Any]
    if "tipo" in data:
        campos = dict(data)
    else:
        nomes = [k for k in data if k in _VERIFICACOES]
        if len(nomes) != 1:
            raise ValueError(f"informe exatamente uma verificação entre: {', '.join(_VERIFICACOES)}")
        nome = nomes[0]
        valor = data[nome]
        campos = {"tipo": nome, **{k: v for k, v in data.items() if k != nome}}
        if isinstance(valor, dict):
            campos.update(valor)
        elif isinstance(valor, list):
            campos["colunas"] = valor
        elif isinstance(valor, str):
            campos["colunas"] = [valor]
        else:
            raise ValueError(f"forma inválida para a verificação '{nome}'")
    if "coluna" in campos:
        if "colunas" in campos:
            raise ValueError("use `coluna` ou `colunas`, não ambos")
        campos["colunas"] = [campos.pop("coluna")]
    return campos


def _expand_checks(value: Any) -> Any:
    """`unico` com lista de listas vira uma verificação por chave composta."""
    if not isinstance(value, list):
        return value
    out: list[Any] = []
    for item in value:
        check = _normalize_check(item)
        colunas = check.get("colunas") if isinstance(check, dict) else None
        if (
            isinstance(check, dict)
            and check.get("tipo") == "unico"
            and isinstance(colunas, list)
            and colunas
            and all(isinstance(c, list) for c in colunas)
        ):
            out.extend({**check, "colunas": chave} for chave in colunas)
        else:
            out.append(check)
    return out


class QualityCheck(_Model):
    """Verificação de qualidade normalizada (aceita as formas curtas do YAML)."""

    tipo: NomeVerificacao
    colunas: list[str] = Field(min_length=1)
    tabela: str | None = None
    minimo: float | None = None
    maximo: float | None = None
    valores: list[Any] | None = None
    padrao: str | None = None
    limiar: float = Field(default=1.0, ge=0, le=1)
    bloqueante: bool = False

    @model_validator(mode="before")
    @classmethod
    def _short_form(cls, data: Any) -> Any:
        return _normalize_check(data)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.tipo == "faixa":
            if self.minimo is None and self.maximo is None:
                raise ValueError("verificação 'faixa' exige `minimo` e/ou `maximo`")
            if self.minimo is not None and self.maximo is not None and self.minimo > self.maximo:
                raise ValueError("verificação 'faixa' com `minimo` maior que `maximo`")
        if self.tipo == "conjunto" and not self.valores:
            raise ValueError("verificação 'conjunto' exige `valores`")
        if self.tipo == "formato" and not self.padrao:
            raise ValueError("verificação 'formato' exige `padrao`")
        return self


QualityChecks: TypeAlias = Annotated[list[QualityCheck], BeforeValidator(_expand_checks)]


# ----------------------------------------------------------------- etapas


class _StepBase(_Model):
    MOTORES: ClassVar[tuple[Motor, ...]] = ()
    MOTOR_PADRAO: ClassVar[Motor | None] = None

    id: str = Field(pattern=_ID)
    depende_de: list[str] = Field(default_factory=list)
    motor: Motor | None = None

    @model_validator(mode="after")
    def _check_motor(self) -> Self:
        if self.motor is None:
            self.motor = self.MOTOR_PADRAO
        elif self.motor not in self.MOTORES:
            tipo = getattr(self, "tipo", "?")
            if not self.MOTORES:
                raise ValueError(f"etapas do tipo '{tipo}' não aceitam `motor` (rodam no BigQuery)")
            raise ValueError(
                f"motor '{self.motor}' incompatível com o tipo '{tipo}'; permitido: {', '.join(self.MOTORES)}"
            )
        return self


class IngestaoStep(_StepBase):
    MOTORES = ("cloud_run", "dataproc")
    MOTOR_PADRAO = "cloud_run"

    tipo: Literal["ingestao"]
    origem: Origem
    destino: Destino
    transformacao: list[TransformOp] = Field(default_factory=list)
    qualidade: QualityChecks = Field(default_factory=list)


class SqlStep(_StepBase):
    tipo: Literal["sql"]
    sql: list[Path] = Field(min_length=1)


class ViewStep(_StepBase):
    tipo: Literal["view"]
    sql: Path
    nome_view: str | None = None


class PythonStep(_StepBase):
    MOTORES = ("cloud_run", "dataproc")
    MOTOR_PADRAO = "cloud_run"

    tipo: Literal["python"]
    funcao: str = Field(pattern=_REF)


class SparkStep(_StepBase):
    MOTORES = ("dataproc",)
    MOTOR_PADRAO = "dataproc"

    tipo: Literal["spark"]
    script: Path
    dataproc: dict[str, Any] = Field(default_factory=dict)


class QualidadeStep(_StepBase):
    tipo: Literal["qualidade"]
    verificacoes: QualityChecks = Field(min_length=1)

    @model_validator(mode="after")
    def _check_tabela(self) -> Self:
        if any(c.tabela is None for c in self.verificacoes):
            raise ValueError("toda verificação de uma etapa 'qualidade' exige `tabela`")
        return self


Step: TypeAlias = Annotated[
    IngestaoStep | SqlStep | ViewStep | PythonStep | SparkStep | QualidadeStep,
    Field(discriminator="tipo"),
]
STEP_TYPES: tuple[str, ...] = ("ingestao", "sql", "view", "python", "spark", "qualidade")


# ---------------------------------------------------------------- pipeline


def _find_cycle(etapas: list[Step]) -> list[str] | None:
    """Primeiro ciclo encontrado (seguindo `depende_de`), em ordem de declaração."""
    deps = {s.id: s.depende_de for s in etapas}
    estado: dict[str, int] = {}  # 1 = visitando, 2 = concluída
    caminho: list[str] = []

    def visit(node: str) -> list[str] | None:
        estado[node] = 1
        caminho.append(node)
        for dep in deps[node]:
            if estado.get(dep) == 1:
                return caminho[caminho.index(dep) :] + [dep]
            if dep not in estado and (ciclo := visit(dep)):
                return ciclo
        caminho.pop()
        estado[node] = 2
        return None

    for s in etapas:
        if s.id not in estado and (ciclo := visit(s.id)):
            return ciclo
    return None


class Pipeline(_Model):
    schema_version: int
    nome: str = Field(pattern=_ID)
    agenda: str
    parametros: dict[str, str] = Field(default_factory=dict)
    etapas: list[Step] = Field(min_length=1)
    execucao_padrao: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_graph(self) -> Self:
        if self.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            suportadas = ", ".join(str(v) for v in SUPPORTED_SCHEMA_VERSIONS)
            raise ValueError(f"schema_version {self.schema_version} não é suportada; versão suportada: {suportadas}")
        ids = [s.id for s in self.etapas]
        repetidos = sorted({i for i in ids if ids.count(i) > 1})
        if repetidos:
            raise ValueError(f"ids de etapa repetidos: {', '.join(repr(i) for i in repetidos)}")
        for anterior, etapa in zip(self.etapas, self.etapas[1:], strict=False):
            if "depende_de" not in etapa.model_fields_set:
                etapa.depende_de = [anterior.id]
        for etapa in self.etapas:
            for dep in etapa.depende_de:
                if dep not in ids:
                    raise ValueError(f"a etapa '{etapa.id}' depende de '{dep}', que não existe")
        ciclo = _find_cycle(self.etapas)
        if ciclo:
            raise ValueError(f"ciclo de dependências: {' -> '.join(ciclo)} (cada etapa depende da seguinte)")
        return self

    def ordered_steps(self) -> list[Step]:
        """Ordem topológica estável: entre as etapas prontas, a declarada antes."""
        feitas: set[str] = set()
        ordem: list[Step] = []
        pendentes = list(self.etapas)
        while pendentes:
            proxima = next(s for s in pendentes if all(d in feitas for d in s.depende_de))
            pendentes.remove(proxima)
            feitas.add(proxima.id)
            ordem.append(proxima)
        return ordem

    def get(self, step_id: str) -> Step:
        for s in self.etapas:
            if s.id == step_id:
                return s
        ids = ", ".join(s.id for s in self.etapas)
        raise ConfigError(f"etapa '{step_id}' não existe; etapas do pipeline: {ids}", operation="pipeline.get")


# ------------------------------------------------------------- carregamento


def _project_root(config_path: Path) -> Path:
    """Raiz do projeto: a pasta acima de `config/`, ou a do próprio arquivo."""
    pasta = config_path.resolve().parent
    return pasta.parent if pasta.name == "config" else pasta


def _loc_text(loc: tuple[int | str, ...], raw: Mapping[str, Any]) -> str:
    """`('etapas', 2, 'view', 'motor')` vira `etapas[2] (id=x).motor`."""
    partes: list[str] = []
    etapas = raw.get("etapas")
    i = 0
    while i < len(loc):
        item = loc[i]
        if isinstance(item, int):
            texto = f"[{item}]"
            if partes == ["etapas"] and isinstance(etapas, list) and item < len(etapas):
                step_id = etapas[item].get("id") if isinstance(etapas[item], dict) else None
                texto += f" (id={step_id})" if step_id else ""
                # pula a etiqueta da união (o `tipo` da etapa)
                if i + 1 < len(loc) and loc[i + 1] in STEP_TYPES:
                    i += 1
            partes.append(texto)
        elif partes and partes[-1] == "origem" and item in ("arquivo", "api", "pubsub"):
            pass
        else:
            partes.append(("." if partes else "") + str(item))
        i += 1
    return "".join(partes) or "<raiz>"


def _error_text(err: Mapping[str, Any]) -> str:
    """Mensagem em português, sem o valor de entrada."""
    tipo = err["type"]
    ctx = err.get("ctx") or {}
    if tipo == "missing":
        return "campo obrigatório ausente"
    if tipo == "extra_forbidden":
        return "campo não permitido"
    if tipo == "union_tag_invalid":
        return f"tipo '{ctx.get('tag')}' desconhecido; tipos válidos: {ctx.get('expected_tags')}"
    if tipo == "union_tag_not_found":
        return f"campo '{ctx.get('discriminator', 'tipo')}' ausente"
    if tipo == "literal_error":
        return f"valor inválido; esperado: {ctx.get('expected')}"
    if tipo == "string_pattern_mismatch":
        return f"formato inválido; padrão esperado: {ctx.get('pattern')}"
    return str(err["msg"]).removeprefix("Value error, ")


def _describe(exc: ValidationError, raw: Mapping[str, Any]) -> str:
    return "; ".join(f"{_loc_text(e['loc'], raw)}: {_error_text(e)}" for e in exc.errors())


def _referenced_files(step: Step) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    if isinstance(step, IngestaoStep):
        if step.origem.schema is not None:
            files.append(("origem.schema", step.origem.schema))
        files.append(("destino.schema", step.destino.schema))
        files += [("destino.ddl", p) for p in step.destino.ddl]
        files += [("destino.dml_pos_carga", p) for p in step.destino.dml_pos_carga]
    elif isinstance(step, SqlStep):
        files += [("sql", p) for p in step.sql]
    elif isinstance(step, ViewStep):
        files.append(("sql", step.sql))
    elif isinstance(step, SparkStep):
        files.append(("script", step.script))
    return files


def _merge_params(
    raw: Mapping[str, Any], env_params: Mapping[str, str], extra: Mapping[str, str], config_path: Path
) -> dict[str, str]:
    """`parametros` do pipeline < ambiente < `extra_params`; valores podem usar `${}`."""
    declarados = raw.get("parametros") or {}
    if not isinstance(declarados, dict):
        raise ConfigError(f"arquivo {config_path}: `parametros` deve ser um mapa", operation="load_pipeline")
    proprios: dict[str, str] = {}
    for nome, valor in declarados.items():
        if isinstance(valor, dict | list) or valor is None:
            raise ConfigError(
                f"arquivo {config_path}: parametros.{nome} deve ser texto ou número", operation="load_pipeline"
            )
        proprios[str(nome)] = str(valor)
    merged = {**proprios, **env_params, **extra}
    return {k: substitute(v, merged) for k, v in merged.items()}


def load_pipeline(
    config_path: Path, *, ambiente: str = "local", extra_params: Mapping[str, str] | None = None
) -> Pipeline:
    """Carrega o pipeline com o ambiente `config/ambientes/<ambiente>.yaml` e valida tudo.

    Precedência dos parâmetros `${}`: `parametros` do YAML < ambiente < `extra_params`.
    Os caminhos de arquivos (sql, schemas, scripts) são relativos à raiz do projeto.
    """
    config_path = Path(config_path)
    root = _project_root(config_path)
    ambientes = root / "config" / "ambientes"
    env_file = ambientes / f"{ambiente}.yaml"
    if not env_file.is_file():
        existentes = sorted(p.stem for p in ambientes.glob("*.yaml")) if ambientes.is_dir() else []
        raise ConfigError(
            f"ambiente '{ambiente}' não encontrado em {ambientes}; disponíveis: {', '.join(existentes) or 'nenhum'}",
            operation="load_pipeline",
        )
    env = load_environment(env_file)
    raw = load_yaml(config_path)

    try:
        params = _merge_params(raw, env.params(), dict(extra_params or {}), config_path)
        data = substitute({k: v for k, v in raw.items() if k != "parametros"}, params)
    except ConfigError as exc:
        raise ConfigError(
            f"arquivo {config_path}: {exc.detail}; defina-o em `parametros`, no ambiente '{ambiente}' "
            "ou como parâmetro extra",
            operation="load_pipeline",
        ) from exc
    data["parametros"] = params

    try:
        pipeline = Pipeline.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"arquivo {config_path}: {_describe(exc, data)}", operation="load_pipeline") from exc

    faltando = [
        f"etapa '{s.id}' campo {campo}: {p.as_posix()}"
        for s in pipeline.etapas
        for campo, p in _referenced_files(s)
        if not (root / p).is_file()
    ]
    if faltando:
        raise ConfigError(
            f"arquivo {config_path}: arquivos inexistentes (relativos a {root}): {'; '.join(faltando)}",
            operation="load_pipeline",
        )
    return pipeline
