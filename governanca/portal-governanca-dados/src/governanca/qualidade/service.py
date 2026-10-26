"""Módulo de Data Quality.

A avaliação das regras é real (roda em pandas sobre as amostras). Só a origem das amostras e das
propostas é simulada; no modo real, o profiling e os scans passam a ser executados no Dataplex.
"""
import re
from dataclasses import asdict, dataclass, field

import pandas as pd
import yaml

from governanca.mock import data as mock

KINDS = {
    "nao_nulo": "Não nulo",
    "unico": "Único",
    "faixa": "Faixa de valores",
    "conjunto": "Conjunto de valores",
    "regex": "Formato (regex)",
}


@dataclass
class Rule:
    dataset: str
    table: str
    column: str
    kind: str
    params: dict
    threshold: float
    origin: str
    confidence: str
    justification: str
    dry_run_rate: float | None = None  # taxa de aprovação em dado limpo
    extra: dict = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.dataset}.{self.table}.{self.column}.{self.kind}"

    @property
    def params_text(self) -> str:
        if self.kind == "faixa":
            return f"{self.params['minimo']} a {self.params['maximo']}"
        if self.kind == "conjunto":
            return f"{len(self.params['valores'])} valores"
        if self.kind == "regex":
            return self.params["padrao"]
        return ""

    @property
    def false_alarm(self) -> bool:
        return self.dry_run_rate is not None and self.dry_run_rate < self.threshold


def evaluate(df: pd.DataFrame, rule: Rule) -> float:
    """Fração de linhas que atendem à regra (1.0 = tudo certo)."""
    series = df[rule.column]
    if rule.kind == "nao_nulo":
        return float(series.notna().mean())
    if rule.kind == "unico":
        return float(1 - series.duplicated(keep=False).mean())

    present = series.dropna()
    if present.empty:
        return 1.0
    if rule.kind == "faixa":
        ok = present.between(rule.params["minimo"], rule.params["maximo"])
    elif rule.kind == "conjunto":
        ok = present.isin(rule.params["valores"])
    elif rule.kind == "regex":
        pattern = re.compile(rule.params["padrao"])
        ok = present.astype(str).map(lambda v: bool(pattern.fullmatch(v)))
    else:
        raise ValueError(f"Tipo de regra desconhecido: {rule.kind}")
    return float(ok.mean())


def profile(dataset: str, table: str) -> pd.DataFrame:
    """Estatísticas por coluna do dado atual (com os defeitos que a realidade tem)."""
    df = mock.get_sample(dataset, table, dirty=True)
    rows = []
    for column in df.columns:
        series = df[column]
        numeric = pd.api.types.is_numeric_dtype(series)
        rows.append({
            "Coluna": column,
            "Tipo": str(series.dtype),
            "% nulos": round(series.isna().mean() * 100, 1),
            "Distintos": int(series.nunique()),
            "Mínimo": str(series.min()) if numeric else "",
            "Máximo": str(series.max()) if numeric else "",
            "Exemplo": str(series.dropna().iloc[0]) if series.notna().any() else "",
        })
    return pd.DataFrame(rows)


def propose(dataset: str, tables: list[str]) -> list[Rule]:
    """Propõe regras e já as testa (dry-run) em dado limpo, para separar as que gerariam falso alarme."""
    rules: list[Rule] = []
    for table in tables:
        clean = mock.get_sample(dataset, table, dirty=False)
        for column, kind, params, threshold, origin, confidence, why in mock.RULE_PROPOSALS[(dataset, table)]:
            rule = Rule(dataset, table, column, kind, params, threshold, origin, confidence, why)
            rule.dry_run_rate = evaluate(clean, rule)
            rules.append(rule)
    return rules


def scan(rule: Rule) -> float:
    """Resultado de um scan agendado: a regra aplicada ao dado atual."""
    return evaluate(mock.get_sample(rule.dataset, rule.table, dirty=True), rule)


class _CompactDumper(yaml.SafeDumper):
    """Listas de valores simples (ex.: as 27 UFs) saem em uma linha, o resto segue em blocos legíveis."""


def _represent_list(dumper: yaml.SafeDumper, data: list):
    simple = all(not isinstance(item, (dict, list)) for item in data)
    return dumper.represent_sequence("tag:yaml.org,2002:seq", data, flow_style=True if simple else None)


_CompactDumper.add_representer(list, _represent_list)


def rule_to_dict(rule: Rule) -> dict:
    return asdict(rule)


def rule_from_dict(data: dict) -> Rule:
    return Rule(**data)


def to_yaml(rules: list[Rule]) -> str:
    """Regras como código: um YAML por tabela, pronto para ir ao git."""
    by_table: dict[str, list[dict]] = {}
    for r in rules:
        entry = {"coluna": r.column, "tipo": r.kind, **r.params, "limiar": r.threshold,
                 "justificativa": r.justification, "origem": r.origin, "confianca": r.confidence}
        by_table.setdefault(f"{r.dataset}.{r.table}", []).append(entry)
    document = [{"tabela": table, "regras": entries} for table, entries in by_table.items()]
    return yaml.dump(document, Dumper=_CompactDumper, allow_unicode=True, sort_keys=False, width=110)
