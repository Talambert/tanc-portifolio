"""Módulo de Metadados. Na simulação as sugestões vêm de dados fictícios; no modo real virão do Gemini."""
from dataclasses import dataclass

from governanca.mock import data as mock

CONFIDENCE_ORDER = {"alta": 0, "média": 1, "baixa": 2}


@dataclass
class Suggestion:
    dataset: str
    table: str
    column: str | None  # None = a própria tabela
    existing: str
    proposed: str
    confidence: str
    evidence: str
    classification: str
    sensitive: bool = False

    @property
    def key(self) -> str:
        return f"{self.dataset}.{self.table}.{self.column or '*'}"

    @property
    def has_human_text(self) -> bool:
        return bool(self.existing.strip())

    @property
    def apply_by_default(self) -> bool:
        """Só pré-seleciona o que é seguro: sem texto humano a sobrescrever, com proposta e confiança suficiente."""
        return bool(self.proposed.strip()) and not self.has_human_text and self.confidence != "baixa"


def list_datasets() -> list[str]:
    return mock.list_datasets()


def list_tables(dataset: str) -> list[str]:
    return mock.list_tables(dataset)


def generate(dataset: str, tables: list[str]) -> list[Suggestion]:
    suggestions: list[Suggestion] = []
    for table in tables:
        info = mock.DATASETS[dataset][table]
        description, confidence, evidence, classification = mock.TABLE_SUGGESTIONS[(dataset, table)]
        suggestions.append(Suggestion(dataset, table, None, info["description"], description, confidence, evidence, classification))

        per_column = mock.COLUMN_SUGGESTIONS[(dataset, table)]
        for name, _type, existing in info["columns"]:
            proposed, conf, evid, klass, sensitive = per_column[name]
            suggestions.append(Suggestion(dataset, table, name, existing, proposed, conf, evid, klass, sensitive))
    return suggestions


def schema_of(dataset: str, table: str) -> list[tuple[str, str, str]]:
    return mock.DATASETS[dataset][table]["columns"]


def documentation_coverage(applied: dict[str, str] | None = None) -> list[dict]:
    """Cobertura de documentação por tabela: objetos (tabela e colunas) com descrição, existente ou gravada no portal."""
    applied = applied or {}
    rows = []
    for dataset, tables in mock.DATASETS.items():
        for table, info in tables.items():
            objects = [(f"{dataset}.{table}.*", info["description"])] + [
                (f"{dataset}.{table}.{name}", existing) for name, _type, existing in info["columns"]
            ]
            documented = sum(bool(applied.get(key, existing).strip()) for key, existing in objects)
            rows.append({"dataset": dataset, "tabela": table, "objetos": len(objects), "documentados": documented})
    return rows
