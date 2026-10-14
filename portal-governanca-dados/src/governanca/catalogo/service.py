"""Módulo de Catálogo.

Na simulação a recuperação é lexical (sobreposição ponderada de termos) e a resposta é montada a partir do
trecho encontrado, sem LLM. O comportamento que importa já está aqui: responder só com o que há nas fontes,
citar a fonte e dizer "não encontrei" quando nenhum trecho sustenta a pergunta. No modo real, entram
embeddings do Vertex AI, busca vetorial no BigQuery e o Gemini para redigir a resposta.
"""
import math
import re
import unicodedata
from dataclasses import dataclass

from governanca.mock import data as mock

ABSTAIN_THRESHOLD = 0.35
STOPWORDS = {
    "que", "como", "qual", "quais", "para", "por", "com", "uma", "uns", "das", "dos", "esta", "este", "isso",
    "aos", "nas", "nos", "sao", "ser", "tem", "foi", "sobre", "entre", "mais", "pela", "pelo", "onde", "quando",
    "significa", "tabela", "coluna", "campo", "dado", "dados", "vem", "de", "da", "do", "em", "no", "na",
}


@dataclass
class Chunk:
    source: str
    kind: str
    reference: str
    text: str


@dataclass
class Answer:
    text: str
    citations: list[tuple[Chunk, float]]
    abstained: bool


def _tokens(text: str) -> list[str]:
    plain = unicodedata.normalize("NFKD", text.lower()).encode("ascii", "ignore").decode()
    return [t for t in re.findall(r"[a-z0-9_]+", plain) if len(t) > 2 and t not in STOPWORDS]


def build_corpus(applied: dict[str, str] | None = None) -> list[Chunk]:
    """Fontes do catálogo: documentos, descrições do esquema e o que o módulo de Metadados já gravou."""
    chunks = [Chunk(*doc) for doc in mock.DOCUMENTS]
    applied = applied or {}
    for dataset, tables in mock.DATASETS.items():
        for table, info in tables.items():
            for name, _type, existing in info["columns"]:
                key = f"{dataset}.{table}.{name}"
                text = applied.get(key, existing)
                if text:
                    chunks.append(Chunk("Esquema do BigQuery" if key not in applied else "Metadados (gravados no portal)",
                                        "esquema", f"{table}.{name}", f"{table}.{name}: {text}"))
    return chunks


def search(question: str, corpus: list[Chunk], k: int = 3) -> list[tuple[Chunk, float]]:
    query = set(_tokens(question))
    if not query:
        return []
    docs = [set(_tokens(c.text + " " + c.reference)) for c in corpus]
    n = len(docs)
    idf = {t: math.log((n + 1) / (1 + sum(t in d for d in docs))) + 1 for t in query}
    total = sum(idf.values())

    scored = []
    for chunk, doc in zip(corpus, docs):
        score = sum(idf[t] for t in query if t in doc) / total
        if score > 0:
            scored.append((chunk, score))
    return sorted(scored, key=lambda item: item[1], reverse=True)[:k]


def answer(question: str, corpus: list[Chunk]) -> Answer:
    hits = search(question, corpus)
    if not hits or hits[0][1] < ABSTAIN_THRESHOLD:
        return Answer(
            "Não encontrei isso nas fontes indexadas. Prefiro não responder a inventar. "
            "Tente reformular a pergunta ou adicione a documentação que cobre esse assunto.",
            citations=[], abstained=True,
        )
    best, _ = hits[0]
    return Answer(f"{best.text}", citations=hits, abstained=False)
