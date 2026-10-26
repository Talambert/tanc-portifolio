"""Trilha de auditoria. Na simulação vive em memória; depois vai para uma tabela do BigQuery."""
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class AuditEntry:
    action: str
    target: str
    before: str = ""
    after: str = ""
    batch: str = ""  # agrupa gravações feitas numa mesma aprovação, para desfazer em bloco
    user: str = "demo@local"
    at: str = field(default_factory=lambda: datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    undone: bool = False


def record(log: list[AuditEntry], action: str, target: str, before: str = "", after: str = "", batch: str = "") -> AuditEntry:
    entry = AuditEntry(action=action, target=target, before=before, after=after, batch=batch)
    log.append(entry)
    return entry


def last_open_batch(log: list[AuditEntry], action: str) -> str | None:
    """Última aprovação em lote que ainda não foi desfeita."""
    for entry in reversed(log):
        if entry.action == action and entry.batch and not entry.undone:
            return entry.batch
    return None
