"""Persistência do estado do portal entre recarregamentos da página.

Na simulação o estado vai para um arquivo JSON local (por padrão `.data/estado.json`, ignorado pelo git).
No modo real, a mesma interface passa a gravar no BigQuery, e a auditoria vira uma tabela compartilhada.

Variável de ambiente `GOVERNANCA_STATE_FILE`: caminho do arquivo, ou `off` para não persistir
(usada pelos testes para isolar cada execução).
"""
import json
import os
from pathlib import Path

DEFAULT_PATH = Path(__file__).resolve().parents[3] / ".data" / "estado.json"


def _path() -> Path | None:
    configured = os.environ.get("GOVERNANCA_STATE_FILE")
    if configured and configured.lower() == "off":
        return None
    return Path(configured) if configured else DEFAULT_PATH


def load() -> dict:
    """Estado salvo, ou {} se não houver arquivo ou se ele estiver ilegível (nunca derruba o portal)."""
    path = _path()
    if path is None or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save(state: dict) -> bool:
    """Grava de forma atômica (arquivo temporário e troca), para nunca deixar um JSON pela metade.

    Devolve False quando não conseguiu gravar. A falha não derruba o portal (o estado da sessão continua
    valendo), mas quem chama deve avisar a pessoa de que as alterações não sobreviverão a um recarregamento.
    """
    path = _path()
    if path is None:
        return True
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(path)
        return True
    except OSError:
        return False


def reset() -> None:
    path = _path()
    if path is not None and path.exists():
        try:
            path.unlink()
        except OSError:
            pass
