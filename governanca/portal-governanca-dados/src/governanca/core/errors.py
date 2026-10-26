"""Erros que o usuário precisa entender e poder resolver.

Cada erro diz o que aconteceu e o que fazer, sem culpar a pessoa nem esconder a causa. No modo real,
as exceções de Dataplex, BigQuery e Vertex AI são convertidas para estes tipos na camada de acesso.
"""
import uuid


class GovernancaError(Exception):
    """Falha esperada de uma operação. `title` resume, `what` explica e `fix` orienta o próximo passo."""

    title = "Não foi possível concluir a operação"
    what = "O serviço retornou um erro."
    fix = "Tente novamente. Se persistir, informe o código abaixo a quem administra o portal."

    def __init__(self, operation: str = "", detail: str = ""):
        super().__init__(detail or self.what)
        self.operation = operation
        self.detail = detail
        self.reference = uuid.uuid4().hex[:8]


class PermissionDenied(GovernancaError):
    title = "Sem permissão para esta operação"
    what = "A conta do portal não tem acesso ao recurso solicitado."
    fix = "Peça a quem administra o projeto o papel necessário na Service Account do portal e tente de novo."


class ServiceTimeout(GovernancaError):
    title = "O serviço demorou demais para responder"
    what = "A consulta ao serviço excedeu o tempo limite. Nada foi alterado."
    fix = "Tente de novo. Com muitas tabelas, escolha menos tabelas por vez."


class QuotaExceeded(GovernancaError):
    title = "Limite de uso atingido"
    what = "O limite de custo ou de cota configurado foi atingido. Nada foi alterado."
    fix = "Aumente o limite em Configurações ou aguarde a renovação da cota."


# Falhas que o modo simulação consegue provocar, para demonstrar e testar o tratamento de erro.
SIMULATED_FAULTS: dict[str, type[GovernancaError] | type[Exception] | None] = {
    "Nenhuma": None,
    "Permissão negada": PermissionDenied,
    "Tempo esgotado": ServiceTimeout,
    "Cota excedida": QuotaExceeded,
    "Erro inesperado": RuntimeError,
}


def maybe_raise(fault: str, operation: str) -> None:
    """Levanta a falha simulada escolhida (ou nada, se for "Nenhuma")."""
    kind = SIMULATED_FAULTS.get(fault)
    if kind is None:
        return
    if issubclass(kind, GovernancaError):
        raise kind(operation, f"Falha simulada em '{operation}'.")
    raise kind(f"Falha simulada em '{operation}': exceção não prevista pelo portal.")
