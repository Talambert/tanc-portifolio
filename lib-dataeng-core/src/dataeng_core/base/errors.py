"""Erros tipados da biblioteca, com texto acionável em português."""

from __future__ import annotations

import secrets


class DataEngError(Exception):
    """Erro base: `title`, `what` (o que aconteceu) e `fix` (o que fazer)."""

    title: str = "Erro do dataeng-core"
    what: str = "Ocorreu um erro inesperado na biblioteca."
    fix: str = "Consulte o detalhe e a referência do erro e tente novamente."

    def __init__(self, detail: str = "", *, operation: str = "") -> None:
        self.detail = detail
        self.operation = operation
        self.reference: str = secrets.token_hex(4)
        parts = [f"[{self.reference}] {self.title}"]
        if operation:
            parts.append(f"operação: {operation}")
        if detail:
            parts.append(detail)
        super().__init__(" - ".join(parts))


class ConfigError(DataEngError):
    title = "Configuração inválida"
    what = "O arquivo de configuração está incorreto ou incompleto."
    fix = "Corrija o campo indicado no YAML e rode `dataeng validate-config`."


class SourceError(DataEngError):
    title = "Falha na origem"
    what = "Não foi possível ler os dados da origem (arquivo, API ou Pub/Sub)."
    fix = "Verifique o caminho, as credenciais e a disponibilidade da origem."


class LoadError(DataEngError):
    title = "Falha na carga"
    what = "Não foi possível gravar os dados no destino."
    fix = "Verifique permissões e o modo de carga; a reexecução é idempotente."


class QualityError(DataEngError):
    title = "Falha de qualidade"
    what = "Os dados não passaram nas regras de qualidade definidas."
    fix = "Revise os registros rejeitados na quarentena e corrija a origem."


class SqlError(DataEngError):
    title = "Erro de SQL"
    what = "O SQL é inválido ou usa um parâmetro não definido."
    fix = "Confira a sintaxe e defina todos os parâmetros `${nome}` usados."


class PubSubError(DataEngError):
    title = "Falha no Pub/Sub"
    what = "Não foi possível consumir ou confirmar mensagens da assinatura."
    fix = "Verifique a assinatura, as permissões e o backlog restante."
