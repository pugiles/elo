class EloError(Exception):
    """Erro base para todos os erros do Elo DB."""


class ConnectionError(EloError):
    """Nao foi possivel conectar ao servidor."""


class AuthenticationError(EloError):
    """API Key invalida (401)."""


class NotFoundError(EloError):
    """No ou aresta nao encontrada (404)."""


class ValidationError(EloError):
    """Dados invalidos enviados (400)."""


class ServerError(EloError):
    """Erro interno no Rust (500)."""
