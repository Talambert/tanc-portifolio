"""Ganchos de usuário (`custom/`): resolve `"modulo:funcao"` a partir da raiz do projeto."""

from __future__ import annotations

import importlib
import inspect
import re
import sys
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from dataeng_core.base.errors import ConfigError

_REF = re.compile(r"^([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*):([A-Za-z_]\w*)$")

Hook = Callable[[pd.DataFrame], pd.DataFrame]

_HOOK_ROOTS: list[str] = []  # raízes que ESTE módulo pôs no sys.path (só a mais recente fica)


def _module_file(root: Path, module: str) -> Path | None:
    base = root.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            resolved = candidate.resolve()
            if resolved.is_relative_to(root.resolve()):
                return resolved
    return None


def _is_under(module_file: str | None, root: Path) -> bool:
    if not module_file:
        return False
    try:
        return Path(module_file).resolve().is_relative_to(root.resolve())
    except OSError:
        return False


def _purge_stale_modules(root: Path) -> None:
    """Esquece módulos vindos de raízes de gancho já registradas (e desta), para o código atual prevalecer.

    Só mexe em módulos cujo arquivo/pacote está sob uma raiz anterior; stdlib e terceiros ficam intactos.
    """
    antigas = [Path(r) for r in _HOOK_ROOTS] + [root]
    for key in list(sys.modules):
        mod = sys.modules[key]
        origens = [getattr(mod, "__file__", None), *list(getattr(mod, "__path__", []) or [])]
        if any(_is_under(o, a) for o in origens for a in antigas):
            del sys.modules[key]


class HookImportError(ConfigError):
    """O módulo do usuário levantou exceção ao ser importado.

    A mensagem traz só a CLASSE da exceção (o texto dela pode conter dados); a exceção original fica em
    `__cause__`, para quem chama registrar o traceback (redigido) no log com a `reference` deste erro.
    """


_HOOK_CONTRACT = "exatamente um argumento (o DataFrame) e devolver um DataFrame"


def resolve_hook(ref: str, *, project_root: Path, contract: str = _HOOK_CONTRACT) -> Hook:
    """Importa `modulo:funcao` de dentro de `project_root` (que fica no início do `sys.path`).

    Só executa o import do módulo; a função não é chamada aqui. Efeito colateral: `sys.path` fica com
    `project_root` na frente até o próximo `resolve_hook` (que troca a raiz). `contract` descreve, na mensagem
    de assinatura incompatível, o que a função deve aceitar e devolver (a etapa `python` recebe o contexto).
    Exceção no import do módulo vira `HookImportError` (sem o texto da exceção).
    """
    match = _REF.match(ref)
    if not match:
        raise ConfigError(
            f"gancho '{ref}' inválido; use o formato modulo:funcao (ex.: custom.hooks:limpar)",
            operation="transform.hook",
        )
    module, func = match.groups()
    root = Path(project_root)
    arquivo = _module_file(root, module)
    if arquivo is None:
        esperado = "/".join(module.split("."))
        raise ConfigError(
            f"gancho '{ref}': módulo '{module}' (com a função '{func}') não encontrado em {root} "
            f"(esperado {esperado}.py ou {esperado}/__init__.py)",
            operation="transform.hook",
        )

    caminho_antes = list(sys.path)
    raizes_antes = list(_HOOK_ROOTS)
    modulos_antes = set(sys.modules)
    raiz = str(root)
    _purge_stale_modules(root)
    # um pacote regular de outra raiz antiga venceria um pacote-namespace desta: mantém só a raiz atual
    for antiga in _HOOK_ROOTS:
        if antiga in sys.path:
            sys.path.remove(antiga)
    _HOOK_ROOTS[:] = [raiz]
    if raiz in sys.path:
        sys.path.remove(raiz)
    sys.path.insert(0, raiz)
    importlib.invalidate_caches()
    try:
        mod = importlib.import_module(module)
    except Exception as exc:
        sys.path[:] = caminho_antes
        # a raiz anterior volta ao sys.path: continua registrada, para o próximo resolve_hook a retirar
        _HOOK_ROOTS[:] = [r for r in raizes_antes if r in sys.path]
        for key in set(sys.modules) - modulos_antes:
            del sys.modules[key]
        # um pacote (ex.: `custom`) que já existia com o mesmo nome foi trocado pelo desta raiz: esquece-o também
        _purge_stale_modules(root)
        raise HookImportError(
            f"gancho '{ref}': falha ao importar o módulo '{module}' ({type(exc).__name__}); o traceback está no log",
            operation="transform.hook",
        ) from exc

    fn = getattr(mod, func, None)
    if fn is None:
        publicas = sorted(n for n, v in vars(mod).items() if callable(v) and not n.startswith("_"))
        raise ConfigError(
            f"gancho '{ref}': o módulo '{module}' não define a função '{func}'; "
            f"funções encontradas: {', '.join(publicas) or 'nenhuma'}",
            operation="transform.hook",
        )
    if not callable(fn):
        raise ConfigError(f"gancho '{ref}': '{func}' em '{module}' não é uma função", operation="transform.hook")
    try:
        inspect.signature(fn).bind(object())
    except TypeError as exc:
        raise ConfigError(
            f"gancho '{ref}': a função '{func}' deve aceitar {contract}",
            operation="transform.hook",
        ) from exc
    except ValueError:
        pass  # assinatura não inspecionável (builtin): deixa para a chamada
    result: Hook = fn
    return result
