"""Carregamento de configuração da pipeline.

Lê ``config/settings.yaml`` resolvendo placeholders ``${VAR:-default}`` a partir
das variáveis de ambiente (opcionalmente de um arquivo ``.env``). Expõe um objeto
``Settings`` simples com acesso por atributo/caminho, usado por todos os jobs.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

try:  # carregamento opcional do .env (não obrigatório em produção/Glue)
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

# Raiz do diretório projeto/ (dois níveis acima deste arquivo: common/config.py)
PROJECT_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = PROJECT_DIR.parent
CONFIG_PATH = PROJECT_DIR / "config" / "settings.yaml"

# ${VAR} ou ${VAR:-default}
_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _resolve_env(value: Any) -> Any:
    """Substitui recursivamente placeholders ``${VAR:-default}`` em strings."""
    if isinstance(value, str):
        def repl(match: re.Match) -> str:
            var, default = match.group(1), match.group(2)
            return os.environ.get(var, default if default is not None else "")

        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _resolve_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env(v) for v in value]
    return value


@dataclass
class Settings:
    """Configuração resolvida da pipeline com acesso por caminho pontual."""

    data: dict = field(default_factory=dict)

    def get(self, path: str, default: Any = None) -> Any:
        """Acessa um valor aninhado via caminho ``a.b.c``."""
        node: Any = self.data
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    # ---- atalhos convenientes -----------------------------------
    @property
    def env(self) -> str:
        return self.get("env", "local")

    @property
    def is_aws(self) -> bool:
        return self.env == "aws"

    @property
    def region(self) -> str:
        return self.get("project.region", "us-east-1")

    @property
    def proficiencia_corte(self) -> int:
        return int(self.get("business.proficiencia_corte", 743))


def load_settings(config_path: str | Path | None = None) -> Settings:
    """Carrega e resolve o ``settings.yaml``.

    Ordem: carrega ``.env`` (se existir e python-dotenv instalado) e então
    resolve os placeholders do YAML contra o ambiente já populado.
    """
    if load_dotenv is not None:
        env_file = PROJECT_DIR / "config" / ".env"
        if env_file.exists():
            load_dotenv(env_file)

    path = Path(config_path) if config_path else CONFIG_PATH
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return Settings(_resolve_env(raw))
