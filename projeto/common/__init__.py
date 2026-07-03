"""Utilitários compartilhados da pipeline (config, Spark, logging, I/O).

Os símbolos são carregados de forma preguiçosa (PEP 562): importar ``common``
não puxa PySpark a menos que ``build_spark`` seja de fato acessado. Isso permite
que jobs sem Spark (ex.: producer Kinesis, relatórios de qualidade) usem apenas
config/logging sem exigir a instalação do PySpark.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "Settings",
    "load_settings",
    "get_logger",
    "build_spark",
    "layer_path",
    "read_source_csv",
]

_LAZY = {
    "Settings": ("common.config", "Settings"),
    "load_settings": ("common.config", "load_settings"),
    "get_logger": ("common.logging_setup", "get_logger"),
    "build_spark": ("common.spark", "build_spark"),
    "layer_path": ("common.io", "layer_path"),
    "read_source_csv": ("common.io", "read_source_csv"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        module_name, attr = _LAZY[name]
        import importlib

        module = importlib.import_module(module_name)
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
