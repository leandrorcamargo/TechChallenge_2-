"""Renderização do relatório de qualidade (JSON e Markdown, sem ícones)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from quality.checks import ERROR, WARN
from quality.checks import CheckResult


def summarize(results: list[CheckResult]) -> dict:
    """Resumo agregado dos resultados."""
    errors = [r for r in results if r.blocking]
    warns = [r for r in results if (not r.passed) and r.severity == WARN]
    passed = [r for r in results if r.passed]
    return {
        "total": len(results),
        "passed": len(passed),
        "warnings": len(warns),
        "errors": len(errors),
        "gate": "FAIL" if errors else ("PASS_WITH_WARNINGS" if warns else "PASS"),
    }


def to_json(results: list[CheckResult], run_id: str, env: str) -> str:
    payload = {
        "run_id": run_id,
        "env": env,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summarize(results),
        "checks": [r.to_dict() for r in results],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _status(r: CheckResult) -> str:
    if r.passed:
        return "PASS"
    return "FAIL" if r.severity == ERROR else "WARN"


def to_markdown(results: list[CheckResult], run_id: str, env: str) -> str:
    s = summarize(results)
    lines = [
        "# Relatorio de Qualidade de Dados",
        "",
        f"- run_id: {run_id}",
        f"- ambiente: {env}",
        f"- gerado em: {datetime.now(timezone.utc).isoformat()}",
        "",
        f"Resultado do gate: **{s['gate']}** "
        f"(checks: {s['total']}, ok: {s['passed']}, avisos: {s['warnings']}, erros: {s['errors']})",
        "",
        "| Status | Severidade | Categoria | Tabela | Check | Observado | Limite | Detalhe |",
        "|--------|-----------|-----------|--------|-------|-----------|--------|---------|",
    ]
    # Falhas primeiro (erros, depois warnings), depois os que passaram.
    order = {"FAIL": 0, "WARN": 1, "PASS": 2}
    for r in sorted(results, key=lambda x: order[_status(x)]):
        lines.append(
            f"| {_status(r)} | {r.severity} | {r.category} | {r.table} | {r.name} "
            f"| {r.observed} | {r.threshold} | {r.message} |"
        )
    return "\n".join(lines) + "\n"
