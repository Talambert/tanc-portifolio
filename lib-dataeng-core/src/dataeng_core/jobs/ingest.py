"""Ponto de entrada de ingestão (`python -m dataeng_core.jobs.ingest`): como `run_step`, só para etapas `ingestao`."""

from __future__ import annotations

from collections.abc import Sequence

from dataeng_core.jobs.run_step import run_job


def main(argv: Sequence[str] | None = None) -> int:
    """Executa uma etapa de ingestão; etapas de outro tipo são recusadas (código 1)."""
    return run_job(
        argv, prog="ingest", description="Executa uma etapa de ingestão do pipeline.", required_type="ingestao"
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
