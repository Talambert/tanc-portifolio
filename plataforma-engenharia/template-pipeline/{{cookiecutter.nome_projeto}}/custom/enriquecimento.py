"""Etapa `python` de EXEMPLO: classifica cada nome do agregado como `alto` ou `baixo` (em relação à média).

Referenciada no YAML como `custom.enriquecimento:rodar`. A função recebe o contexto da etapa (`StepContext`:
warehouse, parâmetros, run_id...) e devolve um `StepResult` (ou `None`, que vira `ok`). É idempotente: grava
com `merge` por `nome`.
"""

from dataeng_core.steps.context import StepContext, StepResult
from dataeng_core.warehouse.base import validate_identifier


def rodar(ctx: StepContext) -> StepResult:
    gold = validate_identifier(ctx.params["dataset_gold"], what="dataset_gold", operation="enriquece")
    destino = f"{gold}.agregado_classificado"
    ctx.warehouse.execute_ddl(
        f"CREATE TABLE IF NOT EXISTS {destino} (nome STRING, total DECIMAL(18, 2), classe STRING)"
    )
    agregado = ctx.warehouse.query(f"SELECT nome, total FROM {gold}.agregado_por_nome")
    if agregado.empty:
        return StepResult("enriquece", "ok", detail="agregado vazio; nada a classificar")
    media = agregado["total"].astype(float).mean()
    agregado["classe"] = ["alto" if float(total) > media else "baixo" for total in agregado["total"]]
    gravadas = ctx.warehouse.load(destino, agregado, mode="merge", merge_keys=["nome"])
    return StepResult("enriquece", "ok", rows_read=len(agregado), rows_written=gravadas)
