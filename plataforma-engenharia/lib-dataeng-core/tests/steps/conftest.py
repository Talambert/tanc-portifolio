from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from tests.steps.helpers import RID1, environment, write_project

from dataeng_core.base.pipeline import Pipeline
from dataeng_core.steps import StepContext, build_context

MakeCtx = Callable[..., StepContext]


@pytest.fixture
def project(tmp_path: Path) -> Path:
    write_project(tmp_path)
    return tmp_path


@pytest.fixture
def make_ctx(project: Path) -> Iterator[MakeCtx]:
    created: list[StepContext] = []

    def factory(pipeline: Pipeline, run_id: str = RID1) -> StepContext:
        ctx = build_context(pipeline, environment(), project_root=project, run_id=run_id)
        created.append(ctx)
        return ctx

    yield factory
    for ctx in created:
        close = getattr(ctx.warehouse, "close", None)
        if close is not None:
            close()
