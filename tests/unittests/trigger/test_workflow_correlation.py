"""Tests for the run-correlation seam.

``WorkflowConfig.run_workflow`` pops the reserved ``__execution_id__`` params key
and stamps it onto the pipeline (via ``change_object_id``) before the run
starts, so a dispatched run reports events under the same id the platform used
to create its ``Execution`` record.
"""

import asyncio
from unittest.mock import MagicMock

from volnux.engine.workflows.workflow import WorkflowConfig


class _FakePipeline:
    """Stands in for a Pipeline subclass: records constructor args and id change."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.changed_id = None
        self.started = False

    def change_object_id(self, oid):
        self.changed_id = oid

    async def start(self, force_rerun=False):
        self.started = True


class _ConcreteConfig(WorkflowConfig):
    """A concrete ``WorkflowConfig`` so ``object.__new__`` passes the ABC check."""

    def ready(self):
        pass


def _make_config(pipeline_class):
    cfg = object.__new__(_ConcreteConfig)
    cfg.name = "wf"
    cfg.check = MagicMock(return_value=[])
    cfg.get_pipeline_class = MagicMock(return_value=pipeline_class)
    return cfg


def test_run_workflow_stamps_the_execution_id_onto_the_pipeline():
    cfg = _make_config(_FakePipeline)

    pipeline = asyncio.run(
        cfg.run_workflow({"param": 1, "__execution_id__": "exec-123"})
    )

    assert pipeline.changed_id == "exec-123"
    assert pipeline.started is True
    # The reserved key is plumbing, not a workflow param — never leaked through.
    assert pipeline.kwargs == {"param": 1}


def test_run_workflow_leaves_the_id_alone_when_absent():
    cfg = _make_config(_FakePipeline)

    pipeline = asyncio.run(cfg.run_workflow({"param": 1}))

    assert pipeline.changed_id is None
    assert pipeline.started is True


def test_run_workflow_does_not_mutate_the_callers_params():
    cfg = _make_config(_FakePipeline)
    params = {"param": 1, "__execution_id__": "exec-123"}
    original = dict(params)

    asyncio.run(cfg.run_workflow(params))

    assert params == original
