from datetime import datetime, timezone
from typing import Any, Dict, Optional

from formax import MiniAnnotated, Attrib, InitStrategy

from volnux.backends.formax_fk import (
    ForeignKeyField,
    FKConfig,
    ForeignKey,
    OnDelete,
)
from volnux.models.enums import WorkflowCategory, WorkflowStatus

from .base import GovernanceModel
from .users import User, Team, Organization
from .utils import pre_format_timestamps, post_format_timestamps


class Workflow(GovernanceModel):
    """The governed workflow artifact — the system of record.

    Reverse Relations:
        versions        — Version history
        variables — Declared variables
        descriptors — Descriptor labels
        executions — Execution history
        approval_steps — Approval chain steps
        hitl_requests — HITL requests
        trigger_configs — Trigger configurations
        break_glass_accesses — Break-glass access records
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    description: MiniAnnotated[Optional[str], Attrib(default=None)]
    category: MiniAnnotated[WorkflowCategory, Attrib(default=WorkflowCategory.STANDARD)]
    status: MiniAnnotated[WorkflowStatus, Attrib(default=WorkflowStatus.DRAFT)]
    pointy_lang_source: str
    compiled_graph: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]
    version: MiniAnnotated[str, Attrib(default="0.1.0")]
    mode: MiniAnnotated[str, Attrib(default="cfg")]

    organization: ForeignKeyField[
        Organization,
        FKConfig(reverse_name="workflows", on_delete=OnDelete.CASCADE),
    ]
    team: ForeignKeyField[
        Team,
        FKConfig(reverse_name="workflows", on_delete=OnDelete.PROTECT),
    ]
    created_by: ForeignKeyField[
        User,
        FKConfig(reverse_name="created_workflows", on_delete=OnDelete.PROTECT),
    ]
    updated_by: ForeignKeyField[
        User,
        FKConfig(
            nullable=True, reverse_name="updated_workflows", on_delete=OnDelete.SET_NULL
        ),
    ]
    approval_chain: ForeignKeyField[
        "volnux.models.ApprovalChain",
        FKConfig(nullable=True, reverse_name="workflows", on_delete=OnDelete.SET_NULL),
    ]

    published_at: MiniAnnotated[
        Optional[float],
        Attrib(
            default=None,
            pre_formatter=pre_format_timestamps,
            post_formatter=post_format_timestamps,
        ),
    ]

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def touch(self) -> None:
        self.updated_time = datetime.now(timezone.utc).timestamp()


class WorkflowVersion(GovernanceModel):
    """Immutable record of a published workflow version."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="versions", on_delete=OnDelete.CASCADE),
    ]
    version_number: str
    pointy_lang_source: str
    compiled_graph: Dict[str, Any]
    published_by: ForeignKeyField[
        User,
        FKConfig(
            reverse_name="published_workflow_versions", on_delete=OnDelete.PROTECT
        ),
    ]
    published_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    changelog: Optional[str] = None

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True


class WorkflowVariable(GovernanceModel):
    """Declared variables in a workflow definition."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="variables", on_delete=OnDelete.CASCADE),
    ]
    name: MiniAnnotated[str, Attrib(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    value: Any
    is_environment: bool = False

    class Config(GovernanceModel.Config):
        pass


class WorkflowDescriptor(GovernanceModel):
    """User-defined descriptor labels for conditional branching (3-9)."""

    workflow: ForeignKeyField[
        Workflow,
        FKConfig(reverse_name="descriptors", on_delete=OnDelete.CASCADE),
    ]
    descriptor_number: MiniAnnotated[int, Attrib(ge=3, le=9)]
    label: MiniAnnotated[str, Attrib(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")]

    class Config(GovernanceModel.Config):
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True
