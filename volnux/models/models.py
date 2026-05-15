import hashlib
import json
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, Type

from formax import BaseModel, MiniAnnotated, Attrib, InitStrategy, ValidationFlags

from volnux.persistence.mixins import (
    KeyValueStoreIntegrationMixin,
    ObjectDoesNotExist,
    ObjectExistError,
    ProtectedError,
    SerializationError,
)
from volnux.persistence.fk import (
    ForeignKey,
    ForeignKeyField,
    OnDelete,
    _ReverseRelationDescriptor,
)


# ============================================================
# ENUMS
# ============================================================


# ============================================================
# BASE GOVERNANCE MODEL
# ============================================================


class GovernanceModel(KeyValueStoreIntegrationMixin, BaseModel):
    """Base model for all governance entities.

    Provides:
        - Self-persistence via KeyValueStoreIntegrationMixin
        - Automatic ForeignKey backreference registration
        - Standard timestamp fields
        - OrJSON serialization support
    """

    creation_time: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    updated_time: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]

    # is_persisted: bool = False
    # autosave: bool = False

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)

    def touch(self) -> None:
        """Update the updated_time to now."""
        self.updated_time = datetime.now(timezone.utc).timestamp()

    @property
    def success(self) -> bool:
        """Compatibility with EventResult pattern."""
        return True


# ============================================================
# ORGANIZATION & USER ENTITIES
# ============================================================


class Organization(GovernanceModel):
    """Top-level organization entity for multi-tenancy.

    Reverse Relations:
        users               — All users in this organization
        teams               — All teams in this organization
        workflows           — All workflows in this organization
        events              — All EventHub components in this organization
        namespaces          — All EventHub namespaces in this organization
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    slug: MiniAnnotated[str, Attrib(pattern=r"^[a-z0-9-]+$")]
    sso_config: Optional[Dict[str, Any]] = None
    policies: Dict[str, Any] = {}
    is_active: bool = True


class User(GovernanceModel):
    """User identity within an organization.

    Reverse Relations (auto-registered by ForeignKeyField):
        workflows_created_by        — Workflows created by this user
        workflows_updated_by        — Workflows last updated by this user
        workflows_approved_by       — Workflows approved by this user
        approval_steps_decided_by   — Approval decisions by this user
        audit_entries_actor_id      — Audit entries for actions by this user
        executions_triggered_by     — Executions triggered by this user
        hitl_requests_assigned_to   — HITL requests assigned to this user
        hitl_requests_resolved_by   — HITL requests resolved by this user
        delegations_delegator_id    — Delegations made by this user
        delegations_delegate_id     — Delegations received by this user
        break_glass_super_admin_id  — Break-glass accesses by this user
        notifications_user_id       — Notification configs for this user
        role_assignments_user_id    — Role assignments for this user
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    email: MiniAnnotated[
        str,
        Attrib(pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
    ]
    sso_identifier: Optional[str] = None
    is_active: bool = True

    # Foreign key to organization
    organization: ForeignKeyField(
        Organization,
        reverse_name="users",
        on_delete=OnDelete.CASCADE,
    )


class Team(GovernanceModel):
    """Team entity for scoped role boundaries.

    Reverse Relations:
        team_members_team_id    — Members of this team
        workflows_team_id       — Workflows owned by this team
        role_assignments_team_id — Role assignments scoped to this team
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    slug: MiniAnnotated[str, Attrib(pattern=r"^[a-z0-9-]+$")]
    description: Optional[str] = None
    namespace: str = "local"

    organization: ForeignKeyField(
        Organization,
        reverse_name="teams",
        on_delete=OnDelete.CASCADE,
    )


class TeamMember(GovernanceModel):
    """Many-to-many relationship between teams and users."""

    team: ForeignKeyField(
        Team,
        reverse_name="team_members",
        on_delete=OnDelete.CASCADE,
    )
    user: ForeignKeyField(
        User,
        reverse_name="team_memberships",
        on_delete=OnDelete.CASCADE,
    )


class Role(GovernanceModel):
    """Role definition with associated permissions.

    Reverse Relations:
        role_assignments_role_id — Assignments of this role
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=100)]
    slug: MiniAnnotated[str, Attrib(pattern=r"^[a-z0-9-]+$")]
    description: Optional[str] = None
    permissions: List[str] = {}


class RoleAssignment(GovernanceModel):
    """Assignment of a role to a user with scope boundaries.

    Reverse Relations:
        (none — leaf entity)
    """

    user: ForeignKeyField(
        User,
        reverse_name="role_assignments",
        on_delete=OnDelete.CASCADE,
    )
    role: ForeignKeyField(
        Role,
        reverse_name="role_assignments",
        on_delete=OnDelete.CASCADE,
    )
    scope_type: ScopeType = ScopeType.TEAM
    team: ForeignKeyField(
        Team,
        nullable=True,
        reverse_name="role_assignments",
        on_delete=OnDelete.SET_NULL,
    )
    environment: Optional[EnvironmentType] = None
    granted_by: ForeignKeyField(
        User,
        reverse_name="granted_role_assignments",
        on_delete=OnDelete.PROTECT,
    )
    revoked_at: Optional[float] = None


# ============================================================
# WORKFLOW ENTITIES
# ============================================================


class Workflow(GovernanceModel):
    """The governed workflow artifact — the system of record.

    Reverse Relations:
        workflow_versions_workflow_id   — Version history
        workflow_variables_workflow_id  — Declared variables
        workflow_descriptors_workflow_id — Descriptor labels
        executions_workflow_id          — Execution history
        approval_steps_workflow_id      — Approval chain steps
        hitl_requests_workflow_id       — HITL requests
        trigger_configs_workflow_id     — Trigger configurations
        break_glass_workflow_id         — Break-glass access records
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    description: Optional[str] = None
    category: WorkflowCategory = WorkflowCategory.STANDARD
    status: WorkflowStatus = WorkflowStatus.DRAFT
    pointy_lang_source: str
    compiled_graph: Optional[Dict[str, Any]] = None
    version: str = "0.1.0"
    mode: MiniAnnotated[str, Attrib(default="cfg")]

    # Foreign keys
    organization: ForeignKeyField(
        Organization,
        reverse_name="workflows",
        on_delete=OnDelete.CASCADE,
    )
    team: ForeignKeyField(
        Team,
        reverse_name="workflows",
        on_delete=OnDelete.PROTECT,
    )
    created_by: ForeignKeyField(
        User,
        reverse_name="created_workflows",
        on_delete=OnDelete.PROTECT,
    )
    updated_by: ForeignKeyField(
        User,
        nullable=True,
        reverse_name="updated_workflows",
        on_delete=OnDelete.SET_NULL,
    )
    approval_chain: ForeignKeyField(
        "ApprovalChain",
        nullable=True,
        reverse_name="workflows",
        on_delete=OnDelete.SET_NULL,
    )

    published_at: Optional[float] = None

    def touch(self) -> None:
        self.updated_time = datetime.now(timezone.utc).timestamp()


class WorkflowVersion(GovernanceModel):
    """Immutable record of a published workflow version."""

    workflow: ForeignKeyField(
        Workflow,
        reverse_name="versions",
        on_delete=OnDelete.CASCADE,
    )
    version_number: str
    pointy_lang_source: str
    compiled_graph: Dict[str, Any]
    published_by: ForeignKeyField(
        User,
        reverse_name="published_workflow_versions",
        on_delete=OnDelete.PROTECT,
    )
    published_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    changelog: Optional[str] = None


class WorkflowVariable(GovernanceModel):
    """Declared variables in a workflow definition."""

    workflow: ForeignKeyField(
        Workflow,
        reverse_name="variables",
        on_delete=OnDelete.CASCADE,
    )
    name: MiniAnnotated[str, Attrib(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    value: Any
    is_environment: bool = False


class WorkflowDescriptor(GovernanceModel):
    """User-defined descriptor labels for conditional branching (3-9)."""

    workflow: ForeignKeyField(
        Workflow,
        reverse_name="descriptors",
        on_delete=OnDelete.CASCADE,
    )
    descriptor_number: MiniAnnotated[int, Attrib(ge=3, le=9)]
    label: MiniAnnotated[str, Attrib(pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")]


# ============================================================
# APPROVAL ENTITIES
# ============================================================


class ApprovalChain(GovernanceModel):
    """Configurable approval chain definition.

    Reverse Relations:
        workflows_approval_chain_id — Workflows using this chain
        approval_steps_chain_id     — Steps in this chain
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    organization: ForeignKeyField(
        Organization,
        reverse_name="approval_chains",
        on_delete=OnDelete.CASCADE,
    )
    applicable_categories: List[WorkflowCategory] = {}
    is_active: bool = True
    failure_behavior: str = "reset_chain"
    require_all_steps_in_order: bool = True
    created_by: ForeignKeyField(
        User,
        reverse_name="created_approval_chains",
        on_delete=OnDelete.PROTECT,
    )


class ApprovalStep(GovernanceModel):
    """A single step within an approval chain.

    Reverse Relations:
        (none — leaf entity)
    """

    chain: ForeignKeyField(
        ApprovalChain,
        reverse_name="steps",
        on_delete=OnDelete.CASCADE,
    )
    workflow: ForeignKeyField(
        Workflow,
        reverse_name="approval_steps",
        on_delete=OnDelete.CASCADE,
    )
    step_order: MiniAnnotated[int, Attrib(ge=1)]
    approver_type: ApproverType
    role: ForeignKeyField(
        Role,
        nullable=True,
        reverse_name="approval_steps",
        on_delete=OnDelete.SET_NULL,
    )
    individual: ForeignKeyField(
        User,
        nullable=True,
        reverse_name="approval_steps_assigned",
        on_delete=OnDelete.SET_NULL,
    )
    team: ForeignKeyField(
        Team,
        nullable=True,
        reverse_name="approval_steps",
        on_delete=OnDelete.SET_NULL,
    )
    required_count: int = 1
    is_mandatory: bool = True
    timeout_hours: Optional[int] = None
    description: Optional[str] = None
    status: ApprovalStatus = ApprovalStatus.PENDING
    decided_by: ForeignKeyField(
        User,
        nullable=True,
        reverse_name="decided_approval_steps",
        on_delete=OnDelete.SET_NULL,
    )
    decision: Optional[str] = None
    comments: Optional[str] = None
    decided_at: Optional[float] = None
    requires_human_readable_review: bool = False


# ============================================================
# EXECUTION ENTITIES
# ============================================================


class Execution(GovernanceModel):
    """A single workflow execution instance.

    Reverse Relations:
        execution_traces_execution_id   — Execution traces
        hitl_requests_execution_id      — HITL requests
        break_glass_execution_id        — Break-glass access records
    """

    workflow: ForeignKeyField(
        Workflow,
        reverse_name="executions",
        on_delete=OnDelete.CASCADE,
    )
    workflow_version: ForeignKeyField(
        WorkflowVersion,
        reverse_name="executions",
        on_delete=OnDelete.PROTECT,
    )
    organization: ForeignKeyField(
        Organization,
        reverse_name="executions",
        on_delete=OnDelete.CASCADE,
    )
    team: ForeignKeyField(
        Team,
        reverse_name="executions",
        on_delete=OnDelete.PROTECT,
    )
    status: ExecutionState = ExecutionState.RUNNING
    triggered_by: ForeignKeyField(
        User,
        reverse_name="triggered_executions",
        on_delete=OnDelete.SET_NULL,
    )
    trigger_type: TriggerType
    trigger_params: Dict[str, Any] = {}
    execution_params: Dict[str, Any] = {}
    started_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    ended_at: Optional[float] = None
    checkpoint_ref: Optional[str] = None
    parent_execution: ForeignKeyField(
        "Execution",
        nullable=True,
        reverse_name="child_executions",
        on_delete=OnDelete.SET_NULL,
    )


class ExecutionTrace(GovernanceModel):
    """A single step in the fractal execution tree."""

    execution: ForeignKeyField(
        Execution,
        reverse_name="traces",
        on_delete=OnDelete.CASCADE,
    )
    event_name: str
    event_version: str
    node: ForeignKeyField(
        "MeshNode",
        nullable=True,
        reverse_name="execution_traces",
        on_delete=OnDelete.SET_NULL,
    )
    parent_trace: ForeignKeyField(
        "ExecutionTrace",
        nullable=True,
        reverse_name="child_traces",
        on_delete=OnDelete.SET_NULL,
    )
    step_order: int = 0
    status: ExecutionState = ExecutionState.RUNNING
    input_data: Optional[Dict[str, Any]] = None
    output_data: Optional[Dict[str, Any]] = None
    error_data: Optional[Dict[str, Any]] = None
    descriptor: Optional[int] = None
    retry_count: int = 0
    max_retries: int = 0
    started_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    ended_at: Optional[float] = None
    checkpoint_ref: Optional[str] = None
    otel_trace_id: Optional[str] = None


# ============================================================
# HITL ENTITIES
# ============================================================


class HITLRequest(GovernanceModel):
    """Human-in-the-Loop request during workflow execution.

    Reverse Relations:
        (none — leaf entity)
    """

    execution: ForeignKeyField(
        Execution,
        reverse_name="hitl_requests",
        on_delete=OnDelete.CASCADE,
    )
    trace: ForeignKeyField(
        ExecutionTrace,
        reverse_name="hitl_requests",
        on_delete=OnDelete.CASCADE,
    )
    workflow: ForeignKeyField(
        Workflow,
        reverse_name="hitl_requests",
        on_delete=OnDelete.PROTECT,
    )
    node_id: str
    status: HITLStatus = HITLStatus.PENDING
    priority: HITLPriority = HITLPriority.MEDIUM
    assigned_role: Optional[str] = None
    assigned_team: ForeignKeyField(
        Team,
        nullable=True,
        reverse_name="hitl_requests",
        on_delete=OnDelete.SET_NULL,
    )
    assigned_user: ForeignKeyField(
        User,
        nullable=True,
        reverse_name="assigned_hitl_requests",
        on_delete=OnDelete.SET_NULL,
    )
    eligible_users: List[str] = {}
    prompt: str
    context_data: Dict[str, Any] = {}
    available_actions: List[str] = {}
    required_inputs: List[Dict[str, Any]] = {}
    sla_deadline: Optional[float] = None
    sla_warning_threshold: float = 0.8
    sla_breach_action: str = "escalate_to_super_admin"
    responded_at: Optional[float] = None
    resolved_by: ForeignKeyField(
        User,
        nullable=True,
        reverse_name="resolved_hitl_requests",
        on_delete=OnDelete.SET_NULL,
    )
    decision: Optional[str] = None
    decision_metadata: Dict[str, Any] = {}
    escalation_count: int = 0
    notification_channels: List[str] = {}


# ============================================================
# AUDIT ENTITIES
# ============================================================


class AuditEntry(GovernanceModel):
    """Immutable audit log entry with cryptographic chain.

    Reverse Relations:
        (none — leaf entity, never deleted)
    """

    organization: ForeignKeyField(
        Organization,
        reverse_name="audit_entries",
        on_delete=OnDelete.PROTECT,
    )
    event_type: AuditEventType
    actor: ForeignKeyField(
        User,
        reverse_name="audit_entries",
        on_delete=OnDelete.PROTECT,
    )
    actor_role: str
    target_type: str
    target_id: str
    action: str
    metadata: Dict[str, Any] = {}
    hash: str = ""
    previous_hash: Optional[str] = None


# ============================================================
# DELEGATION ENTITIES
# ============================================================


class Delegation(GovernanceModel):
    """Time-bounded role delegation.

    Reverse Relations:
        delegation_actions_delegation_id — Actions taken under this delegation
    """

    organization: ForeignKeyField(
        Organization,
        reverse_name="delegations",
        on_delete=OnDelete.CASCADE,
    )
    delegator: ForeignKeyField(
        User,
        reverse_name="delegations_made",
        on_delete=OnDelete.CASCADE,
    )
    delegate: ForeignKeyField(
        User,
        reverse_name="delegations_received",
        on_delete=OnDelete.CASCADE,
    )
    role: ForeignKeyField(
        Role,
        reverse_name="delegations",
        on_delete=OnDelete.CASCADE,
    )
    start_time: float
    end_time: float
    status: DelegationStatus = DelegationStatus.ACTIVE
    scope_team: ForeignKeyField(
        Team,
        nullable=True,
        reverse_name="delegations",
        on_delete=OnDelete.SET_NULL,
    )
    scope_environment: Optional[EnvironmentType] = None
    reason: Optional[str] = None
    revoked_by: ForeignKeyField(
        User,
        nullable=True,
        reverse_name="revoked_delegations",
        on_delete=OnDelete.SET_NULL,
    )
    revoked_at: Optional[float] = None


class DelegationAction(GovernanceModel):
    """Record of an action taken under a delegation."""

    delegation: ForeignKeyField(
        Delegation,
        reverse_name="actions",
        on_delete=OnDelete.CASCADE,
    )
    action_type: str
    target_type: str
    target_id: str
    metadata: Dict[str, Any] = {}


# ============================================================
# BREAK-GLASS ENTITIES
# ============================================================


class BreakGlassAccess(GovernanceModel):
    """Emergency break-glass access record.

    Reverse Relations:
        break_glass_actions_access_id — Actions taken during this session
    """

    super_admin: ForeignKeyField(
        User,
        reverse_name="break_glass_accesses",
        on_delete=OnDelete.PROTECT,
    )
    organization: ForeignKeyField(
        Organization,
        reverse_name="break_glass_accesses",
        on_delete=OnDelete.PROTECT,
    )
    workflow: ForeignKeyField(
        Workflow,
        nullable=True,
        reverse_name="break_glass_accesses",
        on_delete=OnDelete.SET_NULL,
    )
    execution: ForeignKeyField(
        Execution,
        nullable=True,
        reverse_name="break_glass_accesses",
        on_delete=OnDelete.SET_NULL,
    )
    justification: MiniAnnotated[str, Attrib(min_length=10, max_length=2000)]
    access_time: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]
    review_status: BreakGlassReviewStatus = BreakGlassReviewStatus.PENDING
    review_findings: Optional[str] = None
    review_remediation: Optional[str] = None
    reviewed_by: ForeignKeyField(
        User,
        nullable=True,
        reverse_name="reviewed_break_glass",
        on_delete=OnDelete.SET_NULL,
    )
    reviewed_at: Optional[float] = None


class BreakGlassAction(GovernanceModel):
    """Record of actions taken during a break-glass session."""

    access: ForeignKeyField(
        BreakGlassAccess,
        reverse_name="actions",
        on_delete=OnDelete.CASCADE,
    )
    action_type: str
    target_type: str
    target_id: str
    metadata: Dict[str, Any] = {}


# ============================================================
# EVENT ENTITIES (EventHub)
# ============================================================


class Event(GovernanceModel):
    """EventBase component in the EventHub registry.

    Reverse Relations:
        event_versions_event_id    — Version history
        event_dependencies_event_id — Dependencies on this event
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    namespace: NamespaceType = NamespaceType.LOCAL
    organization: ForeignKeyField(
        Organization,
        reverse_name="events",
        on_delete=OnDelete.CASCADE,
    )
    team: ForeignKeyField(
        Team,
        nullable=True,
        reverse_name="events",
        on_delete=OnDelete.SET_NULL,
    )
    version: str = "0.1.0"
    status: EventStatus = EventStatus.PUBLISHED
    manifest: Dict[str, Any]
    publisher: ForeignKeyField(
        User,
        reverse_name="published_events",
        on_delete=OnDelete.PROTECT,
    )


class EventVersion(GovernanceModel):
    """Immutable record of an event version."""

    event: ForeignKeyField(
        Event,
        reverse_name="versions",
        on_delete=OnDelete.CASCADE,
    )
    version_number: str
    manifest: Dict[str, Any]
    changelog: Optional[str] = None
    compatibility_matrix: Optional[Dict[str, Any]] = None
    published_by: ForeignKeyField(
        User,
        reverse_name="published_event_versions",
        on_delete=OnDelete.PROTECT,
    )
    published_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]


class EventDependency(GovernanceModel):
    """Declared dependencies between events."""

    event: ForeignKeyField(
        Event,
        reverse_name="dependencies",
        on_delete=OnDelete.CASCADE,
    )
    depends_on_event: ForeignKeyField(
        Event,
        reverse_name="dependents",
        on_delete=OnDelete.CASCADE,
    )
    version_constraint: str = "*"


class Namespace(GovernanceModel):
    """EventHub namespace configuration."""

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    namespace_type: NamespaceType
    organization: ForeignKeyField(
        Organization,
        reverse_name="namespaces",
        on_delete=OnDelete.CASCADE,
    )
    team: ForeignKeyField(
        Team,
        nullable=True,
        reverse_name="namespaces",
        on_delete=OnDelete.SET_NULL,
    )
    config: Dict[str, Any] = {}
    is_private: bool = False


# ============================================================
# MESH NODE ENTITIES
# ============================================================


class MeshNode(GovernanceModel):
    """Execution node in the P2P mesh.

    Reverse Relations:
        execution_traces_node_id  — Execution traces on this node
        node_heartbeats_node_id   — Heartbeat history
    """

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    organization: ForeignKeyField(
        Organization,
        reverse_name="mesh_nodes",
        on_delete=OnDelete.CASCADE,
    )
    node_type: NodeType = NodeType.GENERAL
    status: NodeStatus = NodeStatus.OFFLINE
    region: Optional[str] = None
    endpoint: Optional[str] = None
    mtls_cert_fingerprint: Optional[str] = None
    capacity_max_concurrent_tasks: MiniAnnotated[int, Attrib(ge=1)] = 10
    capacity_cpu_cores: Optional[int] = None
    capacity_memory_gb: Optional[float] = None
    capacity_disk_gb: Optional[float] = None
    current_load: int = 0
    registered_by: ForeignKeyField(
        User,
        reverse_name="registered_nodes",
        on_delete=OnDelete.PROTECT,
    )
    last_heartbeat: Optional[float] = None
    decommissioned_by: ForeignKeyField(
        User,
        nullable=True,
        reverse_name="decommissioned_nodes",
        on_delete=OnDelete.SET_NULL,
    )
    decommissioned_at: Optional[float] = None


class NodeHeartbeat(GovernanceModel):
    """Heartbeat record for node health monitoring."""

    node: ForeignKeyField(
        MeshNode,
        reverse_name="heartbeats",
        on_delete=OnDelete.CASCADE,
    )
    status: NodeStatus
    current_load: int
    cpu_utilization: Optional[float] = None
    memory_utilization: Optional[float] = None
    disk_utilization: Optional[float] = None
    recorded_at: MiniAnnotated[
        float,
        Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp()),
    ]


# ============================================================
# TRIGGER ENTITIES
# ============================================================


class TriggerConfig(GovernanceModel):
    """Trigger configuration for workflow activation."""

    workflow: ForeignKeyField(
        Workflow,
        reverse_name="trigger_configs",
        on_delete=OnDelete.CASCADE,
    )
    organization: ForeignKeyField(
        Organization,
        reverse_name="trigger_configs",
        on_delete=OnDelete.CASCADE,
    )
    trigger_type: TriggerType
    config: Dict[str, Any] = {}
    is_active: bool = True
    created_by: ForeignKeyField(
        User,
        reverse_name="created_triggers",
        on_delete=OnDelete.PROTECT,
    )


# ============================================================
# NOTIFICATION ENTITIES
# ============================================================


class NotificationConfig(GovernanceModel):
    """User notification preferences."""

    user: ForeignKeyField(
        User,
        reverse_name="notification_configs",
        on_delete=OnDelete.CASCADE,
    )
    organization: ForeignKeyField(
        Organization,
        reverse_name="notification_configs",
        on_delete=OnDelete.CASCADE,
    )
    channel: NotificationChannel
    config: Dict[str, Any] = {}
    is_active: bool = True
    event_types: List[str] = {}
