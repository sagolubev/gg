from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

UTC_TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"

TASK_STATES = frozenset(
    {
        "ExternalTaskReady",
        "Claiming",
        "Queued",
        "RunStarted",
        "TaskAnalysis",
        "Blocked",
        "ReadyForExecution",
        "AgentSelection",
        "AgentRunning",
        "ResultEvaluation",
        "NeedsInput",
        "OutcomePublishing",
        "Completed",
        "TerminalFailure",
        "Cancelled",
    }
)

CANDIDATE_STATUSES = frozenset(
    {
        "running",
        "success",
        "failed",
        "timeout",
        "needs_input",
        "security_violation",
        "resource_exceeded",
        "setup_failed",
    }
)

CHECK_STATUSES = frozenset({"passed", "failed", "timeout", "skipped", "flaky"})


class CompatibleArtifactModel(BaseModel):
    """Base for durable artifacts where additive fields must not break resume."""

    model_config = ConfigDict(extra="ignore")


class StrictArtifactModel(BaseModel):
    """Base for newly generated artifacts with a narrow contract."""

    model_config = ConfigDict(extra="forbid")


class SandboxPolicyModel(StrictArtifactModel):
    allowed_domains: list[str] = Field(default_factory=list)
    denied_domains: list[str] = Field(default_factory=list)
    deny_read: list[str] = Field(default_factory=lambda: ["~/.ssh", ".env"])
    allow_write: list[str] = Field(default_factory=lambda: ["."])
    deny_write: list[str] = Field(default_factory=lambda: [".env"])


class GitConfigModel(StrictArtifactModel):
    default_branch: str = "main"
    author_name: str = "gg-orchestrator"
    author_email: str = "gg-orchestrator@users.noreply.local"


class TaskSystemConfigModel(StrictArtifactModel):
    platform: str = "auto"
    work_label: str = "gg:in-progress"
    done_label: str = "gg:done"
    blocked_label: str = "gg:blocked"


class SelectionConfigModel(StrictArtifactModel):
    include_labels: tuple[str, ...] = ("ai-ready",)
    exclude_labels: tuple[str, ...] = ("gg:in-progress", "gg:blocked", "gg:done")


class RuntimeResourceConfigModel(StrictArtifactModel):
    max_disk_mb: int = Field(default=4096, ge=1)
    disk_poll_interval_seconds: int = Field(default=30, ge=1)
    allow_candidate_downscale: bool = False
    allow_network_fs: bool = False
    allow_unsafe_fs: bool = False


class RuntimeNetworkConfigModel(StrictArtifactModel):
    default: Literal["deny", "allow"] = "deny"
    allowed_hosts: tuple[str, ...] = ()


class RuntimeConfigModel(StrictArtifactModel):
    agent_backend: str = "codex"
    candidates: int = Field(default=1, ge=1)
    max_parallel_candidates: int = Field(default=1, ge=1)
    max_parallel_runs: int = Field(default=1, ge=1)
    max_attempts: int = Field(default=1, ge=1)
    repair_candidates: int = Field(default=1, ge=1)
    use_sandbox_runtime: bool = True
    require_sandbox_runtime: bool = False
    allow_unsafe_direct_exec: bool = False
    analysis_timeout_seconds: int = Field(default=900, ge=1)
    evaluation_timeout_seconds: int = Field(default=900, ge=1)
    candidate_timeout_seconds: int = Field(default=1800, ge=1)
    command_timeout_seconds: int = Field(default=600, ge=1)
    setup_timeout_seconds: int = Field(default=600, ge=1)
    resource: RuntimeResourceConfigModel = Field(default_factory=RuntimeResourceConfigModel)
    network: RuntimeNetworkConfigModel = Field(default_factory=RuntimeNetworkConfigModel)
    port_range: tuple[int, int] = (41000, 45000)
    sandbox_policy: SandboxPolicyModel = Field(default_factory=SandboxPolicyModel)

    @field_validator("port_range")
    @classmethod
    def _port_range(cls, value: tuple[int, int]) -> tuple[int, int]:
        start, end = value
        if start < 1 or end > 65535 or start >= end:
            raise ValueError("port_range must be two increasing TCP ports between 1 and 65535")
        return value

    @model_validator(mode="after")
    def _derive_sandbox_requirement(self) -> "RuntimeConfigModel":
        self.require_sandbox_runtime = not self.allow_unsafe_direct_exec
        return self


class VerifyConfigModel(StrictArtifactModel):
    setup: str = ""
    tests: str = ""
    lint: str = ""
    typecheck: str = ""
    security: str = ""
    custom: tuple[str, ...] = ()
    discovery_enabled: bool = True
    test_retry_count: int = Field(default=0, ge=0)
    allow_known_baseline_failures: bool = False
    block_on_security_high: bool = True


class AuditConfigModel(StrictArtifactModel):
    hash_events: bool = False
    external_sink: str = ""


class SecurityConfigModel(StrictArtifactModel):
    allow_lfs_changes: bool = False
    allow_binary_changes: bool = True
    allow_dependency_changes: bool = True


class CleanupConfigModel(StrictArtifactModel):
    blocked_timeout_days: int | None = Field(default=14, ge=0)


class LogConfigModel(StrictArtifactModel):
    max_size_mb: int = Field(default=50, ge=1)
    max_command_log_chars: int = Field(default=200000, ge=1)
    mask_secrets: bool = True


class CostConfigModel(StrictArtifactModel):
    enabled: bool = False
    mode: str = "duration-only"
    max_usd_per_run: float | None = Field(default=None, ge=0)
    max_tokens_per_run: int | None = Field(default=None, ge=0)


class AnalysisConfigModel(StrictArtifactModel):
    max_context_tokens: int = Field(default=60000, ge=1)
    max_issue_body_chars: int = Field(default=12000, ge=1)
    max_summary_chars: int = Field(default=1200, ge=1)
    max_project_context_chars: int = Field(default=12000, ge=1)
    max_comments: int = Field(default=10, ge=0)
    max_comment_body_chars: int = Field(default=2000, ge=1)
    max_inputs: int = Field(default=10, ge=0)
    max_input_message_chars: int = Field(default=2000, ge=1)
    max_agent_response_chars: int = Field(default=12000, ge=1)


class EvaluationConfigModel(StrictArtifactModel):
    max_context_tokens: int = Field(default=60000, ge=1)
    max_diff_lines_per_candidate: int = Field(default=2000, ge=1)
    max_log_chars_per_check: int = Field(default=12000, ge=1)
    max_total_log_chars: int = Field(default=50000, ge=1)
    prefer_deterministic_when_truncated: bool = True


class CIConfigModel(StrictArtifactModel):
    mode: bool = False
    default_dry_run: bool = False
    forbid_interactive_prompts: bool = True
    clock_skew_tolerance_seconds: int = Field(default=5, ge=0)
    clock_drift_warn_seconds: int = Field(default=60, ge=0)


class RecoveryConfigModel(StrictArtifactModel):
    keep_state_backup: bool = True


class GGConfigModel(StrictArtifactModel):
    git: GitConfigModel
    task_system: TaskSystemConfigModel = Field(default_factory=TaskSystemConfigModel)
    selection: SelectionConfigModel = Field(default_factory=SelectionConfigModel)
    verify: VerifyConfigModel = Field(default_factory=VerifyConfigModel)
    runtime: RuntimeConfigModel = Field(default_factory=RuntimeConfigModel)
    audit: AuditConfigModel = Field(default_factory=AuditConfigModel)
    security: SecurityConfigModel = Field(default_factory=SecurityConfigModel)
    cleanup: CleanupConfigModel = Field(default_factory=CleanupConfigModel)
    log: LogConfigModel = Field(default_factory=LogConfigModel)
    cost: CostConfigModel = Field(default_factory=CostConfigModel)
    analysis: AnalysisConfigModel = Field(default_factory=AnalysisConfigModel)
    evaluation: EvaluationConfigModel = Field(default_factory=EvaluationConfigModel)
    ci: CIConfigModel = Field(default_factory=CIConfigModel)
    recovery: RecoveryConfigModel = Field(default_factory=RecoveryConfigModel)


class RunTransitionModel(CompatibleArtifactModel):
    from_state: str = Field(alias="from")
    to_state: str = Field(alias="to")
    at: str = ""
    reason: str = ""

    @field_validator("from_state", "to_state")
    @classmethod
    def _state_value(cls, value: str) -> str:
        if value not in TASK_STATES:
            raise ValueError(f"unknown state: {value}")
        return value

    @field_validator("at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_optional_timestamp(value)


class CandidateStateModel(CompatibleArtifactModel):
    status: str
    worktree_path: str = ""
    branch: str = ""
    result_path: str | None = None
    started_at: str = ""
    finished_at: str | None = None
    error: str | None = None
    agent_pid: int | None = Field(default=None, ge=1)
    sandbox_pid: int | None = Field(default=None, ge=1)

    @field_validator("status")
    @classmethod
    def _candidate_status(cls, value: str) -> str:
        if value not in CANDIDATE_STATUSES:
            raise ValueError(f"unknown candidate status: {value}")
        return value

    @field_validator("started_at")
    @classmethod
    def _started_timestamp(cls, value: str) -> str:
        return _validate_optional_timestamp(value)

    @field_validator("finished_at")
    @classmethod
    def _finished_timestamp(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_optional_timestamp(value)


class BaselineStateModel(CompatibleArtifactModel):
    status: str = ""
    commit: str = ""
    worktree_path: str = ""
    verification_path: str = ""
    failed_commands: list[str] = Field(default_factory=list)
    checked_at: str = ""

    @field_validator("checked_at")
    @classmethod
    def _checked_timestamp(cls, value: str) -> str:
        return _validate_optional_timestamp(value)


class CostAggregateModel(CompatibleArtifactModel):
    total_usd: float | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    duration_seconds: float | None = Field(default=None, ge=0)
    events: int | None = Field(default=None, ge=0)


class RunStateModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    run_id: str
    issue: dict[str, Any]
    state: str = "ExternalTaskReady"
    attempt: int = Field(default=1, ge=1)
    max_attempts: int = Field(default=1, ge=1)
    created_at: str
    updated_at: str
    baseline: BaselineStateModel | dict[str, Any] = Field(default_factory=dict)
    candidate_states: dict[str, CandidateStateModel] = Field(default_factory=dict)
    stage_attempts: dict[str, int] = Field(default_factory=dict)
    locks: dict[str, Any] = Field(default_factory=dict)
    operator: dict[str, Any] = Field(default_factory=dict)
    cost: CostAggregateModel | dict[str, Any] | None = None
    artifacts: dict[str, str] = Field(default_factory=dict)
    transitions: list[RunTransitionModel] = Field(default_factory=list)
    last_error: dict[str, Any] | None = None
    pr_url: str | None = None
    dry_run: bool = False
    publishing_step: str | None = None
    cancel_requested: bool = False
    blocked_resume_state: str | None = None
    blocked_until: str | None = None

    @field_validator("state")
    @classmethod
    def _state_value(cls, value: str) -> str:
        if value not in TASK_STATES:
            raise ValueError(f"unknown state: {value}")
        return value

    @field_validator("blocked_resume_state")
    @classmethod
    def _blocked_resume_state(cls, value: str | None) -> str | None:
        if value is not None and value not in TASK_STATES:
            raise ValueError(f"unknown blocked_resume_state: {value}")
        return value

    @field_validator("created_at", "updated_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)

    @field_validator("blocked_until")
    @classmethod
    def _blocked_until_timestamp(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_optional_timestamp(value)

    @field_validator("stage_attempts")
    @classmethod
    def _stage_attempts(cls, value: dict[str, int]) -> dict[str, int]:
        invalid = [stage for stage, attempts in value.items() if attempts < 0]
        if invalid:
            raise ValueError(f"stage attempts must be non-negative: {', '.join(sorted(invalid))}")
        return value


class IssueCommentModel(CompatibleArtifactModel):
    author: str = ""
    created_at: str = ""
    url: str = ""
    body: str = ""


class LocalInputSummaryModel(CompatibleArtifactModel):
    source: str = ""
    sequence_number: int = 0
    answered_state: str = ""
    created_at: str = ""
    message: str = ""


class TaskBriefModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    issue: dict[str, Any]
    summary: str
    acceptance_criteria: list[str] = Field(default_factory=list)
    project_context: str = ""
    constraints: list[str] = Field(default_factory=list)
    blocked: bool = False
    missing_questions: list[str] = Field(default_factory=list)
    candidate_files: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    verification_hints: list[str] = Field(default_factory=list)
    context_budget: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _required_issue_fields(self) -> "TaskBriefModel":
        missing = [field for field in ("number", "title") if field not in self.issue]
        if missing:
            raise ValueError(f"issue missing required fields: {', '.join(missing)}")
        return self


class RawIssueArtifactModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    issue: dict[str, Any]
    comments: list[IssueCommentModel] = Field(default_factory=list)
    inputs: list[LocalInputSummaryModel] = Field(default_factory=list)
    limits: dict[str, Any] = Field(default_factory=dict)
    truncated: dict[str, bool] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _required_issue_fields(self) -> "RawIssueArtifactModel":
        missing = [field for field in ("number", "title") if field not in self.issue]
        if missing:
            raise ValueError(f"issue missing required fields: {', '.join(missing)}")
        return self


class AnalysisResultModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    ready: bool = True
    missing_questions: list[str] = Field(default_factory=list)
    summary: str = ""
    acceptance_criteria: list[str] = Field(default_factory=list)
    candidate_files: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    verification_hints: list[str] = Field(default_factory=list)
    context_budget: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _blocked_requires_question(self) -> "AnalysisResultModel":
        if not self.ready and not self.missing_questions:
            raise ValueError("blocked analysis must include missing_questions")
        return self


class AnalysisAgentResponseModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    ok: bool = False
    error: str = ""
    response: str = ""
    truncated: bool = False
    limits: dict[str, Any] = Field(default_factory=dict)
    created_at: str

    @field_validator("created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class CheckResultModel(CompatibleArtifactModel):
    command: str
    status: str
    exit_code: int | None
    id: str = ""
    category: str = "custom"
    required: bool = True
    stdout: str = ""
    stderr: str = ""
    stdout_path: str = ""
    stderr_path: str = ""
    duration_ms: int | None = Field(default=None, ge=0)
    truncated: bool = False
    encoding_errors: bool = False
    baseline_status: str | None = None
    findings: list[dict[str, Any]] = Field(default_factory=list)
    attempts: int = Field(default=1, ge=0)
    flaky: bool = False

    @field_validator("status")
    @classmethod
    def _status(cls, value: str) -> str:
        if value not in CHECK_STATUSES:
            raise ValueError(f"unknown check status: {value}")
        return value


class VerificationArtifactModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    checks: list[CheckResultModel] = Field(default_factory=list)
    failed_commands: list[str] = Field(default_factory=list)
    baseline_status: str | None = None
    required_passed: bool | None = None
    advisory_failed_commands: list[str] = Field(default_factory=list)
    findings: list[dict[str, Any]] = Field(default_factory=list)


class AgentHandoffModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    run_id: str = ""
    candidate_id: str = ""
    issue: dict[str, Any] = Field(default_factory=dict)
    attempt: int = Field(default=1, ge=1)
    created_at: str = ""
    worktree_path: str = ""
    base_commit: str = ""
    task_brief_path: str = ""
    context_snapshot_path: str = ""
    instructions: str = ""
    artifacts: dict[str, str] = Field(default_factory=dict)

    @field_validator("created_at")
    @classmethod
    def _created_timestamp(cls, value: str) -> str:
        return _validate_optional_timestamp(value)


class AgentResultModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    run_id: str = ""
    candidate_id: str = ""
    status: str
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float | None = Field(default=None, ge=0)
    exit_code: int | None = None
    summary: str = ""
    error: str | None = None
    changed_files: list[str] = Field(default_factory=list)
    artifacts: dict[str, str] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)

    @field_validator("status")
    @classmethod
    def _status(cls, value: str) -> str:
        allowed = CANDIDATE_STATUSES | {"cancelled", "interrupted"}
        if value not in allowed:
            raise ValueError(f"unknown agent result status: {value}")
        return value

    @field_validator("started_at", "finished_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_optional_timestamp(value)


class ExecutionEvaluationModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    run_id: str = ""
    attempt: int = Field(default=1, ge=1)
    evaluated_at: str = ""
    selected_candidate_id: str | None = None
    verdict: str = ""
    traffic_light: str = ""
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    required_gates_passed: bool | None = None
    repair_recommended: bool = False
    reasons: list[str] = Field(default_factory=list)
    review_dimensions: dict[str, dict[str, Any]] = Field(default_factory=dict)
    review_independence: dict[str, Any] = Field(default_factory=dict)
    deterministic_gates: dict[str, Any] = Field(default_factory=dict)
    llm_evaluation: dict[str, Any] | None = None

    @field_validator("verdict")
    @classmethod
    def _verdict(cls, value: str) -> str:
        if value and value not in {"accept", "reject", "repair"}:
            raise ValueError(f"unknown execution evaluation verdict: {value}")
        return value

    @field_validator("traffic_light")
    @classmethod
    def _traffic_light(cls, value: str) -> str:
        if value and value not in {"green", "yellow", "red"}:
            raise ValueError(f"unknown execution evaluation traffic_light: {value}")
        return value

    @field_validator("evaluated_at")
    @classmethod
    def _evaluated_timestamp(cls, value: str) -> str:
        return _validate_optional_timestamp(value)


class RunOutcomeModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    run_id: str = ""
    issue: dict[str, Any] = Field(default_factory=dict)
    state: str = ""
    status: str = ""
    completed_at: str = ""
    selected_candidate_id: str | None = None
    pr_url: str | None = None
    summary: str = ""
    artifacts: dict[str, str] = Field(default_factory=dict)
    error: dict[str, Any] | None = None

    @field_validator("state")
    @classmethod
    def _state_value(cls, value: str) -> str:
        if value and value not in TASK_STATES:
            raise ValueError(f"unknown state: {value}")
        return value

    @field_validator("completed_at")
    @classmethod
    def _completed_timestamp(cls, value: str) -> str:
        return _validate_optional_timestamp(value)


class ArchiveSummaryModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    run_id: str = ""
    issue: dict[str, Any] = Field(default_factory=dict)
    archived_at: str = ""
    source_path: str = ""
    archive_path: str = ""
    removed_worktrees: list[str] = Field(default_factory=list)
    retained_artifacts: dict[str, str] = Field(default_factory=dict)
    outcome: RunOutcomeModel | None = None

    @field_validator("archived_at")
    @classmethod
    def _archived_timestamp(cls, value: str) -> str:
        return _validate_optional_timestamp(value)


class PolicyViolationModel(CompatibleArtifactModel):
    code: str
    message: str
    paths: list[str] = Field(default_factory=list)


class CandidateResultModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    candidate_id: str
    status: str
    branch: str
    worktree_path: str
    base_commit: str
    summary: str
    changed_files: list[str] = Field(default_factory=list)
    patch: str = ""
    duration_seconds: float = Field(ge=0)
    error: str | None = None
    setup: CheckResultModel | None = None
    attempt: int = Field(default=1, ge=1)
    strategy: str = "conservative"
    patch_path: str = ""
    verification: str = ""
    verification_passed: bool = False
    verification_mutated_worktree: bool = False
    baseline_failed_commands: list[str] = Field(default_factory=list)
    policy_violations: list[PolicyViolationModel] = Field(default_factory=list)
    effective_status: str | None = None
    agent_pid: int | None = Field(default=None, ge=1)
    sandbox_pid: int | None = Field(default=None, ge=1)

    @field_validator("status", "effective_status")
    @classmethod
    def _status(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value not in CANDIDATE_STATUSES:
            raise ValueError(f"unknown candidate status: {value}")
        return value


class EvaluationCandidateModel(CompatibleArtifactModel):
    candidate_id: str
    status: str
    score: int = 0
    selected: bool = False
    reasons: list[str] = Field(default_factory=list)
    verification_passed: bool = False
    verification_mutated_worktree: bool = False
    changed_files_count: int = Field(default=0, ge=0)
    policy_violations: list[PolicyViolationModel] = Field(default_factory=list)
    result_path: str = ""

    @field_validator("status")
    @classmethod
    def _status(cls, value: str) -> str:
        if value not in CANDIDATE_STATUSES:
            raise ValueError(f"unknown candidate status: {value}")
        return value


class EvaluationArtifactModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    attempt: int = Field(default=1, ge=1)
    max_attempts: int = Field(default=1, ge=1)
    winner: str | None = None
    candidates: list[EvaluationCandidateModel] = Field(default_factory=list)
    rejected_candidates: list[str] = Field(default_factory=list)
    reasoning_summary: str = ""
    deterministic_gates: dict[str, Any] = Field(default_factory=dict)
    llm_evaluation: dict[str, Any] | None = None


class InputArtifactModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    source: str
    sequence_number: int = Field(ge=1)
    content_hash: str
    message: str
    created_at: str
    answered_state: str
    answered_candidate_id: str | None = None

    @field_validator("created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class InputRequestModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    candidate_id: str
    attempt: int = Field(ge=1)
    message: str
    created_at: str
    global_blocker: bool = False

    @field_validator("created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class RateLimitArtifactModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    bucket: str
    remaining: int = Field(ge=0)
    reset_at: str
    limit: int | None = None

    @field_validator("reset_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class ResourcePreflightModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    available_mb: int = Field(ge=0)
    required_mb: int = Field(ge=0)
    max_disk_mb: int = Field(ge=1)
    requested_candidates: int = Field(ge=0)
    allowed_candidates: int = Field(ge=0)
    downscaled: bool = False
    passed: bool = True
    checked_at: str

    @field_validator("checked_at")
    @classmethod
    def _checked_timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class PublishingPreflightModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    candidate_id: str
    branch: str
    base_commit: str
    default_branch: str
    default_commit: str | None = None
    default_commit_source: str = ""
    default_sync_ok: bool = True
    default_sync_attempted: bool = False
    default_sync_message: str = ""
    base_reachable: bool
    base_is_ancestor_of_default: bool
    stale_base: bool
    checked_at: str

    @field_validator("checked_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class PublishingIntegrationModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    candidate_id: str
    source_branch: str
    integration_branch: str
    worktree_path: str
    base_ref: str
    patch_path: str
    created_at: str

    @field_validator("created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class PatchConflictModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    code: str = "patch_conflict"
    candidate_id: str
    patch_path: str
    integration_branch: str
    worktree_path: str
    message: str
    changed_files: list[str] = Field(default_factory=list)
    lfs_unavailable: bool = False
    created_at: str

    @field_validator("created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class ContextSnapshotModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    created_at: str
    run_id: str
    issue: dict[str, Any]
    objects: dict[str, str]

    @field_validator("created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)


class RunSummaryModel(CompatibleArtifactModel):
    schema_version: Literal[1] = 1
    run_id: str
    issue: dict[str, Any]
    state: str
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1)
    created_at: str
    updated_at: str
    dry_run: bool = False
    baseline: dict[str, Any] = Field(default_factory=dict)
    stage_attempts: dict[str, int] = Field(default_factory=dict)
    locks: dict[str, Any] = Field(default_factory=dict)
    operator: dict[str, Any] = Field(default_factory=dict)
    cost: dict[str, Any] | None = None
    publishing_step: str | None = None
    cancel_requested: bool = False
    blocked_resume_state: str | None = None
    blocked_until: str | None = None
    pr_url: str | None = None
    artifacts: dict[str, str] = Field(default_factory=dict)
    candidate_states: dict[str, CandidateStateModel] = Field(default_factory=dict)
    last_error: dict[str, Any] | None = None
    logs: dict[str, str] = Field(default_factory=dict)

    @field_validator("state")
    @classmethod
    def _state_value(cls, value: str) -> str:
        if value not in TASK_STATES:
            raise ValueError(f"unknown state: {value}")
        return value

    @field_validator("blocked_resume_state")
    @classmethod
    def _blocked_resume_state(cls, value: str | None) -> str | None:
        if value is not None and value not in TASK_STATES:
            raise ValueError(f"unknown blocked_resume_state: {value}")
        return value

    @field_validator("created_at", "updated_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return _validate_required_timestamp(value)

    @field_validator("blocked_until")
    @classmethod
    def _blocked_until_timestamp(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_optional_timestamp(value)


def validation_error_message(location: str, exc: Exception) -> str:
    """Return a deterministic path-aware validation message for CLI/errors."""

    if not hasattr(exc, "errors"):
        return f"{location}: {exc}"
    parts: list[str] = []
    for error in exc.errors():  # type: ignore[attr-defined]
        field_path = ".".join(str(item) for item in error.get("loc", ()))
        message = error.get("msg", "invalid value")
        if field_path:
            parts.append(f"{location}.{field_path}: {message}")
        else:
            parts.append(f"{location}: {message}")
    return "; ".join(parts)


def _validate_required_timestamp(value: str) -> str:
    if not re.match(UTC_TIMESTAMP_PATTERN, value):
        raise ValueError("timestamp must be UTC ISO 8601 YYYY-MM-DDTHH:MM:SSZ")
    return value


def _validate_optional_timestamp(value: str) -> str:
    if value:
        return _validate_required_timestamp(value)
    return value
