from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from gg.orchestrator.schemas import RunStateModel


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TaskState(str, Enum):
    EXTERNAL_TASK_READY = "ExternalTaskReady"
    CLAIMING = "Claiming"
    QUEUED = "Queued"
    RUN_STARTED = "RunStarted"
    TASK_ANALYSIS = "TaskAnalysis"
    BLOCKED = "Blocked"
    READY_FOR_EXECUTION = "ReadyForExecution"
    AGENT_SELECTION = "AgentSelection"
    AGENT_RUNNING = "AgentRunning"
    RESULT_EVALUATION = "ResultEvaluation"
    NEEDS_INPUT = "NeedsInput"
    OUTCOME_PUBLISHING = "OutcomePublishing"
    COMPLETED = "Completed"
    TERMINAL_FAILURE = "TerminalFailure"
    CANCELLED = "Cancelled"


TERMINAL_STATES = {
    TaskState.COMPLETED,
    TaskState.TERMINAL_FAILURE,
    TaskState.CANCELLED,
}

RUNNING_CANDIDATE_STATUSES = frozenset({"running"})


ALLOWED_TRANSITIONS: dict[TaskState, set[TaskState]] = {
    TaskState.EXTERNAL_TASK_READY: {TaskState.CLAIMING},
    TaskState.CLAIMING: {
        TaskState.QUEUED,
        TaskState.EXTERNAL_TASK_READY,
        TaskState.TERMINAL_FAILURE,
        TaskState.CANCELLED,
    },
    TaskState.QUEUED: {TaskState.RUN_STARTED, TaskState.CANCELLED},
    TaskState.RUN_STARTED: {TaskState.TASK_ANALYSIS, TaskState.TERMINAL_FAILURE, TaskState.CANCELLED},
    TaskState.TASK_ANALYSIS: {
        TaskState.READY_FOR_EXECUTION,
        TaskState.BLOCKED,
        TaskState.TERMINAL_FAILURE,
        TaskState.CANCELLED,
    },
    TaskState.BLOCKED: {TaskState.TASK_ANALYSIS, TaskState.AGENT_SELECTION, TaskState.CANCELLED},
    TaskState.READY_FOR_EXECUTION: {TaskState.AGENT_SELECTION, TaskState.CANCELLED},
    TaskState.AGENT_SELECTION: {
        TaskState.AGENT_RUNNING,
        TaskState.BLOCKED,
        TaskState.TERMINAL_FAILURE,
        TaskState.CANCELLED,
    },
    TaskState.AGENT_RUNNING: {
        TaskState.RESULT_EVALUATION,
        TaskState.NEEDS_INPUT,
        TaskState.TERMINAL_FAILURE,
        TaskState.CANCELLED,
    },
    TaskState.RESULT_EVALUATION: {
        TaskState.AGENT_RUNNING,
        TaskState.OUTCOME_PUBLISHING,
        TaskState.NEEDS_INPUT,
        TaskState.TERMINAL_FAILURE,
        TaskState.CANCELLED,
    },
    TaskState.NEEDS_INPUT: {TaskState.AGENT_RUNNING, TaskState.CANCELLED, TaskState.TERMINAL_FAILURE},
    TaskState.OUTCOME_PUBLISHING: {TaskState.COMPLETED, TaskState.TERMINAL_FAILURE, TaskState.CANCELLED},
    TaskState.COMPLETED: set(),
    TaskState.TERMINAL_FAILURE: set(),
    TaskState.CANCELLED: set(),
}


class InvalidTransitionError(ValueError):
    pass


@dataclass
class CandidateState:
    status: str
    worktree_path: str = ""
    branch: str = ""
    result_path: str | None = None
    started_at: str = ""
    finished_at: str | None = None
    error: str | None = None


@dataclass
class RunState:
    run_id: str
    issue: dict[str, Any]
    state: TaskState = TaskState.EXTERNAL_TASK_READY
    schema_version: int = 1
    attempt: int = 1
    max_attempts: int = 1
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    baseline: dict[str, Any] = field(default_factory=dict)
    candidate_states: dict[str, CandidateState] = field(default_factory=dict)
    stage_attempts: dict[str, int] = field(default_factory=dict)
    locks: dict[str, Any] = field(default_factory=dict)
    operator: dict[str, Any] = field(default_factory=dict)
    cost: dict[str, Any] | None = None
    artifacts: dict[str, str] = field(default_factory=dict)
    transitions: list[dict[str, Any]] = field(default_factory=list)
    last_error: dict[str, Any] | None = None
    pr_url: str | None = None
    dry_run: bool = False
    publishing_step: str | None = None
    cancel_requested: bool = False

    def transition(self, target: TaskState, *, reason: str = "") -> None:
        if target not in ALLOWED_TRANSITIONS[self.state]:
            raise InvalidTransitionError(f"illegal transition {self.state.value} -> {target.value}")
        self._record_transition(target, reason=reason)

    def recover_to(self, target: TaskState, *, reason: str) -> None:
        self._record_transition(target, reason=f"recovery: {reason}")

    def _record_transition(self, target: TaskState, *, reason: str) -> None:
        now = utc_now()
        self.transitions.append(
            {"from": self.state.value, "to": target.value, "at": now, "reason": reason}
        )
        self.state = target
        self.updated_at = now

    def fail(self, *, code: str, message: str) -> None:
        self.last_error = {"code": code, "message": message, "at": utc_now()}
        if self.state is not TaskState.TERMINAL_FAILURE:
            self.transition(TaskState.TERMINAL_FAILURE, reason=code)

    def has_running_candidates(self) -> bool:
        return any(candidate.status in RUNNING_CANDIDATE_STATUSES for candidate in self.candidate_states.values())

    def candidates_quiescent(self) -> bool:
        return not self.has_running_candidates()

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state"] = self.state.value
        data["candidate_states"] = {
            key: asdict(value) for key, value in self.candidate_states.items()
        }
        RunStateModel.model_validate(data)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunState":
        validated = RunStateModel.model_validate(data)
        candidate_states = {
            key: CandidateState(**value.model_dump())
            for key, value in validated.candidate_states.items()
        }
        transitions = [
            transition.model_dump(by_alias=True)
            for transition in validated.transitions
        ]
        baseline = (
            validated.baseline.model_dump()
            if hasattr(validated.baseline, "model_dump")
            else validated.baseline
        )
        cost = (
            validated.cost.model_dump()
            if hasattr(validated.cost, "model_dump")
            else validated.cost
        )
        return cls(
            run_id=validated.run_id,
            issue=validated.issue,
            state=TaskState(validated.state),
            schema_version=validated.schema_version,
            attempt=validated.attempt,
            max_attempts=validated.max_attempts,
            created_at=validated.created_at,
            updated_at=validated.updated_at,
            baseline=baseline,
            candidate_states=candidate_states,
            stage_attempts=validated.stage_attempts,
            locks=validated.locks,
            operator=validated.operator,
            cost=cost,
            artifacts=validated.artifacts,
            transitions=transitions,
            last_error=validated.last_error,
            pr_url=validated.pr_url,
            dry_run=validated.dry_run,
            publishing_step=validated.publishing_step,
            cancel_requested=validated.cancel_requested,
        )
