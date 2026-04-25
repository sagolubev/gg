from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ErrorCategory(str, Enum):
    TRANSIENT = "transient"
    EXECUTOR_ERROR = "executor_error"
    TOOL_ERROR = "tool_error"
    POLICY_ERROR = "policy_error"
    EXTERNAL_SIDE_EFFECT_ERROR = "external_side_effect_error"
    VALIDATION_FAILED = "validation_failed"
    CONFIGURATION_ERROR = "configuration_error"
    TERMINAL_ERROR = "terminal_error"
    UNKNOWN = "unknown"


class ErrorCode(str, Enum):
    INVALID_CONFIG = "invalid_config"
    AUTH_FAILED = "auth_failed"
    RATE_LIMITED = "rate_limited"
    MISSING_RUNTIME = "missing_runtime"
    BACKEND_UNAVAILABLE = "backend_unavailable"
    ANALYSIS_TIMEOUT = "analysis_timeout"
    CONTEXT_TOO_LARGE = "context_too_large"
    EVALUATION_CONTEXT_TOO_LARGE = "evaluation_context_too_large"
    BASELINE_FAILED = "baseline_failed"
    CANDIDATE_TIMEOUT = "candidate_timeout"
    DISK_QUOTA_EXCEEDED = "disk_quota_exceeded"
    VERIFICATION_FAILED = "verification_failed"
    PATCH_CONFLICT = "patch_conflict"
    STALE_BASE_CONFLICT = "stale_base_conflict"
    BUDGET_EXCEEDED = "budget_exceeded"
    SECURITY_VIOLATION = "security_violation"
    SCHEMA_UNSUPPORTED = "schema_unsupported"
    INVALID_RESUME_TARGET = "invalid_resume_target"
    STATE_CONFLICT = "state_conflict"
    ARTIFACT_CHECKSUM_FAILED = "artifact_checksum_failed"


@dataclass(frozen=True)
class PipelineError:
    category: ErrorCategory
    code: ErrorCode
    phase: str
    message: str
    recoverable: bool = False
    candidate_id: str | None = None
    retry_after: float | None = None
