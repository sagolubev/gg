from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from gg.orchestrator.schemas import (
    EvaluationArtifactModel,
    ExecutionEvaluationModel,
    RunOutcomeModel,
)

REVIEW_DIMENSION_NAMES = ("architecture", "code", "security", "tests", "operability")


@dataclass(frozen=True)
class EvaluationDecision:
    artifact: dict[str, Any]
    winner: dict[str, Any] | None
    execution_evaluation: dict[str, Any] | None = None


class CandidateEvaluator:
    """Deterministic candidate selector.

    The deterministic gates are authoritative. An LLM evaluator can later add
    explanation or tie-breaking, but it should not override hard failures.
    """

    def __init__(self, *, review_backend: str = "deterministic"):
        self.review_backend = review_backend

    def evaluate(
        self,
        records: list[dict[str, Any]],
        *,
        attempt: int,
        max_attempts: int,
        run_id: str = "",
        evaluated_at: str = "",
    ) -> EvaluationDecision:
        scored = [self._score(record) for record in records]
        eligible = [
            item
            for item in scored
            if item["eligible"]
        ]
        winner_score = (
            max(eligible, key=lambda item: (item["score"], -item["index"]))
            if eligible
            else None
        )
        winner_id = winner_score["candidate_id"] if winner_score else None
        candidates = []
        for item in scored:
            candidates.append(
                {
                    "candidate_id": item["candidate_id"],
                    "status": item["status"],
                    "score": item["score"],
                    "selected": item["candidate_id"] == winner_id,
                    "reasons": item["reasons"],
                    "verification_passed": item["verification_passed"],
                    "verification_mutated_worktree": item["verification_mutated_worktree"],
                    "changed_files_count": item["changed_files_count"],
                    "policy_violations": item["policy_violations"],
                    "result_path": item["result_path"],
                }
            )
        artifact = {
            "schema_version": 1,
            "attempt": attempt,
            "max_attempts": max_attempts,
            "winner": winner_id,
            "candidates": candidates,
            "rejected_candidates": [
                item["candidate_id"]
                for item in scored
                if item["candidate_id"] != winner_id
            ],
            "reasoning_summary": self._reasoning_summary(winner_id, scored),
            "deterministic_gates": _deterministic_gates(),
            "llm_evaluation": None,
        }
        EvaluationArtifactModel.model_validate(artifact)
        execution_evaluation = self.build_execution_evaluation(
            scored,
            records=records,
            attempt=attempt,
            max_attempts=max_attempts,
            run_id=run_id,
            evaluated_at=evaluated_at,
            winner_id=winner_id,
        )
        return EvaluationDecision(
            artifact=artifact,
            winner=next(
                (record for record in records if record["candidate"].candidate_id == winner_id),
                None,
            ),
            execution_evaluation=execution_evaluation,
        )

    def build_execution_evaluation(
        self,
        scored: list[dict[str, Any]],
        *,
        records: list[dict[str, Any]],
        attempt: int,
        max_attempts: int,
        run_id: str = "",
        evaluated_at: str = "",
        winner_id: str | None = None,
    ) -> dict[str, Any]:
        selected = next((item for item in scored if item["candidate_id"] == winner_id), None)
        selected_record = next(
            (record for record in records if record["candidate"].candidate_id == winner_id),
            None,
        )
        traffic_light = "green" if selected else "red"
        verdict = "accept" if selected else "reject"
        reasons = self._execution_reasons(winner_id, scored)
        execution_evaluation = {
            "schema_version": 1,
            "run_id": run_id,
            "attempt": attempt,
            "evaluated_at": evaluated_at,
            "selected_candidate_id": winner_id,
            "verdict": verdict,
            "traffic_light": traffic_light,
            "candidates": [
                {
                    "candidate_id": item["candidate_id"],
                    "status": item["status"],
                    "score": item["score"],
                    "eligible": item["eligible"],
                    "selected": item["candidate_id"] == winner_id,
                    "deterministic_reasons": item["reasons"],
                    "verification_passed": item["verification_passed"],
                    "verification_mutated_worktree": item["verification_mutated_worktree"],
                    "changed_files_count": item["changed_files_count"],
                    "policy_violations": item["policy_violations"],
                    "failed_commands": item["failed_commands"],
                    "result_path": item["result_path"],
                }
                for item in scored
            ],
            "required_gates_passed": selected is not None,
            "repair_recommended": selected is None and attempt < max_attempts,
            "reasons": reasons,
            "review_dimensions": self._review_dimensions(selected),
            "review_independence": self._review_independence(selected_record),
            "deterministic_gates": _deterministic_gates(),
            "llm_evaluation": None,
        }
        ExecutionEvaluationModel.model_validate(execution_evaluation)
        return execution_evaluation

    def _score(self, record: dict[str, Any]) -> dict[str, Any]:
        candidate = record["candidate"]
        status = str(record["effective_status"])
        verification_passed = bool(record["verification_passed"])
        verification_mutated = bool(record["verification_mutated_worktree"])
        policy_violations = list(record.get("policy_violations", []))
        changed_files_count = len(record.get("final_files", []))
        failed_commands = _failed_commands(record)
        score = 0
        reasons: list[str] = []

        if status == "success":
            score += 100
            reasons.append("candidate status is success")
        else:
            score -= 100
            reasons.append(f"candidate status is {status}")

        if verification_passed:
            score += 50
            reasons.append("verification passed")
        else:
            score -= 50
            reasons.append("verification failed")

        if verification_mutated:
            score -= 50
            reasons.append("verification mutated the worktree")

        if policy_violations:
            score -= 100 * len(policy_violations)
            reasons.append("policy violations present")

        if failed_commands:
            score -= 10 * len(failed_commands)
            reasons.append("verification commands failed")

        score -= min(changed_files_count, 25)
        if changed_files_count:
            reasons.append(f"{changed_files_count} changed files")

        eligible = (
            status == "success"
            and verification_passed
            and not verification_mutated
            and not policy_violations
        )
        return {
            "index": int(record["index"]),
            "candidate_id": candidate.candidate_id,
            "status": status,
            "score": score,
            "eligible": eligible,
            "reasons": reasons,
            "verification_passed": verification_passed,
            "verification_mutated_worktree": verification_mutated,
            "changed_files_count": changed_files_count,
            "policy_violations": policy_violations,
            "failed_commands": failed_commands,
            "result_path": record["result_path"],
        }

    def _reasoning_summary(self, winner_id: str | None, scored: list[dict[str, Any]]) -> str:
        if winner_id is None:
            return "No candidate passed deterministic eligibility gates."
        winner = next(item for item in scored if item["candidate_id"] == winner_id)
        return f"Selected {winner_id} with deterministic score {winner['score']}."

    def _execution_reasons(self, winner_id: str | None, scored: list[dict[str, Any]]) -> list[str]:
        if winner_id is None:
            return ["No candidate passed deterministic eligibility gates."]
        winner = next(item for item in scored if item["candidate_id"] == winner_id)
        return [
            f"Selected {winner_id} with deterministic score {winner['score']}.",
            *winner["reasons"],
        ]

    def _review_dimensions(
        self,
        selected: dict[str, Any] | None,
    ) -> dict[str, dict[str, Any]]:
        if selected is None:
            reasons = ["No candidate passed deterministic eligibility gates."]
            return {
                name: {"status": "fail", "reasons": reasons}
                for name in REVIEW_DIMENSION_NAMES
            }

        failed_commands = selected["failed_commands"]
        return {
            "architecture": {
                "status": "pass",
                "reasons": [
                    f"selected candidate has deterministic score {selected['score']}",
                    f"{selected['changed_files_count']} changed files considered for tie-breaking",
                ],
            },
            "code": {
                "status": "pass" if selected["status"] == "success" else "fail",
                "reasons": [f"candidate status is {selected['status']}"],
            },
            "security": {
                "status": "pass" if not selected["policy_violations"] else "fail",
                "reasons": (
                    ["no policy violations present"]
                    if not selected["policy_violations"]
                    else ["policy violations present"]
                ),
            },
            "tests": {
                "status": (
                    "pass"
                    if selected["verification_passed"] and not failed_commands
                    else "fail"
                ),
                "reasons": (
                    ["verification passed"]
                    if selected["verification_passed"] and not failed_commands
                    else [
                        "verification failed commands: "
                        f"{', '.join(failed_commands) or 'unknown'}"
                    ]
                ),
            },
            "operability": {
                "status": "pass" if not selected["verification_mutated_worktree"] else "fail",
                "reasons": (
                    ["verification did not mutate the worktree"]
                    if not selected["verification_mutated_worktree"]
                    else ["verification mutated the worktree"]
                ),
            },
        }

    def _review_independence(self, selected_record: dict[str, Any] | None) -> dict[str, Any]:
        candidate_backend = _candidate_backend(selected_record)
        same_backend = bool(candidate_backend and candidate_backend == self.review_backend)
        return {
            "review_backend": self.review_backend,
            "candidate_backend": candidate_backend,
            "same_backend": same_backend,
            "warning": "same_backend_review_not_independent" if same_backend else None,
        }


def build_run_outcome(
    state_like: Any,
    selected_candidate_metadata: Mapping[str, Any] | None = None,
    *,
    completed_at: str | None = None,
) -> dict[str, Any]:
    """Build a validated run outcome from state-like data without changing pipeline flow."""

    state = _state_get(state_like, "state", "")
    state_value = getattr(state, "value", state)
    error = _state_get(state_like, "last_error", None)
    selected_candidate_id = _selected_candidate_id(selected_candidate_metadata)
    artifacts = dict(_state_get(state_like, "artifacts", {}) or {})
    result_path = _metadata_get(selected_candidate_metadata, "result_path")
    verification_path = _metadata_get(selected_candidate_metadata, "verification_path")
    if result_path:
        artifacts.setdefault("selected_candidate_result", str(result_path))
    if verification_path:
        artifacts.setdefault("selected_candidate_verification", str(verification_path))

    outcome = {
        "schema_version": 1,
        "run_id": str(_state_get(state_like, "run_id", "")),
        "issue": dict(_state_get(state_like, "issue", {}) or {}),
        "state": str(state_value or ""),
        "status": _outcome_status(str(state_value or ""), selected_candidate_id, error),
        "completed_at": (
            completed_at
            if completed_at is not None
            else str(_state_get(state_like, "updated_at", ""))
        ),
        "selected_candidate_id": selected_candidate_id,
        "pr_url": _state_get(state_like, "pr_url", None),
        "summary": str(_metadata_get(selected_candidate_metadata, "summary") or ""),
        "artifacts": artifacts,
        "error": error,
    }
    RunOutcomeModel.model_validate(outcome)
    return outcome


def _failed_commands(record: dict[str, Any]) -> list[str]:
    return [
        check.command
        for check in record.get("verification", [])
        if getattr(check, "status", "") in {"failed", "timeout"}
    ]


def _deterministic_gates() -> dict[str, bool]:
    return {
        "requires_success_status": True,
        "requires_verification_passed": True,
        "rejects_verification_mutation": True,
        "rejects_policy_violations": True,
        "prefers_smaller_changed_file_count_on_ties": True,
    }


def _candidate_backend(record: dict[str, Any] | None) -> str | None:
    if not record:
        return None
    candidate = record.get("candidate")
    backend = (
        record.get("agent_backend")
        or record.get("backend")
        or getattr(candidate, "agent_backend", None)
        or getattr(candidate, "backend", None)
    )
    return str(backend) if backend else None


def _state_get(state_like: Any, key: str, default: Any = None) -> Any:
    if isinstance(state_like, Mapping):
        return state_like.get(key, default)
    return getattr(state_like, key, default)


def _metadata_get(metadata: Mapping[str, Any] | None, key: str, default: Any = None) -> Any:
    if not metadata:
        return default
    if key in metadata:
        return metadata[key]
    candidate = metadata.get("candidate")
    return getattr(candidate, key, default)


def _selected_candidate_id(metadata: Mapping[str, Any] | None) -> str | None:
    candidate_id = _metadata_get(metadata, "candidate_id")
    if candidate_id:
        return str(candidate_id)
    candidate = metadata.get("candidate") if metadata else None
    candidate_id = getattr(candidate, "candidate_id", None)
    return str(candidate_id) if candidate_id else None


def _outcome_status(
    state: str,
    selected_candidate_id: str | None,
    error: dict[str, Any] | None,
) -> str:
    if state == "Completed":
        return "success"
    if state == "Cancelled":
        return "cancelled"
    if state == "TerminalFailure" or error:
        return "failed"
    if selected_candidate_id:
        return "selected"
    return "running"
