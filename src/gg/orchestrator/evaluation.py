from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gg.orchestrator.schemas import EvaluationArtifactModel


@dataclass(frozen=True)
class EvaluationDecision:
    artifact: dict[str, Any]
    winner: dict[str, Any] | None


class CandidateEvaluator:
    """Deterministic candidate selector.

    The deterministic gates are authoritative. An LLM evaluator can later add
    explanation or tie-breaking, but it should not override hard failures.
    """

    def evaluate(
        self,
        records: list[dict[str, Any]],
        *,
        attempt: int,
        max_attempts: int,
    ) -> EvaluationDecision:
        scored = [self._score(record) for record in records]
        eligible = [
            item
            for item in scored
            if item["eligible"]
        ]
        winner_score = max(eligible, key=lambda item: (item["score"], -item["index"])) if eligible else None
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
            "deterministic_gates": {
                "requires_success_status": True,
                "requires_verification_passed": True,
                "rejects_verification_mutation": True,
                "rejects_policy_violations": True,
                "prefers_smaller_changed_file_count_on_ties": True,
            },
            "llm_evaluation": None,
        }
        EvaluationArtifactModel.model_validate(artifact)
        return EvaluationDecision(
            artifact=artifact,
            winner=next((record for record in records if record["candidate"].candidate_id == winner_id), None),
        )

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
            "result_path": record["result_path"],
        }

    def _reasoning_summary(self, winner_id: str | None, scored: list[dict[str, Any]]) -> str:
        if winner_id is None:
            return "No candidate passed deterministic eligibility gates."
        winner = next(item for item in scored if item["candidate_id"] == winner_id)
        return f"Selected {winner_id} with deterministic score {winner['score']}."


def _failed_commands(record: dict[str, Any]) -> list[str]:
    return [
        check.command
        for check in record.get("verification", [])
        if getattr(check, "status", "") in {"failed", "timeout"}
    ]
