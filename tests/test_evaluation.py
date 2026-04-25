from __future__ import annotations

from dataclasses import dataclass

from gg.orchestrator.evaluation import CandidateEvaluator, build_run_outcome
from gg.orchestrator.schemas import ExecutionEvaluationModel, RunOutcomeModel
from gg.orchestrator.state import CandidateState, RunState, TaskState
from gg.orchestrator.verification import CheckResult


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    summary: str = "Candidate completed."
    backend: str = "codex"


def _record(
    candidate_id: str,
    *,
    index: int = 1,
    status: str = "success",
    verification_passed: bool = True,
    verification_mutated_worktree: bool = False,
    policy_violations: list[dict] | None = None,
    final_files: list[str] | None = None,
    verification: list[CheckResult] | None = None,
    backend: str = "codex",
) -> dict:
    return {
        "index": index,
        "candidate": Candidate(candidate_id=candidate_id, backend=backend),
        "effective_status": status,
        "verification_passed": verification_passed,
        "verification_mutated_worktree": verification_mutated_worktree,
        "policy_violations": policy_violations or [],
        "final_files": final_files if final_files is not None else ["app.py"],
        "verification": verification or [],
        "result_path": f"candidates/{candidate_id}/candidate-result.json",
        "verification_path": f"candidates/{candidate_id}/verification.json",
        "agent_backend": backend,
    }


def test_single_passing_candidate_is_green_and_selected():
    decision = CandidateEvaluator(review_backend="codex").evaluate(
        [_record("candidate-1")],
        attempt=1,
        max_attempts=1,
        run_id="run-1",
        evaluated_at="2026-04-25T12:00:00Z",
    )

    assert decision.artifact["winner"] == "candidate-1"
    assert decision.winner["candidate"].candidate_id == "candidate-1"
    assert decision.execution_evaluation["traffic_light"] == "green"
    assert decision.execution_evaluation["verdict"] == "accept"
    assert decision.execution_evaluation["selected_candidate_id"] == "candidate-1"
    assert decision.execution_evaluation["required_gates_passed"] is True
    assert decision.execution_evaluation["llm_evaluation"] is None
    assert decision.execution_evaluation["review_independence"] == {
        "review_backend": "codex",
        "candidate_backend": "codex",
        "same_backend": True,
        "warning": "same_backend_review_not_independent",
    }
    ExecutionEvaluationModel.model_validate(decision.execution_evaluation)


def test_all_failed_candidates_are_red_with_no_winner():
    decision = CandidateEvaluator().evaluate(
        [
            _record(
                "candidate-1",
                status="failed",
                verification_passed=False,
                verification=[CheckResult(command="pytest", status="failed", exit_code=1)],
            ),
            _record("candidate-2", status="success", verification_mutated_worktree=True),
        ],
        attempt=1,
        max_attempts=2,
    )

    assert decision.artifact["winner"] is None
    assert decision.winner is None
    assert decision.execution_evaluation["traffic_light"] == "red"
    assert decision.execution_evaluation["verdict"] == "reject"
    assert decision.execution_evaluation["required_gates_passed"] is False
    assert decision.execution_evaluation["repair_recommended"] is True
    assert decision.execution_evaluation["reasons"] == [
        "No candidate passed deterministic eligibility gates."
    ]


def test_mixed_candidates_choose_eligible_candidate():
    decision = CandidateEvaluator().evaluate(
        [
            _record("candidate-1", status="success", verification_passed=False, index=1),
            _record(
                "candidate-2",
                status="success",
                final_files=["app.py", "tests/test_app.py"],
                index=2,
            ),
        ],
        attempt=1,
        max_attempts=1,
    )

    assert decision.artifact["winner"] == "candidate-2"
    assert decision.winner["candidate"].candidate_id == "candidate-2"
    execution_candidates = decision.execution_evaluation["candidates"]
    assert execution_candidates[0]["eligible"] is False
    assert execution_candidates[1]["eligible"] is True
    assert execution_candidates[1]["selected"] is True
    assert "verification passed" in execution_candidates[1]["deterministic_reasons"]


def test_execution_evaluation_includes_review_dimensions():
    decision = CandidateEvaluator().evaluate(
        [_record("candidate-1")],
        attempt=1,
        max_attempts=1,
    )

    dimensions = decision.execution_evaluation["review_dimensions"]
    assert set(dimensions) == {"architecture", "code", "security", "tests", "operability"}
    assert all(dimensions[name]["status"] == "pass" for name in dimensions)
    assert dimensions["security"]["reasons"] == ["no policy violations present"]
    assert dimensions["operability"]["reasons"] == ["verification did not mutate the worktree"]


def test_build_run_outcome_validates_state_like_data_and_selected_candidate_metadata():
    state = RunState(
        run_id="run-1",
        issue={"number": 42, "title": "Implement feature"},
        state=TaskState.COMPLETED,
        created_at="2026-04-25T12:00:00Z",
        updated_at="2026-04-25T12:30:00Z",
        artifacts={"evaluation": ".gg/runs/run-1/artifacts/evaluation.json"},
        candidate_states={
            "candidate-1": CandidateState(
                status="success",
                result_path=".gg/runs/run-1/candidates/candidate-1/candidate-result.json",
            )
        },
        pr_url="https://github.com/example/repo/pull/1",
    )

    outcome = build_run_outcome(
        state,
        {
            "candidate_id": "candidate-1",
            "summary": "Candidate completed.",
            "result_path": ".gg/runs/run-1/candidates/candidate-1/candidate-result.json",
            "verification_path": ".gg/runs/run-1/candidates/candidate-1/verification.json",
        },
    )

    assert outcome["status"] == "success"
    assert outcome["selected_candidate_id"] == "candidate-1"
    assert outcome["completed_at"] == "2026-04-25T12:30:00Z"
    assert outcome["artifacts"]["selected_candidate_result"].endswith("candidate-result.json")
    assert outcome["artifacts"]["selected_candidate_verification"].endswith("verification.json")
    RunOutcomeModel.model_validate(outcome)
