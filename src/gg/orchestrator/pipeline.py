from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any

from gg.agents.base import AgentBackend
from gg.knowledge.engine import KnowledgeEngine
from gg.orchestrator.config import GGConfig, load_config
from gg.orchestrator.context import ContextSnapshotStore
from gg.orchestrator.executor import CandidateExecutor
from gg.orchestrator.git import binary_changed_files as git_binary_changed_files
from gg.orchestrator.git import changed_files as git_changed_files
from gg.orchestrator.git import dependency_changed_files as git_dependency_changed_files
from gg.orchestrator.git import commit_all, diff as git_diff, push_branch
from gg.orchestrator.git import lfs_changed_files as git_lfs_changed_files
from gg.orchestrator.lock import LockManager
from gg.orchestrator.plugins import create_agent_backend, create_platform
from gg.orchestrator.rate_limit import RateLimitThrottleError
from gg.orchestrator.state import CandidateState, TaskState
from gg.orchestrator.state import TERMINAL_STATES
from gg.orchestrator.store import RunStore
from gg.orchestrator.task_analysis import TaskAnalyzer, TaskBrief
from gg.orchestrator.verification import VerificationRunner
from gg.platforms.base import GitPlatform, Issue
from gg.utils.git_ops import find_repo_root


class OrchestratorPipeline:
    def __init__(
        self,
        project_path: str | Path = ".",
        *,
        platform: GitPlatform | None = None,
        agent: AgentBackend | None = None,
    ):
        root = find_repo_root(project_path) or Path(project_path).resolve()
        self.project_path = Path(root).resolve()
        self.config: GGConfig = load_config(self.project_path)
        self.store = RunStore(
            self.project_path,
            audit_hash_events=self.config.audit.hash_events,
            audit_sink_path=self.config.audit.external_sink or None,
        )
        self.locks = LockManager(self.project_path)
        self.platform = platform or create_platform(self.config.task_system.platform, self.project_path)
        self.agent = agent or create_agent_backend(self.config.runtime.agent_backend)
        self.knowledge = KnowledgeEngine(self.project_path)

    def run_issue(self, issue_number: int, *, dry_run: bool = False, no_pr: bool = False) -> dict[str, Any]:
        state = None
        try:
            issue = self.platform.get_issue(issue_number)
            state = self.store.create(issue, dry_run=dry_run)
            state.max_attempts = self.config.runtime.max_attempts
            self.knowledge.record_issue_picked(issue_number=issue.number, title=issue.title, labels=issue.labels)

            with self.locks.issue(issue_number):
                state.transition(TaskState.CLAIMING, reason="issue selected")
                self.store.write(state)
                if not dry_run:
                    if self.config.task_system.work_label:
                        self.platform.add_labels(issue.number, [self.config.task_system.work_label])
                    self.platform.add_comment(
                        issue.number,
                        f"<!-- gg-run-id={state.run_id} stage=claim -->\n"
                        f"gg picked this issue for implementation. Run: `{state.run_id}`",
                    )
                state.transition(TaskState.QUEUED, reason="claim complete")
                state.transition(TaskState.RUN_STARTED, reason="start pipeline")
                state.transition(TaskState.TASK_ANALYSIS, reason="create task brief")
                self.store.write(state)

                brief = self._refresh_task_analysis(state, issue)
                state.transition(TaskState.READY_FOR_EXECUTION, reason="task brief ready")
                self.store.write(state)

                if dry_run:
                    return {"run_id": state.run_id, "state": state.state.value, "dry_run": True}

                return self._execute_ready_state(state, issue, brief, no_pr=no_pr)
        except RateLimitThrottleError as exc:
            if state is None:
                return self._throttled_response(exc)
            return self._block_on_rate_limit(state, issue_number, exc)
        except KeyboardInterrupt:
            if state is not None:
                self._mark_interrupted(state)
            raise
        except Exception as exc:
            if state is None:
                raise
            try:
                state.fail(code="pipeline_error", message=str(exc))
                self.knowledge.record_error(issue_number=issue.number, message=str(exc), pattern=type(exc).__name__)
                self.store.write(state)
                if not dry_run:
                    self._mark_issue_failed(issue.number, str(exc))
                return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
            except Exception:
                raise

    def run_next(self, *, dry_run: bool = False, no_pr: bool = False) -> dict[str, Any]:
        batch = self.run_batch(batch_size=1, dry_run=dry_run, no_pr=no_pr)
        if batch["state"] == "DryRun":
            issues = batch.get("issues", [])
            if not issues:
                return {"state": "NoEligibleIssue", "message": "No eligible open issues found."}
            return {"state": "DryRun", "issue": issues[0]}
        if batch["state"] == "NoEligibleIssue" or "results" not in batch:
            return batch
        return batch["results"][0]

    def run_batch(self, *, batch_size: int, dry_run: bool = False, no_pr: bool = False) -> dict[str, Any]:
        requested = max(1, batch_size)
        with self.locks.queue():
            try:
                issues = self.platform.list_issues(limit=max(30, requested))
            except RateLimitThrottleError as exc:
                return self._throttled_response(exc)
            selected = self._eligible_issues(issues)[:requested]
            if not selected:
                return {"state": "NoEligibleIssue", "message": "No eligible open issues found."}
            if dry_run:
                return {
                    "state": "DryRun",
                    "issues": [
                        {"number": issue.number, "title": issue.title, "labels": issue.labels}
                        for issue in selected
                    ],
                    "count": len(selected),
                }
            issue_numbers = [issue.number for issue in selected]
        workers = min(len(issue_numbers), self.config.runtime.max_parallel_runs)
        if workers <= 1:
            results = [self.run_issue(issue_number, no_pr=no_pr) for issue_number in issue_numbers]
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                results = list(pool.map(lambda number: self.run_issue(number, no_pr=no_pr), issue_numbers))
        return {
            "state": "BatchCompleted",
            "count": len(results),
            "max_parallel_runs": workers,
            "results": results,
        }

    def status(self) -> list[dict[str, Any]]:
        return [run.to_dict() for run in self.store.list_runs()]

    def resume(self, run_id: str, *, no_pr: bool = False) -> dict[str, Any]:
        state = self.store.load(run_id)
        issue_number = int(state.issue["number"])
        try:
            with self.locks.issue(issue_number):
                state = self.store.load(run_id)
                if state.state in TERMINAL_STATES:
                    return {
                        "run_id": run_id,
                        "state": state.state.value,
                        "resumed": False,
                        "message": "Terminal runs are immutable; start a new run for a fresh attempt.",
                    }
                brief_path = state.artifacts.get("task_brief")
                if not brief_path:
                    state.fail(code="missing_task_brief", message="cannot resume without task brief artifact")
                    self.store.write(state)
                    return {"run_id": run_id, "state": state.state.value, "error": state.last_error}
                brief_data = json.loads((self.project_path / brief_path).read_text(encoding="utf-8"))
                brief = TaskBrief.from_dict(brief_data)
                issue = self.platform.get_issue(issue_number)
                if state.artifacts.get("last_input"):
                    brief = self._refresh_task_analysis(state, issue)
                if state.state is TaskState.OUTCOME_PUBLISHING:
                    return self._resume_publishing(state, issue, no_pr=no_pr)
                for candidate in state.candidate_states.values():
                    if candidate.status == "running":
                        candidate.status = "failed"
                        candidate.finished_at = _now_placeholder()
                        candidate.error = "interrupted before completion"
                if state.state is not TaskState.READY_FOR_EXECUTION:
                    state.recover_to(TaskState.READY_FOR_EXECUTION, reason=f"resume from {state.state.value}")
                    self.store.write(state)
                state.dry_run = False
                return self._execute_ready_state(state, issue, brief, no_pr=no_pr)
        except KeyboardInterrupt:
            self._mark_interrupted(state)
            raise

    def retry(self, run_id: str, *, no_pr: bool = False) -> dict[str, Any]:
        state = self.store.load(run_id)
        if state.state in TERMINAL_STATES:
            return {
                "run_id": run_id,
                "state": state.state.value,
                "retried": False,
                "message": "Terminal runs are immutable; start a new run for a fresh attempt.",
            }
        if state.state in {
            TaskState.READY_FOR_EXECUTION,
            TaskState.AGENT_SELECTION,
            TaskState.AGENT_RUNNING,
            TaskState.RESULT_EVALUATION,
            TaskState.BLOCKED,
            TaskState.NEEDS_INPUT,
        }:
            return {**self.resume(run_id, no_pr=no_pr), "retried": True}
        return {
            "run_id": run_id,
            "state": state.state.value,
            "retried": False,
            "message": "Retry is equivalent to resume only after task analysis has produced a task brief.",
        }

    def clean(self, *, dry_run: bool = True) -> dict[str, Any]:
        targets = self.store.clean_terminal_runs(dry_run=dry_run)
        orphans = self.store.clean_orphan_worktrees(dry_run=dry_run)
        return {
            "dry_run": dry_run,
            "runs": targets,
            "orphan_worktrees": orphans,
            "count": len(targets),
        }

    def cancel(self, run_id: str, *, reason: str = "operator requested cancellation") -> dict[str, Any]:
        state = self.store.load(run_id)
        if state.state in TERMINAL_STATES:
            return {"run_id": run_id, "state": state.state.value, "cancelled": False}
        if state.has_running_candidates():
            state.cancel_requested = True
            state.last_error = {"code": "cancel_requested", "message": reason, "at": _now_placeholder()}
            with self.locks.run(run_id):
                self.store.write(state)
            return {"run_id": run_id, "state": state.state.value, "cancelled": False, "cancel_requested": True}
        if state.state is TaskState.OUTCOME_PUBLISHING and state.publishing_step not in {None, "started"}:
            state.cancel_requested = True
            state.last_error = {"code": "cancel_requested", "message": reason, "at": _now_placeholder()}
            with self.locks.run(run_id):
                self.store.write(state)
            return {"run_id": run_id, "state": state.state.value, "cancelled": False, "cancel_requested": True}
        state.transition(TaskState.CANCELLED, reason=reason)
        state.last_error = {"code": "cancelled", "message": reason, "at": _now_placeholder()}
        with self.locks.run(run_id):
            self.store.write(state)
        return {"run_id": run_id, "state": state.state.value, "cancelled": True}

    def provide(self, run_id: str, *, message: str, source: str = "cli") -> dict[str, Any]:
        state = self.store.load(run_id)
        with self.locks.issue(int(state.issue["number"])):
            state = self.store.load(run_id)
            if state.state not in {TaskState.BLOCKED, TaskState.NEEDS_INPUT}:
                return {
                    "run_id": run_id,
                    "state": state.state.value,
                    "accepted": False,
                    "message": "Input is accepted only for Blocked or NeedsInput runs.",
                }
            content_hash = hashlib.sha256(message.encode("utf-8")).hexdigest()
            existing = sorted((self.store.path_for(run_id) / "inputs").glob("input-v1-*.json"))
            sequence_number = len(existing) + 1
            artifact = {
                "schema_version": 1,
                "source": source,
                "sequence_number": sequence_number,
                "content_hash": content_hash,
                "message": message,
                "created_at": _now_placeholder(),
                "answered_state": state.state.value,
            }
            path = self.store.write_json(run_id, f"inputs/input-v1-{sequence_number:04d}.json", artifact)
            state.artifacts["last_input"] = path
            if state.state is TaskState.BLOCKED:
                state.transition(TaskState.TASK_ANALYSIS, reason="operator provided input")
            else:
                state.transition(TaskState.AGENT_RUNNING, reason="operator provided input")
            self._best_effort_labels(
                int(state.issue["number"]),
                add=[self.config.task_system.work_label],
                remove=[self.config.task_system.blocked_label],
            )
            self.store.write(state)
            return {
                "run_id": run_id,
                "state": state.state.value,
                "accepted": True,
                "input": path,
                "content_hash": content_hash,
                "sequence_number": sequence_number,
            }

    def _execute_ready_state(
        self,
        state,
        issue: Issue,
        brief: TaskBrief,
        *,
        no_pr: bool,
    ) -> dict[str, Any]:
        state.transition(TaskState.AGENT_SELECTION, reason="select codex backend")
        if not self.agent.is_available():
            state.transition(TaskState.BLOCKED, reason="agent backend unavailable")
            state.last_error = {"code": "missing_agent", "message": "Codex CLI is not available"}
            self.store.write(state)
            self._mark_issue_blocked(issue.number, "Codex CLI is not available")
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}

        baseline = VerificationRunner(
            self.config.verify.commands(),
            timeout=self.config.runtime.command_timeout_seconds,
        ).run(self.project_path)
        baseline_path = self.store.write_json(
            state.run_id,
            "artifacts/baseline-verification.json",
            {
                "schema_version": 1,
                "checks": [check.to_dict() for check in baseline],
                "failed_commands": _failed_commands(baseline),
            },
        )
        state.artifacts["baseline_verification"] = baseline_path

        state.transition(TaskState.AGENT_RUNNING, reason="run candidates")
        self.store.write(state)

        executor = CandidateExecutor(self.project_path, self.agent, self.config)
        candidate_records: list[dict[str, Any]] = []
        selected: dict[str, Any] | None = None

        while state.attempt <= state.max_attempts and selected is None:
            candidate_count = (
                self.config.runtime.candidates if state.attempt == 1 else self.config.runtime.repair_candidates
            )
            strategies = _candidate_strategies(candidate_count)
            if state.attempt > 1:
                strategies = [f"repair:{strategy}" for strategy in strategies]
            planned_candidates: list[tuple[int, str, str]] = []
            for index, strategy in enumerate(strategies, start=1):
                cancelled = self._cancelled_response(state)
                if cancelled:
                    return cancelled
                base_candidate_id = (
                    f"candidate-{index}" if state.attempt == 1 else f"repair-{state.attempt}-{index}"
                )
                candidate_id = _unique_candidate_id(state, base_candidate_id)
                state.candidate_states[candidate_id] = CandidateState(status="running", started_at=_now_placeholder())
                self.store.write(state)
                planned_candidates.append((index, candidate_id, strategy))
            attempt_records = self._run_candidate_batch(
                state=state,
                issue=issue,
                brief=brief,
                executor=executor,
                baseline=baseline,
                planned_candidates=planned_candidates,
            )
            for record in attempt_records:
                candidate = record["candidate"]
                effective_status = record["effective_status"]
                state.candidate_states[candidate.candidate_id] = CandidateState(
                    status=effective_status,
                    worktree_path=candidate.worktree_path,
                    branch=candidate.branch,
                    result_path=record["result_path"],
                    started_at=state.candidate_states[candidate.candidate_id].started_at,
                    finished_at=_now_placeholder(),
                    error=record["error"],
                )
                self.knowledge.record_implementation_done(
                    issue_number=issue.number,
                    files_changed=record["final_files"],
                )
                self.knowledge.record_tests_run(
                    issue_number=issue.number,
                    passed=record["verification_passed"],
                    output="\n".join(check.stderr or check.stdout for check in record["verification"])[:500],
                )
                candidate_records.append(record)
                if selected is None and effective_status == "success":
                    selected = record
                self._merge_cancel_request(state)
                self.store.write(state)

            cancelled = self._cancelled_response(state)
            if cancelled:
                return cancelled
            state.transition(TaskState.RESULT_EVALUATION, reason="candidate set quiescent")
            evaluation_path = self._write_evaluation(state, selected, candidate_records)
            state.artifacts["evaluation"] = evaluation_path
            self.store.write(state)
            needs_input = next(
                (record for record in attempt_records if record["effective_status"] == "needs_input"),
                None,
            )
            if selected is None and needs_input is not None:
                request_path = self.store.write_json(
                    state.run_id,
                    "artifacts/input-request.json",
                    {
                        "schema_version": 1,
                        "candidate_id": needs_input["candidate"].candidate_id,
                        "attempt": needs_input["attempt"],
                        "message": needs_input["error"] or "Agent requested additional input.",
                        "created_at": _now_placeholder(),
                    },
                )
                state.artifacts["input_request"] = request_path
                state.transition(TaskState.NEEDS_INPUT, reason="candidate requested operator input")
                self.store.write(state)
                self._mark_issue_needs_input(issue.number, needs_input["error"] or "agent requested additional input")
                return {
                    "run_id": state.run_id,
                    "state": state.state.value,
                    "message": needs_input["error"] or "Agent requested additional input.",
                    "input_request": request_path,
                }
            if selected is None and state.attempt < state.max_attempts:
                state.attempt += 1
                state.transition(TaskState.AGENT_RUNNING, reason="repair candidate requested")
                self.store.write(state)
            else:
                break

        if selected is None:
            state.fail(code="candidate_failed", message="no candidate passed execution and verification")
            self.store.write(state)
            self._mark_issue_failed(issue.number, "no candidate passed execution and verification")
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
        winner = selected["candidate"]
        verification_path = selected["verification_path"]

        return self._publish_winner(
            state,
            issue,
            {
                "candidate_id": winner.candidate_id,
                "worktree_path": winner.worktree_path,
                "branch": winner.branch,
                "summary": winner.summary,
                "verification_path": verification_path,
            },
            no_pr=no_pr,
        )

    def _run_candidate_batch(
        self,
        *,
        state,
        issue: Issue,
        brief: TaskBrief,
        executor: CandidateExecutor,
        baseline,
        planned_candidates: list[tuple[int, str, str]],
    ) -> list[dict[str, Any]]:
        workers = min(len(planned_candidates), self.config.runtime.max_parallel_candidates)
        if workers <= 1:
            return [
                self._run_candidate_attempt(
                    state=state,
                    issue=issue,
                    brief=brief,
                    executor=executor,
                    baseline=baseline,
                    index=index,
                    candidate_id=candidate_id,
                    strategy=strategy,
                )
                for index, candidate_id, strategy in planned_candidates
            ]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(
                    self._run_candidate_attempt,
                    state=state,
                    issue=issue,
                    brief=brief,
                    executor=executor,
                    baseline=baseline,
                    index=index,
                    candidate_id=candidate_id,
                    strategy=strategy,
                )
                for index, candidate_id, strategy in planned_candidates
            ]
            results = [future.result() for future in futures]
        return sorted(results, key=lambda item: item["index"])

    def _run_candidate_attempt(
        self,
        *,
        state,
        issue: Issue,
        brief: TaskBrief,
        executor: CandidateExecutor,
        baseline,
        index: int,
        candidate_id: str,
        strategy: str,
    ) -> dict[str, Any]:
        candidate = executor.run(
            run_id=state.run_id,
            issue_number=issue.number,
            brief=brief,
            candidate_id=candidate_id,
            strategy=strategy,
        )
        verification_started = time.monotonic()
        candidate_dir = f"candidates/{candidate.candidate_id}"
        verification = VerificationRunner(
            self.config.verify.commands(),
            timeout=self.config.runtime.command_timeout_seconds,
        ).run(candidate.worktree_path)
        final_files = git_changed_files(candidate.worktree_path)
        final_patch = git_diff(candidate.worktree_path) if final_files else ""
        verification_mutated_worktree = (
            final_files != candidate.changed_files or final_patch != candidate.patch
        )
        patch_path = self.store.write_text(state.run_id, f"{candidate_dir}/patch.diff", final_patch)
        verification_path = self.store.write_json(
            state.run_id,
            f"{candidate_dir}/verification.json",
            {"schema_version": 1, "checks": [check.to_dict() for check in verification]},
        )
        verification_passed = _verification_passed(
            verification,
            baseline,
            allow_known_baseline_failures=self.config.verify.allow_known_baseline_failures,
        )
        policy_violations = self._candidate_policy_violations(candidate.worktree_path, final_files)
        candidate_data = candidate.to_dict()
        candidate_data["changed_files"] = final_files
        candidate_data["attempt"] = state.attempt
        candidate_data["strategy"] = strategy
        candidate_data["patch_path"] = patch_path
        candidate_data["verification"] = verification_path
        candidate_data["verification_passed"] = verification_passed
        candidate_data["verification_mutated_worktree"] = verification_mutated_worktree
        candidate_data["baseline_failed_commands"] = _failed_commands(baseline)
        candidate_data["policy_violations"] = policy_violations
        result_path = self.store.write_json(
            state.run_id,
            f"{candidate_dir}/candidate-result.json",
            {**candidate_data, "patch": ""},
        )
        effective_status = candidate.status
        error = candidate.error
        if candidate.status == "success" and not verification_passed:
            effective_status = "failed"
            error = "verification failed"
        if candidate.status == "success" and verification_mutated_worktree:
            effective_status = "failed"
            error = "verification mutated worktree"
        if candidate.status == "success" and policy_violations:
            effective_status = "failed"
            error = "; ".join(item["message"] for item in policy_violations)
        self.store.append_cost(
            state.run_id,
            {
                "event": "candidate_metrics",
                "at": _now_placeholder(),
                "run_id": state.run_id,
                "candidate_id": candidate.candidate_id,
                "attempt": state.attempt,
                "strategy": strategy,
                "status": effective_status,
                "error": error,
                "duration_seconds": candidate.duration_seconds,
                "verification_duration_seconds": round(time.monotonic() - verification_started, 3),
                "verification_passed": verification_passed,
                "verification_mutated_worktree": verification_mutated_worktree,
                "verification_failed_commands": _failed_commands(verification),
                "changed_files": final_files,
                "changed_files_count": len(final_files),
                "total_usd": None,
                "token_usage": None,
            },
        )
        return {
            "index": index,
            "candidate": candidate,
            "attempt": state.attempt,
            "strategy": strategy,
            "result_path": result_path,
            "verification_path": verification_path,
            "verification": verification,
            "verification_passed": verification_passed,
            "verification_mutated_worktree": verification_mutated_worktree,
            "effective_status": effective_status,
            "error": error,
            "final_files": final_files,
        }

    def _candidate_policy_violations(self, worktree_path: str, files: list[str]) -> list[dict[str, Any]]:
        violations: list[dict[str, Any]] = []
        if not self.config.security.allow_lfs_changes:
            paths = git_lfs_changed_files(worktree_path, files)
            if paths:
                violations.append(
                    {
                        "code": "lfs_changes_blocked",
                        "message": "LFS file changes are disabled by policy",
                        "paths": paths,
                    },
                )
        if not self.config.security.allow_binary_changes:
            paths = git_binary_changed_files(worktree_path, files)
            if paths:
                violations.append(
                    {
                        "code": "binary_changes_blocked",
                        "message": "Binary file changes are disabled by policy",
                        "paths": paths,
                    },
                )
        if not self.config.security.allow_dependency_changes:
            paths = git_dependency_changed_files(files)
            if paths:
                violations.append(
                    {
                        "code": "dependency_changes_blocked",
                        "message": "Dependency manifest changes are disabled by policy",
                        "paths": paths,
                    },
                )
        return violations

    def _publish_winner(
        self,
        state,
        issue: Issue,
        winner: dict[str, Any],
        *,
        no_pr: bool,
    ) -> dict[str, Any]:
        if state.state is not TaskState.OUTCOME_PUBLISHING:
            state.transition(TaskState.OUTCOME_PUBLISHING, reason="publish selected candidate")
        cancelled = self._cancelled_response(state)
        if cancelled:
            return cancelled
        if state.publishing_step is None:
            state.publishing_step = "started"
        self.store.write(state)
        if no_pr:
            state.pr_url = None
            state.publishing_step = "local_no_pr"
        else:
            if state.publishing_step not in {"committed", "branch_pushed", "pr_created", "result_commented"}:
                committed = commit_all(
                    winner["worktree_path"],
                    message=f"Implement issue #{issue.number}",
                    author_name=self.config.git.author_name,
                    author_email=self.config.git.author_email,
                )
                if not committed:
                    state.fail(code="empty_patch", message="no changes to publish")
                    self.store.write(state)
                    return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
                state.publishing_step = "committed"
                self.store.write(state)
            cancelled = self._cancelled_response(state)
            if cancelled:
                return cancelled
            if state.publishing_step == "committed":
                push_branch(winner["worktree_path"], winner["branch"])
                state.publishing_step = "branch_pushed"
                self.store.write(state)
            cancelled = self._cancelled_response(state)
            if cancelled:
                return cancelled
            pr_url = state.pr_url or self.platform.find_pr(head=winner["branch"])
            if pr_url is None:
                pr_url = self.platform.create_pr(
                    title=f"Implement #{issue.number}: {issue.title}",
                    body=self._pr_body(issue, state.run_id, winner["summary"], winner["verification_path"]),
                    head=winner["branch"],
                    base=self.config.git.default_branch,
                )
            latest = self.store.load(state.run_id)
            if latest.cancel_requested or latest.state is TaskState.CANCELLED:
                latest.pr_url = pr_url
                if latest.state is not TaskState.CANCELLED:
                    latest.transition(TaskState.CANCELLED, reason="cancel requested during publishing")
                self.store.write(latest)
                return {"run_id": state.run_id, "state": latest.state.value, "cancelled": True}
            state.pr_url = pr_url
            state.publishing_step = "pr_created"
            self.store.write(state)
            cancelled = self._cancelled_response(state)
            if cancelled:
                return cancelled
            self.knowledge.record_pr_created(
                issue_number=issue.number,
                pr_url=pr_url,
                pr_number=_parse_pr_number(pr_url),
            )
            self.platform.add_comment(
                issue.number,
                f"<!-- gg-run-id={state.run_id} stage=result -->\n"
                f"gg completed this run.\n\nPR: {pr_url}",
            )
            state.publishing_step = "result_commented"
            self.store.write(state)
            cancelled = self._cancelled_response(state)
            if cancelled:
                return cancelled

        state.transition(TaskState.COMPLETED, reason="walking skeleton complete")
        state.publishing_step = "completed"
        self.store.write(state)
        self._mark_issue_done(issue.number)
        return {
            "run_id": state.run_id,
            "state": state.state.value,
            "pr_url": state.pr_url,
            "winner": winner["candidate_id"],
        }

    def _resume_publishing(self, state, issue: Issue, *, no_pr: bool) -> dict[str, Any]:
        evaluation_path = state.artifacts.get("evaluation")
        if not evaluation_path:
            state.fail(code="missing_evaluation", message="cannot resume publishing without evaluation artifact")
            self.store.write(state)
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
        evaluation = json.loads((self.project_path / evaluation_path).read_text(encoding="utf-8"))
        winner_id = evaluation.get("winner")
        candidate = state.candidate_states.get(winner_id)
        if not winner_id or candidate is None or not candidate.result_path:
            state.fail(code="missing_winner", message="cannot resume publishing without selected candidate")
            self.store.write(state)
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
        result = json.loads((self.project_path / candidate.result_path).read_text(encoding="utf-8"))
        return self._publish_winner(
            state,
            issue,
            {
                "candidate_id": winner_id,
                "worktree_path": candidate.worktree_path,
                "branch": candidate.branch,
                "summary": result.get("summary", "Agent completed."),
                "verification_path": result.get("verification", ""),
            },
            no_pr=no_pr,
        )

    def _cancelled_response(self, state) -> dict[str, Any] | None:
        try:
            latest = self.store.load(state.run_id)
        except FileNotFoundError:
            return None
        if latest.state is TaskState.CANCELLED:
            if not latest.candidates_quiescent():
                return None
            return {"run_id": state.run_id, "state": latest.state.value, "cancelled": True}
        if latest.cancel_requested:
            if not latest.candidates_quiescent():
                return None
            latest.transition(TaskState.CANCELLED, reason="cancel requested during publishing")
            latest.last_error = latest.last_error or {
                "code": "cancel_requested",
                "message": "Run cancelled after publish side effects started",
                "at": _now_placeholder(),
            }
            self.store.write(latest)
            return {"run_id": state.run_id, "state": latest.state.value, "cancelled": True}
        return None

    def _merge_cancel_request(self, state) -> None:
        try:
            latest = self.store.load(state.run_id)
        except FileNotFoundError:
            return
        if latest.cancel_requested and not state.cancel_requested:
            state.cancel_requested = True
            state.last_error = latest.last_error or state.last_error

    def _mark_interrupted(self, state) -> None:
        try:
            latest = self.store.load(state.run_id)
        except FileNotFoundError:
            return
        for candidate in latest.candidate_states.values():
            if candidate.status == "running":
                candidate.status = "failed"
                candidate.finished_at = _now_placeholder()
                candidate.error = "interrupted by signal"
        if latest.state is TaskState.OUTCOME_PUBLISHING:
            latest.last_error = {"code": "interrupted", "message": "Publishing interrupted by operator signal", "at": _now_placeholder()}
            self.store.write(latest)
            return
        if latest.state not in TERMINAL_STATES and latest.state is not TaskState.READY_FOR_EXECUTION:
            latest.recover_to(TaskState.READY_FOR_EXECUTION, reason=f"interrupted from {latest.state.value}")
        latest.last_error = {"code": "interrupted", "message": "Run interrupted by operator signal", "at": _now_placeholder()}
        self.store.write(latest)

    def _mark_issue_blocked(self, issue_number: int, message: str) -> None:
        self._best_effort_labels(
            issue_number,
            add=[self.config.task_system.blocked_label],
            remove=[self.config.task_system.work_label],
        )
        self._best_effort_comment(
            issue_number,
            f"<!-- gg-stage=blocked -->\ngg blocked this issue: {message}",
        )

    def _mark_issue_needs_input(self, issue_number: int, message: str) -> None:
        self._best_effort_labels(
            issue_number,
            add=[self.config.task_system.blocked_label],
            remove=[self.config.task_system.work_label],
        )
        self._best_effort_comment(
            issue_number,
            f"<!-- gg-stage=needs-input -->\ngg needs local input to continue: {message}",
        )

    def _mark_issue_failed(self, issue_number: int, message: str) -> None:
        self._best_effort_labels(issue_number, add=[], remove=[self.config.task_system.work_label])
        self._best_effort_comment(
            issue_number,
            f"<!-- gg-stage=failed -->\ngg could not complete this issue: {message}",
        )

    def _mark_issue_done(self, issue_number: int) -> None:
        self._best_effort_labels(
            issue_number,
            add=[self.config.task_system.done_label],
            remove=[self.config.task_system.work_label, self.config.task_system.blocked_label],
        )

    def _best_effort_labels(self, issue_number: int, *, add: list[str], remove: list[str]) -> None:
        try:
            add_labels = [label for label in add if label]
            remove_labels = [label for label in remove if label]
            if add_labels:
                self.platform.add_labels(issue_number, add_labels)
            if remove_labels:
                self.platform.remove_labels(issue_number, remove_labels)
        except Exception:
            return

    def _best_effort_comment(self, issue_number: int, body: str) -> None:
        try:
            self.platform.add_comment(issue_number, body)
        except Exception:
            return

    def _block_on_rate_limit(self, state, issue_number: int, exc: RateLimitThrottleError) -> dict[str, Any]:
        artifact = self.store.write_json(
            state.run_id,
            "artifacts/rate-limit.json",
            {
                "schema_version": 1,
                "issue_number": issue_number,
                "bucket": exc.snapshot.bucket,
                "remaining": exc.snapshot.remaining,
                "reset_at": exc.snapshot.reset_at,
                "limit": exc.snapshot.limit,
                "message": str(exc),
                "captured_at": _now_placeholder(),
            },
        )
        state.artifacts["rate_limit"] = artifact
        state.last_error = {
            "code": "rate_limited",
            "message": str(exc),
            "bucket": exc.snapshot.bucket,
            "reset_at": exc.snapshot.reset_at,
            "at": _now_placeholder(),
        }
        if state.state not in TERMINAL_STATES:
            state.recover_to(TaskState.BLOCKED, reason=f"rate limited: {exc.snapshot.bucket}")
        self.store.write(state)
        self.knowledge.record_error(issue_number=issue_number, message=str(exc), pattern="RateLimitThrottleError")
        return {
            "run_id": state.run_id,
            "state": state.state.value,
            "error": state.last_error,
            "rate_limit": artifact,
        }

    def _throttled_response(self, exc: RateLimitThrottleError) -> dict[str, Any]:
        return {
            "state": "Throttled",
            "message": str(exc),
            "bucket": exc.snapshot.bucket,
            "reset_at": exc.snapshot.reset_at,
            "remaining": exc.snapshot.remaining,
        }

    def _eligible_issues(self, issues: list[Issue]) -> list[Issue]:
        include = set(self.config.selection.include_labels)
        exclude = set(self.config.selection.exclude_labels)

        def is_eligible(issue: Issue) -> bool:
            labels = set(issue.labels)
            if labels & exclude:
                return False
            return not include or bool(labels & include)

        return sorted(
            [issue for issue in issues if is_eligible(issue)],
            key=lambda issue: (_priority_rank(issue.labels), issue.number),
        )

    def _pr_body(self, issue: Issue, run_id: str, summary: str, verification_path: str) -> str:
        return (
            f"Implements #{issue.number}.\n\n"
            f"Run: `{run_id}`\n\n"
            f"Summary:\n{summary}\n\n"
            f"Verification artifact: `{verification_path}`\n"
        )

    def _write_evaluation(
        self,
        state,
        selected: dict[str, Any] | None,
        candidate_records: list[dict[str, Any]],
    ) -> str:
        return self.store.write_json(
            state.run_id,
            "artifacts/evaluation.json",
            {
                "schema_version": 1,
                "attempt": state.attempt,
                "max_attempts": state.max_attempts,
                "winner": selected["candidate"].candidate_id if selected else None,
                "candidates": [
                    {
                        "candidate_id": item["candidate"].candidate_id,
                        "attempt": item["attempt"],
                        "strategy": item["strategy"],
                        "status": item["effective_status"],
                        "verification_passed": item["verification_passed"],
                        "result_path": item["result_path"],
                    }
                    for item in candidate_records
                ],
            },
        )

    def _refresh_task_analysis(self, state, issue: Issue) -> TaskBrief:
        brief = TaskAnalyzer(str(self.project_path)).analyze(issue, inputs=self._load_inputs(state.run_id))
        brief_path = self.store.write_json(state.run_id, "artifacts/task-brief.json", brief.to_dict())
        state.artifacts["task_brief"] = brief_path
        snapshot_path = ContextSnapshotStore(self.project_path).write_task_snapshot(state.run_id, brief)
        state.artifacts["context_snapshot"] = snapshot_path
        return brief

    def _load_inputs(self, run_id: str) -> list[dict[str, Any]]:
        inputs_dir = self.store.path_for(run_id) / "inputs"
        artifacts: list[dict[str, Any]] = []
        for path in sorted(inputs_dir.glob("input-v1-*.json")):
            try:
                artifacts.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue
        return artifacts


def _parse_pr_number(pr_url: str) -> int:
    match = re.search(r"/pull/(\d+)|/merge_requests/(\d+)", pr_url)
    if not match:
        return 0
    return int(next(group for group in match.groups() if group))


def _candidate_strategies(count: int) -> list[str]:
    strategies = ["conservative", "test-first", "architecture-aware"]
    return [strategies[index % len(strategies)] for index in range(max(1, count))]


def _unique_candidate_id(state, base: str) -> str:
    if base not in state.candidate_states:
        return base
    suffix = 2
    while f"{base}-retry-{suffix}" in state.candidate_states:
        suffix += 1
    return f"{base}-retry-{suffix}"


def _failed_commands(checks) -> list[str]:
    return [check.command for check in checks if check.status not in {"passed", "skipped"}]


def _verification_passed(checks, baseline, *, allow_known_baseline_failures: bool) -> bool:
    failed = set(_failed_commands(checks))
    if not failed:
        return True
    if not allow_known_baseline_failures:
        return False
    baseline_failures = {
        check.command: _check_fingerprint(check)
        for check in baseline
        if check.status not in {"passed", "skipped"}
    }
    for check in checks:
        if check.status in {"passed", "skipped"}:
            continue
        if baseline_failures.get(check.command) != _check_fingerprint(check):
            return False
    return True


def _check_fingerprint(check) -> tuple:
    return (check.status, check.exit_code, check.stdout, check.stderr)


def _priority_rank(labels: list[str]) -> int:
    for label in labels:
        if label.upper() == "P0":
            return 0
        if label.upper() == "P1":
            return 1
        if label.upper() == "P2":
            return 2
        if label.upper() == "P3":
            return 3
    return 99


def _now_placeholder() -> str:
    from gg.orchestrator.state import utc_now

    return utc_now()
