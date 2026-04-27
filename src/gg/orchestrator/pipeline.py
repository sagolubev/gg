from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import re
import signal
import shutil
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("gg.pipeline")

from gg.agents.base import AgentBackend
from gg.knowledge.engine import KnowledgeEngine
from gg.orchestrator.config import GGConfig, load_config
from gg.orchestrator.context import ContextSnapshotStore
from gg.orchestrator.evaluation import build_run_outcome, CandidateEvaluator
from gg.orchestrator.executor import CandidateExecutor
from gg.orchestrator.git import binary_changed_files as git_binary_changed_files
from gg.orchestrator.git import changed_files as git_changed_files
from gg.orchestrator.git import dependency_changed_files as git_dependency_changed_files
from gg.orchestrator.git import commit_all, diff as git_diff, push_branch
from gg.orchestrator.git import apply_patch as git_apply_patch
from gg.orchestrator.git import fetch_default_branch as git_fetch_default_branch
from gg.orchestrator.git import commit_exists as git_commit_exists
from gg.orchestrator.git import is_ancestor as git_is_ancestor
from gg.orchestrator.git import lfs_available as git_lfs_available
from gg.orchestrator.git import lfs_changed_files as git_lfs_changed_files
from gg.orchestrator.git import patch_changed_files as git_patch_changed_files
from gg.orchestrator.git import remove_worktree as git_remove_worktree
from gg.orchestrator.git import reset_worktree as git_reset_worktree
from gg.orchestrator.git import resolve_ref as git_resolve_ref
from gg.orchestrator.git import safe_branch_slug, WorktreeManager
from gg.orchestrator.git import workspace_changes as git_workspace_changes
from gg.orchestrator.lock import LockManager
from gg.orchestrator.logging import mask_secrets
from gg.orchestrator.plugins import create_agent_backend, create_platform
from gg.orchestrator.rate_limit import RateLimitThrottleError
from gg.orchestrator.state import CandidateState, TaskState
from gg.orchestrator.state import TERMINAL_STATES
from gg.orchestrator.store import RunStore
from gg.orchestrator.task_analysis import (
    MAX_COMMENTS,
    MAX_COMMENT_BODY_CHARS,
    MAX_INPUTS,
    MAX_INPUT_MESSAGE_CHARS,
    MAX_ISSUE_BODY_CHARS,
    TaskAnalyzer,
    TaskBrief,
)
from gg.orchestrator.verification import (
    CheckResult,
    VerificationCommand,
    VerificationRunner,
    verification_gate_summary,
)
from gg.platforms.base import GitPlatform, Issue
from gg.utils.git_ops import find_repo_root


class OrchestratorPipeline:
    def __init__(
        self,
        project_path: str | Path = ".",
        *,
        platform: GitPlatform | None = None,
        agent: AgentBackend | None = None,
        profile: str | None = None,
        debug: bool = False,
    ):
        root = find_repo_root(project_path) or Path(project_path).resolve()
        self.project_path = Path(root).resolve()
        self.config: GGConfig = load_config(self.project_path, profile=profile)
        self._debug = debug
        self.store = RunStore(
            self.project_path,
            audit_hash_events=self.config.audit.hash_events,
            hash_artifacts=self.config.audit.hash_artifacts,
            audit_sink_path=self.config.audit.external_sink or None,
            keep_state_backup=self.config.recovery.keep_state_backup,
        )
        self.locks = LockManager(self.project_path)
        self.platform = platform or create_platform(
            self.config.task_system.platform, self.project_path, debug=debug,
        )
        self.agent = agent or create_agent_backend(
            self.config.runtime.agent_backend,
            command=_agent_command(self.config, self.config.runtime.agent_backend),
            debug=debug,
        )
        self.knowledge = KnowledgeEngine(self.project_path)
        self._projects = self._build_projects_client()
        self._state_update_lock = threading.Lock()
        self._shutdown_requested = False
        self._port_allocations: dict[str, int] = {}
        self._explicit_base_ref: str | None = None
        self._active_run_id: str | None = None

    def _install_signal_handlers(self) -> None:
        def _handler(signum, frame):
            self._shutdown_requested = True
            if self._active_run_id:
                self._mark_interrupted_by_id(self._active_run_id)
        try:
            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)
        except (OSError, ValueError):
            pass

    def configure_runtime(
        self,
        *,
        max_attempts: int | None = None,
        candidates: int | None = None,
        max_parallel_candidates: int | None = None,
        repair_fanout: int | None = None,
        timeout: int | None = None,
        base: str | None = None,
    ) -> "OrchestratorPipeline":
        updates: dict[str, int] = {}
        if max_attempts is not None:
            updates["max_attempts"] = max(1, max_attempts)
        if candidates is not None:
            updates["candidates"] = max(1, candidates)
        if max_parallel_candidates is not None:
            updates["max_parallel_candidates"] = max(1, max_parallel_candidates)
        if repair_fanout is not None:
            updates["repair_candidates"] = max(1, repair_fanout)
        if timeout is not None:
            updates["candidate_timeout_seconds"] = max(1, timeout)
        if updates:
            self.config = replace(self.config, runtime=replace(self.config.runtime, **updates))
        if base is not None and base.strip():
            explicit_base = base.strip()
            self._explicit_base_ref = explicit_base
            self.config = replace(self.config, git=replace(self.config.git, default_branch=explicit_base))
        return self

    def run_issue(
        self,
        issue_number: int,
        *,
        dry_run: bool = False,
        no_pr: bool = False,
        skip_existing: bool = False,
    ) -> dict[str, Any]:
        if dry_run:
            return self._dry_run_issue(issue_number, skip_existing=skip_existing)
        self._install_signal_handlers()
        state = None
        issue = None
        external_side_effect_started = False
        try:
            with self.locks.issue(issue_number):
                if skip_existing:
                    existing = self._existing_local_issue_run(issue_number)
                    if existing is not None:
                        return {
                            "run_id": existing.run_id,
                            "state": "AlreadyClaimed",
                            "existing_state": existing.state.value,
                            "issue": existing.issue,
                        }
                issue = self.platform.get_issue(issue_number)
                state = self.store.create(issue, dry_run=dry_run)
                self._active_run_id = state.run_id
                state.max_attempts = self.config.runtime.max_attempts
                log.info("[%s] issue #%d: %s", state.run_id, issue.number, issue.title)
                self.knowledge.record_issue_picked(issue_number=issue.number, title=issue.title, labels=issue.labels)
                state.transition(TaskState.CLAIMING, reason="issue selected")
                self.store.write(state)
                log.info("[%s] claiming issue #%d", state.run_id, issue.number)
                dirty_error = self._dirty_workspace_preflight(state)
                if dirty_error is not None:
                    state.fail(code="dirty_workspace", message=dirty_error)
                    self.store.write(state)
                    return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
                if not dry_run:
                    self.platform.validate_auth()
                    self._ensure_gg_labels()
                if not dry_run:
                    external_side_effect_started = True
                    self.platform.claim_task(
                        issue,
                        run_id=state.run_id,
                        work_label=self.config.task_system.work_label,
                    )
                    self._move_to_project_status(issue.number, self.config.project_board.status_in_progress)
                state.transition(TaskState.QUEUED, reason="claim complete")
                state.transition(TaskState.RUN_STARTED, reason="start pipeline")
                state.transition(TaskState.TASK_ANALYSIS, reason="create task brief")
                self.store.write(state)
                log.info("[%s] analyzing task brief for issue #%d", state.run_id, issue.number)

                brief = self._refresh_task_analysis(state, issue)
                if brief.blocked:
                    return self._block_on_task_analysis(state, issue, brief, dry_run=dry_run)
                budget_error = self._enforce_context_budget(state, brief)
                if budget_error:
                    return self._handle_context_too_large(state, budget_error)
                state.transition(TaskState.READY_FOR_EXECUTION, reason="task brief ready")
                self.store.write(state)
                log.info("[%s] task brief ready, starting execution", state.run_id)

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
                failed_before_claim = state.state is TaskState.EXTERNAL_TASK_READY
                if failed_before_claim:
                    state.last_error = {"code": "pipeline_error", "message": str(exc), "at": _now_placeholder()}
                    state.recover_to(TaskState.TERMINAL_FAILURE, reason="pipeline_error before claim")
                else:
                    state.fail(code="pipeline_error", message=str(exc))
                self.knowledge.record_error(issue_number=issue_number, message=str(exc), pattern=type(exc).__name__)
                self.store.write(state)
                if issue is not None and not dry_run and external_side_effect_started:
                    self._mark_issue_failed(issue.number, state.run_id, str(exc))
                return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
            except Exception:
                raise
        finally:
            self._active_run_id = None

    def _build_projects_client(self):
        cfg = self.config.project_board
        if not cfg.enabled or not cfg.project_number:
            return None
        from gg.platforms.github_projects import GitHubProjectsClient
        owner = cfg.owner or self.config.git.default_branch  # fallback; real owner resolved later
        # Try to derive owner from git remote
        try:
            from gg.utils.git_ops import get_remote_url, parse_remote_url
            url = get_remote_url(str(self.project_path))
            parsed_owner, _ = parse_remote_url(url)
            owner = parsed_owner or cfg.owner
        except Exception:
            owner = cfg.owner
        return GitHubProjectsClient(
            owner=owner,
            project_number=cfg.project_number,
            status_field=cfg.status_field,
            cwd=str(self.project_path),
            cache_ttl_seconds=self.config.polling.poll_interval_seconds,
        )

    def _move_to_project_status(self, issue_number: int, status: str) -> None:
        if self._projects is None:
            return
        self._projects.move_issue(issue_number, status)

    def _ensure_gg_labels(self) -> None:
        if getattr(self, "_labels_ensured", False):
            return
        labels = {
            self.config.task_system.work_label: "fbca04",
            self.config.task_system.in_review_label: "0075ca",
            self.config.task_system.done_label: "0e8a16",
            self.config.task_system.blocked_label: "d93f0b",
        }
        labels = {k: v for k, v in labels.items() if k}
        if labels:
            self.platform.ensure_labels(labels)
        self._labels_ensured = True

    def _dirty_workspace_preflight(self, state) -> str | None:
        changes = git_workspace_changes(self.project_path)
        artifact = {
            "schema_version": 1,
            "passed": not changes or bool(self._explicit_base_ref),
            "dirty_paths": changes,
            "explicit_base_ref": self._explicit_base_ref,
            "checked_at": _now_placeholder(),
        }
        state.artifacts["workspace_preflight"] = self.store.write_json(
            state.run_id,
            "artifacts/workspace-preflight.json",
            artifact,
        )
        if not changes or self._explicit_base_ref:
            return None
        preview = ", ".join(changes[:5])
        suffix = "" if len(changes) <= 5 else f", and {len(changes) - 5} more"
        return (
            "working tree has non-.gg changes; commit/stash them or pass --base to use an explicit base "
            f"({preview}{suffix})"
        )

    def _dry_run_issue(self, issue_number: int, *, skip_existing: bool = False) -> dict[str, Any]:
        with self.locks.issue(issue_number):
            if skip_existing:
                existing = self._existing_local_issue_run(issue_number)
                if existing is not None:
                    return {
                        "run_id": existing.run_id,
                        "state": "AlreadyClaimed",
                        "existing_state": existing.state.value,
                        "issue": existing.issue,
                        "dry_run": True,
                    }
            issue = self.platform.get_issue(issue_number)
            with tempfile.TemporaryDirectory(prefix="gg-dry-run-") as shadow_dir:
                shadow_root = Path(shadow_dir)
                shadow_store = RunStore(shadow_root)
                state = shadow_store.create(issue, dry_run=True)
                state.max_attempts = self.config.runtime.max_attempts
                state.transition(TaskState.CLAIMING, reason="dry-run issue selected")
                shadow_store.write(state)
                state.transition(TaskState.QUEUED, reason="dry-run claim simulated")
                state.transition(TaskState.RUN_STARTED, reason="dry-run start pipeline")
                state.transition(TaskState.TASK_ANALYSIS, reason="dry-run create task brief")
                shadow_store.write(state)
                analysis_agent = self._task_analysis_agent()
                analyzer = TaskAnalyzer(
                    str(self.project_path),
                    agent=analysis_agent,
                    timeout=self.config.runtime.analysis_timeout_seconds,
                    max_context_tokens=self.config.analysis.max_context_tokens,
                    model_context_tokens=_agent_context_window_tokens(analysis_agent),
                    limits=self.config.analysis.to_limits(),
                )
                brief = analyzer.analyze(issue, inputs=[])
                self._write_task_analysis_artifacts(shadow_store, state, issue, brief)
                self._write_analysis_agent_response_artifact(shadow_store, state, analyzer)
                snapshot_path = ContextSnapshotStore(
                    shadow_root,
                    hash_artifacts=self.config.audit.hash_artifacts,
                ).write_task_snapshot(state.run_id, brief)
                state.artifacts["context_snapshot"] = snapshot_path
                if brief.blocked:
                    state.transition(TaskState.BLOCKED, reason="dry-run task analysis missing information")
                    state.last_error = {
                        "code": "missing_task_info",
                        "message": "; ".join(brief.missing_questions)
                        or "task analysis needs more information",
                        "at": _now_placeholder(),
                    }
                    shadow_store.write(state)
                    return {
                        "run_id": state.run_id,
                        "state": state.state.value,
                        "dry_run": True,
                        "blocked": True,
                        "missing_questions": brief.missing_questions,
                        "error": state.last_error,
                        "planned_operations": self._planned_claim_operations(issue, state.run_id),
                    }
                state.transition(TaskState.READY_FOR_EXECUTION, reason="dry-run task brief ready")
                shadow_store.write(state)
                return {
                    "run_id": state.run_id,
                    "state": state.state.value,
                    "dry_run": True,
                    "planned_operations": self._planned_claim_operations(issue, state.run_id),
                }

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
            eligible = self._eligible_issues(issues)
            selected = eligible[:requested]
            if not selected:
                return {"state": "NoEligibleIssue", "message": "No eligible open issues found."}
            if dry_run:
                selected_numbers = {issue.number for issue in selected}
                eligible_numbers = {issue.number for issue in eligible}
                excluded = [
                    self._issue_selection_summary(issue, override_reason="not_selected_batch_limit")
                    for issue in eligible[requested:]
                ]
                excluded.extend(
                    self._issue_selection_summary(issue)
                    for issue in issues
                    if issue.number not in eligible_numbers and issue.number not in selected_numbers
                )
                return {
                    "state": "DryRun",
                    "issues": [
                        {"number": issue.number, "title": issue.title, "labels": issue.labels}
                        for issue in selected
                    ],
                    "eligible": [
                        self._issue_selection_summary(issue, override_reason="eligible")
                        for issue in eligible
                    ],
                    "excluded": excluded,
                    "count": len(selected),
                }
            issue_numbers = [issue.number for issue in selected]
        workers = min(len(issue_numbers), self.config.runtime.max_parallel_runs)
        if workers <= 1:
            results = [
                self.run_issue(issue_number, no_pr=no_pr, skip_existing=True)
                for issue_number in issue_numbers
            ]
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                results = list(
                    pool.map(
                        lambda number: self.run_issue(number, no_pr=no_pr, skip_existing=True),
                        issue_numbers,
                    )
                )
        return {
            "state": "BatchCompleted",
            "count": len(results),
            "max_parallel_runs": workers,
            "results": results,
        }

    def status(self) -> list[dict[str, Any]]:
        return [run.to_dict() for run in self.store.list_runs()]

    def _existing_local_issue_run(self, issue_number: int):
        for run in self.store.list_runs():
            if int(run.issue.get("number", 0)) != issue_number:
                continue
            if run.state not in {TaskState.TERMINAL_FAILURE, TaskState.CANCELLED}:
                return run
        return None

    def resume(self, run_id: str, *, no_pr: bool = False) -> dict[str, Any]:
        self._install_signal_handlers()
        state = self.store.load(run_id)
        issue_number = int(state.issue["number"])
        try:
            with self.locks.issue(issue_number):
                state = self.store.load(run_id)
                reconciliation_issues = self._reconcile_state_events(state)
                if reconciliation_issues:
                    self.knowledge.record_error(
                        message=f"State/event reconciliation issues: {reconciliation_issues}",
                        pattern="reconciliation_mismatch",
                    )
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
                brief_data = self.store.read_json(brief_path)
                brief = TaskBrief.from_dict(brief_data)
                issue = self.platform.get_issue(issue_number)
                if state.state in {TaskState.BLOCKED, TaskState.NEEDS_INPUT}:
                    if state.blocked_until and not self._timestamp_is_elapsed(state.blocked_until, 0):
                        return {
                            "run_id": run_id,
                            "state": state.state.value,
                            "resumed": False,
                            "message": f"Blocked until {state.blocked_until} (clock skew tolerance: {self.config.ci.clock_skew_tolerance_seconds}s).",
                        }
                    self._ingest_issue_comment_input(state, issue)
                    state = self.store.load(run_id)
                    if _waiting_for_input(state) and not self._has_current_input(state):
                        return {
                            "run_id": run_id,
                            "state": state.state.value,
                            "resumed": False,
                            "message": "Waiting for operator input.",
                        }
                if state.artifacts.get("last_input"):
                    brief = self._refresh_task_analysis(state, issue)
                    if brief.blocked:
                        return self._block_on_task_analysis(state, issue, brief, dry_run=False)
                    budget_error = self._enforce_context_budget(state, brief)
                    if budget_error:
                        return self._handle_context_too_large(state, budget_error)
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
        if state.state in {TaskState.AGENT_RUNNING, TaskState.RESULT_EVALUATION}:
            if state.attempt >= state.max_attempts:
                return {
                    "run_id": run_id,
                    "state": state.state.value,
                    "retried": False,
                    "message": "Execution attempt budget is exhausted; resume can continue recovery without creating a new attempt.",
                }
            self._mark_running_candidates_failed(state, reason="manual retry requested")
            state.attempt += 1
            state.recover_to(TaskState.READY_FOR_EXECUTION, reason=f"retry from {state.state.value}")
            self.store.write(state)
            return {**self.resume(run_id, no_pr=no_pr), "retried": True}
        if state.state in {
            TaskState.READY_FOR_EXECUTION,
            TaskState.AGENT_SELECTION,
            TaskState.BLOCKED,
            TaskState.NEEDS_INPUT,
            TaskState.TASK_ANALYSIS,
            TaskState.CLAIMING,
            TaskState.RUN_STARTED,
        }:
            return {
                **self.resume(run_id, no_pr=no_pr),
                "retried": False,
                "retry_equivalent_to_resume": True,
            }
        return {
            "run_id": run_id,
            "state": state.state.value,
            "retried": False,
            "message": "Retry is only available for recoverable runs.",
        }

    def clean(self, *, dry_run: bool = True, run_id: str | None = None) -> dict[str, Any]:
        with self.locks.queue():
            target_runs = self.store.clean_terminal_runs(
                dry_run=True,
                run_id=run_id,
                keep_last=self.config.cleanup.keep_last,
                ttl_days=self.config.cleanup.ttl_days,
            )
            stale_runs = self.store.clean_stale_waiting_runs(
                blocked_timeout_days=self.config.cleanup.blocked_timeout_days,
                clock_skew_tolerance_seconds=self.config.ci.clock_skew_tolerance_seconds,
                dry_run=True,
            )
            excluding_runs = set(target_runs + stale_runs)
            cas_objects = self.store.clean_unreferenced_objects(
                dry_run=True,
                excluding_runs=excluding_runs,
            )
            if dry_run:
                targets = target_runs
                stale_targets = stale_runs
                orphans = self.store.clean_orphan_worktrees(dry_run=True)
            else:
                targets = self.store.clean_terminal_runs(
                    dry_run=False,
                    run_id=run_id,
                    keep_last=self.config.cleanup.keep_last,
                    ttl_days=self.config.cleanup.ttl_days,
                )
                stale_targets = self.store.clean_stale_waiting_runs(
                    blocked_timeout_days=self.config.cleanup.blocked_timeout_days,
                    clock_skew_tolerance_seconds=self.config.ci.clock_skew_tolerance_seconds,
                    dry_run=False,
                )
                orphans = self.store.clean_orphan_worktrees(dry_run=False)
                cas_objects = self.store.clean_unreferenced_objects(dry_run=False)
        return {
            "dry_run": dry_run,
            "runs": targets,
            "stale_runs": stale_targets,
            "orphan_worktrees": orphans,
            "cas_objects": cas_objects,
            "count": len(targets) + len(stale_targets),
            "cleanup_policy": {
                "keep_last": self.config.cleanup.keep_last,
                "ttl_days": self.config.cleanup.ttl_days,
                "blocked_timeout_days": self.config.cleanup.blocked_timeout_days,
            },
            "reclaimed_bytes": self.store.estimate_reclaimed_bytes(
                [*targets, *stale_targets],
                orphans,
                cas_objects,
            ),
        }

    def _mark_running_candidates_failed(self, state, *, reason: str) -> None:
        for candidate in state.candidate_states.values():
            if candidate.status == "running":
                candidate.status = "failed"
                candidate.finished_at = _now_placeholder()
                candidate.error = reason

    def _abandon_run_worktrees(self, state) -> list[str]:
        removed: list[str] = []
        for candidate in state.candidate_states.values():
            if not candidate.worktree_path:
                continue
            path = Path(candidate.worktree_path)
            if not path.exists():
                continue
            git_remove_worktree(self.project_path, path)
            removed.append(str(path))
        return removed

    def cancel(
        self,
        run_id: str,
        *,
        reason: str = "operator requested cancellation",
        abandon_worktrees: bool = False,
    ) -> dict[str, Any]:
        state = self.store.load(run_id)
        if state.state in TERMINAL_STATES:
            return {"run_id": run_id, "state": state.state.value, "cancelled": False}
        if state.has_running_candidates():
            terminated_pids = self._terminate_running_candidate_processes(state)
            abandoned = self._abandon_run_worktrees(state) if abandon_worktrees else []
            state.cancel_requested = True
            state.last_error = {"code": "cancel_requested", "message": reason, "at": _now_placeholder()}
            with self.locks.run(run_id):
                self.store.write(state)
            return {
                "run_id": run_id,
                "state": state.state.value,
                "cancelled": False,
                "cancel_requested": True,
                "terminated_pids": terminated_pids,
                "abandoned_worktrees": abandoned,
            }
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
                "answered_candidate_id": self._input_request_candidate_id(state),
            }
            path = self.store.write_json(run_id, f"inputs/input-v1-{sequence_number:04d}.json", artifact)
            state.artifacts["last_input"] = path
            if state.state is TaskState.BLOCKED:
                state.transition(
                    state.blocked_resume_state or TaskState.TASK_ANALYSIS,
                    reason="operator provided input",
                )
            else:
                state.transition(TaskState.AGENT_RUNNING, reason="operator provided input")
            state.blocked_resume_state = None
            state.blocked_until = None
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

    def _ingest_issue_comment_input(self, state, issue: Issue) -> str | None:
        comment = self._first_new_external_comment(state, issue)
        if comment is None:
            return None
        message = comment.body.strip()
        content_hash = hashlib.sha256(message.encode("utf-8")).hexdigest()
        inputs_dir = self.store.path_for(state.run_id) / "inputs"
        existing = sorted(inputs_dir.glob("input-v1-*.json"))
        for path in existing:
            try:
                artifact = self.store.read_json(str(path.relative_to(self.project_path)))
            except OSError:
                continue
            if artifact.get("content_hash") == content_hash and self._input_is_current(state, artifact):
                state.artifacts["last_input"] = str(path.relative_to(self.project_path))
                self._transition_after_input(state, reason="issue comment provided input")
                self.store.write(state)
                return state.artifacts["last_input"]
        sequence_number = len(existing) + 1
        path = self.store.write_json(
            state.run_id,
            f"inputs/input-v1-{sequence_number:04d}.json",
            {
                "schema_version": 1,
                "source": f"{self.platform.platform_name()}-comment",
                "sequence_number": sequence_number,
                "content_hash": content_hash,
                "message": message,
                "created_at": comment.created_at or _now_placeholder(),
                "answered_state": state.state.value,
                "answered_candidate_id": self._input_request_candidate_id(state),
            },
        )
        state.artifacts["last_input"] = path
        self._transition_after_input(state, reason="issue comment provided input")
        self.store.write(state)
        return path

    def _transition_after_input(self, state, *, reason: str) -> None:
        if state.state is TaskState.BLOCKED:
            state.transition(state.blocked_resume_state or TaskState.TASK_ANALYSIS, reason=reason)
        elif state.state is TaskState.NEEDS_INPUT:
            state.transition(TaskState.AGENT_RUNNING, reason=reason)
        state.blocked_resume_state = None
        state.blocked_until = None

    def _first_new_external_comment(self, state, issue: Issue):
        threshold = self._input_request_created_at(state) or state.updated_at
        candidates = [
            comment
            for comment in issue.comments
            if comment.body.strip()
            and not comment.body.lstrip().startswith("<!-- gg-run-id=")
            and not comment.body.lstrip().startswith("<!-- gg-stage=")
            and (not threshold or comment.created_at > threshold)
        ]
        if not candidates:
            return None
        return sorted(candidates, key=lambda comment: comment.created_at)[0]

    def _input_request_created_at(self, state) -> str | None:
        request_path = state.artifacts.get("input_request")
        if not request_path:
            return None
        try:
            data = self.store.read_json(request_path)
        except (OSError, ValueError):
            return None
        return data.get("created_at")

    def _has_current_input(self, state) -> bool:
        input_path = state.artifacts.get("last_input")
        if not input_path:
            return False
        request_created_at = self._input_request_created_at(state)
        if not request_created_at:
            return True
        try:
            data = self.store.read_json(input_path)
        except (OSError, ValueError):
            return False
        return self._input_is_current(state, data)

    def _input_is_current(self, state, data: dict[str, Any]) -> bool:
        request_created_at = self._input_request_created_at(state)
        if not request_created_at:
            return True
        return str(data.get("created_at", "")) > request_created_at

    def _execute_ready_state(
        self,
        state,
        issue: Issue,
        brief: TaskBrief,
        *,
        no_pr: bool,
        initial_repair_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        log.info("[%s] selecting agent backend %s", state.run_id, self.config.runtime.agent_backend)
        state.transition(TaskState.AGENT_SELECTION, reason="select agent backend")
        if not self.agent.is_available():
            state.transition(TaskState.BLOCKED, reason="agent backend unavailable")
            state.blocked_resume_state = TaskState.AGENT_SELECTION
            state.blocked_until = None
            backend_name = self.config.runtime.agent_backend
            state.last_error = {"code": "missing_agent", "message": f"{backend_name} CLI is not available"}
            self.store.write(state)
            self._mark_issue_blocked(issue.number, state.run_id, f"{backend_name} CLI is not available")
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}

        executor = CandidateExecutor(self.project_path, self.agent, self.config)
        sandbox_preflight = executor.sandbox_preflight()
        sandbox_preflight["checked_at"] = _now_placeholder()
        state.artifacts["sandbox_preflight"] = self.store.write_json(
            state.run_id,
            "artifacts/sandbox-preflight.json",
            sandbox_preflight,
        )
        sandbox_error = sandbox_preflight.get("error") or executor.sandbox_preflight_error()
        if sandbox_error is not None:
            state.transition(TaskState.BLOCKED, reason="sandbox runtime unavailable")
            state.blocked_resume_state = TaskState.AGENT_SELECTION
            state.blocked_until = None
            state.last_error = {"code": "missing_sandbox_runtime", "message": sandbox_error}
            self.store.write(state)
            self._mark_issue_blocked(issue.number, state.run_id, sandbox_error)
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}

        initial_candidate_count = self.config.runtime.candidates
        resource_preflight = self._resource_preflight(state, initial_candidate_count)
        if not resource_preflight["passed"]:
            state.transition(TaskState.BLOCKED, reason="insufficient disk for candidate execution")
            state.blocked_resume_state = TaskState.AGENT_SELECTION
            state.blocked_until = None
            message = (
                f"insufficient disk for run: {resource_preflight['available_mb']}MB available, "
                f"{resource_preflight['required_mb']}MB required"
            )
            state.last_error = {"code": "insufficient_disk", "message": message, "at": _now_placeholder()}
            self.store.write(state)
            self._mark_issue_blocked(issue.number, state.run_id, message)
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
        if not self._check_disk_usage(self.project_path):
            message = (
                "insufficient disk for candidate workspace: "
                f"less than {self.config.runtime.resource.max_disk_mb}MB available"
            )
            state.transition(TaskState.BLOCKED, reason="insufficient disk for candidate workspace")
            state.blocked_resume_state = TaskState.AGENT_SELECTION
            state.blocked_until = None
            state.last_error = {"code": "insufficient_disk", "message": message, "at": _now_placeholder()}
            self.store.write(state)
            self._mark_issue_blocked(issue.number, state.run_id, message)
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}

        baseline = self._run_baseline_verification(state) if self.config.verify.baseline_check else []

        log.info("[%s] launching %d candidate(s)", state.run_id, self.config.runtime.candidates)
        state.transition(TaskState.AGENT_RUNNING, reason="run candidates")
        self.store.write(state)

        evaluator = CandidateEvaluator()
        candidate_records: list[dict[str, Any]] = []
        selected: dict[str, Any] | None = None
        repair_context: dict[str, Any] | None = initial_repair_context
        state.operator.setdefault("candidates_started", 0)
        state.operator.setdefault("best_score", None)
        state.operator.setdefault("no_progress_rounds", 0)
        self.store.write(state)

        while state.attempt <= state.max_attempts and selected is None:
            runtime_limit = self._runtime_limit_error(state)
            if runtime_limit is not None:
                state.fail(code=runtime_limit["code"], message=runtime_limit["message"])
                self.store.write(state)
                return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
            _increment_stage_attempt(state, "execution")
            self.store.write(state)
            candidate_count = (
                self.config.runtime.candidates if state.attempt == 1 else self.config.runtime.repair_candidates
            )
            if state.attempt == 1:
                candidate_count = min(candidate_count, int(resource_preflight["allowed_candidates"]))
            runtime_limit = self._runtime_limit_error(state, next_candidates=candidate_count)
            if runtime_limit is not None:
                state.fail(code=runtime_limit["code"], message=runtime_limit["message"])
                self.store.write(state)
                return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
            strategies = _candidate_strategies(candidate_count)
            if state.attempt > 1:
                strategies = [f"repair:{strategy}" for strategy in strategies]
            planned_candidates: list[tuple[int, str, str, dict[str, Any] | None, int]] = []
            for index, strategy in enumerate(strategies, start=1):
                cancelled = self._cancelled_response(state)
                if cancelled:
                    return cancelled
                base_candidate_id = (
                    f"candidate-{index}" if state.attempt == 1 else f"repair-{state.attempt}-{index}"
                )
                candidate_id = _unique_candidate_id(state, base_candidate_id)
                port = self._allocate_port(candidate_id)
                state.candidate_states[candidate_id] = CandidateState(
                    status="running",
                    started_at=_now_placeholder(),
                    port=port,
                )
                state.operator["candidates_started"] = int(state.operator.get("candidates_started", 0)) + 1
                self.store.write(state)
                planned_candidates.append((index, candidate_id, strategy, repair_context, port))
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
                latest_candidate_state = self.store.load(state.run_id).candidate_states.get(candidate.candidate_id)
                state.candidate_states[candidate.candidate_id] = CandidateState(
                    status=effective_status,
                    worktree_path=candidate.worktree_path,
                    branch=candidate.branch,
                    result_path=record["result_path"],
                    started_at=state.candidate_states[candidate.candidate_id].started_at,
                    finished_at=_now_placeholder(),
                    error=record["error"],
                    agent_pid=candidate.agent_pid or (latest_candidate_state.agent_pid if latest_candidate_state else None),
                    sandbox_pid=candidate.sandbox_pid
                    or (latest_candidate_state.sandbox_pid if latest_candidate_state else None),
                    port=latest_candidate_state.port if latest_candidate_state else None,
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
                self._merge_cancel_request(state)
                self.store.write(state)

            cancelled = self._cancelled_response(state)
            if cancelled:
                return cancelled
            log.info("[%s] evaluating candidate results", state.run_id)
            state.transition(TaskState.RESULT_EVALUATION, reason="candidate set quiescent")
            _increment_stage_attempt(state, "evaluation")
            evaluation = evaluator.evaluate(
                candidate_records,
                attempt=state.attempt,
                max_attempts=state.max_attempts,
                run_id=state.run_id,
                evaluated_at=_now_placeholder(),
            )
            selected = evaluation.winner
            state.artifacts["candidate_selection"] = self._write_candidate_selection(
                state,
                evaluation.artifact,
            )
            if evaluation.execution_evaluation is None:
                state.fail(code="missing_evaluation", message="candidate evaluator did not produce execution evaluation")
                self.store.write(state)
                return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
            evaluation_path = self._write_evaluation(state, evaluation.execution_evaluation)
            state.artifacts["evaluation"] = evaluation_path
            state.artifacts["execution_evaluation"] = evaluation_path
            if selected is None:
                if self._record_no_progress_and_should_stop(state, evaluation.artifact):
                    state.fail(
                        code="no_progress",
                        message=(
                            "repair loop made no measurable progress for "
                            f"{self.config.runtime.stop_if_no_progress_after_rounds} round(s)"
                        ),
                    )
                    self.store.write(state)
                    return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
            else:
                state.operator["no_progress_rounds"] = 0
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
                state.artifacts.pop("last_input", None)
                state.transition(TaskState.NEEDS_INPUT, reason="candidate requested operator input")
                state.blocked_resume_state = TaskState.AGENT_RUNNING
                state.blocked_until = None
                self.store.write(state)
                self._mark_issue_needs_input(
                    issue.number,
                    state.run_id,
                    needs_input["error"] or "agent requested additional input",
                )
                return {
                    "run_id": state.run_id,
                    "state": state.state.value,
                    "message": needs_input["error"] or "Agent requested additional input.",
                    "input_request": request_path,
                }
            if selected is None and state.attempt < state.max_attempts:
                repair_context = _build_repair_context(attempt_records, evaluation.execution_evaluation)
                state.attempt += 1
                state.transition(TaskState.AGENT_RUNNING, reason="repair candidate requested")
                self.store.write(state)
            else:
                break

        if selected is None:
            log.info("[%s] all candidates failed", state.run_id)
            state.fail(code="candidate_failed", message="no candidate passed execution and verification")
            self.store.write(state)
            self._mark_issue_failed(issue.number, state.run_id, "no candidate passed execution and verification")
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
        winner = selected["candidate"]
        verification_path = selected["verification_path"]
        log.info("[%s] winner: %s (files: %s)", state.run_id, winner.candidate_id, winner.changed_files)

        return self._publish_winner(
            state,
            issue,
            {
                "candidate_id": winner.candidate_id,
                "worktree_path": winner.worktree_path,
                "branch": winner.branch,
                "base_commit": winner.base_commit,
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
        planned_candidates: list[tuple[int, str, str, dict[str, Any] | None, int]],
    ) -> list[dict[str, Any]]:
        workers = min(len(planned_candidates), self.config.runtime.max_parallel_candidates)
        if workers <= 1:
            results = []
            for index, candidate_id, strategy, repair_context, port in planned_candidates:
                if self._shutdown_requested:
                    break
                results.append(self._run_candidate_attempt(
                    state=state,
                    issue=issue,
                    brief=brief,
                    executor=executor,
                    baseline=baseline,
                    index=index,
                    candidate_id=candidate_id,
                    strategy=strategy,
                    repair_context=repair_context,
                    port=port,
                ))
            return results
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            for index, candidate_id, strategy, repair_context, port in planned_candidates:
                if self._shutdown_requested:
                    break
                futures.append(pool.submit(
                    self._run_candidate_attempt,
                    state=state,
                    issue=issue,
                    brief=brief,
                    executor=executor,
                    baseline=baseline,
                    index=index,
                    candidate_id=candidate_id,
                    strategy=strategy,
                    repair_context=repair_context,
                    port=port,
                ))
            results = [future.result() for future in futures]
        return sorted(results, key=lambda item: item["index"])

    def _resource_preflight(self, state, requested_candidates: int) -> dict[str, Any]:
        resource = self.config.runtime.resource
        available_mb = _available_disk_mb(self.project_path)
        repo_size_mb = _repo_size_mb(self.project_path)
        per_candidate_mb = max(resource.max_disk_mb, repo_size_mb)
        required_mb = max(1, requested_candidates) * per_candidate_mb
        allowed_candidates = requested_candidates
        downscaled = False
        passed = available_mb >= required_mb
        if not passed and resource.allow_candidate_downscale:
            allowed_candidates = max(1, available_mb // per_candidate_mb)
            downscaled = allowed_candidates < requested_candidates
            passed = allowed_candidates >= 1
        payload = {
            "schema_version": 1,
            "available_mb": available_mb,
            "required_mb": required_mb,
            "max_disk_mb": resource.max_disk_mb,
            "repo_size_mb": repo_size_mb,
            "per_candidate_mb": per_candidate_mb,
            "estimate_strategy": "max(configured_candidate_limit, repo_checkout_size)",
            "requested_candidates": requested_candidates,
            "allowed_candidates": allowed_candidates if passed else 0,
            "downscaled": downscaled,
            "passed": passed,
            "checked_at": _now_placeholder(),
        }
        state.artifacts["resource_preflight"] = self.store.write_json(
            state.run_id,
            "artifacts/resource-preflight.json",
            payload,
        )
        self.store.write(state)
        return payload

    def _update_candidate_runtime_state(
        self,
        run_id: str,
        candidate_id: str,
        payload: dict[str, Any],
    ) -> None:
        allowed = {"worktree_path", "branch", "agent_pid", "sandbox_pid", "port"}
        updates = {key: value for key, value in payload.items() if key in allowed and value}
        message = str(payload.get("message", "")).strip()
        if message:
            log.info("[%s] %s: %s", run_id, candidate_id, message)
        if not updates:
            return
        with self._state_update_lock:
            latest = self.store.load(run_id)
            current = latest.candidate_states.get(candidate_id)
            if current is None:
                current = CandidateState(status="running", started_at=_now_placeholder())
            latest.candidate_states[candidate_id] = replace(current, **updates)
            self.store.write(latest)

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
        repair_context: dict[str, Any] | None,
        port: int | None = None,
    ) -> dict[str, Any]:
        candidate_dir = f"candidates/{candidate_id}"
        log.info("[%s] running %s (strategy=%s)", state.run_id, candidate_id, strategy)
        handoff_artifact: dict[str, str] = {}

        def persist_handoff(handoff) -> str:
            path = self.store.write_json(
                state.run_id,
                f"{candidate_dir}/agent-handoff.json",
                handoff.to_dict(),
            )
            handoff_artifact["path"] = path
            return path

        candidate = executor.run(
            run_id=state.run_id,
            issue_number=issue.number,
            brief=brief,
            candidate_id=candidate_id,
            strategy=strategy,
            repair_context=repair_context,
            attempt=state.attempt,
            task_brief_path=state.artifacts.get("task_brief", ""),
            context_snapshot_path=state.artifacts.get("context_snapshot", ""),
            port=port,
            on_status=lambda payload: self._update_candidate_runtime_state(
                state.run_id,
                candidate_id,
                payload,
            ),
            on_handoff=persist_handoff,
        )
        verification_started = time.monotonic()
        handoff_path = handoff_artifact.get("path")
        if not handoff_path:
            candidate_dir = f"candidates/{candidate.candidate_id}"
            handoff_path = self.store.write_json(
                state.run_id,
                f"{candidate_dir}/agent-handoff.json",
                executor.build_agent_handoff(
                    run_id=state.run_id,
                    candidate_id=candidate.candidate_id,
                    issue=brief.issue,
                    worktree_path=candidate.worktree_path,
                    base_commit=candidate.base_commit,
                    instructions=f"strategy={strategy}\n{_repair_context_summary(repair_context)}".strip(),
                    attempt=state.attempt,
                    task_brief_path=state.artifacts.get("task_brief", ""),
                    context_snapshot_path=state.artifacts.get("context_snapshot", ""),
                    port=port,
                ).to_dict(),
            )
        if candidate.status == "setup_failed":
            verification = [CheckResult(command="", status="skipped", exit_code=None, attempts=0)]
        else:
            verification = _with_baseline_status(
                VerificationRunner(
                    self._verification_commands(),
                    timeout=self.config.runtime.command_timeout_seconds,
                    retry_count=self.config.verify.test_retry_count,
                ).run(candidate.worktree_path),
                baseline,
            )
        final_files = git_changed_files(candidate.worktree_path)
        final_patch = git_diff(candidate.worktree_path) if final_files else ""
        verification_mutated_worktree = (
            final_files != candidate.changed_files or final_patch != candidate.patch
        )
        patch_path = self.store.write_text(state.run_id, f"{candidate_dir}/patch.diff", final_patch)
        verification_summary = verification_gate_summary(verification)
        verification_path = self.store.write_json(
            state.run_id,
            f"{candidate_dir}/verification.json",
            {
                "schema_version": 1,
                "checks": [check.to_dict() for check in verification],
                "failed_commands": _failed_commands(verification),
                "required_passed": verification_summary["required_passed"],
                "advisory_failed_commands": verification_summary["advisory_failed_commands"],
                "findings": verification_summary["findings"],
            },
        )
        verification_passed = _verification_passed(
            verification,
            baseline,
            allow_known_baseline_failures=self.config.verify.allow_known_baseline_failures,
            block_on_security_high=self.config.verify.block_on_security_high,
        )
        policy_violations = self._candidate_policy_violations(candidate.worktree_path, final_files)
        effective_status = candidate.status
        error = candidate.error
        log.info("[%s] %s finished: status=%s files=%s", state.run_id, candidate_id, candidate.status, candidate.changed_files)
        if candidate.status == "success" and not verification_passed:
            effective_status = "failed"
            error = "verification failed"
        if candidate.status == "success" and verification_mutated_worktree:
            effective_status = "failed"
            error = "verification mutated worktree"
        if candidate.status == "success" and policy_violations:
            effective_status = "failed"
            error = "; ".join(item["message"] for item in policy_violations)
        candidate_data = candidate.to_dict()
        candidate_data["changed_files"] = final_files
        candidate_data["attempt"] = state.attempt
        candidate_data["strategy"] = strategy
        candidate_data["repair_context"] = repair_context or {}
        candidate_data["patch_path"] = patch_path
        candidate_data["verification"] = verification_path
        candidate_data["verification_passed"] = verification_passed
        candidate_data["verification_mutated_worktree"] = verification_mutated_worktree
        candidate_data["baseline_failed_commands"] = _failed_commands(baseline)
        candidate_data["policy_violations"] = policy_violations
        candidate_data["effective_status"] = effective_status
        agent_result_path = self.store.write_json(
            state.run_id,
            f"{candidate_dir}/agent-result.json",
            executor.build_agent_result(
                run_id=state.run_id,
                candidate=candidate,
                artifacts={
                    "agent_handoff": handoff_path,
                    "patch": patch_path,
                    "verification": verification_path,
                },
                metrics={
                    "verification_duration_seconds": round(time.monotonic() - verification_started, 3),
                    "verification_passed": verification_passed,
                },
            ).to_dict(),
        )
        candidate_data["agent_handoff"] = handoff_path
        candidate_data["agent_result"] = agent_result_path
        result_path = self.store.write_json(
            state.run_id,
            f"{candidate_dir}/candidate-result.json",
            {**candidate_data, "patch": ""},
        )
        cost_payload = {
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
        }
        budget_error = self._projected_budget_error(state.run_id, cost_payload)
        if budget_error:
            effective_status = "failed"
            error = budget_error
            cost_payload["status"] = effective_status
            cost_payload["error"] = error
        self.store.append_cost(state.run_id, cost_payload)
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

    def _projected_budget_error(self, run_id: str, event: dict[str, Any]) -> str | None:
        cost = self.store.aggregate_cost(run_id)
        max_usd = self.config.cost.max_usd_per_run
        event_usd = event.get("total_usd")
        if max_usd is not None and isinstance(event_usd, (int, float)):
            current = cost.get("total_usd") if isinstance(cost.get("total_usd"), (int, float)) else 0.0
            if current + float(event_usd) > max_usd:
                return f"budget_exceeded: projected USD cost {current + float(event_usd):.4f} exceeds limit {max_usd:.4f}"
        max_tokens = self.config.cost.max_tokens_per_run
        token_usage = event.get("token_usage")
        if max_tokens is not None and isinstance(token_usage, dict):
            event_tokens = (
                token_usage.get("total_tokens")
                or (token_usage.get("input_tokens") or 0) + (token_usage.get("output_tokens") or 0)
            )
            if isinstance(event_tokens, int):
                current_tokens = cost.get("total_tokens") if isinstance(cost.get("total_tokens"), int) else 0
                if current_tokens + event_tokens > max_tokens:
                    return (
                        f"budget_exceeded: projected token usage {current_tokens + event_tokens} "
                        f"exceeds limit {max_tokens}"
                    )
        return None

    def _run_baseline_verification(self, state) -> list[CheckResult]:
        if state.artifacts.get("baseline_verification"):
            return self._load_baseline_verification(state)

        base_commit = git_resolve_ref(self.project_path, "HEAD") or ""
        run_hash = hashlib.sha256(state.run_id.encode("utf-8")).hexdigest()[:8]
        branch = f"gg/baseline-{run_hash}"
        worktree = WorktreeManager(self.project_path).create(
            run_id=state.run_id,
            candidate_id="baseline",
            branch=branch,
            base_ref=base_commit or "HEAD",
        )
        setup = VerificationRunner(
            [self.config.verify.setup] if self.config.verify.setup.strip() else [],
            timeout=self.config.runtime.setup_timeout_seconds,
        ).run(worktree)
        setup_path = self.store.write_json(
            state.run_id,
            "artifacts/baseline-setup.json",
            {
                "schema_version": 1,
                "checks": [check.to_dict() for check in setup],
                "failed_commands": _failed_commands(setup),
            },
        )
        verification = _with_baseline_status(
            VerificationRunner(
                self._verification_commands(),
                timeout=self.config.runtime.command_timeout_seconds,
                retry_count=self.config.verify.test_retry_count,
            ).run(worktree),
            self._load_baseline_verification(state),
        )
        failed_commands = _failed_commands(verification)
        verification_summary = verification_gate_summary(verification)
        verification_path = self.store.write_json(
            state.run_id,
            "artifacts/baseline-verification.json",
            {
                "schema_version": 1,
                "checks": [check.to_dict() for check in verification],
                "failed_commands": failed_commands,
                "required_passed": verification_summary["required_passed"],
                "advisory_failed_commands": verification_summary["advisory_failed_commands"],
                "findings": verification_summary["findings"],
            },
        )
        state.artifacts["baseline_setup"] = setup_path
        state.artifacts["baseline_verification"] = verification_path
        state.baseline = {
            "status": "failed" if failed_commands else "passed",
            "commit": base_commit,
            "branch": branch,
            "worktree_path": str(worktree),
            "verification_path": verification_path,
            "failed_commands": failed_commands,
            "checked_at": _now_placeholder(),
        }
        self.store.write(state)
        return verification

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
        log.info("[%s] publishing results", state.run_id)
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
                preflight = self._publish_preflight(state, winner)
                if not preflight["default_sync_ok"]:
                    state.fail(
                        code="default_sync_failed",
                        message=preflight["default_sync_message"] or "default branch sync failed",
                    )
                    self.store.write(state)
                    return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
                if not preflight["base_reachable"]:
                    state.fail(
                        code="base_rewritten",
                        message=f"base commit {preflight['base_commit']} is not reachable",
                    )
                    self.store.write(state)
                    return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
                target = self._prepare_integration_target(state, winner, preflight)
                if target.get("error"):
                    repaired = self._repair_after_publish_failure(
                        state,
                        issue,
                        winner,
                        preflight=preflight,
                        code=str(target.get("code") or "patch_conflict"),
                        message=str(target["error"]),
                        verification_path=str(target.get("verification_path") or ""),
                        no_pr=no_pr,
                    )
                    if repaired is not None:
                        return repaired
                    state.fail(code="patch_conflict", message=target["error"])
                    self.store.write(state)
                    return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
                winner = {**winner, **target}
                committed = commit_all(
                    winner["worktree_path"],
                    message=f"Implement issue #{issue.number}",
                    author_name=self.config.git.author_name,
                    author_email=self.config.git.author_email,
                )
                if not committed:
                    integration = self._integration_artifact(state)
                    if not integration or git_resolve_ref(winner["worktree_path"], "HEAD") == integration.get("base_ref"):
                        state.fail(code="empty_patch", message="no changes to publish")
                        self.store.write(state)
                        return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
                state.publishing_step = "committed"
                self.store.write(state)
            else:
                try:
                    winner = self._publishing_target(state, winner)
                except ValueError as exc:
                    state.fail(code="invalid_publishing_integration", message=str(exc))
                    self.store.write(state)
                    return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
                if not winner.get("integration_ready"):
                    state.fail(
                        code="missing_publishing_integration",
                        message="cannot resume publishing side effects without a verified integration artifact",
                    )
                    self.store.write(state)
                    return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
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
            if state.publishing_step in {"branch_pushed", "committed"}:
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
                self._move_to_project_status(issue.number, self.config.project_board.status_in_review)
                cancelled = self._cancelled_response(state)
                if cancelled:
                    return cancelled
                self.knowledge.record_pr_created(
                    issue_number=issue.number,
                    pr_url=pr_url,
                    pr_number=_parse_pr_number(pr_url),
                )

        winner["result_path"] = state.candidate_states.get(winner["candidate_id"], CandidateState(status="")).result_path or ""
        state.artifacts["run_outcome"] = self._write_run_outcome(state, winner)
        state.artifacts["final_verification"] = self._write_final_verification(state, winner)
        self.store.write(state)
        if not no_pr and state.publishing_step != "result_commented":
            try:
                self.platform.publish_outcome(
                    issue.number,
                    run_id=state.run_id,
                    pr_url=state.pr_url or "",
                    selected_candidate_id=winner["candidate_id"],
                    branch=winner["branch"],
                    evaluation_path=state.artifacts.get("evaluation", ""),
                    run_outcome_path=state.artifacts.get("run_outcome", ""),
                    verification_path=winner.get("verification_path", ""),
                )
            except Exception as exc:
                state.fail(code="publish_outcome_failed", message=str(exc))
                self.store.write(state)
                return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
            state.publishing_step = "result_commented"
            self.store.write(state)
            cancelled = self._cancelled_response(state)
            if cancelled:
                return cancelled
        self._cleanup_integration_worktree(state)
        if no_pr:
            done_error = self._mark_issue_done(issue.number)
            if done_error:
                state.fail(code="publish_done_failed", message=done_error)
                self.store.write(state)
                return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
        else:
            in_review_error = self._mark_issue_in_review(issue.number)
            if in_review_error:
                log.warning("[%s] in-review label swap failed (non-fatal): %s", state.run_id, in_review_error)
        state.publishing_step = "done_marked"
        self.store.write(state)
        state.transition(TaskState.COMPLETED, reason="all publish side effects complete")
        state.publishing_step = "completed"
        state.artifacts["run_outcome"] = self._write_run_outcome(state, winner)
        self.store.write(state)
        log.info("[%s] completed -- PR: %s", state.run_id, state.pr_url or "(no PR)")
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
        evaluation = self.store.read_json(evaluation_path)
        winner_id = evaluation.get("selected_candidate_id") or evaluation.get("winner")
        candidate = state.candidate_states.get(winner_id)
        if not winner_id or candidate is None or not candidate.result_path:
            state.fail(code="missing_winner", message="cannot resume publishing without selected candidate")
            self.store.write(state)
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
        result = self.store.read_json(candidate.result_path)
        return self._publish_winner(
            state,
            issue,
            {
                "candidate_id": winner_id,
                "worktree_path": candidate.worktree_path,
                "branch": candidate.branch,
                "base_commit": result.get("base_commit", ""),
                "patch_path": result.get("patch_path", ""),
                "summary": result.get("summary", "Agent completed."),
                "verification_path": result.get("verification", ""),
            },
            no_pr=no_pr,
        )

    def _publish_preflight(self, state, winner: dict[str, Any]) -> dict[str, Any]:
        worktree_path = winner["worktree_path"]
        base_commit = winner.get("base_commit", "")
        default_ref = self.config.git.default_branch
        sync_ok, sync_attempted, sync_message = git_fetch_default_branch(worktree_path, default_ref)
        origin_ref = git_resolve_ref(worktree_path, f"origin/{default_ref}")
        local_ref = git_resolve_ref(worktree_path, default_ref)
        default_commit = origin_ref or local_ref
        base_reachable = bool(base_commit and git_commit_exists(worktree_path, base_commit))
        base_is_ancestor_of_default = (
            bool(default_commit and base_reachable)
            and git_is_ancestor(worktree_path, base_commit, default_commit)
        )
        payload = {
            "schema_version": 1,
            "candidate_id": winner["candidate_id"],
            "branch": winner["branch"],
            "base_commit": base_commit,
            "default_branch": default_ref,
            "default_commit": default_commit,
            "default_commit_source": f"origin/{default_ref}" if origin_ref else default_ref,
            "default_sync_ok": sync_ok,
            "default_sync_attempted": sync_attempted,
            "default_sync_message": sync_message,
            "base_reachable": base_reachable,
            "base_is_ancestor_of_default": base_is_ancestor_of_default,
            "stale_base": bool(default_commit and base_reachable and base_commit != default_commit),
            "checked_at": _now_placeholder(),
        }
        state.artifacts["publishing_preflight"] = self.store.write_json(
            state.run_id,
            "artifacts/publishing-preflight.json",
            payload,
        )
        self.store.write(state)
        return payload

    def _prepare_integration_target(self, state, winner: dict[str, Any], preflight: dict[str, Any]) -> dict[str, Any]:
        existing = self._publishing_target(state, winner)
        if existing.get("integration_ready"):
            return existing
        patch_path = self._winner_patch_path(state, winner)
        if not patch_path:
            self._write_patch_conflict(
                state,
                winner,
                patch_path="",
                integration_branch="",
                worktree_path="",
                message="selected candidate has no patch artifact",
            )
            return {"error": "selected candidate has no patch artifact"}
        patch_text = (self.project_path / patch_path).read_text(encoding="utf-8")
        if not patch_text.strip():
            self._write_patch_conflict(
                state,
                winner,
                patch_path=patch_path,
                integration_branch="",
                worktree_path="",
                message="selected candidate patch is empty",
            )
            return {"error": "selected candidate patch is empty"}
        try:
            integration = self._integration_artifact(
                state,
                required=bool(state.artifacts.get("publishing_integration")),
            )
        except ValueError as exc:
            return {"error": str(exc)}
        if (
            integration
            and state.publishing_step in {"integration_created", "patch_applied", "verified"}
            and Path(integration.get("worktree_path", "")).exists()
        ):
            integration_branch = integration["integration_branch"]
            worktree = Path(integration["worktree_path"])
            base_ref = integration["base_ref"]
            patch_path = integration.get("patch_path", patch_path)
            git_reset_worktree(worktree)
            state.publishing_step = "integration_created"
            self.store.write(state)
        else:
            issue_number = int(state.issue.get("number", 0))
            run_hash = hashlib.sha256(state.run_id.encode("utf-8")).hexdigest()[:8]
            title_slug = safe_branch_slug(str(state.issue.get("title", "task")))[:32]
            candidate_slug = safe_branch_slug(str(winner["candidate_id"]))[:24]
            integration_branch = f"gg/issue-{issue_number}-{title_slug}-publish-{candidate_slug}-{run_hash}"
            base_ref = preflight.get("default_commit") or preflight["base_commit"]
            worktree = WorktreeManager(self.project_path).create(
                run_id=state.run_id,
                candidate_id=f"integration-{candidate_slug}",
                branch=integration_branch,
                base_ref=base_ref,
            )
            state.publishing_step = "integration_created"
            integration_artifact = self.store.write_json(
                state.run_id,
                "artifacts/publishing-integration.json",
                {
                    "schema_version": 1,
                    "candidate_id": winner["candidate_id"],
                    "source_branch": winner["branch"],
                    "integration_branch": integration_branch,
                    "worktree_path": str(worktree),
                    "base_ref": base_ref,
                    "patch_path": patch_path,
                    "created_at": _now_placeholder(),
                },
            )
            state.artifacts["publishing_integration"] = integration_artifact
            self.store.write(state)
        if state.publishing_step != "patch_applied":
            lfs_paths = self._lfs_paths_requiring_git_lfs(worktree, patch_text)
            if lfs_paths and not git_lfs_available(worktree):
                message = "git lfs is required to apply LFS file changes"
                self._write_patch_conflict(
                    state,
                    winner,
                    patch_path=patch_path,
                    integration_branch=integration_branch,
                    worktree_path=str(worktree),
                    message=message,
                    code="lfs_unavailable",
                    changed_files=lfs_paths,
                    lfs_unavailable=True,
                )
                return {"error": message, "code": "lfs_unavailable"}
            applied, message = git_apply_patch(worktree, patch_text)
            if not applied:
                self._write_patch_conflict(
                    state,
                    winner,
                    patch_path=patch_path,
                    integration_branch=integration_branch,
                    worktree_path=str(worktree),
                    message=message,
                )
                return {"error": message}
            state.publishing_step = "patch_applied"
            self.store.write(state)
        verification = _with_baseline_status(
            VerificationRunner(
                self._verification_commands(),
                timeout=self.config.runtime.command_timeout_seconds,
                retry_count=self.config.verify.test_retry_count,
            ).run(worktree),
            self._load_baseline_verification(state),
        )
        verification_summary = verification_gate_summary(verification)
        verification_path = self.store.write_json(
            state.run_id,
            "artifacts/integration-verification.json",
            {
                "schema_version": 1,
                "checks": [check.to_dict() for check in verification],
                "failed_commands": _failed_commands(verification),
                "required_passed": verification_summary["required_passed"],
                "advisory_failed_commands": verification_summary["advisory_failed_commands"],
                "findings": verification_summary["findings"],
            },
        )
        if not _verification_passed(
            verification,
            self._load_baseline_verification(state),
            allow_known_baseline_failures=self.config.verify.allow_known_baseline_failures,
            block_on_security_high=self.config.verify.block_on_security_high,
        ):
            state.artifacts["integration_verification"] = verification_path
            return {"error": "integration verification failed", "verification_path": verification_path}
        state.artifacts["integration_verification"] = verification_path
        state.publishing_step = "verified"
        self.store.write(state)
        return {
            "integration_ready": True,
            "worktree_path": str(worktree),
            "branch": integration_branch,
            "verification_path": verification_path,
        }

    def _repair_after_publish_failure(
        self,
        state,
        issue: Issue,
        winner: dict[str, Any],
        *,
        preflight: dict[str, Any],
        code: str,
        message: str,
        verification_path: str,
        no_pr: bool,
    ) -> dict[str, Any] | None:
        if state.attempt >= state.max_attempts:
            return None
        if state.publishing_step in {"committed", "branch_pushed", "pr_created", "result_commented", "completed"}:
            return None
        brief_path = state.artifacts.get("task_brief")
        if not brief_path:
            return None
        try:
            brief = TaskBrief.from_dict(self.store.read_json(brief_path))
        except (OSError, ValueError):
            return None
        failed_commands: list[str] = []
        if verification_path:
            try:
                verification = self.store.read_json(verification_path)
            except (OSError, ValueError):
                verification = {}
            failed_commands = [str(command) for command in verification.get("failed_commands", [])]
        repair_context = {
            "parent_candidate_id": winner["candidate_id"],
            "parent_result_path": (
                state.candidate_states.get(winner["candidate_id"], CandidateState(status="")).result_path or ""
            ),
            "feedback": (
                f"Publishing failed before external side effects. "
                f"code={code}; message={message}; "
                f"stale_base={preflight.get('stale_base')}; "
                f"default_commit={preflight.get('default_commit')}; base_commit={preflight.get('base_commit')}"
            ),
            "failed_commands": failed_commands,
            "publishing_failure": {
                "code": code,
                "message": message,
                "preflight": dict(preflight),
                "preflight_path": state.artifacts.get("publishing_preflight", ""),
                "patch_conflict_path": state.artifacts.get("patch_conflict", ""),
                "verification_path": verification_path,
            },
        }
        next_attempt = state.attempt + 1
        state.artifacts["publishing_repair_context"] = self.store.write_json(
            state.run_id,
            f"artifacts/publishing-repair-context-attempt-{next_attempt}.json",
            {"schema_version": 1, **repair_context, "created_at": _now_placeholder()},
        )
        self._cleanup_integration_worktree(state)
        state.attempt = next_attempt
        state.publishing_step = None
        state.recover_to(TaskState.READY_FOR_EXECUTION, reason=f"repair after publishing failure: {code}")
        self.store.write(state)
        return self._execute_ready_state(
            state,
            issue,
            brief,
            no_pr=no_pr,
            initial_repair_context=repair_context,
        )

    def _lfs_paths_requiring_git_lfs(self, worktree: Path, patch_text: str) -> list[str]:
        patch_files = git_patch_changed_files(patch_text)
        if not patch_files:
            return []
        try:
            return git_lfs_changed_files(worktree, patch_files)
        except RuntimeError:
            return []

    def _publishing_target(self, state, winner: dict[str, Any]) -> dict[str, Any]:
        if state.publishing_step not in {"committed", "branch_pushed", "pr_created", "result_commented"}:
            return winner
        data = self._integration_artifact(state, required=True)
        verification_path = state.artifacts.get("integration_verification", winner.get("verification_path", ""))
        return {
            **winner,
            "integration_ready": True,
            "worktree_path": data.get("worktree_path", winner["worktree_path"]),
            "branch": data.get("integration_branch", winner["branch"]),
            "verification_path": verification_path,
        }

    def _integration_artifact(self, state, *, required: bool = False) -> dict[str, Any] | None:
        artifact_path = state.artifacts.get("publishing_integration")
        if not artifact_path:
            if required:
                raise ValueError("missing publishing integration artifact")
            return None
        try:
            return self.store.read_json(artifact_path)
        except OSError as exc:
            if required:
                raise ValueError(f"{artifact_path}: {exc}") from exc
            return None
        except ValueError:
            if required:
                raise
            return None

    def _load_baseline_verification(self, state) -> list[CheckResult]:
        artifact_path = state.artifacts.get("baseline_verification")
        if not artifact_path:
            return []
        data = self.store.read_json(artifact_path)
        return [CheckResult(**check) for check in data.get("checks", [])]

    def _winner_patch_path(self, state, winner: dict[str, Any]) -> str:
        if winner.get("patch_path"):
            return winner["patch_path"]
        candidate = state.candidate_states.get(winner["candidate_id"])
        if not candidate or not candidate.result_path:
            return ""
        try:
            result = self.store.read_json(candidate.result_path)
        except (OSError, ValueError):
            return ""
        return str(result.get("patch_path", ""))

    def _write_patch_conflict(
        self,
        state,
        winner: dict[str, Any],
        *,
        patch_path: str,
        integration_branch: str,
        worktree_path: str,
        message: str,
        code: str = "patch_conflict",
        changed_files: list[str] | None = None,
        lfs_unavailable: bool = False,
    ) -> None:
        state.artifacts["patch_conflict"] = self.store.write_json(
            state.run_id,
            "artifacts/patch-conflict.json",
            {
                "schema_version": 1,
                "code": code,
                "candidate_id": winner["candidate_id"],
                "patch_path": patch_path,
                "integration_branch": integration_branch,
                "worktree_path": worktree_path,
                "message": message,
                "changed_files": (
                    changed_files
                    if changed_files is not None
                    else git_changed_files(worktree_path)
                    if worktree_path
                    else []
                ),
                "lfs_unavailable": lfs_unavailable,
                "created_at": _now_placeholder(),
            },
        )

    def _cleanup_integration_worktree(self, state) -> None:
        artifact_path = state.artifacts.get("publishing_integration")
        if not artifact_path:
            return
        try:
            data = self.store.read_json(artifact_path)
        except (OSError, ValueError):
            return
        worktree_path = data.get("worktree_path")
        if worktree_path:
            git_remove_worktree(self.project_path, worktree_path)

    def _terminate_running_candidate_processes(self, state) -> list[int]:
        terminated: list[int] = []
        for candidate in state.candidate_states.values():
            if candidate.status != "running":
                continue
            for pid in (candidate.sandbox_pid, candidate.agent_pid):
                if not pid or pid in terminated:
                    continue
                if _terminate_process_group(pid):
                    terminated.append(pid)
        return terminated

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
        self._mark_interrupted_by_id(state.run_id)

    def _mark_interrupted_by_id(self, run_id: str) -> None:
        try:
            latest = self.store.load(run_id)
        except FileNotFoundError:
            return
        self._terminate_running_candidate_processes(latest)
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

    def _mark_issue_blocked(self, issue_number: int, run_id: str, message: str) -> None:
        try:
            self.platform.publish_blocked(
                issue_number,
                run_id=run_id,
                message=f"gg blocked this issue: {message}",
                work_label=self.config.task_system.work_label,
                blocked_label=self.config.task_system.blocked_label,
            )
        except Exception:
            return
        self._move_to_project_status(issue_number, self.config.project_board.status_backlog)

    def _mark_issue_needs_input(self, issue_number: int, run_id: str, message: str) -> None:
        try:
            self.platform.publish_blocked(
                issue_number,
                run_id=run_id,
                message=f"gg needs local input to continue: {message}",
                work_label=self.config.task_system.work_label,
                blocked_label=self.config.task_system.blocked_label,
                stage="needs-input",
            )
        except Exception:
            return

    def _mark_issue_failed(self, issue_number: int, run_id: str, message: str) -> None:
        try:
            self.platform.publish_failed(
                issue_number,
                run_id=run_id,
                message=f"gg could not complete this issue: {message}",
                work_label=self.config.task_system.work_label,
                blocked_label=self.config.task_system.blocked_label,
            )
        except Exception:
            return

    def _mark_issue_in_review(self, issue_number: int) -> str | None:
        try:
            self.platform.publish_in_review(
                issue_number,
                work_label=self.config.task_system.work_label,
                in_review_label=self.config.task_system.in_review_label,
            )
        except Exception as exc:
            return str(exc)
        return None

    def _mark_issue_done(self, issue_number: int) -> str | None:
        self._move_to_project_status(issue_number, self.config.project_board.status_done)
        try:
            self.platform.publish_done(
                issue_number,
                work_label=self.config.task_system.work_label,
                in_review_label=self.config.task_system.in_review_label,
                blocked_label=self.config.task_system.blocked_label,
                done_label=self.config.task_system.done_label,
            )
        except Exception as exc:
            return str(exc)
        return None

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

    def _planned_claim_operations(self, issue: Issue, run_id: str) -> list[dict[str, Any]]:
        return self.platform.planned_claim_operations(
            issue,
            run_id=run_id,
            work_label=self.config.task_system.work_label,
        )

    def _block_on_rate_limit(self, state, issue_number: int, exc: RateLimitThrottleError) -> dict[str, Any]:
        resume_state = state.state
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
            state.blocked_resume_state = resume_state
            state.blocked_until = exc.snapshot.reset_at
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

    def _block_on_task_analysis(
        self,
        state,
        issue: Issue,
        brief: TaskBrief,
        *,
        dry_run: bool,
    ) -> dict[str, Any]:
        message = "; ".join(brief.missing_questions) or "task analysis needs more information"
        if state.state is TaskState.TASK_ANALYSIS:
            state.blocked_resume_state = TaskState.TASK_ANALYSIS
            state.transition(TaskState.BLOCKED, reason="task analysis missing information")
        elif state.state is not TaskState.BLOCKED:
            state.blocked_resume_state = TaskState.TASK_ANALYSIS
            state.recover_to(TaskState.BLOCKED, reason="task analysis missing information")
        elif state.blocked_resume_state is None:
            state.blocked_resume_state = TaskState.TASK_ANALYSIS
        state.blocked_until = None
        state.last_error = {"code": "missing_task_info", "message": message, "at": _now_placeholder()}
        self.store.write(state)
        if not dry_run:
            self._mark_issue_blocked(issue.number, state.run_id, message)
        return {
            "run_id": state.run_id,
            "state": state.state.value,
            "blocked": True,
            "missing_questions": brief.missing_questions,
            "error": state.last_error,
        }

    def _eligible_issues(self, issues: list[Issue]) -> list[Issue]:
        board_status = self.config.selection.board_status.strip()
        if board_status and self._projects is not None:
            try:
                allowed = self._projects.get_issues_in_status(board_status)
                # The initial list_issues fetch uses a low limit and may miss older issues.
                # Supplement with any board-listed issue not already present.
                by_number = {i.number: i for i in issues}
                for num in allowed:
                    if num not in by_number:
                        try:
                            by_number[num] = self.platform.get_issue(num)
                        except Exception:
                            pass
                issues = list(by_number.values())
                eligible = [i for i in issues if self._issue_eligibility_reason(i) == "eligible"]
                before = len(eligible)
                eligible = [i for i in eligible if i.number in allowed]
                log.info("board_status filter %r: %d -> %d eligible issues", board_status, before, len(eligible))
            except Exception as exc:
                log.warning("board_status filter failed, skipping: %s", exc)
                eligible = [i for i in issues if self._issue_eligibility_reason(i) == "eligible"]
        else:
            eligible = [issue for issue in issues if self._issue_eligibility_reason(issue) == "eligible"]
        return sorted(eligible, key=lambda issue: (_priority_rank(issue.labels), issue.number))

    def _issue_selection_summary(self, issue: Issue, *, override_reason: str | None = None) -> dict[str, Any]:
        return {
            "number": issue.number,
            "title": issue.title,
            "labels": issue.labels,
            "reason": override_reason or self._issue_eligibility_reason(issue),
        }

    def _issue_eligibility_reason(self, issue: Issue) -> str:
        include = set(self.config.selection.include_labels)
        exclude = set(self.config.selection.exclude_labels)
        labels = set(issue.labels)
        if labels & exclude:
            return "excluded_label"
        if include and not labels & include:
            return "missing_include_label"
        return "eligible"

    def _pr_body(self, issue: Issue, run_id: str, summary: str, verification_path: str) -> str:
        return (
            f"Implements #{issue.number}.\n\n"
            f"Run: `{run_id}`\n\n"
            f"Summary:\n{summary}\n\n"
            f"Verification artifact: `{verification_path}`\n"
        )

    def _write_candidate_selection(self, state, artifact: dict[str, Any]) -> str:
        return self.store.write_json(state.run_id, "artifacts/candidate-selection.json", artifact)

    def _write_evaluation(self, state, artifact: dict[str, Any]) -> str:
        return self.store.write_json(state.run_id, "artifacts/evaluation.json", artifact)

    def _write_run_outcome(self, state, selected_candidate_metadata: dict[str, Any]) -> str:
        return self.store.write_json(
            state.run_id,
            "artifacts/run-outcome.json",
            build_run_outcome(state, selected_candidate_metadata, completed_at=_now_placeholder()),
        )

    def _write_final_verification(self, state, selected_candidate_metadata: dict[str, Any]) -> str:
        evaluation_path = state.artifacts.get("execution_evaluation") or state.artifacts.get("evaluation")
        evaluation = self.store.read_json(evaluation_path) if evaluation_path else {}
        review_dimensions = dict(evaluation.get("review_dimensions") or {})
        blockers = [
            f"{name}: {', '.join(map(str, details.get('reasons') or [])) or details.get('status', 'failed')}"
            for name, details in review_dimensions.items()
            if str(details.get("status", "")).lower() != "pass"
        ]
        verification_sources = {
            "candidate_verification": str(selected_candidate_metadata.get("verification_path") or ""),
            "integration_verification": state.artifacts.get("integration_verification", ""),
            "execution_evaluation": evaluation_path or "",
        }
        payload = {
            "schema_version": 1,
            "run_id": state.run_id,
            "candidate_id": str(selected_candidate_metadata.get("candidate_id") or ""),
            "verified_at": _now_placeholder(),
            "publish_ready": not blockers,
            "verdict": str(evaluation.get("verdict") or ""),
            "traffic_light": str(evaluation.get("traffic_light") or ""),
            "review_dimensions": review_dimensions,
            "source_artifacts": verification_sources,
            "blockers": blockers,
            "summary": (
                "Final verification passed across architecture, code, security, tests, and operability."
                if not blockers
                else "Final verification found blocking review dimensions."
            ),
        }
        return self.store.write_json(
            state.run_id,
            "artifacts/final-verification.json",
            payload,
        )

    def _runtime_limit_error(self, state, *, next_candidates: int = 0) -> dict[str, str] | None:
        max_duration = self.config.runtime.max_run_duration_seconds
        if max_duration is not None:
            elapsed = _elapsed_seconds(state.created_at)
            if elapsed >= max_duration:
                return {
                    "code": "run_timeout",
                    "message": f"run exceeded max duration of {max_duration}s",
                }
        max_candidates = self.config.runtime.max_total_candidates_per_run
        if max_candidates is not None:
            launched = int(state.operator.get("candidates_started", 0))
            if launched + next_candidates > max_candidates:
                return {
                    "code": "candidate_limit_exceeded",
                    "message": (
                        f"run would exceed max_total_candidates_per_run={max_candidates} "
                        f"(started={launched}, next_batch={next_candidates})"
                    ),
                }
        return None

    def _record_no_progress_and_should_stop(self, state, artifact: dict[str, Any]) -> bool:
        threshold = self.config.runtime.stop_if_no_progress_after_rounds
        if threshold is None:
            return False
        candidates = artifact.get("candidates") or []
        current_best = max((int(item.get("score", 0)) for item in candidates), default=0)
        previous_best = state.operator.get("best_score")
        if previous_best is not None and current_best <= int(previous_best):
            state.operator["no_progress_rounds"] = int(state.operator.get("no_progress_rounds", 0)) + 1
        else:
            state.operator["no_progress_rounds"] = 0
        state.operator["best_score"] = current_best
        return int(state.operator.get("no_progress_rounds", 0)) >= threshold

    def _verification_commands(self) -> list[VerificationCommand]:
        commands: list[VerificationCommand] = []
        configured_categories: set[str] = set()
        configured = (
            ("tests", "test", self.config.verify.tests, True, ""),
            ("lint", "lint", self.config.verify.lint, True, ""),
            ("typecheck", "typecheck", self.config.verify.typecheck, True, ""),
            ("security", "security", self.config.verify.security, True, ""),
            ("coverage", "coverage", self.config.verify.coverage, not self.config.verify.advisory_checks, ""),
            ("format", "format", self.config.verify.format_check, not self.config.verify.advisory_checks, ""),
            (
                "dependency-audit",
                "dependency-audit",
                self.config.verify.dependency_audit,
                not self.config.verify.advisory_checks,
                "",
            ),
            ("secret-scan", "security", self.config.verify.secret_scan, True, "secret-scan"),
        )
        for id_, category, command, required, parser in configured:
            if command.strip():
                configured_categories.add(category)
                commands.append(
                    VerificationCommand(
                        id=id_,
                        category=category,
                        command=command,
                        required=required,
                        parser=parser or _default_verification_parser(category, command),
                    )
                )
        if self.config.verify.discovery_enabled:
            commands.extend(_discover_verification_commands(self.project_path, configured_categories))
        for index, command in enumerate(self.config.verify.custom, start=1):
            if command.strip():
                commands.append(
                    VerificationCommand(
                        id=f"custom-{index}",
                        category="custom",
                        command=command,
                        required=True,
                    )
                )
        return commands

    def _write_task_analysis_artifacts(
        self,
        store: RunStore,
        state,
        issue: Issue,
        brief: TaskBrief,
    ) -> None:
        version = _next_artifact_version(store.path_for(state.run_id) / "artifacts", "task-brief")
        raw_issue_path = store.write_json(
            state.run_id,
            f"artifacts/raw-issue-v{version}.json",
            _raw_issue_artifact(issue, brief, self.config.analysis),
        )
        brief_path = store.write_json(
            state.run_id,
            f"artifacts/task-brief-v{version}.json",
            brief.to_dict(),
        )
        state.artifacts["raw_issue"] = raw_issue_path
        state.artifacts["task_brief"] = brief_path
        state.artifacts["task_brief_version"] = str(version)

    def _write_analysis_agent_response_artifact(
        self,
        store: RunStore,
        state,
        analyzer: TaskAnalyzer,
    ) -> None:
        if not analyzer.last_agent_error:
            return
        version = _next_artifact_version(store.path_for(state.run_id) / "artifacts", "analysis-agent-response")
        response = mask_secrets(analyzer.last_agent_response)
        error = mask_secrets(analyzer.last_agent_error)
        path = store.write_json(
            state.run_id,
            f"artifacts/analysis-agent-response-v{version}.json",
            {
                "schema_version": 1,
                "ok": False,
                "error": str(error),
                "response": str(response),
                "truncated": analyzer.last_agent_response_truncated,
                "limits": {
                    "max_agent_response_chars": self.config.analysis.max_agent_response_chars,
                },
                "created_at": _now_placeholder(),
            },
        )
        state.artifacts["analysis_agent_response"] = path

    def _refresh_task_analysis(self, state, issue: Issue) -> TaskBrief:
        _increment_stage_attempt(state, "analysis")
        self.store.write(state)
        analysis_agent = self._task_analysis_agent()
        analyzer = TaskAnalyzer(
            str(self.project_path),
            agent=analysis_agent,
            timeout=self.config.runtime.analysis_timeout_seconds,
            max_context_tokens=self.config.analysis.max_context_tokens,
            model_context_tokens=_agent_context_window_tokens(analysis_agent),
            limits=self.config.analysis.to_limits(),
        )
        brief = analyzer.analyze(issue, inputs=self._load_inputs(state.run_id))
        self._write_task_analysis_artifacts(self.store, state, issue, brief)
        self._write_analysis_agent_response_artifact(self.store, state, analyzer)
        snapshot_path = ContextSnapshotStore(
            self.project_path,
            hash_artifacts=self.config.audit.hash_artifacts,
        ).write_task_snapshot(state.run_id, brief)
        state.artifacts["context_snapshot"] = snapshot_path
        return brief

    def _task_analysis_agent(self) -> AgentBackend | None:
        if getattr(self.agent, "supports_task_analysis", False):
            return self.agent
        return None

    def _load_inputs(self, run_id: str) -> list[dict[str, Any]]:
        inputs_dir = self.store.path_for(run_id) / "inputs"
        artifacts: list[dict[str, Any]] = []
        for path in sorted(inputs_dir.glob("input-v1-*.json")):
            try:
                artifacts.append(self.store.read_json(str(path.relative_to(self.project_path))))
            except OSError:
                continue
        return artifacts

    def _input_request_candidate_id(self, state) -> str | None:
        request_path = state.artifacts.get("input_request")
        if not request_path:
            return None
        try:
            data = self.store.read_json(request_path)
        except (OSError, ValueError):
            return None
        return data.get("candidate_id")

    def _enforce_context_budget(self, state, brief: "TaskBrief") -> str | None:
        """Return error code if context budget is exceeded, None otherwise."""
        analysis = self.config.analysis
        if len(brief.candidate_files) > analysis.max_candidate_files:
            return "context_too_large"
        budget = brief.context_budget or {}
        effective_tokens = budget.get("effective_context_tokens") or analysis.max_context_tokens
        estimated_tokens = budget.get("estimated_tokens")
        if isinstance(estimated_tokens, int) and isinstance(effective_tokens, int):
            if estimated_tokens > effective_tokens:
                return "context_too_large"
        for relative_path in brief.candidate_files:
            path = (self.project_path / relative_path).resolve()
            try:
                path.relative_to(self.project_path)
            except ValueError:
                return "context_too_large"
            if not path.is_file():
                continue
            try:
                if len(path.read_text(encoding="utf-8", errors="ignore")) > analysis.max_file_chars:
                    return "context_too_large"
            except OSError:
                continue
        return None

    def _handle_context_too_large(self, state, code: str) -> dict[str, Any]:
        policy = self.config.analysis.context_too_large_policy
        if policy == "blocked":
            if state.state is TaskState.TASK_ANALYSIS:
                state.transition(
                    TaskState.BLOCKED,
                    reason=f"context too large (policy={policy})",
                )
            else:
                state.recover_to(TaskState.BLOCKED, reason=f"context too large (policy={policy})")
            state.last_error = {"code": code, "message": "Context budget exceeded", "at": _now_placeholder()}
            self.store.write(state)
            return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}
        state.fail(code=code, message="Context budget exceeded")
        self.store.write(state)
        return {"run_id": state.run_id, "state": state.state.value, "error": state.last_error}

    def _check_disk_usage(self, worktree_path: str | Path) -> bool:
        """Return True if disk is OK, False if over quota."""
        try:
            path = Path(worktree_path) if worktree_path else self.project_path
            available_mb = _available_disk_mb(path)
            return available_mb >= self.config.runtime.resource.max_disk_mb
        except OSError:
            return True

    def _allocate_port(self, candidate_id: str, port_range: tuple[int, int] | None = None) -> int:
        """Allocate a port for a candidate deterministically, probing for availability."""
        import socket as _socket
        lo, hi = port_range or self.config.runtime.port_range
        span = hi - lo
        digest = int(hashlib.sha256(candidate_id.encode()).hexdigest(), 16)
        base = lo + (digest % span)
        used = set(self._port_allocations.values())
        for offset in range(span):
            port = lo + (base - lo + offset) % span
            if port in used:
                continue
            try:
                with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
                    s.bind(("127.0.0.1", port))
                self._port_allocations[candidate_id] = port
                return port
            except OSError:
                continue
        raise RuntimeError(f"No available port in range {lo}-{hi} for candidate {candidate_id}")

    def _timestamp_is_elapsed(self, ts: str, threshold_seconds: float) -> bool:
        """Check if a timestamp is older than threshold, with clock skew tolerance."""
        tolerance = self.config.ci.clock_skew_tolerance_seconds
        try:
            from datetime import datetime, timezone
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - dt).total_seconds()
            return age > (threshold_seconds + tolerance)
        except (ValueError, TypeError):
            return False

    def _reconcile_state_events(self, state) -> list[dict[str, Any]]:
        """Compare state.transitions against pipeline.jsonl state_transition events."""
        mismatches: list[dict[str, Any]] = []
        run_dir = self.store.path_for(state.run_id)
        pipeline_log = run_dir / "pipeline.jsonl"
        if not pipeline_log.exists():
            return mismatches
        try:
            log_transitions: list[dict[str, Any]] = []
            for line in pipeline_log.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("event") == "state_transition":
                    log_transitions.append(entry)
            state_transitions = [
                {
                    "from": t.from_state if hasattr(t, "from_state") else t.get("from", ""),
                    "to": t.to_state if hasattr(t, "to_state") else t.get("to", ""),
                }
                for t in state.transitions
            ]
            if len(state_transitions) != len(log_transitions):
                mismatches.append({
                    "mismatch": "count",
                    "state_count": len(state_transitions),
                    "log_count": len(log_transitions),
                })
        except OSError:
            pass
        return mismatches


def _parse_pr_number(pr_url: str) -> int:
    match = re.search(r"/pull/(\d+)|/merge_requests/(\d+)", pr_url)
    if not match:
        return 0
    return int(next(group for group in match.groups() if group))


def _waiting_for_input(state) -> bool:
    if state.state is TaskState.NEEDS_INPUT:
        return True
    return state.state is TaskState.BLOCKED and (state.last_error or {}).get("code") == "missing_task_info"


def _candidate_strategies(count: int) -> list[str]:
    strategies = ["conservative", "test-first", "architecture-aware"]
    return [strategies[index % len(strategies)] for index in range(max(1, count))]


def _increment_stage_attempt(state, stage: str) -> None:
    attempts = dict(state.stage_attempts)
    attempts[stage] = int(attempts.get(stage, 0)) + 1
    state.stage_attempts = attempts


def _unique_candidate_id(state, base: str) -> str:
    if base not in state.candidate_states:
        return base
    suffix = 2
    while f"{base}-retry-{suffix}" in state.candidate_states:
        suffix += 1
    return f"{base}-retry-{suffix}"


def _build_repair_context(
    attempt_records: list[dict[str, Any]],
    execution_evaluation: dict[str, Any] | None,
) -> dict[str, Any]:
    parent = next((record for record in attempt_records if record["effective_status"] != "needs_input"), None)
    if parent is None and attempt_records:
        parent = attempt_records[0]
    failed_commands: list[str] = []
    feedback = "No candidate passed deterministic eligibility gates."
    if execution_evaluation:
        reasons = execution_evaluation.get("reasons") or []
        feedback = "; ".join(str(reason) for reason in reasons)[:2000] or feedback
        for candidate in execution_evaluation.get("candidates", []):
            failed_commands.extend(str(command) for command in candidate.get("failed_commands", []))
    return {
        "parent_candidate_id": parent["candidate"].candidate_id if parent else "",
        "parent_result_path": parent.get("result_path", "") if parent else "",
        "feedback": feedback,
        "failed_commands": sorted(set(failed_commands)),
    }


def _repair_context_summary(repair_context: dict[str, Any] | None) -> str:
    if not repair_context:
        return ""
    parent = repair_context.get("parent_candidate_id") or "unknown"
    feedback = str(repair_context.get("feedback") or "").strip()
    return f"repair parent={parent}; feedback={feedback[:500]}"


def _next_artifact_version(artifacts_dir: Path, prefix: str) -> int:
    versions: list[int] = []
    for path in artifacts_dir.glob(f"{prefix}-v*.json"):
        match = re.fullmatch(rf"{re.escape(prefix)}-v(\d+)\.json", path.name)
        if match:
            versions.append(int(match.group(1)))
    return (max(versions) + 1) if versions else 1


def _raw_issue_artifact(issue: Issue, brief: TaskBrief, analysis_config=None) -> dict[str, Any]:
    body = str(brief.issue.get("body", ""))
    comments = list(brief.issue.get("comments", []))
    inputs = list(brief.issue.get("inputs", []))
    max_issue_body_chars = getattr(analysis_config, "max_issue_body_chars", MAX_ISSUE_BODY_CHARS)
    max_comments = getattr(analysis_config, "max_comments", MAX_COMMENTS)
    max_comment_body_chars = getattr(analysis_config, "max_comment_body_chars", MAX_COMMENT_BODY_CHARS)
    max_inputs = getattr(analysis_config, "max_inputs", MAX_INPUTS)
    max_input_message_chars = getattr(analysis_config, "max_input_message_chars", MAX_INPUT_MESSAGE_CHARS)
    return {
        "schema_version": 1,
        "issue": {
            "number": issue.number,
            "title": issue.title,
            "body": body,
            "labels": issue.labels,
            "url": issue.url,
        },
        "comments": comments,
        "inputs": inputs,
        "limits": {
            "max_issue_body_chars": max_issue_body_chars,
            "max_comments": max_comments,
            "max_comment_body_chars": max_comment_body_chars,
            "max_inputs": max_inputs,
            "max_input_message_chars": max_input_message_chars,
        },
        "truncated": {
            "issue_body": len(issue.body or "") > max_issue_body_chars,
            "comments": len(issue.comments) > max_comments
            or any(len(comment.body or "") > max_comment_body_chars for comment in issue.comments),
            "inputs": len(inputs) >= max_inputs,
        },
    }


def _failed_commands(checks) -> list[str]:
    return [check.command for check in checks if check.status not in {"passed", "skipped", "flaky"}]


def _default_verification_parser(category: str, command: str) -> str:
    if category == "test":
        lowered = command.lower()
        if any(tool in lowered for tool in ("npm", "yarn", "pnpm", "bun", "vitest", "jest")):
            return "npm,vitest,jest"
        return "pytest"
    if category == "lint":
        return "ruff"
    if category == "typecheck":
        return "mypy"
    if category == "security":
        parsers = ["secret-scan"]
        if "bandit" in command.lower():
            parsers.insert(0, "bandit")
        return ",".join(parsers)
    return ""


def _discover_verification_commands(
    project_path: Path,
    configured_categories: set[str],
) -> list[VerificationCommand]:
    commands: list[VerificationCommand] = []
    package_scripts = _package_json_scripts(project_path / "package.json")

    if "test" not in configured_categories:
        if "test" in package_scripts:
            commands.append(_discovered_command("tests", "test", "npm test", required=True))
        elif _has_pytest_surface(project_path):
            commands.append(_discovered_command("tests", "test", "pytest", required=True))

    if "lint" not in configured_categories:
        if "lint" in package_scripts:
            commands.append(_discovered_command("lint", "lint", "npm run lint", required=False))
        elif _has_ruff_surface(project_path):
            commands.append(_discovered_command("lint", "lint", "ruff check .", required=False))

    if "typecheck" not in configured_categories:
        if "typecheck" in package_scripts:
            commands.append(
                _discovered_command("typecheck", "typecheck", "npm run typecheck", required=False)
            )
        elif _has_mypy_surface(project_path):
            commands.append(_discovered_command("typecheck", "typecheck", "mypy .", required=False))

    if "security" not in configured_categories and _has_bandit_surface(project_path):
        commands.append(_discovered_command("security", "security", "bandit -r .", required=False))

    return commands


def _discovered_command(
    id_: str,
    category: str,
    command: str,
    *,
    required: bool,
) -> VerificationCommand:
    return VerificationCommand(
        id=id_,
        category=category,
        command=command,
        required=required,
        parser=_default_verification_parser(category, command),
    )


def _package_json_scripts(path: Path) -> dict[str, str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    scripts = payload.get("scripts") if isinstance(payload, dict) else None
    return {str(key): str(value) for key, value in scripts.items()} if isinstance(scripts, dict) else {}


def _has_pytest_surface(project_path: Path) -> bool:
    return any(
        (project_path / name).exists()
        for name in ("pytest.ini", "tox.ini", "setup.cfg", "pyproject.toml", "tests")
    )


def _has_ruff_surface(project_path: Path) -> bool:
    if (project_path / "ruff.toml").exists() or (project_path / ".ruff.toml").exists():
        return True
    return _file_contains(project_path / "pyproject.toml", "[tool.ruff")


def _has_mypy_surface(project_path: Path) -> bool:
    if (project_path / "mypy.ini").exists():
        return True
    return _file_contains(project_path / "pyproject.toml", "[tool.mypy")


def _has_bandit_surface(project_path: Path) -> bool:
    return (
        (project_path / ".bandit").exists()
        or _file_contains(project_path / "pyproject.toml", "[tool.bandit")
        or _file_contains(project_path / "pyproject.toml", "bandit")
    )


def _file_contains(path: Path, needle: str) -> bool:
    try:
        return needle in path.read_text(encoding="utf-8")
    except OSError:
        return False


def _agent_context_window_tokens(agent: AgentBackend | None) -> int | None:
    if agent is None:
        return None
    value = getattr(agent, "context_window_tokens", None)
    if callable(value):
        value = value()
    return value if isinstance(value, int) and value > 0 else None


def _agent_command(config: GGConfig, backend: str) -> str | None:
    selected = backend.strip().lower()
    if selected == "codex":
        return config.agent.codex_command
    if selected == "claude":
        return config.agent.claude_command
    return None


def _terminate_process_group(pid: int) -> bool:
    try:
        os.killpg(pid, signal.SIGTERM)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        try:
            os.kill(pid, signal.SIGTERM)
            return True
        except ProcessLookupError:
            return False
        except OSError:
            return False


def _available_disk_mb(path: Path) -> int:
    usage = shutil.disk_usage(path)
    return usage.free // (1024 * 1024)


def _repo_size_mb(path: Path) -> int:
    total = 0
    excluded = {".git", ".gg", ".gg-cache"}
    deadline = time.monotonic() + 5
    pending = [path]
    while pending:
        if time.monotonic() > deadline:
            return 0
        current = pending.pop()
        try:
            entries = list(os.scandir(current))
        except OSError:
            continue
        for entry in entries:
            name = entry.name
            try:
                if entry.is_dir(follow_symlinks=False):
                    if current == path and name in excluded:
                        continue
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
            except OSError:
                continue
    return (total + (1024 * 1024) - 1) // (1024 * 1024)


def _verification_passed(
    checks,
    baseline,
    *,
    allow_known_baseline_failures: bool,
    block_on_security_high: bool,
) -> bool:
    if block_on_security_high and _new_high_security_findings(checks, baseline):
        return False
    failed = {
        check.command
        for check in checks
        if getattr(check, "required", True)
        and check.status not in {"passed", "skipped", "flaky"}
    }
    if not failed:
        return True
    if not allow_known_baseline_failures:
        return False
    baseline_failures = {
        check.command: _check_fingerprint(check)
        for check in baseline
        if check.status not in {"passed", "skipped", "flaky"}
    }
    for check in checks:
        if not getattr(check, "required", True):
            continue
        if check.status in {"passed", "skipped", "flaky"}:
            continue
        if baseline_failures.get(check.command) != _check_fingerprint(check):
            return False
    return True


def _new_high_security_findings(checks, baseline) -> list[tuple]:
    baseline_findings = {
        _security_finding_fingerprint(finding)
        for check in baseline
        for finding in (check.findings or [])
        if _is_high_security_finding(finding)
    }
    return [
        fingerprint
        for check in checks
        for finding in (check.findings or [])
        if _is_high_security_finding(finding)
        for fingerprint in (_security_finding_fingerprint(finding),)
        if fingerprint not in baseline_findings
    ]


def _is_high_security_finding(finding: dict[str, Any]) -> bool:
    if str(finding.get("category", "")).lower() != "security":
        return False
    return str(finding.get("severity", "")).lower() in {"high", "critical"}


def _security_finding_fingerprint(finding: dict[str, Any]) -> tuple:
    return (
        str(finding.get("parser", "")),
        str(finding.get("code", "")),
        str(finding.get("file", "")),
        int(finding.get("line") or 0),
        str(finding.get("message", "")),
        str(finding.get("severity", "")).lower(),
    )


def _with_baseline_status(checks, baseline) -> list[CheckResult]:
    baseline_failures = {
        check.command: _check_fingerprint(check)
        for check in baseline
        if check.status not in {"passed", "skipped", "flaky"}
    }
    annotated: list[CheckResult] = []
    for check in checks:
        if check.status in {"passed", "skipped", "flaky"}:
            baseline_status = "passed"
        elif check.command not in baseline_failures:
            baseline_status = "new_failure"
        elif baseline_failures[check.command] == _check_fingerprint(check):
            baseline_status = "known_failure"
        else:
            baseline_status = "changed_failure"
        annotated.append(replace(check, baseline_status=baseline_status))
    return annotated


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


def _elapsed_seconds(started_at: str) -> int:
    try:
        started = datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return 0
    return max(0, int((datetime.now(timezone.utc) - started).total_seconds()))
