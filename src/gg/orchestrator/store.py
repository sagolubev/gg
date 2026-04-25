from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from gg.orchestrator.logging import append_jsonl, mask_secrets
from gg.orchestrator.schemas import (
    AgentHandoffModel,
    AgentResultModel,
    AnalysisAgentResponseModel,
    ArchiveSummaryModel,
    CandidateResultModel,
    EvaluationArtifactModel,
    ExecutionEvaluationModel,
    InputArtifactModel,
    InputRequestModel,
    PatchConflictModel,
    PublishingIntegrationModel,
    PublishingPreflightModel,
    RateLimitArtifactModel,
    RawIssueArtifactModel,
    ResourcePreflightModel,
    RunOutcomeModel,
    RunSummaryModel,
    TaskBriefModel,
    VerificationArtifactModel,
    validation_error_message,
)
from gg.orchestrator.state import TERMINAL_STATES, RunState, TaskState, utc_now
from gg.platforms.base import Issue


def _slug(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return value[:40] or "task"


class RunStore:
    def __init__(
        self,
        project_path: str | Path,
        *,
        audit_hash_events: bool = False,
        audit_sink_path: str | Path | None = None,
        keep_state_backup: bool = False,
    ):
        self.project_path = Path(project_path).resolve()
        self.runs_dir = self.project_path / ".gg" / "runs"
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.audit_hash_events = audit_hash_events
        self.audit_sink_path = self._resolve_audit_sink(audit_sink_path)
        self.keep_state_backup = keep_state_backup

    def create(self, issue: Issue, *, dry_run: bool = False) -> RunState:
        stamp = utc_now().replace("-", "").replace(":", "").replace("T", "-").rstrip("Z")
        base_run_id = f"issue-{issue.number}-{stamp}-{_slug(issue.title)}"
        run_id = base_run_id
        suffix = 2
        while self.path_for(run_id).exists():
            run_id = f"{base_run_id}-{suffix}"
            suffix += 1
        state = RunState(
            run_id=run_id,
            issue={
                "platform": "github",
                "number": issue.number,
                "title": issue.title,
                "url": issue.url,
            },
            dry_run=dry_run,
        )
        self.write(state)
        return state

    def path_for(self, run_id: str) -> Path:
        return self.runs_dir / run_id

    def artifact_dir(self, run_id: str) -> Path:
        path = self.path_for(run_id) / "artifacts"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def candidate_dir(self, run_id: str, candidate_id: str) -> Path:
        path = self.path_for(run_id) / "candidates" / candidate_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_json(self, run_id: str, relative_path: str, data: dict) -> str:
        _validate_json_artifact(relative_path, data)
        path = self.path_for(run_id) / relative_path
        _atomic_write_text(path, json.dumps(data, indent=2, ensure_ascii=False) + "\n")
        return str(path.relative_to(self.project_path))

    def read_json(self, relative_path: str) -> dict[str, Any]:
        path = self.project_path / relative_path
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}: invalid JSON: {exc.msg}") from exc
        validation_path = _validation_relative_path(relative_path)
        _validate_json_artifact(validation_path, data)
        return data

    def write_text(self, run_id: str, relative_path: str, text: str) -> str:
        path = self.path_for(run_id) / relative_path
        _atomic_write_text(path, text)
        return str(path.relative_to(self.project_path))

    def write(self, state: RunState) -> None:
        run_dir = self.path_for(state.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "state.json"
        current: RunState | None = None
        if path.exists():
            current = self._load_state_file(path)
            if current.state in TERMINAL_STATES and state.state is not current.state:
                raise RuntimeError(f"refusing to overwrite terminal run state {current.state.value}")
        state.artifacts.setdefault("run_summary", self._run_summary_relative_path(state.run_id))
        if self.keep_state_backup and path.exists():
            backup_path = path.with_name("state.json.bak")
            _atomic_write_bytes(backup_path, path.read_bytes())
        _atomic_write_text(path, json.dumps(state.to_dict(), indent=2, ensure_ascii=False) + "\n")
        self._write_run_summary(run_dir, state)
        self._write_logs(run_dir, state, current)

    def load(self, run_id: str) -> RunState:
        path = self.path_for(run_id) / "state.json"
        return self._load_state_file(path)

    def list_runs(self) -> list[RunState]:
        runs: list[RunState] = []
        for path in sorted(self.runs_dir.glob("*/state.json")):
            try:
                runs.append(self._load_state_file(path))
            except (OSError, json.JSONDecodeError, KeyError, ValueError):
                continue
        return sorted(runs, key=lambda run: run.updated_at, reverse=True)

    def _load_state_file(self, path: Path) -> RunState:
        try:
            return self._read_state_file(path)
        except ValueError:
            if self.keep_state_backup and path.name == "state.json":
                backup_path = path.with_name("state.json.bak")
                if backup_path.exists():
                    return self._read_state_file(backup_path)
            raise

    def _read_state_file(self, path: Path) -> RunState:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}: invalid JSON: {exc.msg}") from exc
        try:
            return RunState.from_dict(data)
        except Exception as exc:
            raise ValueError(validation_error_message(str(path), exc)) from exc

    def clean_terminal_runs(self, *, dry_run: bool = True) -> list[str]:
        target_runs = [run for run in self.list_runs() if run.state in TERMINAL_STATES]
        targets = [run.run_id for run in target_runs]
        if not dry_run:
            for run in target_runs:
                self._archive_run(run)
                self._remove_worktrees(run)
                shutil.rmtree(self.path_for(run.run_id), ignore_errors=True)
        return targets

    def clean_stale_waiting_runs(
        self,
        *,
        blocked_timeout_days: int | None,
        dry_run: bool = True,
    ) -> list[str]:
        if blocked_timeout_days is None:
            return []
        cutoff = datetime.now(timezone.utc) - timedelta(days=blocked_timeout_days)
        target_runs = [
            run
            for run in self.list_runs()
            if run.state in {TaskState.BLOCKED, TaskState.NEEDS_INPUT}
            and _parse_utc(run.updated_at) <= cutoff
        ]
        targets = [run.run_id for run in target_runs]
        if not dry_run:
            for run in target_runs:
                self._archive_run(run)
                self._remove_worktrees(run)
                shutil.rmtree(self.path_for(run.run_id), ignore_errors=True)
        return targets

    def clean_orphan_worktrees(self, *, dry_run: bool = True) -> list[str]:
        root = self.project_path.parent / ".gg-worktrees" / self.project_path.name
        if not root.exists():
            return []
        referenced = {
            Path(candidate.worktree_path).resolve()
            for run in self.list_runs()
            for candidate in run.candidate_states.values()
            if candidate.worktree_path
        }
        orphans = [
            path.resolve()
            for path in root.glob("*/*")
            if path.is_dir() and path.resolve() not in referenced
        ]
        if not dry_run:
            for path in orphans:
                self._remove_worktree_path(path)
            subprocess.run(
                ["git", "worktree", "prune"],
                cwd=str(self.project_path),
                capture_output=True,
                text=True,
                timeout=60,
            )
        return [str(path) for path in orphans]

    def clean_unreferenced_objects(
        self,
        *,
        dry_run: bool = True,
        excluding_runs: set[str] | None = None,
    ) -> list[str]:
        objects_dir = self.project_path / ".gg" / "objects"
        if not objects_dir.exists():
            return []
        referenced = self._referenced_object_hashes(excluding_runs=excluding_runs or set())
        object_paths = [
            path
            for path in objects_dir.glob("*/*")
            if path.is_file() and path.name not in referenced
        ]
        if not dry_run:
            for path in object_paths:
                path.unlink(missing_ok=True)
            for directory in sorted(objects_dir.glob("*"), reverse=True):
                if directory.is_dir():
                    try:
                        directory.rmdir()
                    except OSError:
                        pass
        return [str(path) for path in object_paths]

    def _remove_worktrees(self, run: RunState) -> None:
        baseline_path = run.baseline.get("worktree_path") if isinstance(run.baseline, dict) else None
        if baseline_path:
            path = Path(baseline_path)
            if path.exists():
                self._remove_worktree_path(path)
            baseline_branch = run.baseline.get("branch")
            if baseline_branch:
                self._delete_branch(str(baseline_branch))
        for candidate in run.candidate_states.values():
            if not candidate.worktree_path:
                continue
            path = Path(candidate.worktree_path)
            if not path.exists():
                continue
            self._remove_worktree_path(path)
            if candidate.branch:
                self._delete_branch(candidate.branch)
        integration_path = self._integration_worktree_path(run)
        if integration_path is not None:
            self._remove_worktree_path(integration_path)
        subprocess.run(
            ["git", "worktree", "prune"],
            cwd=str(self.project_path),
            capture_output=True,
            text=True,
            timeout=60,
        )

    def _archive_run(self, run: RunState) -> None:
        archive_dir = self.project_path / ".gg" / "runs-archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"{run.run_id}.json"
        outcome = self._read_outcome(run)
        summary = {
            "schema_version": 1,
            "run_id": run.run_id,
            "issue": run.issue,
            "archived_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_path": str(self.path_for(run.run_id)),
            "archive_path": str(archive_path),
            "removed_worktrees": self._run_worktree_paths(run),
            "retained_artifacts": {
                "state": str(self.path_for(run.run_id) / "state.json"),
                **({"run_outcome": run.artifacts["run_outcome"]} if "run_outcome" in run.artifacts else {}),
            },
            "outcome": outcome,
        }
        ArchiveSummaryModel.model_validate(summary)
        _atomic_write_text(archive_path, json.dumps(summary, indent=2, ensure_ascii=False) + "\n")

    def _read_outcome(self, run: RunState) -> dict[str, Any] | None:
        outcome_path = run.artifacts.get("run_outcome")
        if not outcome_path:
            return None
        try:
            data = json.loads((self.project_path / outcome_path).read_text(encoding="utf-8"))
            RunOutcomeModel.model_validate(data)
            return data
        except (OSError, json.JSONDecodeError, ValueError):
            return None

    def _run_worktree_paths(self, run: RunState) -> list[str]:
        paths = [
            candidate.worktree_path
            for candidate in run.candidate_states.values()
            if candidate.worktree_path
        ]
        if isinstance(run.baseline, dict) and run.baseline.get("worktree_path"):
            paths.append(str(run.baseline["worktree_path"]))
        integration_path = self._integration_worktree_path(run)
        if integration_path is not None:
            paths.append(str(integration_path))
        return paths

    def _referenced_object_hashes(self, *, excluding_runs: set[str]) -> set[str]:
        referenced: set[str] = set()
        for run_dir in self.runs_dir.glob("*"):
            if run_dir.name in excluding_runs:
                continue
            for snapshot_path in (run_dir / "artifacts").glob("context-snapshot-v*.json"):
                try:
                    data = json.loads(snapshot_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                objects = data.get("objects", {})
                if isinstance(objects, dict):
                    referenced.update(str(value) for value in objects.values() if value)
        return referenced

    def _remove_worktree_path(self, path: Path) -> None:
        result = subprocess.run(
            ["git", "worktree", "remove", "--force", str(path)],
            cwd=str(self.project_path),
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            shutil.rmtree(path, ignore_errors=True)

    def _delete_branch(self, branch: str) -> None:
        current = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=str(self.project_path),
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
        if branch == current:
            return
        subprocess.run(
            ["git", "branch", "-D", branch],
            cwd=str(self.project_path),
            capture_output=True,
            text=True,
            timeout=60,
        )

    def _integration_worktree_path(self, run: RunState) -> Path | None:
        artifact_path = run.artifacts.get("publishing_integration")
        if not artifact_path:
            return None
        try:
            data = json.loads((self.project_path / artifact_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        worktree_path = data.get("worktree_path")
        return Path(worktree_path) if worktree_path else None

    def append_cost(self, run_id: str, payload: dict) -> None:
        append_jsonl(self.path_for(run_id) / "cost.jsonl", payload)

    def append_event(self, run_id: str, payload: dict) -> None:
        log_path = self.path_for(run_id) / "pipeline.jsonl"
        event = self._audit_payload(log_path, payload) if self.audit_hash_events else payload
        append_jsonl(log_path, event)
        if self.audit_sink_path is not None:
            append_jsonl(self.audit_sink_path, event)

    def append_error(self, run_id: str, payload: dict) -> None:
        append_jsonl(self.path_for(run_id) / "errors.jsonl", payload)

    def _write_logs(self, run_dir: Path, state: RunState, current: RunState | None) -> None:
        if current is None:
            self.append_event(
                state.run_id,
                {
                    "event": "run_created",
                    "at": state.created_at,
                    "run_id": state.run_id,
                    "state": state.state.value,
                    "issue": state.issue,
                    "dry_run": state.dry_run,
                    "attempt": state.attempt,
                },
            )
        previous_transitions = len(current.transitions) if current else 0
        for transition in state.transitions[previous_transitions:]:
            self.append_event(
                state.run_id,
                {
                    "event": "state_transition",
                    "at": transition["at"],
                    "run_id": state.run_id,
                    "from_state": transition["from"],
                    "to_state": transition["to"],
                    "reason": transition.get("reason", ""),
                    "attempt": state.attempt,
                    "publishing_step": state.publishing_step,
                    "cancel_requested": state.cancel_requested,
                },
            )
        current_artifacts = current.artifacts if current else {}
        for name, artifact_path in sorted(state.artifacts.items()):
            if current_artifacts.get(name) == artifact_path:
                continue
            self.append_event(
                state.run_id,
                {
                    "event": "artifact_updated",
                    "at": state.updated_at,
                    "run_id": state.run_id,
                    "artifact": name,
                    "path": artifact_path,
                    "state": state.state.value,
                },
            )
        current_candidates = current.candidate_states if current else {}
        for candidate_id, candidate in sorted(state.candidate_states.items()):
            if current_candidates.get(candidate_id) == candidate:
                continue
            previous = current_candidates.get(candidate_id)
            self.append_event(
                state.run_id,
                {
                    "event": "candidate_state",
                    "at": candidate.finished_at or candidate.started_at or state.updated_at,
                    "run_id": state.run_id,
                    "candidate_id": candidate_id,
                    "status": candidate.status,
                    "previous_status": previous.status if previous else None,
                    "branch": candidate.branch,
                    "worktree_path": candidate.worktree_path,
                    "result_path": candidate.result_path,
                    "error": candidate.error,
                },
            )
        if current is None or current.publishing_step != state.publishing_step:
            if state.publishing_step is not None:
                self.append_event(
                    state.run_id,
                    {
                        "event": "publishing_step",
                        "at": state.updated_at,
                        "run_id": state.run_id,
                        "state": state.state.value,
                        "publishing_step": state.publishing_step,
                        "previous_step": current.publishing_step if current else None,
                    },
                )
        if current is None or current.cancel_requested != state.cancel_requested:
            self.append_event(
                state.run_id,
                {
                    "event": "cancel_request",
                    "at": state.updated_at,
                    "run_id": state.run_id,
                    "state": state.state.value,
                    "cancel_requested": state.cancel_requested,
                },
            )
        if (
            current is None
            or current.blocked_resume_state != state.blocked_resume_state
            or current.blocked_until != state.blocked_until
        ):
            if state.blocked_resume_state is not None or state.blocked_until is not None:
                self.append_event(
                    state.run_id,
                    {
                        "event": "blocked_resume",
                        "at": state.updated_at,
                        "run_id": state.run_id,
                        "state": state.state.value,
                        "blocked_resume_state": state.blocked_resume_state.value
                        if state.blocked_resume_state
                        else None,
                        "blocked_until": state.blocked_until,
                    },
                )
        if state.last_error and (current is None or current.last_error != state.last_error):
            self.append_error(
                state.run_id,
                {
                    "event": "run_error",
                    "at": state.last_error.get("at", utc_now()),
                    "run_id": state.run_id,
                    "state": state.state.value,
                    "attempt": state.attempt,
                    "publishing_step": state.publishing_step,
                    "cancel_requested": state.cancel_requested,
                    "candidate_statuses": {
                        candidate_id: candidate.status
                        for candidate_id, candidate in sorted(state.candidate_states.items())
                    },
                    **state.last_error,
                },
            )

    def _run_summary_relative_path(self, run_id: str) -> str:
        return str((self.path_for(run_id) / "artifacts" / "run-summary.json").relative_to(self.project_path))

    def _write_run_summary(self, run_dir: Path, state: RunState) -> None:
        summary_path = run_dir / "artifacts" / "run-summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": 1,
            "run_id": state.run_id,
            "issue": state.issue,
            "state": state.state.value,
            "attempt": state.attempt,
            "max_attempts": state.max_attempts,
            "created_at": state.created_at,
            "updated_at": state.updated_at,
            "dry_run": state.dry_run,
            "baseline": state.baseline,
            "stage_attempts": state.stage_attempts,
            "locks": state.locks,
            "operator": state.operator,
            "cost": state.cost,
            "publishing_step": state.publishing_step,
            "cancel_requested": state.cancel_requested,
            "blocked_resume_state": state.blocked_resume_state.value if state.blocked_resume_state else None,
            "blocked_until": state.blocked_until,
            "pr_url": state.pr_url,
            "artifacts": state.artifacts,
            "candidate_states": {
                candidate_id: asdict(candidate)
                for candidate_id, candidate in sorted(state.candidate_states.items())
            },
            "last_error": state.last_error,
            "logs": {
                "state": str((run_dir / "state.json").relative_to(self.project_path)),
                "pipeline": str((run_dir / "pipeline.jsonl").relative_to(self.project_path)),
                "errors": str((run_dir / "errors.jsonl").relative_to(self.project_path)),
                "cost": str((run_dir / "cost.jsonl").relative_to(self.project_path)),
            },
        }
        try:
            RunSummaryModel.model_validate(payload)
        except Exception as exc:
            raise ValueError(validation_error_message("artifacts/run-summary.json", exc)) from exc
        _atomic_write_text(summary_path, json.dumps(mask_secrets(payload), indent=2, ensure_ascii=False) + "\n")

    def _resolve_audit_sink(self, audit_sink_path: str | Path | None) -> Path | None:
        if not audit_sink_path:
            return None
        path = Path(audit_sink_path)
        if not path.is_absolute():
            path = self.project_path / path
        return path

    def _audit_payload(self, log_path: Path, payload: dict) -> dict:
        sanitized = mask_secrets(payload)
        previous_hash = self._last_audit_hash(log_path)
        digest = hashlib.sha256(
            (
                previous_hash
                + "\n"
                + json.dumps(sanitized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            ).encode("utf-8"),
        ).hexdigest()
        return {
            **sanitized,
            "audit": {
                "schema_version": 1,
                "hash": digest,
                "previous_hash": previous_hash,
                "algorithm": "sha256",
            },
        }

    def _last_audit_hash(self, log_path: Path) -> str:
        if not log_path.exists():
            return ""
        try:
            for line in reversed(log_path.read_text(encoding="utf-8").splitlines()):
                if not line.strip():
                    continue
                audit = json.loads(line).get("audit", {})
                return str(audit.get("hash", ""))
        except (OSError, json.JSONDecodeError):
            return ""
        return ""


def _validate_json_artifact(relative_path: str, data: dict[str, Any]) -> None:
    schema: type | None = None
    if relative_path == "artifacts/task-brief.json" or re.match(r"artifacts/task-brief-v\d+\.json$", relative_path):
        schema = TaskBriefModel
    elif relative_path == "artifacts/raw-issue.json" or re.match(r"artifacts/raw-issue-v\d+\.json$", relative_path):
        schema = RawIssueArtifactModel
    elif re.match(r"artifacts/analysis-agent-response-v\d+\.json$", relative_path):
        schema = AnalysisAgentResponseModel
    elif relative_path == "artifacts/candidate-selection.json":
        schema = EvaluationArtifactModel
    elif relative_path == "artifacts/evaluation.json" or relative_path.endswith("/execution-evaluation.json"):
        schema = ExecutionEvaluationModel
    elif relative_path == "artifacts/run-outcome.json" or relative_path.endswith("/run-outcome.json"):
        schema = RunOutcomeModel
    elif relative_path == "artifacts/archive-summary.json" or relative_path.endswith("/archive-summary.json"):
        schema = ArchiveSummaryModel
    elif relative_path == "artifacts/input-request.json":
        schema = InputRequestModel
    elif relative_path == "artifacts/rate-limit.json":
        schema = RateLimitArtifactModel
    elif relative_path == "artifacts/resource-preflight.json":
        schema = ResourcePreflightModel
    elif relative_path == "artifacts/publishing-preflight.json":
        schema = PublishingPreflightModel
    elif relative_path == "artifacts/publishing-integration.json":
        schema = PublishingIntegrationModel
    elif relative_path == "artifacts/patch-conflict.json":
        schema = PatchConflictModel
    elif relative_path in {
        "artifacts/baseline-setup.json",
        "artifacts/baseline-verification.json",
        "artifacts/integration-verification.json",
    } or relative_path.endswith("/verification.json"):
        schema = VerificationArtifactModel
    elif relative_path.startswith("inputs/input-v1-") and relative_path.endswith(".json"):
        schema = InputArtifactModel
    elif relative_path.endswith("/agent-handoff.json"):
        schema = AgentHandoffModel
    elif relative_path.endswith("/agent-result.json"):
        schema = AgentResultModel
    elif relative_path.endswith("/candidate-result.json"):
        schema = CandidateResultModel
    if schema is None:
        return
    try:
        schema.model_validate(data)
    except Exception as exc:
        raise ValueError(validation_error_message(relative_path, exc)) from exc


def _atomic_write_text(path: Path, text: str) -> None:
    _atomic_write_bytes(path, text.encode("utf-8"))


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp_path = handle.name
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        Path(tmp_path).replace(path)
        _fsync_directory(path.parent)
    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _validation_relative_path(relative_path: str) -> str:
    parts = Path(relative_path).parts
    try:
        runs_index = parts.index("runs")
    except ValueError:
        return relative_path
    if runs_index + 2 >= len(parts):
        return relative_path
    return str(Path(*parts[runs_index + 2:]))


def _parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
