from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

from gg.orchestrator.logging import append_jsonl
from gg.orchestrator.state import TERMINAL_STATES, RunState, utc_now
from gg.platforms.base import Issue


def _slug(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return value[:40] or "task"


class RunStore:
    def __init__(self, project_path: str | Path):
        self.project_path = Path(project_path).resolve()
        self.runs_dir = self.project_path / ".gg" / "runs"
        self.runs_dir.mkdir(parents=True, exist_ok=True)

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
        path = self.path_for(run_id) / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
        return str(path.relative_to(self.project_path))

    def write_text(self, run_id: str, relative_path: str, text: str) -> str:
        path = self.path_for(run_id) / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
        return str(path.relative_to(self.project_path))

    def write(self, state: RunState) -> None:
        run_dir = self.path_for(state.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "state.json"
        current: RunState | None = None
        if path.exists():
            current = RunState.from_dict(json.loads(path.read_text(encoding="utf-8")))
            if current.state in TERMINAL_STATES and state.state is not current.state:
                raise RuntimeError(f"refusing to overwrite terminal run state {current.state.value}")
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
        self._write_logs(run_dir, state, current)

    def load(self, run_id: str) -> RunState:
        path = self.path_for(run_id) / "state.json"
        return RunState.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def list_runs(self) -> list[RunState]:
        runs: list[RunState] = []
        for path in sorted(self.runs_dir.glob("*/state.json")):
            try:
                runs.append(RunState.from_dict(json.loads(path.read_text(encoding="utf-8"))))
            except (OSError, json.JSONDecodeError, KeyError, ValueError):
                continue
        return sorted(runs, key=lambda run: run.updated_at, reverse=True)

    def clean_terminal_runs(self, *, dry_run: bool = True) -> list[str]:
        target_runs = [run for run in self.list_runs() if run.state in TERMINAL_STATES]
        targets = [run.run_id for run in target_runs]
        if not dry_run:
            for run in target_runs:
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

    def _remove_worktrees(self, run: RunState) -> None:
        for candidate in run.candidate_states.values():
            if not candidate.worktree_path:
                continue
            path = Path(candidate.worktree_path)
            if not path.exists():
                continue
            self._remove_worktree_path(path)
            if candidate.branch:
                self._delete_branch(candidate.branch)
        subprocess.run(
            ["git", "worktree", "prune"],
            cwd=str(self.project_path),
            capture_output=True,
            text=True,
            timeout=60,
        )

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

    def append_cost(self, run_id: str, payload: dict) -> None:
        append_jsonl(self.path_for(run_id) / "cost.jsonl", payload)

    def append_event(self, run_id: str, payload: dict) -> None:
        append_jsonl(self.path_for(run_id) / "pipeline.jsonl", payload)

    def append_error(self, run_id: str, payload: dict) -> None:
        append_jsonl(self.path_for(run_id) / "errors.jsonl", payload)

    def _write_logs(self, run_dir: Path, state: RunState, current: RunState | None) -> None:
        if current is None or current.state != state.state or current.updated_at != state.updated_at:
            self.append_event(
                state.run_id,
                {
                    "at": state.updated_at,
                    "run_id": state.run_id,
                    "state": state.state.value,
                    "attempt": state.attempt,
                    "publishing_step": state.publishing_step,
                    "cancel_requested": state.cancel_requested,
                },
            )
        if state.last_error and (current is None or current.last_error != state.last_error):
            self.append_error(
                state.run_id,
                {
                    "at": state.last_error.get("at", utc_now()),
                    "run_id": state.run_id,
                    "state": state.state.value,
                    **state.last_error,
                },
            )
