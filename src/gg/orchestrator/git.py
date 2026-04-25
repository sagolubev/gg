from __future__ import annotations

import re
import os
import shutil
import subprocess
from pathlib import Path


def run_git(args: list[str], cwd: str | Path, *, timeout: int = 60, env: dict | None = None) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {completed.stderr.strip()}")
    return completed.stdout.strip()


def safe_branch_slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return slug[:48] or "task"


def current_commit(cwd: str | Path) -> str:
    return run_git(["rev-parse", "HEAD"], cwd)


def changed_files(cwd: str | Path) -> list[str]:
    output = run_git(["status", "--short"], cwd)
    files: list[str] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        if " -> " in line:
            path = line.split(" -> ", 1)[1].strip()
        else:
            path = line[3:].strip()
        if path == ".gg-cache" or path.startswith(".gg-cache/"):
            continue
        files.append(path)
    return files


def lfs_changed_files(cwd: str | Path, files: list[str]) -> list[str]:
    if not files:
        return []
    output = run_git(["check-attr", "filter", "--", *files], cwd)
    lfs_paths: list[str] = []
    for line in output.splitlines():
        path, _, value = line.partition(": filter:")
        if value.strip() == "lfs":
            lfs_paths.append(path.strip())
    return lfs_paths


def binary_changed_files(cwd: str | Path, files: list[str]) -> list[str]:
    root = Path(cwd)
    binary_files: list[str] = []
    for rel_path in files:
        path = root / rel_path
        if not path.is_file():
            continue
        with path.open("rb") as handle:
            chunk = handle.read(8192)
        if b"\0" in chunk:
            binary_files.append(rel_path)
    return binary_files


def dependency_changed_files(files: list[str]) -> list[str]:
    dependency_names = {
        "package.json",
        "package-lock.json",
        "pnpm-lock.yaml",
        "yarn.lock",
        "bun.lockb",
        "pyproject.toml",
        "uv.lock",
        "poetry.lock",
        "Pipfile",
        "Pipfile.lock",
        "go.mod",
        "go.sum",
        "Cargo.toml",
        "Cargo.lock",
        "Gemfile",
        "Gemfile.lock",
        "composer.json",
        "composer.lock",
    }
    return [
        file
        for file in files
        if Path(file).name in dependency_names or Path(file).name.startswith("requirements")
    ]


def diff(cwd: str | Path) -> str:
    tracked = run_git(["diff", "--binary"], cwd)
    untracked = run_git(["ls-files", "--others", "--exclude-standard"], cwd)
    if not untracked:
        return tracked
    untracked_blocks = []
    for rel_path in untracked.splitlines():
        if rel_path == ".gg-cache" or rel_path.startswith(".gg-cache/"):
            continue
        path = Path(cwd) / rel_path
        if path.is_file():
            text = path.read_text(encoding="utf-8", errors="replace")
            untracked_blocks.append(
                f"diff --git a/{rel_path} b/{rel_path}\n"
                "new file mode 100644\n"
                "--- /dev/null\n"
                f"+++ b/{rel_path}\n"
                + "".join(f"+{line}" for line in text.splitlines(keepends=True))
            )
    return "\n".join(part for part in [tracked, *untracked_blocks] if part)


def commit_all(cwd: str | Path, *, message: str, author_name: str, author_email: str) -> bool:
    shutil.rmtree(Path(cwd) / ".gg-cache", ignore_errors=True)
    if not changed_files(cwd):
        return False
    run_git(["add", "-A"], cwd)
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": author_name,
        "GIT_AUTHOR_EMAIL": author_email,
        "GIT_COMMITTER_NAME": author_name,
        "GIT_COMMITTER_EMAIL": author_email,
    }
    run_git(["commit", "-m", message, "--no-gpg-sign"], cwd, env=env)
    return True


def push_branch(cwd: str | Path, branch: str) -> None:
    run_git(["push", "-u", "origin", branch], cwd, timeout=180)


class WorktreeManager:
    def __init__(self, project_path: str | Path):
        self.project_path = Path(project_path).resolve()
        parent = self.project_path.parent / ".gg-worktrees" / self.project_path.name
        self.root = parent

    def create(self, *, run_id: str, candidate_id: str, branch: str, base_ref: str) -> Path:
        path = self.root / run_id / candidate_id
        path.parent.mkdir(parents=True, exist_ok=True)
        run_git(["worktree", "add", "-b", branch, str(path), base_ref], self.project_path, timeout=120)
        return path
