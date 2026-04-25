from __future__ import annotations

import re
import os
import shutil
import subprocess
import tempfile
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


def commit_exists(cwd: str | Path, commit: str) -> bool:
    completed = subprocess.run(
        ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.returncode == 0


def is_ancestor(cwd: str | Path, ancestor: str, descendant: str) -> bool:
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.returncode == 0


def resolve_ref(cwd: str | Path, ref: str) -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "--verify", ref],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def fetch_default_branch(cwd: str | Path, default_branch: str) -> tuple[bool, bool, str]:
    remote = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if remote.returncode != 0:
        return True, False, "origin remote is not configured"
    completed = subprocess.run(
        ["git", "fetch", "origin", default_branch],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=180,
    )
    if completed.returncode != 0:
        return False, True, completed.stderr.strip() or completed.stdout.strip() or "git fetch failed"
    return True, True, completed.stderr.strip() or completed.stdout.strip()


def changed_files(cwd: str | Path) -> list[str]:
    output = run_git(["status", "--short"], cwd)
    files: list[str] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        path = _status_path(line)
        if path == ".gg-cache" or path.startswith(".gg-cache/"):
            continue
        files.append(path)
    return files


def workspace_changes(cwd: str | Path) -> list[str]:
    output = run_git(["status", "--short"], cwd)
    files: list[str] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        path = _status_path(line)
        if (
            path == ".gg"
            or path.startswith(".gg/")
            or path == ".gg-cache"
            or path.startswith(".gg-cache/")
        ):
            continue
        files.append(path)
    return files


def _status_path(line: str) -> str:
    if " -> " in line:
        return line.split(" -> ", 1)[1].strip()
    if len(line) >= 4 and line[2] == " ":
        return line[3:].strip()
    if len(line) >= 3 and line[1] == " ":
        return line[2:].strip()
    return line[3:].strip()


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


def lfs_available(cwd: str | Path) -> bool:
    completed = subprocess.run(
        ["git", "lfs", "version"],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=30,
    )
    return completed.returncode == 0


def patch_changed_files(patch_text: str) -> list[str]:
    files: list[str] = []
    seen: set[str] = set()
    for line in patch_text.splitlines():
        if not line.startswith("diff --git a/"):
            continue
        left_right = line.removeprefix("diff --git a/")
        if " b/" not in left_right:
            continue
        _left, right = left_right.split(" b/", 1)
        path = right.strip().strip('"')
        if path and path != "/dev/null" and path not in seen:
            seen.add(path)
            files.append(path)
    return files


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


def apply_patch(cwd: str | Path, patch_text: str) -> tuple[bool, str]:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".diff", delete=False) as handle:
        handle.write(patch_text)
        patch_path = handle.name
    try:
        completed = subprocess.run(
            ["git", "apply", "--3way", "--index", patch_path],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if completed.returncode == 0:
            return True, completed.stderr.strip() or completed.stdout.strip()
        return False, completed.stderr.strip() or completed.stdout.strip() or "git apply --3way --index failed"
    finally:
        Path(patch_path).unlink(missing_ok=True)


def reset_worktree(cwd: str | Path) -> None:
    run_git(["reset", "--hard"], cwd)
    run_git(["clean", "-fd"], cwd)


def remove_worktree(repo_path: str | Path, path: str | Path) -> None:
    worktree = Path(path)
    if not worktree.exists():
        return
    result = subprocess.run(
        ["git", "worktree", "remove", "--force", str(worktree)],
        cwd=str(repo_path),
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        shutil.rmtree(worktree, ignore_errors=True)


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
