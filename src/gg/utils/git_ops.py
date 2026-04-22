from __future__ import annotations

import subprocess
from pathlib import Path


def find_repo_root(path: str | Path) -> Path | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=5,
            cwd=str(path),
        )
        if result.returncode == 0:
            return Path(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def get_remote_url(path: str | Path) -> str:
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=5,
            cwd=str(path),
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return ""


def get_main_branch(path: str | Path) -> str:
    for candidate in ("main", "master"):
        result = subprocess.run(
            ["git", "rev-parse", "--verify", candidate],
            capture_output=True, text=True, timeout=5,
            cwd=str(path),
        )
        if result.returncode == 0:
            return candidate
    return "main"


def parse_remote_url(url: str) -> tuple[str, str]:
    """Extract owner and repo from git remote URL."""
    url = url.rstrip("/")
    if url.endswith(".git"):
        url = url[:-4]

    if "github.com" in url or "gitlab.com" in url or "gitlab" in url:
        parts = url.split("/")
        if len(parts) >= 2:
            return parts[-2], parts[-1]

    if url.startswith("git@"):
        _, path_part = url.split(":", 1)
        parts = path_part.split("/")
        if len(parts) >= 2:
            return parts[-2], parts[-1]

    return "", ""
