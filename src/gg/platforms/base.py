from __future__ import annotations

import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Issue:
    number: int
    title: str
    body: str
    labels: list[str] = field(default_factory=list)
    assignees: list[str] = field(default_factory=list)
    state: str = "open"
    url: str = ""


class GitPlatform(ABC):
    @abstractmethod
    def list_issues(self, state: str = "open", limit: int = 30) -> list[Issue]:
        """List issues from the tracker."""

    @abstractmethod
    def get_issue(self, number: int) -> Issue:
        """Get a single issue by number."""

    @abstractmethod
    def create_pr(self, *, title: str, body: str, head: str, base: str) -> str:
        """Create a pull/merge request. Returns URL."""

    def find_pr(self, *, head: str) -> str | None:
        """Find an existing open pull/merge request by head branch when supported."""
        return None

    @abstractmethod
    def add_comment(self, issue_number: int, body: str) -> None:
        """Add a comment to an issue."""

    def add_labels(self, issue_number: int, labels: list[str]) -> None:
        """Add labels to an issue when the platform supports it."""

    def remove_labels(self, issue_number: int, labels: list[str]) -> None:
        """Remove labels from an issue when the platform supports it."""

    @abstractmethod
    def cli_name(self) -> str:
        """Name of the CLI tool (gh or glab)."""

    @abstractmethod
    def platform_name(self) -> str:
        """Platform identifier (github or gitlab)."""


def detect_platform(project_path: str | Path) -> str:
    """Auto-detect platform from git remote URL. Returns 'github', 'gitlab', or 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=5,
            cwd=str(project_path),
        )
        url = result.stdout.strip().lower()
        if "github.com" in url:
            return "github"
        if "gitlab" in url:
            return "gitlab"
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return "unknown"
