from __future__ import annotations

import os
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

from gg.orchestrator.rate_limit import (
    RateLimitSnapshot,
    RateLimitStore,
    RateLimitThrottleError,
    extract_retry_after_seconds,
)
from gg.utils.git_ops import get_remote_url, parse_remote_url


@dataclass(frozen=True)
class Issue:
    number: int
    title: str
    body: str
    labels: list[str] = field(default_factory=list)
    assignees: list[str] = field(default_factory=list)
    state: str = "open"
    url: str = ""
    comments: list[IssueComment] = field(default_factory=list)


class GitPlatform(ABC):
    def __init__(self, cwd: str = ".", *, rate_limit_store: RateLimitStore | None = None):
        self._cwd = str(cwd)
        self._rate_limit_store = rate_limit_store or RateLimitStore(cwd)
        remote_url = get_remote_url(cwd)
        owner, repo = parse_remote_url(remote_url)
        self._rate_limit_repo = "/".join(part for part in (owner, repo) if part) or Path(cwd).resolve().name

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

    def _command_env(self) -> dict[str, str]:
        return os.environ.copy()

    def _run_command(self, args: list[str], *, bucket: str) -> str:
        self._raise_if_throttled(bucket)
        result = subprocess.run(
            [self.cli_name(), *args],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=self._cwd,
            env=self._command_env(),
        )
        header_text = "\n".join(part for part in (result.stderr, result.stdout) if part)
        snapshot = self._rate_limit_store.record_http_headers(bucket, header_text)
        if result.returncode != 0:
            if self._looks_rate_limited(result.stderr) or self._looks_rate_limited(result.stdout):
                snapshot = snapshot or self._backoff(bucket, header_text)
                raise RateLimitThrottleError(snapshot)
            raise RuntimeError(f"{self.cli_name()} {' '.join(args)} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def _bucket(self, scope: str) -> str:
        return f"{self.platform_name()}:{self._rate_limit_repo}:{scope}"

    def _raise_if_throttled(self, bucket: str) -> None:
        snapshot = self._rate_limit_store.get(bucket)
        if snapshot is None or not self._rate_limit_store.should_throttle(bucket):
            return
        raise RateLimitThrottleError(snapshot)

    def _backoff(self, bucket: str, text: str) -> RateLimitSnapshot:
        retry_after = extract_retry_after_seconds(text) or 60
        return self._rate_limit_store.backoff(bucket, retry_after_seconds=retry_after)

    def _looks_rate_limited(self, text: str) -> bool:
        lowered = text.lower()
        return "rate limit" in lowered or "too many requests" in lowered


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
