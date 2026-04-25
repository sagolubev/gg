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
class IssueComment:
    body: str
    author: str = ""
    created_at: str = ""
    url: str = ""


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


@dataclass(frozen=True)
class PlatformCapabilities:
    issue_listing: bool = True
    issue_comments: bool = True
    labels: bool = False
    pull_requests: bool = True
    find_pr: bool = False
    assign: bool = False

    def to_dict(self) -> dict[str, bool]:
        return {
            "issue_listing": self.issue_listing,
            "issue_comments": self.issue_comments,
            "labels": self.labels,
            "pull_requests": self.pull_requests,
            "find_pr": self.find_pr,
            "assign": self.assign,
        }


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

    def capabilities(self) -> PlatformCapabilities:
        """Describe mutation/read features the adapter supports."""
        return PlatformCapabilities()

    def validate_auth(self) -> None:
        """Validate tracker CLI authentication before mutating external state."""

    def planned_claim_operations(self, issue: Issue, *, run_id: str, work_label: str) -> list[dict]:
        """Return the external mutations claim_task would perform."""
        operations: list[dict] = []
        if work_label:
            operations.append(
                {
                    "operation": "add_labels",
                    "issue_number": issue.number,
                    "labels": [work_label],
                }
            )
        operations.append(
            {
                "operation": "add_comment",
                "issue_number": issue.number,
                "marker": self.stage_marker(run_id, "claim"),
                "body": f"gg picked this issue for implementation. Run: `{run_id}`",
            }
        )
        return operations

    def claim_task(self, issue: Issue, *, run_id: str, work_label: str) -> None:
        """Mark a tracker task as claimed for this orchestrator run."""
        if work_label:
            self.add_labels(issue.number, [work_label])
        self.add_stage_comment_once(
            issue.number,
            run_id,
            "claim",
            f"gg picked this issue for implementation. Run: `{run_id}`",
        )

    def publish_blocked(
        self,
        issue_number: int,
        *,
        run_id: str,
        message: str,
        work_label: str,
        blocked_label: str,
        stage: str = "blocked",
    ) -> None:
        """Publish a blocked/needs-input state to the external tracker."""
        self.apply_labels(issue_number, add=[blocked_label], remove=[work_label])
        self.add_stage_comment_once(issue_number, run_id, stage, message)

    def publish_failed(
        self,
        issue_number: int,
        *,
        run_id: str,
        message: str,
        work_label: str,
        blocked_label: str,
    ) -> None:
        """Publish a terminal failure to the external tracker."""
        self.apply_labels(issue_number, add=[], remove=[work_label, blocked_label])
        self.add_stage_comment_once(issue_number, run_id, "failed", message)

    def publish_done(
        self,
        issue_number: int,
        *,
        work_label: str,
        blocked_label: str,
        done_label: str,
    ) -> None:
        """Publish successful completion labels to the external tracker."""
        self.apply_labels(issue_number, add=[done_label], remove=[work_label, blocked_label])

    def publish_outcome(self, issue_number: int, *, run_id: str, pr_url: str) -> None:
        """Publish the final result comment for an issue."""
        self.add_stage_comment_once(
            issue_number,
            run_id,
            "result",
            f"gg completed this run.\n\nPR: {pr_url}",
        )

    def cleanup_claim(self, issue_number: int, *, work_label: str, blocked_label: str) -> None:
        """Remove transient claim labels from a tracker task."""
        self.apply_labels(issue_number, add=[], remove=[work_label, blocked_label])

    def apply_labels(self, issue_number: int, *, add: list[str], remove: list[str]) -> None:
        add_labels = [label for label in add if label]
        remove_labels = [label for label in remove if label]
        if add_labels:
            self.add_labels(issue_number, add_labels)
        if remove_labels:
            self.remove_labels(issue_number, remove_labels)

    def add_stage_comment_once(self, issue_number: int, run_id: str, stage: str, message: str) -> None:
        marker = self.stage_marker(run_id, stage)
        if self.issue_has_comment_marker(issue_number, marker):
            return
        self.add_comment(issue_number, f"{marker}\n{message}")

    def issue_has_comment_marker(self, issue_number: int, marker: str) -> bool:
        try:
            issue = self.get_issue(issue_number)
        except Exception:
            return False
        return any(marker in comment.body for comment in issue.comments)

    def stage_marker(self, run_id: str, stage: str) -> str:
        return f"<!-- gg-run-id={run_id} stage={stage} -->"

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
