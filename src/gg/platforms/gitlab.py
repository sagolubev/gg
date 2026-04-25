from __future__ import annotations

import json
import subprocess

from gg.platforms.base import GitPlatform, Issue


class GitLabPlatform(GitPlatform):
    def __init__(self, cwd: str = "."):
        self._cwd = cwd

    def _run(self, args: list[str]) -> str:
        result = subprocess.run(
            ["glab", *args],
            capture_output=True, text=True, timeout=30,
            cwd=self._cwd,
        )
        if result.returncode != 0:
            raise RuntimeError(f"glab {' '.join(args)} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def list_issues(self, state: str = "opened", limit: int = 30) -> list[Issue]:
        raw = self._run([
            "issue", "list",
            "--state", state,
            "--per-page", str(limit),
            "--output", "json",
        ])
        items = json.loads(raw) if raw else []
        return [
            Issue(
                number=i.get("iid", 0),
                title=i.get("title", ""),
                body=i.get("description", ""),
                labels=i.get("labels", []),
                assignees=[a.get("username", "") for a in i.get("assignees", [])],
                state=i.get("state", "opened"),
                url=i.get("web_url", ""),
            )
            for i in items
        ]

    def get_issue(self, number: int) -> Issue:
        raw = self._run(["issue", "view", str(number), "--output", "json"])
        i = json.loads(raw)
        return Issue(
            number=i.get("iid", number),
            title=i.get("title", ""),
            body=i.get("description", ""),
            labels=i.get("labels", []),
            assignees=[a.get("username", "") for a in i.get("assignees", [])],
            state=i.get("state", "opened"),
            url=i.get("web_url", ""),
        )

    def create_pr(self, *, title: str, body: str, head: str, base: str) -> str:
        raw = self._run([
            "mr", "create",
            "--title", title,
            "--description", body,
            "--source-branch", head,
            "--target-branch", base,
            "--yes",
        ])
        return raw

    def find_pr(self, *, head: str) -> str | None:
        raw = self._run([
            "mr", "list",
            "--source-branch", head,
            "--output", "json",
        ])
        items = json.loads(raw) if raw else []
        return items[0].get("web_url") if items else None

    def add_comment(self, issue_number: int, body: str) -> None:
        self._run(["issue", "note", str(issue_number), "--message", body])

    def add_labels(self, issue_number: int, labels: list[str]) -> None:
        for label in labels:
            self._run(["issue", "update", str(issue_number), "--label", label])

    def remove_labels(self, issue_number: int, labels: list[str]) -> None:
        for label in labels:
            self._run(["issue", "update", str(issue_number), "--unlabel", label])

    def cli_name(self) -> str:
        return "glab"

    def platform_name(self) -> str:
        return "gitlab"
