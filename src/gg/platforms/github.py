from __future__ import annotations

import json
import subprocess

from gg.platforms.base import GitPlatform, Issue


class GitHubPlatform(GitPlatform):
    def __init__(self, cwd: str = "."):
        self._cwd = cwd

    def _run(self, args: list[str]) -> str:
        result = subprocess.run(
            ["gh", *args],
            capture_output=True, text=True, timeout=30,
            cwd=self._cwd,
        )
        if result.returncode != 0:
            raise RuntimeError(f"gh {' '.join(args)} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def list_issues(self, state: str = "open", limit: int = 30) -> list[Issue]:
        raw = self._run([
            "issue", "list",
            "--state", state,
            "--limit", str(limit),
            "--json", "number,title,body,labels,assignees,state,url",
        ])
        items = json.loads(raw) if raw else []
        return [
            Issue(
                number=i["number"],
                title=i["title"],
                body=i.get("body", ""),
                labels=[la["name"] for la in i.get("labels", [])],
                assignees=[a["login"] for a in i.get("assignees", [])],
                state=i.get("state", "open"),
                url=i.get("url", ""),
            )
            for i in items
        ]

    def get_issue(self, number: int) -> Issue:
        raw = self._run([
            "issue", "view", str(number),
            "--json", "number,title,body,labels,assignees,state,url",
        ])
        i = json.loads(raw)
        return Issue(
            number=i["number"],
            title=i["title"],
            body=i.get("body", ""),
            labels=[la["name"] for la in i.get("labels", [])],
            assignees=[a["login"] for a in i.get("assignees", [])],
            state=i.get("state", "open"),
            url=i.get("url", ""),
        )

    def create_pr(self, *, title: str, body: str, head: str, base: str) -> str:
        raw = self._run([
            "pr", "create",
            "--title", title,
            "--body", body,
            "--head", head,
            "--base", base,
        ])
        return raw

    def find_pr(self, *, head: str) -> str | None:
        raw = self._run([
            "pr", "list",
            "--state", "open",
            "--head", head,
            "--limit", "1",
            "--json", "url",
        ])
        items = json.loads(raw) if raw else []
        return items[0]["url"] if items else None

    def add_comment(self, issue_number: int, body: str) -> None:
        self._run(["issue", "comment", str(issue_number), "--body", body])

    def add_labels(self, issue_number: int, labels: list[str]) -> None:
        if labels:
            self._run(["issue", "edit", str(issue_number), "--add-label", ",".join(labels)])

    def remove_labels(self, issue_number: int, labels: list[str]) -> None:
        if labels:
            self._run(["issue", "edit", str(issue_number), "--remove-label", ",".join(labels)])

    def cli_name(self) -> str:
        return "gh"

    def platform_name(self) -> str:
        return "github"
