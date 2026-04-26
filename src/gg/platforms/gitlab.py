from __future__ import annotations

import json

from gg.platforms.base import GitPlatform, Issue, IssueComment, PlatformCapabilities

MAX_COMMENTS = 10
MAX_COMMENT_CHARS = 4000


def _parse_comments(payload: dict) -> list[IssueComment]:
    raw_comments = payload.get("comments") or payload.get("notes") or payload.get("discussions") or []
    if isinstance(raw_comments, dict):
        raw_comments = raw_comments.get("nodes") or raw_comments.get("items") or []
    if not isinstance(raw_comments, list):
        return []
    comments: list[IssueComment] = []
    for item in raw_comments:
        node = item if isinstance(item, dict) else {}
        if "notes" in node and isinstance(node["notes"], list):
            nested = node["notes"]
        else:
            nested = [node]
        for note in nested:
            body = str(note.get("body") or note.get("note") or note.get("text") or "").strip()
            if not body:
                continue
            author = note.get("author") or {}
            if isinstance(author, dict):
                author = author.get("username") or author.get("name") or ""
            comments.append(
                IssueComment(
                    body=body[:MAX_COMMENT_CHARS],
                    author=str(author or ""),
                    created_at=str(note.get("created_at") or note.get("createdAt") or ""),
                    url=str(note.get("web_url") or note.get("url") or ""),
                )
            )
    return comments[-MAX_COMMENTS:]


class GitLabPlatform(GitPlatform):
    def __init__(self, cwd: str = ".", *, rate_limit_store=None, debug: bool = False):
        super().__init__(cwd, rate_limit_store=rate_limit_store, debug=debug)

    def _run(self, args: list[str], *, bucket: str) -> str:
        return self._run_command(args, bucket=bucket)

    def _command_env(self) -> dict[str, str]:
        env = super()._command_env()
        env.setdefault("GLAB_DEBUG_HTTP", "true")
        return env

    def capabilities(self) -> PlatformCapabilities:
        return PlatformCapabilities(labels=True, find_pr=True)

    def list_issues(self, state: str = "opened", limit: int = 30) -> list[Issue]:
        raw = self._run([
            "issue", "list",
            "--state", state,
            "--per-page", str(limit),
            "--output", "json",
        ], bucket=self._bucket("issues:read"))
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
        raw = self._run(["issue", "view", str(number), "--output", "json"], bucket=self._bucket("issues:read"))
        i = json.loads(raw)
        return Issue(
            number=i.get("iid", number),
            title=i.get("title", ""),
            body=i.get("description", ""),
            labels=i.get("labels", []),
            assignees=[a.get("username", "") for a in i.get("assignees", [])],
            state=i.get("state", "opened"),
            url=i.get("web_url", ""),
            comments=_parse_comments(i),
        )

    def create_pr(self, *, title: str, body: str, head: str, base: str) -> str:
        raw = self._run([
            "mr", "create",
            "--title", title,
            "--description", body,
            "--source-branch", head,
            "--target-branch", base,
            "--yes",
        ], bucket=self._bucket("merge-requests:write"))
        return raw

    def find_pr(self, *, head: str) -> str | None:
        raw = self._run([
            "mr", "list",
            "--source-branch", head,
            "--output", "json",
        ], bucket=self._bucket("merge-requests:read"))
        items = json.loads(raw) if raw else []
        return items[0].get("web_url") if items else None

    def validate_auth(self) -> None:
        self._run(["auth", "status"], bucket=self._bucket("auth"))

    def add_comment(self, issue_number: int, body: str) -> None:
        self._run(["issue", "note", str(issue_number), "--message", body], bucket=self._bucket("issues:comment"))

    def add_labels(self, issue_number: int, labels: list[str]) -> None:
        for label in labels:
            self._run(["issue", "update", str(issue_number), "--label", label], bucket=self._bucket("issues:labels"))

    def remove_labels(self, issue_number: int, labels: list[str]) -> None:
        for label in labels:
            self._run(
                ["issue", "update", str(issue_number), "--unlabel", label],
                bucket=self._bucket("issues:labels"),
            )

    def cli_name(self) -> str:
        return "glab"

    def platform_name(self) -> str:
        return "gitlab"
