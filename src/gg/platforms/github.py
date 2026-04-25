from __future__ import annotations

import json

from gg.platforms.base import GitPlatform, Issue, IssueComment

MAX_COMMENTS = 10
MAX_COMMENT_CHARS = 4000


def _parse_comments(payload: dict) -> list[IssueComment]:
    raw_comments = payload.get("comments") or []
    if isinstance(raw_comments, dict):
        raw_comments = raw_comments.get("nodes") or raw_comments.get("edges") or raw_comments.get("items") or []
    if not isinstance(raw_comments, list):
        return []
    comments: list[IssueComment] = []
    for item in raw_comments:
        node = item.get("node", item) if isinstance(item, dict) else {}
        body = str(node.get("body") or node.get("bodyText") or "").strip()
        if not body:
            continue
        author = node.get("author") or {}
        if isinstance(author, dict):
            author = author.get("login") or author.get("name") or ""
        comments.append(
            IssueComment(
                body=body[:MAX_COMMENT_CHARS],
                author=str(author or ""),
                created_at=str(node.get("createdAt") or ""),
                url=str(node.get("url") or ""),
            )
        )
    return comments[-MAX_COMMENTS:]


class GitHubPlatform(GitPlatform):
    def __init__(self, cwd: str = ".", *, rate_limit_store=None):
        super().__init__(cwd, rate_limit_store=rate_limit_store)

    def _run(self, args: list[str], *, bucket: str) -> str:
        return self._run_command(args, bucket=bucket)

    def _command_env(self) -> dict[str, str]:
        env = super()._command_env()
        env.setdefault("GH_DEBUG", "api")
        return env

    def list_issues(self, state: str = "open", limit: int = 30) -> list[Issue]:
        raw = self._run([
            "issue", "list",
            "--state", state,
            "--limit", str(limit),
            "--json", "number,title,body,labels,assignees,state,url",
        ], bucket=self._bucket("issues:read"))
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
        ], bucket=self._bucket("issues:read"))
        i = json.loads(raw)
        return Issue(
            number=i["number"],
            title=i["title"],
            body=i.get("body", ""),
            labels=[la["name"] for la in i.get("labels", [])],
            assignees=[a["login"] for a in i.get("assignees", [])],
            state=i.get("state", "open"),
            url=i.get("url", ""),
            comments=_parse_comments(i),
        )

    def create_pr(self, *, title: str, body: str, head: str, base: str) -> str:
        raw = self._run([
            "pr", "create",
            "--title", title,
            "--body", body,
            "--head", head,
            "--base", base,
        ], bucket=self._bucket("pull-requests:write"))
        return raw

    def find_pr(self, *, head: str) -> str | None:
        raw = self._run([
            "pr", "list",
            "--state", "open",
            "--head", head,
            "--limit", "1",
            "--json", "url",
        ], bucket=self._bucket("pull-requests:read"))
        items = json.loads(raw) if raw else []
        return items[0]["url"] if items else None

    def add_comment(self, issue_number: int, body: str) -> None:
        self._run(["issue", "comment", str(issue_number), "--body", body], bucket=self._bucket("issues:comment"))

    def add_labels(self, issue_number: int, labels: list[str]) -> None:
        if labels:
            self._run(
                ["issue", "edit", str(issue_number), "--add-label", ",".join(labels)],
                bucket=self._bucket("issues:labels"),
            )

    def remove_labels(self, issue_number: int, labels: list[str]) -> None:
        if labels:
            self._run(
                ["issue", "edit", str(issue_number), "--remove-label", ",".join(labels)],
                bucket=self._bucket("issues:labels"),
            )

    def cli_name(self) -> str:
        return "gh"

    def platform_name(self) -> str:
        return "github"
