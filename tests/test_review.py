from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from gg.agents.base import AgentBackend
from gg.cli import cli
from gg.orchestrator.review import review_pull_request
from gg.platforms.base import GitPlatform, Issue, PullRequest
from gg.platforms.github import GitHubPlatform


class ReviewAgent(AgentBackend):
    def __init__(self) -> None:
        self.prompts: list[str] = []
        self.contexts: list[str | None] = []
        self.cwd_values: list[str | None] = []

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        self.prompts.append(prompt)
        self.contexts.append(context)
        self.cwd_values.append(cwd)
        assert cwd is not None
        assert "PR #7" not in prompt
        assert context is not None
        assert "PR #7" in context
        assert "diff --git a/app.py b/app.py" in context
        return "No blocking findings.\n\n- Tests are present."

    def is_available(self) -> bool:
        return True


class ReviewPlatform(GitPlatform):
    def __init__(self, cwd: str = ".") -> None:
        super().__init__(cwd)
        self.comments: list[tuple[int, str]] = []

    def list_issues(self, state: str = "open", limit: int = 30) -> list[Issue]:
        return []

    def get_issue(self, number: int) -> Issue:
        return Issue(number=number, title="", body="")

    def create_pr(self, *, title: str, body: str, head: str, base: str) -> str:
        return ""

    def get_pr(self, number: int) -> PullRequest:
        return PullRequest(
            number=number,
            title="Harden uploads",
            body="Fixes #42",
            author="ada",
            state="open",
            url="https://github.com/example/repo/pull/7",
            head_ref="feature/uploads",
            base_ref="main",
        )

    def get_pr_diff(self, number: int) -> str:
        return "diff --git a/app.py b/app.py\n+validate_upload()\n"

    def add_pr_comment(self, number: int, body: str) -> None:
        self.comments.append((number, body))

    def add_comment(self, issue_number: int, body: str) -> None:
        pass

    def cli_name(self) -> str:
        return "fake"

    def platform_name(self) -> str:
        return "github"


def test_review_pull_request_uses_agent_and_can_comment(tmp_path):
    (tmp_path / ".gg").mkdir()
    (tmp_path / ".gg" / "params.yaml").write_text(
        "runtime:\n  agent_backend: codex\n",
        encoding="utf-8",
    )
    agent = ReviewAgent()
    platform = ReviewPlatform(str(tmp_path))

    result = review_pull_request(tmp_path, 7, platform=platform, agent=agent, comment=True)

    assert result["pr"]["number"] == 7
    assert result["posted"] is True
    assert result["review"].startswith("No blocking findings")
    assert result["artifact"].startswith(".gg/reviews/pr-7-review-")
    assert (tmp_path / result["artifact"]).read_text(encoding="utf-8").startswith("# Review PR #7")
    assert platform.comments == [(7, result["review"])]
    assert "Base: main" in (agent.contexts[0] or "")
    assert agent.cwd_values[0] != str(tmp_path.resolve())


def test_cli_review_prints_machine_readable_result(monkeypatch, tmp_path):
    captured = {}

    def fake_review_pull_request(project_path, pr_number, *, agent_backend=None, comment=False, debug=False):
        captured.update(
            {
                "project_path": project_path,
                "pr_number": pr_number,
                "agent_backend": agent_backend,
                "comment": comment,
                "debug": debug,
            }
        )
        return {
            "schema_version": 1,
            "pr": {"number": pr_number, "title": "Harden uploads"},
            "review": "No blocking findings.",
            "posted": comment,
            "artifact": "",
        }

    monkeypatch.setattr("gg.orchestrator.review.review_pull_request", fake_review_pull_request)

    result = CliRunner().invoke(
        cli,
        [
            "review",
            "7",
            "--path",
            str(tmp_path),
            "--agent-backend",
            "claude",
            "--comment",
            "--debug",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["posted"] is True
    assert captured == {
        "project_path": str(tmp_path),
        "pr_number": 7,
        "agent_backend": "claude",
        "comment": True,
        "debug": True,
    }


def test_github_pr_review_methods_use_gh_contract(tmp_path):
    platform = GitHubPlatform(str(tmp_path))
    seen: list[list[str]] = []

    def fake_run(args, **kwargs):
        seen.append(args)
        if args[:2] == ["pr", "view"]:
            return json.dumps(
                {
                    "number": 7,
                    "title": "Harden uploads",
                    "body": "Fixes #42",
                    "author": {"login": "ada"},
                    "state": "OPEN",
                    "url": "https://github.com/example/repo/pull/7",
                    "headRefName": "feature/uploads",
                    "baseRefName": "main",
                }
            )
        if args[:2] == ["pr", "diff"]:
            return "diff --git a/app.py b/app.py\n+validate_upload()\n"
        if args[:2] == ["pr", "comment"]:
            return ""
        raise AssertionError(args)

    platform._run = fake_run  # type: ignore[method-assign]

    pr = platform.get_pr(7)
    diff = platform.get_pr_diff(7)
    platform.add_pr_comment(7, "review body")

    assert pr.title == "Harden uploads"
    assert diff.startswith("diff --git")
    assert ["pr", "diff", "7", "--patch"] in seen
    assert ["pr", "comment", "7", "--body", "review body"] in seen
