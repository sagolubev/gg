from __future__ import annotations

from dataclasses import asdict, dataclass, field

from gg.knowledge.engine import KnowledgeEngine
from gg.platforms.base import Issue


@dataclass(frozen=True)
class TaskBrief:
    schema_version: int
    issue: dict
    summary: str
    acceptance_criteria: list[str]
    project_context: str = ""
    constraints: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TaskBrief":
        return cls(
            schema_version=int(data.get("schema_version", 1)),
            issue=dict(data.get("issue", {})),
            summary=str(data.get("summary", "")),
            acceptance_criteria=list(data.get("acceptance_criteria", [])),
            project_context=str(data.get("project_context", "")),
            constraints=list(data.get("constraints", [])),
        )


class TaskAnalyzer:
    def __init__(self, project_path: str):
        self.project_path = project_path

    def analyze(self, issue: Issue) -> TaskBrief:
        context = ""
        try:
            context = KnowledgeEngine(self.project_path).context_for_issue(issue.title, issue.body)
        except Exception:
            context = ""
        body = issue.body.strip()
        summary = body[:1200] if body else issue.title
        return TaskBrief(
            schema_version=1,
            issue={
                "number": issue.number,
                "title": issue.title,
                "body": body[:12000],
                "labels": issue.labels,
                "url": issue.url,
            },
            summary=summary,
            acceptance_criteria=[
                "Implement the requested issue behavior.",
                "Keep the change focused and consistent with the existing codebase.",
                "Run configured verification commands and report the result.",
            ],
            project_context=context[:12000],
        )
