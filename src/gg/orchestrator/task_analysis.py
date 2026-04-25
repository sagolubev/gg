from __future__ import annotations

from dataclasses import asdict, dataclass, field

from gg.knowledge.engine import KnowledgeEngine
from gg.orchestrator.schemas import TaskBriefModel
from gg.platforms.base import Issue, IssueComment

MAX_ISSUE_BODY_CHARS = 12000
MAX_SUMMARY_CHARS = 1200
MAX_PROJECT_CONTEXT_CHARS = 12000
MAX_COMMENTS = 10
MAX_COMMENT_BODY_CHARS = 2000
MAX_INPUTS = 10
MAX_INPUT_MESSAGE_CHARS = 2000


def _serialize_comments(comments: list[IssueComment]) -> list[dict[str, str]]:
    return [
        {
            "author": comment.author,
            "created_at": comment.created_at,
            "url": comment.url,
            "body": comment.body[:MAX_COMMENT_BODY_CHARS],
        }
        for comment in comments[-MAX_COMMENTS:]
        if comment.body.strip()
    ]


def _serialize_inputs(inputs: list[dict]) -> list[dict[str, str | int]]:
    serialized: list[dict[str, str | int]] = []
    for item in inputs[-MAX_INPUTS:]:
        message = str(item.get("message", "")).strip()
        if not message:
            continue
        serialized.append(
            {
                "source": str(item.get("source", "")),
                "sequence_number": int(item.get("sequence_number", 0)),
                "answered_state": str(item.get("answered_state", "")),
                "created_at": str(item.get("created_at", "")),
                "message": message[:MAX_INPUT_MESSAGE_CHARS],
            }
        )
    return serialized


def _comments_section(comments: list[dict[str, str]]) -> str:
    if not comments:
        return ""
    lines = ["Recent issue comments:"]
    for comment in comments:
        author = comment.get("author") or "unknown"
        created_at = comment.get("created_at") or "unknown time"
        body = str(comment.get("body", "")).strip()
        lines.append(f"- {author} @ {created_at}: {body}")
    return "\n".join(lines)


def _inputs_section(inputs: list[dict[str, str | int]]) -> str:
    if not inputs:
        return ""
    lines = ["Local operator input artifacts:"]
    for item in inputs:
        source = item.get("source") or "unknown"
        sequence_number = item.get("sequence_number") or 0
        answered_state = item.get("answered_state") or "unknown"
        created_at = item.get("created_at") or "unknown time"
        message = str(item.get("message", "")).strip()
        lines.append(
            f"- Input #{sequence_number} from {source} for {answered_state} @ {created_at}: {message}"
        )
    return "\n".join(lines)


@dataclass(frozen=True)
class TaskBrief:
    schema_version: int
    issue: dict
    summary: str
    acceptance_criteria: list[str]
    project_context: str = ""
    constraints: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        data = asdict(self)
        TaskBriefModel.model_validate(data)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "TaskBrief":
        validated = TaskBriefModel.model_validate(data)
        return cls(
            schema_version=validated.schema_version,
            issue=validated.issue,
            summary=validated.summary,
            acceptance_criteria=list(validated.acceptance_criteria),
            project_context=validated.project_context,
            constraints=list(validated.constraints),
        )


class TaskAnalyzer:
    def __init__(self, project_path: str):
        self.project_path = project_path

    def analyze(self, issue: Issue, *, inputs: list[dict] | None = None) -> TaskBrief:
        serialized_comments = _serialize_comments(issue.comments)
        serialized_inputs = _serialize_inputs(inputs or [])
        issue_text_parts = [
            issue.body.strip(),
            _comments_section(serialized_comments),
            _inputs_section(serialized_inputs),
        ]
        combined_issue_text = "\n\n".join(part for part in issue_text_parts if part).strip()
        context = ""
        try:
            context = KnowledgeEngine(self.project_path).context_for_issue(issue.title, combined_issue_text)
        except Exception:
            context = ""
        body = issue.body.strip()
        summary = combined_issue_text[:MAX_SUMMARY_CHARS] if combined_issue_text else issue.title
        return TaskBrief(
            schema_version=1,
            issue={
                "number": issue.number,
                "title": issue.title,
                "body": body[:MAX_ISSUE_BODY_CHARS],
                "labels": issue.labels,
                "url": issue.url,
                "comments": serialized_comments,
                "inputs": serialized_inputs,
            },
            summary=summary,
            acceptance_criteria=[
                "Implement the requested issue behavior.",
                "Keep the change focused and consistent with the existing codebase.",
                "Run configured verification commands and report the result.",
            ],
            project_context=context[:MAX_PROJECT_CONTEXT_CHARS],
        )
