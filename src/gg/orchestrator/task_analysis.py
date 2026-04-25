from __future__ import annotations

import json
import tempfile
from dataclasses import asdict, dataclass, field
from typing import Any

from gg.agents.base import AgentBackend
from gg.knowledge.engine import KnowledgeEngine
from gg.orchestrator.prompts import build_analysis_prompt
from gg.orchestrator.schemas import AnalysisResultModel, TaskBriefModel
from gg.platforms.base import Issue, IssueComment

MAX_ISSUE_BODY_CHARS = 12000
MAX_SUMMARY_CHARS = 1200
MAX_PROJECT_CONTEXT_CHARS = 12000
MAX_COMMENTS = 10
MAX_COMMENT_BODY_CHARS = 2000
MAX_INPUTS = 10
MAX_INPUT_MESSAGE_CHARS = 2000
MAX_AGENT_RESPONSE_CHARS = 12000


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
    blocked: bool = False
    missing_questions: list[str] = field(default_factory=list)
    candidate_files: list[str] = field(default_factory=list)
    risk_flags: list[str] = field(default_factory=list)
    verification_hints: list[str] = field(default_factory=list)
    context_budget: dict[str, Any] = field(default_factory=dict)

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
            blocked=validated.blocked,
            missing_questions=list(validated.missing_questions),
            candidate_files=list(validated.candidate_files),
            risk_flags=list(validated.risk_flags),
            verification_hints=list(validated.verification_hints),
            context_budget=dict(validated.context_budget),
        )


class TaskAnalyzer:
    def __init__(self, project_path: str, *, agent: AgentBackend | None = None, timeout: int = 600):
        self.project_path = project_path
        self.agent = agent
        self.timeout = timeout
        self.last_agent_response: str = ""
        self.last_agent_error: str = ""
        self.last_agent_response_truncated = False

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
        issue_payload = {
            "number": issue.number,
            "title": issue.title,
            "body": body[:MAX_ISSUE_BODY_CHARS],
            "labels": issue.labels,
            "url": issue.url,
            "comments": serialized_comments,
            "inputs": serialized_inputs,
        }
        analysis = self._try_agent_analysis(issue_payload, context)
        if analysis is not None:
            return TaskBrief(
                schema_version=1,
                issue=issue_payload,
                summary=analysis.summary or issue.title,
                acceptance_criteria=analysis.acceptance_criteria
                or ["Clarify the missing task details." if not analysis.ready else "Implement the requested issue behavior."],
                project_context=context[:MAX_PROJECT_CONTEXT_CHARS],
                blocked=not analysis.ready,
                missing_questions=list(analysis.missing_questions),
                candidate_files=list(analysis.candidate_files),
                risk_flags=list(analysis.risk_flags),
                verification_hints=list(analysis.verification_hints),
                context_budget=dict(analysis.context_budget),
            )
        summary = combined_issue_text[:MAX_SUMMARY_CHARS] if combined_issue_text else issue.title
        return TaskBrief(
            schema_version=1,
            issue=issue_payload,
            summary=summary,
            acceptance_criteria=[
                "Implement the requested issue behavior.",
                "Keep the change focused and consistent with the existing codebase.",
                "Run configured verification commands and report the result.",
            ],
            project_context=context[:MAX_PROJECT_CONTEXT_CHARS],
        )

    def _try_agent_analysis(self, issue_payload: dict[str, Any], context: str) -> AnalysisResultModel | None:
        if self.agent is None or not self.agent.is_available():
            return None
        prompt = build_analysis_prompt(issue_payload=issue_payload, project_context=context[:MAX_PROJECT_CONTEXT_CHARS])
        self.last_agent_response = ""
        self.last_agent_error = ""
        self.last_agent_response_truncated = False
        try:
            raw = self.agent.generate(
                prompt,
                cwd=tempfile.gettempdir(),
                timeout=self.timeout,
                context="Task analysis only. Return exactly one JSON object and do not edit files.",
            )
            self.last_agent_response_truncated = len(raw) > MAX_AGENT_RESPONSE_CHARS
            self.last_agent_response = raw[:MAX_AGENT_RESPONSE_CHARS]
            payload = extract_single_json_object(raw)
            return AnalysisResultModel.model_validate(payload)
        except Exception as exc:
            self.last_agent_error = str(exc)
            return None


def extract_single_json_object(text: str) -> dict[str, Any]:
    objects: list[dict[str, Any]] = []
    for candidate in _json_object_candidates(text):
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            objects.append(value)
    if not objects:
        raise ValueError("no JSON object found in model response")
    canonical = {
        json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        for item in objects
    }
    if len(canonical) > 1:
        raise ValueError("multiple conflicting JSON objects found in model response")
    return objects[0]


def _json_object_candidates(text: str) -> list[str]:
    stripped = _strip_markdown_fence(text.strip())
    candidates = _balanced_json_objects(stripped)
    if candidates:
        return candidates
    return _balanced_json_objects(text)


def _strip_markdown_fence(text: str) -> str:
    if not text.startswith("```") or not text.endswith("```"):
        return text
    lines = text.splitlines()
    if len(lines) < 3:
        return text
    return "\n".join(lines[1:-1]).strip()


def _balanced_json_objects(text: str) -> list[str]:
    objects: list[str] = []
    start: int | None = None
    depth = 0
    in_string = False
    escaped = False
    for index, char in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == "{":
            if depth == 0:
                start = index
            depth += 1
        elif char == "}" and depth:
            depth -= 1
            if depth == 0 and start is not None:
                objects.append(text[start:index + 1])
                start = None
    return objects
