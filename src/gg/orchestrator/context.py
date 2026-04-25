from __future__ import annotations

import hashlib
import json
from pathlib import Path

from gg.orchestrator.state import utc_now
from gg.orchestrator.task_analysis import TaskBrief


def _comments_text(comments: list[dict]) -> str:
    return "\n".join(
        f"{comment.get('author') or 'unknown'} @ {comment.get('created_at') or 'unknown time'}: {comment.get('body', '')}"
        for comment in comments
        if str(comment.get("body", "")).strip()
    )


def _inputs_text(inputs: list[dict]) -> str:
    return "\n".join(
        (
            f"Input #{item.get('sequence_number', 0)} "
            f"from {item.get('source') or 'unknown'} "
            f"for {item.get('answered_state') or 'unknown'}: "
            f"{item.get('message', '')}"
        )
        for item in inputs
        if str(item.get("message", "")).strip()
    )


class ContextSnapshotStore:
    def __init__(self, project_path: str | Path):
        self.project_path = Path(project_path).resolve()
        self.objects_dir = self.project_path / ".gg" / "objects"
        self.objects_dir.mkdir(parents=True, exist_ok=True)

    def write_task_snapshot(self, run_id: str, brief: TaskBrief) -> str:
        refs = {
            "issue_body": self._put_text(str(brief.issue.get("body", ""))),
            "issue_comments": self._put_text(_comments_text(list(brief.issue.get("comments", [])))),
            "local_inputs": self._put_text(_inputs_text(list(brief.issue.get("inputs", [])))),
            "summary": self._put_text(brief.summary),
            "project_context": self._put_text(brief.project_context),
        }
        snapshot = {
            "schema_version": 1,
            "created_at": utc_now(),
            "run_id": run_id,
            "issue": {
                "number": brief.issue.get("number"),
                "title": brief.issue.get("title"),
                "labels": brief.issue.get("labels", []),
                "url": brief.issue.get("url", ""),
            },
            "objects": refs,
        }
        path = self.project_path / ".gg" / "runs" / run_id / "artifacts" / "context-snapshot-v1.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
        return str(path.relative_to(self.project_path))

    def read_text(self, sha256: str) -> str:
        return (self.objects_dir / sha256[:2] / sha256).read_text(encoding="utf-8")

    def _put_text(self, text: str) -> str:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        path = self.objects_dir / digest[:2] / digest
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            tmp = path.with_suffix(".tmp")
            tmp.write_text(text, encoding="utf-8")
            tmp.replace(path)
        return digest
