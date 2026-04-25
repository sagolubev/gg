from __future__ import annotations

import hashlib
import json
from pathlib import Path

from gg.orchestrator.schemas import ContextSnapshotModel, validation_error_message
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
    def __init__(self, project_path: str | Path, *, hash_artifacts: bool = False):
        self.project_path = Path(project_path).resolve()
        self.objects_dir = self.project_path / ".gg" / "objects"
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.hash_artifacts = hash_artifacts

    def write_task_snapshot(self, run_id: str, brief: TaskBrief) -> str:
        run_artifacts = self.project_path / ".gg" / "runs" / run_id / "artifacts"
        version = _next_snapshot_version(run_artifacts)
        refs = {
            "issue_body": self._put_text(str(brief.issue.get("body", ""))),
            "issue_comments": self._put_text(_comments_text(list(brief.issue.get("comments", [])))),
            "local_inputs": self._put_text(_inputs_text(list(brief.issue.get("inputs", [])))),
            "summary": self._put_text(brief.summary),
            "project_context": self._put_text(brief.project_context),
        }
        object_metadata = {
            name: _object_metadata(name, self.read_text(digest))
            for name, digest in refs.items()
        }
        snapshot = {
            "schema_version": 1,
            "snapshot_version": version,
            "created_at": utc_now(),
            "run_id": run_id,
            "purpose": "task_analysis_handoff",
            "issue": {
                "number": brief.issue.get("number"),
                "title": brief.issue.get("title"),
                "labels": brief.issue.get("labels", []),
                "url": brief.issue.get("url", ""),
            },
            "objects": refs,
            "object_metadata": object_metadata,
            "source_refs": _source_refs(brief),
            "summaries": {
                "summary": brief.summary,
                "acceptance_criteria": list(brief.acceptance_criteria),
                "candidate_files": list(brief.candidate_files),
                "risk_flags": list(brief.risk_flags),
                "classification": dict(brief.classification),
            },
            "prior_answer_refs": _prior_answer_refs(brief),
        }
        try:
            ContextSnapshotModel.model_validate(snapshot)
        except Exception as exc:
            raise ValueError(
                validation_error_message(f"artifacts/context-snapshot-v{version}.json", exc)
            ) from exc
        path = run_artifacts / f"context-snapshot-v{version}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
        if self.hash_artifacts:
            _write_snapshot_hash(path)
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


def _next_snapshot_version(artifacts_dir: Path) -> int:
    existing = []
    if artifacts_dir.exists():
        for path in artifacts_dir.glob("context-snapshot-v*.json"):
            try:
                existing.append(int(path.stem.rsplit("-v", 1)[1]))
            except (IndexError, ValueError):
                continue
    return (max(existing) + 1) if existing else 1


def _object_metadata(name: str, text: str) -> dict:
    return {
        "name": name,
        "chars": len(text),
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def _source_refs(brief: TaskBrief) -> list[dict]:
    refs = [
        {
            "kind": "issue",
            "number": brief.issue.get("number"),
            "title": brief.issue.get("title"),
            "url": brief.issue.get("url", ""),
        }
    ]
    refs.extend(
        {
            "kind": "issue_comment",
            "author": comment.get("author", ""),
            "created_at": comment.get("created_at", ""),
            "url": comment.get("url", ""),
        }
        for comment in list(brief.issue.get("comments", []))
    )
    return refs


def _prior_answer_refs(brief: TaskBrief) -> list[dict]:
    return [
        {
            "kind": "local_input",
            "sequence_number": item.get("sequence_number", 0),
            "source": item.get("source", ""),
            "created_at": item.get("created_at", ""),
            "answered_state": item.get("answered_state", ""),
        }
        for item in list(brief.issue.get("inputs", []))
    ]


def _write_snapshot_hash(path: Path) -> None:
    payload = {
        "schema_version": 1,
        "algorithm": "sha256",
        "hash": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    tmp = path.with_name(f"{path.name}.sha256.tmp")
    target = path.with_name(f"{path.name}.sha256")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp.replace(target)
