from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("gg.github_projects")


@dataclass
class _ProjectCache:
    project_id: str = ""
    field_id: str = ""
    options: dict[str, str] = field(default_factory=dict)  # name -> option_id
    item_ids: dict[int, str] = field(default_factory=dict)  # issue_number -> item_id


class GitHubProjectsClient:
    """Moves issues between statuses in a GitHub Projects v2 board."""

    def __init__(
        self,
        *,
        owner: str,
        project_number: int,
        status_field: str = "Status",
        cwd: str = ".",
    ):
        self.owner = owner
        self.project_number = project_number
        self.status_field = status_field
        self.cwd = cwd
        self._cache = _ProjectCache()

    # ------------------------------------------------------------------ public

    def get_issues_in_status(self, status_name: str) -> set[int]:
        """Return issue numbers whose project status matches status_name (case-insensitive)."""
        items = self._paginate_items()
        result: set[int] = set()
        for item in items:
            content = item.get("content") or {}
            num = content.get("number")
            if num is None:
                continue
            field_values = item.get("fieldValues") or {}
            # gh project item-list --format json returns fieldValues as a dict or list
            status_val = ""
            if isinstance(field_values, dict):
                for key, val in field_values.items():
                    if isinstance(val, dict) and "name" in val:
                        status_val = val["name"]
                        break
            elif isinstance(field_values, list):
                for fv in field_values:
                    if isinstance(fv, dict) and fv.get("field", {}).get("name", "").lower() == self.status_field.lower():
                        status_val = fv.get("value", {}).get("name", "")
                        break
            if status_val.lower() == status_name.lower():
                result.add(num)
        return result

    def move_issue(self, issue_number: int, status_name: str) -> bool:
        """Move issue to status_name. Returns True on success, False on any error."""
        try:
            item_id = self._get_item_id(issue_number)
            if not item_id:
                return False
            project_id, field_id, option_id = self._get_field_option(status_name)
            if not option_id:
                log.warning("status %r not found in project %s", status_name, self.project_number)
                return False
            self._gh(
                "project", "item-edit",
                "--id", item_id,
                "--field-id", field_id,
                "--project-id", project_id,
                "--single-select-option-id", option_id,
            )
            return True
        except Exception as exc:
            log.warning("project status update failed: %s", exc)
            return False

    # ----------------------------------------------------------------- private

    def _get_item_id(self, issue_number: int) -> str:
        if issue_number in self._cache.item_ids:
            return self._cache.item_ids[issue_number]
        items = self._paginate_items()
        for item in items:
            num = (item.get("content") or {}).get("number")
            if num == issue_number:
                item_id = item["id"]
                self._cache.item_ids[issue_number] = item_id
                return item_id
        return ""

    def _paginate_items(self) -> list[dict[str, Any]]:
        out = self._gh(
            "project", "item-list", str(self.project_number),
            "--owner", self.owner,
            "--format", "json",
            "--limit", "500",
        )
        data = json.loads(out) if out else {}
        return data.get("items") or []

    def _get_field_option(self, status_name: str) -> tuple[str, str, str]:
        if self._cache.project_id and self._cache.field_id and status_name in self._cache.options:
            return self._cache.project_id, self._cache.field_id, self._cache.options[status_name]

        raw = self._gh("project", "view", str(self.project_number), "--owner", self.owner, "--format", "json")
        project_data = json.loads(raw) if raw else {}
        project_id = project_data.get("id", "")
        self._cache.project_id = project_id

        fields_raw = self._gh(
            "project", "field-list", str(self.project_number),
            "--owner", self.owner,
            "--format", "json",
        )
        fields_data = json.loads(fields_raw) if fields_raw else {}
        for f in fields_data.get("fields") or []:
            if f.get("name", "").lower() == self.status_field.lower():
                self._cache.field_id = f["id"]
                self._cache.options = {
                    opt["name"]: opt["id"]
                    for opt in f.get("options") or []
                }
                break

        option_id = self._cache.options.get(status_name, "")
        if not option_id:
            # case-insensitive fallback
            for name, oid in self._cache.options.items():
                if name.lower() == status_name.lower():
                    option_id = oid
                    break

        return self._cache.project_id, self._cache.field_id, option_id

    def _gh(self, *args: str) -> str:
        result = subprocess.run(
            ["gh", *args],
            capture_output=True, text=True, timeout=30, cwd=self.cwd,
        )
        if result.returncode != 0:
            raise RuntimeError(f"gh {' '.join(args[:3])} failed: {result.stderr.strip()[:200]}")
        return result.stdout.strip()
