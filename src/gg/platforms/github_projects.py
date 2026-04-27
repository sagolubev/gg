from __future__ import annotations

import json
import logging
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any

from gg.orchestrator.rate_limit import RateLimitStore, RateLimitThrottleError, extract_retry_after_seconds

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
        rate_limit_store: RateLimitStore | None = None,
        cache_ttl_seconds: int = 60,
    ):
        self.owner = owner
        self.project_number = project_number
        self.status_field = status_field
        self.cwd = cwd
        self._rate_limit_store = rate_limit_store or RateLimitStore(cwd)
        self._cache_ttl_seconds = max(1, cache_ttl_seconds)
        self._cache = _ProjectCache()
        self._items_cache: list[dict[str, Any]] = []
        self._items_cache_expires_at: float = 0.0

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
            status_val = self._extract_status(item)
            if status_val.lower() == status_name.lower():
                result.add(num)
        return result

    def _extract_status(self, item: dict) -> str:
        # gh project item-list --format json exposes status as a top-level "status" string
        top = item.get("status")
        if isinstance(top, str) and top:
            return top
        # fallback: fieldValues dict {"Status": {"name": "..."}} or list of field-value objects
        field_values = item.get("fieldValues") or {}
        if isinstance(field_values, dict):
            for val in field_values.values():
                if isinstance(val, dict) and "name" in val:
                    return val["name"]
        elif isinstance(field_values, list):
            for fv in field_values:
                if isinstance(fv, dict) and fv.get("field", {}).get("name", "").lower() == self.status_field.lower():
                    return fv.get("value", {}).get("name", "")
        return ""

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
        now = time.monotonic()
        if self._items_cache and now < self._items_cache_expires_at:
            return list(self._items_cache)
        try:
            out = self._gh(
                "project", "item-list", str(self.project_number),
                "--owner", self.owner,
                "--format", "json",
                "--limit", "500",
            )
        except RateLimitThrottleError:
            if self._items_cache:
                return list(self._items_cache)
            raise
        data = json.loads(out) if out else {}
        items = data.get("items") or []
        self._items_cache = list(items)
        self._items_cache_expires_at = now + self._cache_ttl_seconds
        return items

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
        bucket = self._bucket("project-item-list" if args[:2] == ("project", "item-list") else "project-mutation")
        self._raise_if_throttled(bucket)
        result = subprocess.run(
            ["gh", *args],
            capture_output=True, text=True, timeout=30, cwd=self.cwd,
        )
        header_text = "\n".join(part for part in (result.stderr, result.stdout) if part)
        snapshot = self._rate_limit_store.record_http_headers(bucket, header_text)
        if result.returncode != 0:
            if self._looks_rate_limited(result.stderr) or self._looks_rate_limited(result.stdout):
                snapshot = snapshot or self._rate_limit_store.backoff(
                    bucket,
                    retry_after_seconds=extract_retry_after_seconds(header_text) or 60,
                )
                raise RateLimitThrottleError(snapshot)
            raise RuntimeError(f"gh {' '.join(args[:3])} failed: {result.stderr.strip()[:200]}")
        return result.stdout.strip()

    def _bucket(self, scope: str) -> str:
        return f"github-projects:{self.owner}:{self.project_number}:{scope}"

    def _raise_if_throttled(self, bucket: str) -> None:
        snapshot = self._rate_limit_store.get(bucket)
        if snapshot is None or not self._rate_limit_store.should_throttle(bucket):
            return
        raise RateLimitThrottleError(snapshot)

    def _looks_rate_limited(self, text: str) -> bool:
        lowered = text.lower()
        return "rate limit" in lowered or "too many requests" in lowered
