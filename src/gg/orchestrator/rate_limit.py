from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
import re

from gg.orchestrator.state import utc_now


@dataclass(frozen=True)
class RateLimitSnapshot:
    bucket: str
    remaining: int
    reset_at: str
    limit: int | None = None
    updated_at: str = ""


class RateLimitThrottleError(RuntimeError):
    def __init__(self, snapshot: RateLimitSnapshot, *, message: str | None = None):
        self.snapshot = snapshot
        self.bucket = snapshot.bucket
        super().__init__(message or f"Rate limit active for {snapshot.bucket} until {snapshot.reset_at}")


class RateLimitStore:
    """SQLite WAL backed cross-process rate-limit state."""

    def __init__(self, project_path: str | Path):
        self.project_path = Path(project_path).resolve()
        self.path = self.project_path / ".gg" / "rate-limits.sqlite3"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rate_limits (
                    bucket TEXT PRIMARY KEY,
                    remaining INTEGER NOT NULL,
                    reset_at TEXT NOT NULL,
                    limit_value INTEGER,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def update(
        self,
        bucket: str,
        *,
        remaining: int,
        reset_at: str,
        limit: int | None = None,
    ) -> RateLimitSnapshot:
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO rate_limits (bucket, remaining, reset_at, limit_value, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(bucket) DO UPDATE SET
                    remaining = excluded.remaining,
                    reset_at = excluded.reset_at,
                    limit_value = excluded.limit_value,
                    updated_at = excluded.updated_at
                """,
                (bucket, remaining, reset_at, limit, now),
            )
        return RateLimitSnapshot(bucket=bucket, remaining=remaining, reset_at=reset_at, limit=limit, updated_at=now)

    def backoff(
        self,
        bucket: str,
        *,
        retry_after_seconds: int,
        limit: int | None = None,
        now: str | None = None,
    ) -> RateLimitSnapshot:
        base = _parse_utc(now or utc_now())
        reset_at = _format_utc(base + timedelta(seconds=max(retry_after_seconds, 0)))
        return self.update(bucket, remaining=0, reset_at=reset_at, limit=limit)

    def record_http_headers(self, bucket: str, text: str, *, now: str | None = None) -> RateLimitSnapshot | None:
        headers = _extract_headers(text)
        remaining = _parse_int(headers.get("x-ratelimit-remaining") or headers.get("ratelimit-remaining"))
        limit = _parse_int(headers.get("x-ratelimit-limit") or headers.get("ratelimit-limit"))
        reset_at = _parse_reset_at(headers, now=now)
        if remaining is None and reset_at is None:
            return None
        if remaining is None:
            remaining = 0
        if reset_at is None:
            return None
        return self.update(bucket, remaining=max(remaining, 0), reset_at=reset_at, limit=limit)

    def get(self, bucket: str) -> RateLimitSnapshot | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT bucket, remaining, reset_at, limit_value, updated_at FROM rate_limits WHERE bucket = ?",
                (bucket,),
            ).fetchone()
        if row is None:
            return None
        return RateLimitSnapshot(
            bucket=row["bucket"],
            remaining=row["remaining"],
            reset_at=row["reset_at"],
            limit=row["limit_value"],
            updated_at=row["updated_at"],
        )

    def should_throttle(self, bucket: str, *, now: str | None = None) -> bool:
        snapshot = self.get(bucket)
        if snapshot is None or snapshot.remaining > 0:
            return False
        return _parse_utc(snapshot.reset_at) > _parse_utc(now or utc_now())


def _parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def extract_retry_after_seconds(text: str) -> int | None:
    headers = _extract_headers(text)
    if retry_after := headers.get("retry-after"):
        return _parse_retry_after_seconds(retry_after)
    match = re.search(r"retry(?:ing)?(?: after)?\s+(\d+)\s*(?:seconds?|secs?|s)\b", text, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _extract_headers(text: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        match = re.match(r"^[^A-Za-z]*([A-Za-z][A-Za-z0-9-]*)\s*:\s*(.+?)\s*$", line)
        if not match:
            continue
        headers[match.group(1).lower()] = match.group(2).strip()
    return headers


def _parse_reset_at(headers: dict[str, str], *, now: str | None = None) -> str | None:
    for key in ("x-ratelimit-reset", "ratelimit-reset"):
        if value := headers.get(key):
            return _parse_reset_value(value)
    if retry_after := headers.get("retry-after"):
        seconds = _parse_retry_after_seconds(retry_after)
        if seconds is None:
            return None
        base = _parse_utc(now or utc_now())
        return _format_utc(base + timedelta(seconds=seconds))
    return None


def _parse_retry_after_seconds(value: str) -> int | None:
    value = value.strip()
    if value.isdigit():
        return int(value)
    try:
        target = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    target = target.astimezone(timezone.utc)
    delta = target - _parse_utc(utc_now())
    return max(int(delta.total_seconds()), 0)


def _parse_reset_value(value: str) -> str | None:
    stripped = value.strip()
    if stripped.isdigit():
        epoch = int(stripped)
        if epoch > 10_000_000_000:
            epoch //= 1000
        return _format_utc(datetime.fromtimestamp(epoch, tz=timezone.utc))
    try:
        parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(stripped)
        except (TypeError, ValueError, IndexError):
            return None
    return _format_utc(parsed.astimezone(timezone.utc))


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    match = re.search(r"-?\d+", value)
    if not match:
        return None
    return int(match.group(0))


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
