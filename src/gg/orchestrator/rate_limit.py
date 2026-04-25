from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from gg.orchestrator.state import utc_now


@dataclass(frozen=True)
class RateLimitSnapshot:
    bucket: str
    remaining: int
    reset_at: str
    limit: int | None = None
    updated_at: str = ""


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
