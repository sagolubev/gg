from __future__ import annotations

import shutil
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
import re

from gg.orchestrator.state import utc_now

_SCHEMA_VERSION = 1
_CB_STATE_CLOSED = "closed"
_CB_STATE_OPEN = "open"
_CB_STATE_HALF_OPEN = "half_open"


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
        try:
            conn = sqlite3.connect(self.path, timeout=15)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=15000")
            conn.execute("PRAGMA integrity_check")
            return conn
        except sqlite3.DatabaseError:
            corrupt_path = self.path.with_suffix(
                f".corrupt.{int(time.time())}.sqlite3"
            )
            shutil.move(str(self.path), str(corrupt_path))
            conn = sqlite3.connect(self.path, timeout=15)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=15000")
            self._do_init(conn)
            return conn

    def _do_init(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO metadata (key, value) VALUES ('schema_version', ?)",
            (str(_SCHEMA_VERSION),),
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rate_limit_entries (
                key TEXT PRIMARY KEY,
                provider TEXT NOT NULL DEFAULT '',
                repo_id TEXT NOT NULL DEFAULT '',
                operation TEXT NOT NULL DEFAULT '',
                failures INTEGER NOT NULL DEFAULT 0,
                window_started_at TEXT NOT NULL DEFAULT '',
                cooldown_until TEXT NOT NULL DEFAULT '',
                retry_after TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'closed',
                remaining INTEGER NOT NULL DEFAULT -1,
                reset_at TEXT NOT NULL DEFAULT '',
                limit_value INTEGER,
                updated_at TEXT NOT NULL
            )
            """
        )
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

    def _init(self) -> None:
        with self._connect() as conn:
            self._do_init(conn)

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

    def record_failure(
        self,
        key: str,
        *,
        failure_threshold: int = 5,
        window_seconds: int = 600,
        cooldown_seconds: int = 900,
        provider: str = "",
        repo_id: str = "",
        operation: str = "",
    ) -> str:
        now = utc_now()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT failures, window_started_at, state FROM rate_limit_entries WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                failures = 1
                window_started_at = now
            else:
                window_dt = _try_parse_utc(str(row["window_started_at"]))
                now_dt = _parse_utc(now)
                if window_dt and (now_dt - window_dt).total_seconds() > window_seconds:
                    failures = 1
                    window_started_at = now
                else:
                    failures = (row["failures"] or 0) + 1
                    window_started_at = row["window_started_at"] or now
            new_state = _CB_STATE_OPEN if failures >= failure_threshold else _CB_STATE_CLOSED
            cooldown_until = ""
            if new_state == _CB_STATE_OPEN:
                cooldown_until = _format_utc(
                    _parse_utc(now) + timedelta(seconds=cooldown_seconds)
                )
            conn.execute(
                """
                INSERT INTO rate_limit_entries
                    (key, provider, repo_id, operation, failures, window_started_at,
                     cooldown_until, state, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    failures = excluded.failures,
                    window_started_at = excluded.window_started_at,
                    cooldown_until = excluded.cooldown_until,
                    state = excluded.state,
                    updated_at = excluded.updated_at
                """,
                (key, provider, repo_id, operation, failures,
                 window_started_at, cooldown_until, new_state, now),
            )
        return new_state

    def record_success(self, key: str) -> None:
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO rate_limit_entries (key, failures, state, updated_at)
                VALUES (?, 0, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    failures = 0,
                    state = ?,
                    cooldown_until = '',
                    updated_at = ?
                """,
                (key, _CB_STATE_CLOSED, now, _CB_STATE_CLOSED, now),
            )

    def is_open(self, key: str, *, now: str | None = None) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT state, cooldown_until FROM rate_limit_entries WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return False
        state = row["state"]
        if state == _CB_STATE_CLOSED:
            return False
        if state == _CB_STATE_OPEN:
            cooldown_until = _try_parse_utc(str(row["cooldown_until"] or ""))
            if cooldown_until and _parse_utc(now or utc_now()) >= cooldown_until:
                return False
            return True
        return False

    def try_half_open(self, key: str, *, now: str | None = None) -> bool:
        """Transition to half-open if cooldown has elapsed. Returns True if transitioned."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT state, cooldown_until FROM rate_limit_entries WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None or row["state"] != _CB_STATE_OPEN:
                return False
            cooldown_until = _try_parse_utc(str(row["cooldown_until"] or ""))
            if cooldown_until and _parse_utc(now or utc_now()) < cooldown_until:
                return False
            conn.execute(
                "UPDATE rate_limit_entries SET state = ?, updated_at = ? WHERE key = ?",
                (_CB_STATE_HALF_OPEN, now or utc_now(), key),
            )
        return True

    def prune_stale(self, max_age_seconds: int) -> int:
        cutoff = _format_utc(_parse_utc(utc_now()) - timedelta(seconds=max_age_seconds))
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM rate_limit_entries WHERE updated_at < ? AND state = ?",
                (cutoff, _CB_STATE_CLOSED),
            )
            deleted = cur.rowcount
            cur2 = conn.execute(
                "DELETE FROM rate_limits WHERE updated_at < ?",
                (cutoff,),
            )
            deleted += cur2.rowcount
        return deleted


def _try_parse_utc(value: str) -> datetime | None:
    try:
        return _parse_utc(value)
    except ValueError:
        return None


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
