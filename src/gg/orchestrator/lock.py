from __future__ import annotations

import fcntl
import json
import os
import re
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class FileLock:
    path: Path
    timeout_seconds: float = 30.0
    poll_interval_seconds: float = 0.1
    _handle: object | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+", encoding="utf-8")
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._write_metadata(acquired_at=_utc_now())
                return self
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    self._handle.close()
                    self._handle = None
                    raise TimeoutError(f"timed out acquiring lock {self.path}")
                time.sleep(self.poll_interval_seconds)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        self._handle.seek(0)
        self._handle.truncate()
        self._handle.flush()
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None

    def heartbeat(self) -> dict[str, Any]:
        if self._handle is None:
            raise RuntimeError(f"cannot heartbeat unacquired lock {self.path}")
        metadata = self.metadata() or self._owner_metadata(acquired_at=_utc_now())
        metadata["heartbeat_at"] = _utc_now()
        self._write_metadata(metadata=metadata)
        return metadata

    def metadata(self) -> dict[str, Any] | None:
        return self.read_metadata(self.path)

    def _write_metadata(
        self,
        *,
        acquired_at: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if self._handle is None:
            raise RuntimeError(f"cannot write metadata for unacquired lock {self.path}")
        payload = metadata or self._owner_metadata(acquired_at=acquired_at or _utc_now())
        self._handle.seek(0)
        self._handle.truncate()
        self._handle.write(json.dumps(payload, sort_keys=True) + "\n")
        self._handle.flush()

    def _owner_metadata(self, *, acquired_at: str) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "owner_pid": os.getpid(),
            "hostname": socket.gethostname(),
            "cwd": str(Path.cwd()),
            "command": " ".join(sys.argv),
            "acquired_at": acquired_at,
            "heartbeat_at": acquired_at,
        }

    @staticmethod
    def read_metadata(path: str | Path) -> dict[str, Any] | None:
        lock_path = Path(path)
        if not lock_path.exists():
            return None
        raw = lock_path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {
                "schema_version": 1,
                "stale_reason": "invalid_metadata",
                "raw": raw,
            }
        return data if isinstance(data, dict) else None

    @staticmethod
    def owner_is_alive(metadata: dict[str, Any]) -> bool:
        pid = metadata.get("owner_pid")
        if not isinstance(pid, int) or pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    @classmethod
    def stale_owner(
        cls,
        path: str | Path,
        *,
        max_heartbeat_age_seconds: float | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        metadata = cls.read_metadata(path)
        if metadata is None:
            return None
        reason = metadata.get("stale_reason")
        if reason:
            return {"path": str(path), "reason": reason, "metadata": metadata}
        if not cls.owner_is_alive(metadata):
            return {"path": str(path), "reason": "owner_not_running", "metadata": metadata}
        if max_heartbeat_age_seconds is None:
            return None
        heartbeat_at = _parse_timestamp(str(metadata.get("heartbeat_at", "")))
        if heartbeat_at is None:
            return {"path": str(path), "reason": "missing_heartbeat", "metadata": metadata}
        now = now or datetime.now(timezone.utc)
        if (now - heartbeat_at).total_seconds() > max_heartbeat_age_seconds:
            return {"path": str(path), "reason": "heartbeat_stale", "metadata": metadata}
        return None


class LockManager:
    def __init__(self, project_path: str | Path):
        self.project_path = Path(project_path).resolve()
        self.root = self.project_path / ".gg" / "locks"

    def queue(self) -> FileLock:
        return FileLock(self.root / "run-queue.lock")

    def issue(self, issue_number: int) -> FileLock:
        return FileLock(self.root / f"issue-{issue_number}.lock")

    def run(self, run_id: str) -> FileLock:
        safe_run_id = re.sub(r"[^A-Za-z0-9._-]+", "-", run_id).strip("-") or "run"
        return FileLock(self.root / f"run-{safe_run_id}.lock")

    def scan_stale(
        self,
        *,
        max_heartbeat_age_seconds: float | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if not self.root.exists():
            return []
        stale: list[dict[str, Any]] = []
        for path in sorted(self.root.glob("*.lock")):
            stale_owner = FileLock.stale_owner(
                path,
                max_heartbeat_age_seconds=max_heartbeat_age_seconds,
                now=now,
            )
            if stale_owner is not None:
                stale.append(stale_owner)
        return stale


def _parse_timestamp(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
