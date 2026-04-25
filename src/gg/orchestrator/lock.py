from __future__ import annotations

import fcntl
import time
from dataclasses import dataclass
from pathlib import Path


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
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None


class LockManager:
    def __init__(self, project_path: str | Path):
        self.project_path = Path(project_path).resolve()
        self.root = self.project_path / ".gg" / "locks"

    def queue(self) -> FileLock:
        return FileLock(self.root / "run-queue.lock")

    def issue(self, issue_number: int) -> FileLock:
        return FileLock(self.root / f"issue-{issue_number}.lock")
