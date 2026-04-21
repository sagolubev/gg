from __future__ import annotations

import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from gg.agents.base import AgentBackend

CODEX_TIMEOUT = 600
MAX_RETRIES = 1


def _progress_ticker(console, stop_event: threading.Event, label: str) -> None:
    """Print elapsed time every 10 seconds while Codex works."""
    start = time.monotonic()
    while not stop_event.is_set():
        stop_event.wait(10)
        if not stop_event.is_set():
            elapsed = int(time.monotonic() - start)
            console.print(f"    [dim]... {label} ({elapsed}s elapsed)[/dim]")


class CodexAgent(AgentBackend):
    def __init__(self, console=None):
        self._console = console

    def generate(self, prompt: str, *, cwd: str | None = None) -> str:
        out_path = Path(tempfile.mktemp(suffix=".md"))

        for attempt in range(MAX_RETRIES + 1):
            stop_event = threading.Event()
            ticker = None
            if self._console:
                ticker = threading.Thread(
                    target=_progress_ticker,
                    args=(self._console, stop_event, "Codex working"),
                    daemon=True,
                )
                ticker.start()

            try:
                result = subprocess.run(
                    ["codex", "exec", "-o", str(out_path), prompt],
                    capture_output=True,
                    text=True,
                    timeout=CODEX_TIMEOUT,
                    cwd=cwd,
                )
            except subprocess.TimeoutExpired:
                stop_event.set()
                out_path.unlink(missing_ok=True)
                if attempt < MAX_RETRIES:
                    continue
                raise RuntimeError(f"Codex timed out after {CODEX_TIMEOUT}s")
            finally:
                stop_event.set()

            output = ""
            if out_path.exists():
                output = out_path.read_text(encoding="utf-8").strip()
                out_path.unlink(missing_ok=True)

            if output:
                return output
            if attempt < MAX_RETRIES:
                continue
            if result.stderr.strip():
                raise RuntimeError(f"Codex failed: {result.stderr.strip()}")
            return result.stdout.strip()
        return ""

    def is_available(self) -> bool:
        return shutil.which("codex") is not None
