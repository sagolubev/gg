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


def _stream_stderr_debug(proc: subprocess.Popen, console, stop_event: threading.Event) -> None:
    from rich.text import Text

    if not proc.stderr:
        return
    for raw_line in proc.stderr:
        if stop_event.is_set():
            break
        line = raw_line.rstrip()
        if line:
            console.print(Text(f"    {line}", style="dim"))


def _progress_ticker(console, stop_event: threading.Event) -> None:
    start = time.monotonic()
    while not stop_event.is_set():
        stop_event.wait(15)
        if not stop_event.is_set():
            elapsed = int(time.monotonic() - start)
            console.print(f"    [dim]... Codex working ({elapsed}s elapsed)[/dim]")


class CodexAgent(AgentBackend):
    def __init__(self, console=None, debug: bool = False):
        self._console = console
        self._debug = debug

    def generate(self, prompt: str, *, cwd: str | None = None, timeout: int | None = None,
                 context: str | None = None) -> str:
        effective_timeout = timeout or CODEX_TIMEOUT
        retries = 0 if (timeout or context) else MAX_RETRIES
        out_path = Path(tempfile.mktemp(suffix=".md"))

        for attempt in range(retries + 1):
            try:
                if context:
                    output = self._run_fast(prompt, context, out_path, cwd, effective_timeout)
                elif self._console:
                    output = self._run_with_progress(prompt, out_path, cwd, effective_timeout)
                else:
                    output = self._run_silent(prompt, out_path, cwd, effective_timeout)

                if output:
                    return output
                if attempt < retries:
                    if self._console:
                        self._console.print("    [yellow]Empty response, retrying...[/yellow]")
                    continue
                return ""
            except subprocess.TimeoutExpired:
                out_path.unlink(missing_ok=True)
                if attempt < retries:
                    if self._console:
                        self._console.print(f"    [yellow]Timeout after {effective_timeout}s, retrying...[/yellow]")
                    continue
                raise RuntimeError(f"Codex timed out after {effective_timeout}s")
            except RuntimeError:
                out_path.unlink(missing_ok=True)
                if attempt < retries:
                    continue
                raise
        return ""

    def _run_fast(self, prompt: str, context: str, out_path: Path,
                  cwd: str | None, timeout: int) -> str:
        """Fast mode: pipe context via stdin, read-only sandbox (no tool calls)."""
        stop_event = threading.Event()
        full_input = f"{context}\n\n---\n\n{prompt}"

        proc = subprocess.Popen(
            [
                "codex", "exec",
                "--sandbox", "read-only",
                "--skip-git-repo-check",
                "--ephemeral",
                "--disable", "mcp",
                "-o", str(out_path),
                "-",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=cwd,
        )

        if self._console:
            if self._debug:
                worker = threading.Thread(
                    target=_stream_stderr_debug,
                    args=(proc, self._console, stop_event),
                    daemon=True,
                )
            else:
                worker = threading.Thread(
                    target=_progress_ticker,
                    args=(self._console, stop_event),
                    daemon=True,
                )
            worker.start()

        try:
            proc.stdin.write(full_input)
            proc.stdin.close()
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            stop_event.set()
            proc.kill()
            proc.wait()
            raise
        finally:
            stop_event.set()

        output = ""
        if out_path.exists():
            output = out_path.read_text(encoding="utf-8").strip()
            out_path.unlink(missing_ok=True)

        if not output and proc.returncode != 0:
            stderr = proc.stderr.read() if proc.stderr else ""
            raise RuntimeError(f"Codex failed (rc={proc.returncode}): {stderr[:200]}")

        return output

    def _run_with_progress(self, prompt: str, out_path: Path, cwd: str | None,
                           timeout: int = CODEX_TIMEOUT) -> str:
        """Full agent mode: Codex reads files, uses tools."""
        stop_event = threading.Event()
        proc = subprocess.Popen(
            ["codex", "exec", "-o", str(out_path), prompt],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=cwd,
        )

        if self._debug:
            worker = threading.Thread(
                target=_stream_stderr_debug,
                args=(proc, self._console, stop_event),
                daemon=True,
            )
        else:
            worker = threading.Thread(
                target=_progress_ticker,
                args=(self._console, stop_event),
                daemon=True,
            )
        worker.start()

        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            stop_event.set()
            proc.kill()
            proc.wait()
            raise

        stop_event.set()
        worker.join(timeout=2)

        output = ""
        if out_path.exists():
            output = out_path.read_text(encoding="utf-8").strip()
            out_path.unlink(missing_ok=True)

        if not output and proc.returncode != 0:
            stderr = proc.stderr.read() if proc.stderr else ""
            raise RuntimeError(f"Codex failed (rc={proc.returncode}): {stderr[:200]}")

        return output

    def _run_silent(self, prompt: str, out_path: Path, cwd: str | None,
                    timeout: int = CODEX_TIMEOUT) -> str:
        result = subprocess.run(
            ["codex", "exec", "-o", str(out_path), prompt],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
        output = ""
        if out_path.exists():
            output = out_path.read_text(encoding="utf-8").strip()
            out_path.unlink(missing_ok=True)

        if not output and result.returncode != 0:
            raise RuntimeError(f"Codex failed: {result.stderr.strip()[:200]}")
        return output

    def is_available(self) -> bool:
        return shutil.which("codex") is not None
