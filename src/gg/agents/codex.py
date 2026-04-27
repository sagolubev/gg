from __future__ import annotations

import shlex
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable

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


def _progress_ticker(console, stop_event: threading.Event, progress_callback: Callable[[str], None] | None = None) -> None:
    start = time.monotonic()
    while not stop_event.is_set():
        stop_event.wait(15)
        if not stop_event.is_set():
            elapsed = int(time.monotonic() - start)
            message = f"Codex working ({elapsed}s elapsed)"
            if console is not None:
                console.print(f"    [dim]... {message}[/dim]")
            if progress_callback is not None:
                progress_callback(message)


def _get_fast_mode_flags() -> list[str]:
    """Generate -c flags to disable hooks and all MCP servers for fast mode."""
    flags = ["features.codex_hooks=false"]

    config_path = Path.home() / ".codex" / "config.toml"
    if config_path.exists():
        try:
            text = config_path.read_text(encoding="utf-8")
            import re
            servers = re.findall(r"\[mcp_servers\.(\w+)\]", text)
            flags = [*flags, *[f"mcp_servers.{s}.enabled=false" for s in servers]]
        except OSError:
            pass

    return flags


class CodexAgent(AgentBackend):
    supports_task_analysis = True

    def __init__(self, console=None, debug: bool = False, command: str = "codex", progress_callback: Callable[[str], None] | None = None):
        self._console = console
        self._debug = debug
        self._command = command
        self._progress_callback = progress_callback

    def generate(self, prompt: str, *, cwd: str | None = None, timeout: int | None = None,
                 context: str | None = None) -> str:
        effective_timeout = timeout or CODEX_TIMEOUT
        retries = 0 if (timeout or context) else MAX_RETRIES
        out_path = Path(tempfile.mktemp(suffix=".md"))

        for attempt in range(retries + 1):
            try:
                self._emit_progress(
                    f"starting {'fast' if context else 'full'} Codex run"
                    + (f" (attempt {attempt + 1}/{retries + 1})" if retries else "")
                )
                if context:
                    output = self._run_fast(prompt, context, out_path, cwd, effective_timeout)
                elif self._console:
                    output = self._run_with_progress(prompt, out_path, cwd, effective_timeout)
                else:
                    output = self._run_silent(prompt, out_path, cwd, effective_timeout)

                if output:
                    self._emit_progress("Codex produced output")
                    return output
                if attempt < retries:
                    if self._console:
                        self._console.print("    [yellow]Empty response, retrying...[/yellow]")
                    self._emit_progress("Codex returned empty output; retrying")
                    continue
                return ""
            except subprocess.TimeoutExpired:
                out_path.unlink(missing_ok=True)
                if attempt < retries:
                    if self._console:
                        self._console.print(f"    [yellow]Timeout after {effective_timeout}s, retrying...[/yellow]")
                    self._emit_progress(f"Codex timed out after {effective_timeout}s; retrying")
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
        """Fast mode: pipe context via stdin, read-only sandbox, no hooks, no MCP."""
        full_input = f"{context}\n\n---\n\n{prompt}"

        cmd = [
            *self._command_args(), "exec",
            "--sandbox", "read-only",
            "--skip-git-repo-check",
            "--ephemeral",
        ]
        for flag in _get_fast_mode_flags():
            cmd = [*cmd, "-c", flag]
        cmd = [*cmd, "-o", str(out_path), "-"]

        if self._console and self._debug:
            from rich.text import Text
            self._console.print(Text(f"    cmd: {' '.join(cmd[:8])}...", style="dim"))
            self._console.print(Text(f"    input: {len(full_input)} chars", style="dim"))

        result = subprocess.run(
            cmd,
            input=full_input,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )

        if self._debug and self._console and result.stderr:
            from rich.text import Text
            for line in result.stderr.strip().splitlines()[-5:]:
                self._console.print(Text(f"    {line}", style="dim"))

        output = ""
        if out_path.exists():
            output = out_path.read_text(encoding="utf-8").strip()
            out_path.unlink(missing_ok=True)

        if not output and result.returncode != 0:
            raise RuntimeError(f"Codex failed (rc={result.returncode}): {result.stderr.strip()[:200]}")

        return output

    def _run_with_progress(self, prompt: str, out_path: Path, cwd: str | None,
                           timeout: int = CODEX_TIMEOUT) -> str:
        """Full agent mode: Codex reads files, uses tools."""
        stop_event = threading.Event()
        proc = subprocess.Popen(
            [*self._command_args(), "exec", "-o", str(out_path), prompt],
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
                args=(self._console, stop_event, self._progress_callback),
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
            [*self._command_args(), "exec", "-o", str(out_path), prompt],
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
        binary = self._command_args()[0] if self._command_args() else "codex"
        return shutil.which(binary) is not None

    def supports_sandbox_execution(self) -> bool:
        return True

    def build_sandbox_command(self, prompt: str, *, output_path: str | None = None) -> list[str]:
        if not output_path:
            raise ValueError("Codex sandbox execution requires output_path")
        return [
            *self._command_args(),
            "exec",
            "--dangerously-bypass-approvals-and-sandbox",
            "-o",
            output_path,
            prompt,
        ]

    def _command_args(self) -> list[str]:
        return shlex.split(self._command.strip() or "codex")

    def _emit_progress(self, message: str) -> None:
        if self._progress_callback is not None:
            self._progress_callback(message)
