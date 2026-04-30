from __future__ import annotations

import shlex
import shutil
import subprocess
from typing import Callable

from gg.agents.base import AgentBackend

CLAUDE_TIMEOUT = 600
CLAUDE_CONTEXT_WINDOW_TOKENS = 200_000


class ClaudeAgent(AgentBackend):
    supports_task_analysis = True

    def __init__(
        self,
        console=None,
        debug: bool = False,
        command: str = "claude",
        progress_callback: Callable[[str], None] | None = None,
        model: str = "",
        effort: str = "",
        profile: str = "",
    ):
        self._console = console
        self._debug = debug
        self._command = command
        self._progress_callback = progress_callback
        self._model = model
        self._effort = effort
        self._profile = profile

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        effective_timeout = timeout or CLAUDE_TIMEOUT
        full_prompt = _merge_context(context, prompt)
        cmd = self._fast_command(full_prompt) if context else self._full_command(full_prompt)
        self._emit_progress(f"starting {'fast' if context else 'full'} Claude run")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=effective_timeout,
            cwd=cwd,
        )
        if self._debug and self._console and result.stderr:
            for line in result.stderr.strip().splitlines()[-5:]:
                self._console.print(f"    {line}", style="dim")
        output = result.stdout.strip()
        if not output and result.returncode != 0:
            raise RuntimeError(f"Claude failed (rc={result.returncode}): {result.stderr.strip()[:200]}")
        if output:
            self._emit_progress("Claude produced output")
        return output

    def is_available(self) -> bool:
        binary = self._command_args()[0] if self._command_args() else "claude"
        return shutil.which(binary) is not None

    def context_window_tokens(self) -> int | None:
        return CLAUDE_CONTEXT_WINDOW_TOKENS

    def supports_sandbox_execution(self) -> bool:
        return True

    def build_sandbox_command(self, prompt: str, *, output_path: str | None = None) -> list[str]:
        return self._full_command(prompt)

    def _fast_command(self, prompt: str) -> list[str]:
        return [
            *self._command_args(),
            *self._model_args(),
            "--bare",
            "--output-format",
            "text",
            "--permission-mode",
            "bypassPermissions",
            "--tools",
            "",
            "-p",
            prompt,
        ]

    def _full_command(self, prompt: str) -> list[str]:
        return [
            *self._command_args(),
            *self._model_args(),
            "--dangerously-skip-permissions",
            "--output-format",
            "text",
            "-p",
            prompt,
        ]

    def _command_args(self) -> list[str]:
        return shlex.split(self._command.strip() or "claude")

    def _model_args(self) -> list[str]:
        if not self._model:
            return []
        return ["--model", self._model]

    def effective_profile(self) -> dict[str, str]:
        return {
            "backend": "claude",
            "model": getattr(self, "_model", ""),
            "effort": getattr(self, "_effort", ""),
            "profile": getattr(self, "_profile", "claude"),
        }

    def _emit_progress(self, message: str) -> None:
        if self._progress_callback is not None:
            self._progress_callback(message)


def _merge_context(context: str | None, prompt: str) -> str:
    if not context:
        return prompt
    return f"{context}\n\n---\n\n{prompt}"
