from __future__ import annotations

import shutil
import subprocess

from gg.agents.base import AgentBackend

CODEX_TIMEOUT = 180
MAX_RETRIES = 1


class CodexAgent(AgentBackend):
    def generate(self, prompt: str, *, cwd: str | None = None) -> str:
        for attempt in range(MAX_RETRIES + 1):
            try:
                result = subprocess.run(
                    ["codex", "--quiet", "--prompt", prompt],
                    capture_output=True,
                    text=True,
                    timeout=CODEX_TIMEOUT,
                    cwd=cwd,
                )
                if result.returncode == 0 and result.stdout.strip():
                    return result.stdout.strip()
                if attempt < MAX_RETRIES:
                    continue
                if result.stderr.strip():
                    raise RuntimeError(f"Codex failed: {result.stderr.strip()}")
                return result.stdout.strip()
            except subprocess.TimeoutExpired:
                if attempt < MAX_RETRIES:
                    continue
                raise RuntimeError(f"Codex timed out after {CODEX_TIMEOUT}s")
        return ""

    def is_available(self) -> bool:
        return shutil.which("codex") is not None
