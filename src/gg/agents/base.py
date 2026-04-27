from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence


class AgentBackend(ABC):
    @abstractmethod
    def generate(self, prompt: str, *, cwd: str | None = None, timeout: int | None = None,
                 context: str | None = None) -> str:
        """Run the agent with a prompt. If context given, pipe via stdin (fast, no file reads)."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this agent backend is ready to use."""

    def context_window_tokens(self) -> int | None:
        """Return the backend's hard context limit when it is known."""
        return None

    def backend_name(self) -> str:
        """Stable backend identifier used in logs and artifacts."""
        return self.__class__.__name__.removesuffix("Agent").lower()

    def supports_sandbox_execution(self) -> bool:
        """Whether the backend can execute coding prompts via sandbox-runtime."""
        return False

    def build_sandbox_command(self, prompt: str, *, output_path: str | None = None) -> Sequence[str]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support sandbox execution")
