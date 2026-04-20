from __future__ import annotations

from abc import ABC, abstractmethod


class AgentBackend(ABC):
    @abstractmethod
    def generate(self, prompt: str, *, cwd: str | None = None) -> str:
        """Run the agent with a prompt. Returns generated text."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this agent backend is ready to use."""
