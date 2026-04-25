"""Walking-skeleton orchestrator for gg."""

from __future__ import annotations

from typing import Any

__all__ = ["OrchestratorPipeline"]


def __getattr__(name: str) -> Any:
    if name == "OrchestratorPipeline":
        from gg.orchestrator.pipeline import OrchestratorPipeline

        return OrchestratorPipeline
    raise AttributeError(name)
