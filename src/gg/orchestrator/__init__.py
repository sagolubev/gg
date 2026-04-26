"""Walking-skeleton orchestrator for gg."""

__all__ = ["OrchestratorPipeline"]


def __getattr__(name: str):
    if name == "OrchestratorPipeline":
        from gg.orchestrator.pipeline import OrchestratorPipeline

        return OrchestratorPipeline
    raise AttributeError(name)
