from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from gg.agents.base import AgentBackend
from gg.agents.claude import ClaudeAgent
from gg.agents.codex import CodexAgent
from gg.platforms.base import GitPlatform, detect_platform
from gg.platforms.github import GitHubPlatform
from gg.platforms.gitlab import GitLabPlatform

PlatformFactory = Callable[..., GitPlatform]
AgentFactory = Callable[..., AgentBackend]


_PLATFORM_FACTORIES: dict[str, PlatformFactory] = {
    "github": lambda project_path, **kw: GitHubPlatform(str(project_path), **kw),
    "gitlab": lambda project_path, **kw: GitLabPlatform(str(project_path), **kw),
}

_AGENT_FACTORIES: dict[str, AgentFactory] = {
    "codex": CodexAgent,
    "claude": ClaudeAgent,
}


def register_platform(name: str, factory: PlatformFactory) -> None:
    normalized = _normalize_name(name)
    if normalized == "auto":
        raise ValueError("'auto' is reserved for platform auto-detection")
    _PLATFORM_FACTORIES[normalized] = factory


def register_agent_backend(name: str, factory: AgentFactory) -> None:
    _AGENT_FACTORIES[_normalize_name(name)] = factory


def available_platforms() -> tuple[str, ...]:
    return tuple(sorted(_PLATFORM_FACTORIES))


def available_agent_backends() -> tuple[str, ...]:
    return tuple(sorted(_AGENT_FACTORIES))


def create_platform(name: str, project_path: str | Path, *, debug: bool = False) -> GitPlatform:
    selected = _normalize_name(name)
    if selected == "auto":
        detected = detect_platform(project_path)
        selected = detected if detected in _PLATFORM_FACTORIES else "github"
    try:
        factory = _PLATFORM_FACTORIES[selected]
    except KeyError as exc:
        supported = ", ".join(("auto", *available_platforms()))
        raise ValueError(f"Unsupported task platform '{name}'. Supported: {supported}") from exc
    try:
        return factory(project_path, debug=debug)
    except TypeError:
        return factory(project_path)


def create_agent_backend(name: str, **kwargs) -> AgentBackend:
    selected = _normalize_name(name)
    try:
        factory = _AGENT_FACTORIES[selected]
    except KeyError as exc:
        supported = ", ".join(available_agent_backends())
        raise ValueError(f"Unsupported agent backend '{name}'. Supported: {supported}") from exc
    try:
        return factory(**kwargs)
    except TypeError:
        return factory()


def _normalize_name(name: str) -> str:
    return name.strip().lower().replace("_", "-")
