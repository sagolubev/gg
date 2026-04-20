from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.prompt import Prompt

from gg.agents.base import AgentBackend

CODEX_ANALYSIS_PROMPT = """\
Проанализируй всю кодовую базу этого проекта. На основе анализа файлов, \
зависимостей и структуры директорий, создай сводку ключевых принципов \
и технических ограничений проекта.

Твоя задача - сформулировать набор правил ("конституцию"), которым должен \
следовать разработчик при добавлении нового кода.

Сводка должна включать:
**Основной стек технологий:** Языки программирования, фреймворки, библиотеки.
**Архитектура и структура:** Как организованы файлы и где должны располагаться новые компоненты.
**Стилизация:** Как принято писать стили (например, CSS-модули, Tailwind, отдельные файлы).
**Управление данными:** Где в каком формате хранятся данные (например, в TOML/JSON файлах, в базе данных).
**Особые практики:** Есть ли тесты, линтеры или другие устоявшиеся в проекте подходы.

Подготовь документацию, как если бы это были артефакты для openspec.
"""


@dataclass(frozen=True)
class UserContext:
    description: str = ""
    domains: str = ""
    integrations: str = ""


def ask_user_context(console: Console) -> UserContext:
    description = Prompt.ask(
        "[bold]Опишите проект[/bold] в 1-2 предложениях",
        default="",
        console=console,
    )
    domains = Prompt.ask(
        "[bold]Основные домены/модули[/bold] (через запятую)",
        default="",
        console=console,
    )
    integrations = Prompt.ask(
        "[bold]Внешние интеграции[/bold] (APIs, DBs, сервисы)",
        default="",
        console=console,
    )
    return UserContext(
        description=description.strip(),
        domains=domains.strip(),
        integrations=integrations.strip(),
    )


def _build_full_prompt(user_ctx: UserContext, analyzer_context: str) -> str:
    parts = [CODEX_ANALYSIS_PROMPT]
    if user_ctx.description:
        parts = [*parts, f"\nОписание проекта от разработчика: {user_ctx.description}"]
    if user_ctx.domains:
        parts = [*parts, f"Основные домены/модули: {user_ctx.domains}"]
    if user_ctx.integrations:
        parts = [*parts, f"Внешние интеграции: {user_ctx.integrations}"]
    if analyzer_context:
        parts = [*parts, f"\nДополнительный контекст из локального анализа:\n{analyzer_context}"]
    return "\n".join(parts)


def _init_openspec(project_path: Path, console: Console) -> bool:
    if shutil.which("openspec"):
        console.print("  Running [bold]openspec init[/bold]...")
        result = subprocess.run(
            ["openspec", "init", "--tools", "codex,claude"],
            capture_output=True, text=True, timeout=30,
            cwd=str(project_path),
        )
        if result.returncode == 0:
            console.print("  [green]openspec initialized[/green]")
            return True
        console.print(f"  [yellow]openspec init failed, creating manually[/yellow]")
    return False


def _create_openspec_dirs(project_path: Path) -> None:
    dirs = [
        project_path / "openspec",
        project_path / "openspec" / "specs",
        project_path / "openspec" / "changes",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def _parse_codex_output(raw: str) -> dict[str, str]:
    """Split Codex output into sections by markdown headers."""
    sections: dict[str, str] = {}
    current_key = "constitution"
    current_lines: list[str] = []

    for line in raw.splitlines():
        if line.startswith("## ") or line.startswith("# "):
            if current_lines:
                sections[current_key] = "\n".join(current_lines).strip()
            header = line.lstrip("#").strip().lower()
            if "стек" in header or "stack" in header or "технолог" in header:
                current_key = "stack"
            elif "архитектур" in header or "структур" in header or "architect" in header:
                current_key = "architecture"
            elif "стил" in header or "style" in header or "css" in header:
                current_key = "styling"
            elif "данн" in header or "data" in header:
                current_key = "data"
            elif "практик" in header or "practice" in header or "тест" in header or "линт" in header:
                current_key = "practices"
            else:
                current_key = header.replace(" ", "_")[:30]
            current_lines = []
        else:
            current_lines = [*current_lines, line]

    if current_lines:
        sections[current_key] = "\n".join(current_lines).strip()

    return sections


def _write_constitution(project_path: Path, raw_output: str, sections: dict[str, str]) -> None:
    gg_dir = project_path / ".gg"
    gg_dir.mkdir(parents=True, exist_ok=True)
    (gg_dir / "constitution.md").write_text(
        f"# Project Constitution\n\n{raw_output}\n",
        encoding="utf-8",
    )


def _write_specs(project_path: Path, sections: dict[str, str], user_ctx: UserContext) -> None:
    specs_dir = project_path / "openspec" / "specs"
    specs_dir.mkdir(parents=True, exist_ok=True)

    if "stack" in sections:
        (specs_dir / "stack.md").write_text(
            f"# Technology Stack\n\n{sections['stack']}\n",
            encoding="utf-8",
        )

    if "architecture" in sections:
        (specs_dir / "architecture.md").write_text(
            f"# Architecture\n\n{sections['architecture']}\n",
            encoding="utf-8",
        )

    remaining = {k: v for k, v in sections.items() if k not in ("stack", "architecture", "constitution")}
    if remaining:
        combined = "\n\n".join(f"## {k.replace('_', ' ').title()}\n\n{v}" for k, v in remaining.items())
        (specs_dir / "conventions.md").write_text(
            f"# Conventions & Practices\n\n{combined}\n",
            encoding="utf-8",
        )

    concept_parts = []
    if user_ctx.description:
        concept_parts = [*concept_parts, f"## Overview\n\n{user_ctx.description}"]
    if user_ctx.domains:
        concept_parts = [*concept_parts, f"## Domains\n\n{user_ctx.domains}"]
    if user_ctx.integrations:
        concept_parts = [*concept_parts, f"## Integrations\n\n{user_ctx.integrations}"]

    concept_text = "\n\n".join(concept_parts) if concept_parts else "Project concept -- to be filled."
    (project_path / "openspec" / "concept.md").write_text(
        f"# Project Concept\n\n{concept_text}\n",
        encoding="utf-8",
    )


def _write_openspec_config(project_path: Path, user_ctx: UserContext) -> None:
    import yaml

    config = {
        "project": {
            "name": project_path.name,
            "description": user_ctx.description or f"{project_path.name} project",
        },
        "tools": ["codex", "claude"],
    }
    (project_path / "openspec" / "config.yaml").write_text(
        yaml.dump(config, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )


def _generate_local_fallback(
    project_path: Path, analyzer_context: str, user_ctx: UserContext,
) -> None:
    """Generate basic specs without Codex, from local analysis only."""
    _write_specs(project_path, {}, user_ctx)

    gg_dir = project_path / ".gg"
    gg_dir.mkdir(parents=True, exist_ok=True)
    (gg_dir / "constitution.md").write_text(
        f"# Project Constitution\n\n"
        f"Generated from local analysis (Codex unavailable).\n\n"
        f"{analyzer_context}\n",
        encoding="utf-8",
    )


def generate_specs(
    *,
    project_path: str | Path,
    agent: AgentBackend | None,
    analyzer_context: str,
    user_ctx: UserContext | None = None,
    interactive: bool = True,
    console: Console | None = None,
) -> None:
    console = console or Console()
    root = Path(project_path).resolve()

    if user_ctx is None and interactive:
        user_ctx = ask_user_context(console)
    elif user_ctx is None:
        user_ctx = UserContext()

    if not _init_openspec(root, console):
        _create_openspec_dirs(root)

    _write_openspec_config(root, user_ctx)

    if agent and agent.is_available():
        console.print("  [bold]Running Codex analysis...[/bold]")
        prompt = _build_full_prompt(user_ctx, analyzer_context)
        try:
            raw = agent.generate(prompt, cwd=str(root))
            sections = _parse_codex_output(raw)
            _write_constitution(root, raw, sections)
            _write_specs(root, sections, user_ctx)
            console.print("  [green]Specs and constitution generated via Codex[/green]")
        except RuntimeError as e:
            console.print(f"  [yellow]Codex failed: {e}. Using local fallback.[/yellow]")
            _generate_local_fallback(root, analyzer_context, user_ctx)
    else:
        console.print("  [yellow]Codex not available, using local analysis only[/yellow]")
        _generate_local_fallback(root, analyzer_context, user_ctx)
