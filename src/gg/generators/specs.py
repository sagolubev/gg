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


CODEX_RESEARCH_PROMPT = """\
Проанализируй кодовую базу этого проекта и ответь коротко в формате:

DESCRIPTION: <что делает проект, 1-2 предложения>
DOMAINS: <основные домены/модули через запятую>
INTEGRATIONS: <внешние интеграции: APIs, базы данных, сервисы через запятую>

Анализируй файлы, зависимости и структуру. Если чего-то нет -- напиши "none".
"""


@dataclass(frozen=True)
class UserContext:
    description: str = ""
    domains: str = ""
    integrations: str = ""


def discover_context_via_codex(
    agent: "AgentBackend", project_path: str, console: Console,
) -> UserContext:
    """Let Codex discover project context instead of asking the user."""
    console.print("  [bold]Codex researching project context...[/bold]")
    try:
        raw = agent.generate(CODEX_RESEARCH_PROMPT, cwd=project_path, timeout=120)
        return _parse_research_output(raw)
    except RuntimeError as e:
        console.print(f"  [yellow]Research failed: {e}[/yellow]")
        return UserContext()


def _parse_research_output(raw: str) -> UserContext:
    description = ""
    domains = ""
    integrations = ""
    for line in raw.splitlines():
        line = line.strip()
        upper = line.upper()
        if upper.startswith("DESCRIPTION:"):
            description = line.split(":", 1)[1].strip()
        elif upper.startswith("DOMAINS:") or upper.startswith("DOMAIN:"):
            domains = line.split(":", 1)[1].strip()
        elif upper.startswith("INTEGRATIONS:") or upper.startswith("INTEGRATION:"):
            integrations = line.split(":", 1)[1].strip()
    if domains.lower() == "none":
        domains = ""
    if integrations.lower() == "none":
        integrations = ""
    return UserContext(description=description, domains=domains, integrations=integrations)


def ask_user_context(console: Console) -> UserContext:
    """Fallback: ask user manually if Codex is not available."""
    description = Prompt.ask(
        "[bold]Опишите проект[/bold] в 1-2 предложениях",
        default="",
        console=console,
    )
    return UserContext(description=description.strip())


def _build_full_prompt(
    user_ctx: UserContext, analyzer_context: str, existing_agents_md: str = "",
) -> str:
    parts = [CODEX_ANALYSIS_PROMPT]
    if existing_agents_md:
        parts = [*parts, f"\nВ проекте уже есть AGENTS.md. Учитывай его при формировании конституции:\n{existing_agents_md}"]
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
    """Generate structured specs and constitution from local analysis."""
    _write_specs(project_path, {}, user_ctx)

    from gg.analyzers.dependencies import analyze_dependencies
    from gg.analyzers.languages import analyze_languages
    from gg.analyzers.structure import analyze_structure

    root = project_path
    langs = analyze_languages(root)
    deps = analyze_dependencies(root)
    struct = analyze_structure(root)

    rules: list[str] = ["# Project Constitution", ""]

    rules.append("## Technology Stack")
    rules.append(f"- Primary language: **{langs.primary_language}**")
    if langs.frameworks:
        rules.append(f"- Frameworks: {', '.join(langs.frameworks)}")
    if deps.package_manager != "unknown":
        rules.append(f"- Package manager: **{deps.package_manager}**")
    if langs.styling:
        rules.append(f"- Styling: {', '.join(langs.styling)}")
    rules.append("")

    rules.append("## Architecture")
    if struct.is_monorepo:
        rules.append("- This is a **monorepo**. Each package has its own dependencies.")
    rules.append(f"- Top-level directories: {', '.join(f'`{d}/`' for d in struct.top_level_dirs)}")
    if struct.classifications:
        for d, role in struct.classifications.items():
            rules.append(f"  - `{d}/`: {role}")
    rules.append("- New code should be placed in the appropriate existing directory.")
    rules.append("- Do not create new top-level directories without explicit approval.")
    rules.append("")

    if langs.styling:
        rules.append("## Styling")
        for s in langs.styling:
            rules.append(f"- Use **{s}** for styling. Do not introduce alternative CSS approaches.")
        rules.append("")

    if struct.data_patterns:
        rules.append("## Data Management")
        for p in struct.data_patterns:
            rules.append(f"- {p}")
        rules.append("")

    rules.append("## Development Practices")
    existing = deps.existing_tools
    if "linters" in existing:
        rules.append(f"- Linters: {', '.join(existing['linters'])}. Run before committing.")
    if "test_frameworks" in existing:
        rules.append(f"- Tests: {', '.join(existing['test_frameworks'])}. All changes must pass tests.")
    if "ci" in existing:
        rules.append(f"- CI/CD: {', '.join(existing['ci'])}. Do not bypass CI checks.")
    if "pre_commit" in existing:
        rules.append("- Pre-commit hooks are configured. Do not skip them.")

    # Commit style from git
    rules.append("- Follow the existing commit message style of this project.")
    rules.append("")

    rules.append("## File Conventions")
    rules.append(f"- Source files: {langs.total_files} {langs.primary_language} files")
    if deps.runtime_deps:
        top_deps = list(deps.runtime_deps.keys())[:10]
        rules.append(f"- Key dependencies: {', '.join(top_deps)}")
    rules.append("- Do not add new dependencies without justification.")
    rules.append("")

    gg_dir = root / ".gg"
    gg_dir.mkdir(parents=True, exist_ok=True)
    (gg_dir / "constitution.md").write_text("\n".join(rules), encoding="utf-8")


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

    console.print("    Initializing openspec directory...")
    if not _init_openspec(root, console):
        _create_openspec_dirs(root)
        console.print("    Created openspec/ manually")

    console.print("    Writing openspec config...")
    _write_openspec_config(root, user_ctx)

    existing_agents_md = ""
    agents_path = root / "AGENTS.md"
    if agents_path.exists():
        existing_agents_md = agents_path.read_text(encoding="utf-8").strip()
        console.print(f"    Found existing AGENTS.md ({len(existing_agents_md)} chars), will incorporate")

    existing_claude_md = ""
    claude_path = root / "CLAUDE.md"
    if claude_path.exists():
        existing_claude_md = claude_path.read_text(encoding="utf-8").strip()
        console.print(f"    Found existing CLAUDE.md ({len(existing_claude_md)} chars), will incorporate")

    if agent and agent.is_available():
        console.print("    Sending collected context to Codex (no file reads)...")
        context_parts = [analyzer_context]
        if existing_agents_md:
            context_parts = [*context_parts, f"\n## Existing AGENTS.md\n\n{existing_agents_md[:3000]}"]
        full_context = "\n\n".join(context_parts)
        prompt = _build_full_prompt(user_ctx, "", existing_agents_md="")
        try:
            raw = agent.generate(prompt, cwd=str(root), context=full_context, timeout=120)
            console.print("    Parsing Codex response...")
            sections = _parse_codex_output(raw)
            console.print(f"    Found {len(sections)} sections: {', '.join(sections.keys())}")
            console.print("    Writing constitution...")
            _write_constitution(root, raw, sections)
            console.print("    Writing specs...")
            _write_specs(root, sections, user_ctx)
            console.print("    Writing concept...")
            spec_files = [f for f in (root / "openspec" / "specs").iterdir() if f.is_file()]
            console.print(f"  [green]  -> constitution.md + {len(spec_files)} spec files via Codex[/green]")
        except RuntimeError as e:
            console.print(f"    [yellow]Codex failed: {e}[/yellow]")
            console.print("    Falling back to local analysis...")
            _generate_local_fallback(root, analyzer_context, user_ctx)
            console.print("  [green]  -> constitution.md from local analysis[/green]")
    else:
        console.print("    [yellow]Codex not available[/yellow]")
        console.print("    Generating from local analysis...")
        _generate_local_fallback(root, analyzer_context, user_ctx)
        console.print("  [green]  -> constitution.md from local analysis[/green]")
