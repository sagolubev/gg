from __future__ import annotations

from pathlib import Path

from gg.analyzers.dependencies import DependencyReport
from gg.analyzers.languages import LanguageProfile
from gg.analyzers.structure import StructureMap


def generate_agent_files(
    *,
    project_path: str | Path,
    languages: LanguageProfile,
    dependencies: DependencyReport,
    structure: StructureMap,
    constitution_path: Path | None = None,
    preserve_existing: bool = True,
) -> None:
    root = Path(project_path).resolve()
    project_name = root.name

    commands = _detect_commands(dependencies)
    constitution_ref = ""
    if constitution_path and constitution_path.exists():
        constitution_ref = constitution_path.read_text(encoding="utf-8")

    existing_agents = ""
    if preserve_existing and (root / "AGENTS.md").exists():
        existing_agents = (root / "AGENTS.md").read_text(encoding="utf-8")

    existing_claude = ""
    if preserve_existing and (root / "CLAUDE.md").exists():
        existing_claude = (root / "CLAUDE.md").read_text(encoding="utf-8")

    agents_md = _build_agents_md(project_name, languages, dependencies, structure, commands, constitution_ref)
    if existing_agents:
        agents_md = _merge_with_existing(existing_agents, agents_md, "AGENTS.md")
    (root / "AGENTS.md").write_text(agents_md, encoding="utf-8")

    claude_md = _build_claude_md(project_name, languages, dependencies, structure, commands, constitution_ref)
    if existing_claude:
        claude_md = _merge_with_existing(existing_claude, claude_md, "CLAUDE.md")
    (root / "CLAUDE.md").write_text(claude_md, encoding="utf-8")


def _detect_commands(deps: DependencyReport) -> dict[str, str]:
    commands: dict[str, str] = {}
    pm = deps.package_manager

    if pm in ("npm", "yarn", "pnpm", "bun"):
        run = f"{pm} run" if pm != "yarn" else "yarn"
        commands = {
            **commands,
            "install": f"{pm} install",
            "dev": f"{run} dev",
            "build": f"{run} build",
            "test": f"{run} test",
        }
    elif pm in ("pip", "uv", "poetry", "pdm"):
        if pm == "poetry":
            commands = {**commands, "install": "poetry install", "test": "poetry run pytest"}
        elif pm == "uv":
            commands = {**commands, "install": "uv sync", "test": "uv run pytest"}
        else:
            commands = {**commands, "install": "pip install -e .[dev]", "test": "pytest"}
    elif pm == "go":
        commands = {**commands, "build": "go build ./...", "test": "go test ./..."}
    elif pm == "cargo":
        commands = {**commands, "build": "cargo build", "test": "cargo test"}

    tools = deps.existing_tools
    if "linters" in tools:
        linter = tools["linters"][0]
        if linter == "ruff":
            commands = {**commands, "lint": "ruff check ."}
        elif linter == "eslint":
            commands = {**commands, "lint": f"{pm} run lint" if pm in ("npm", "yarn", "pnpm") else "eslint ."}

    return commands


def _build_agents_md(
    name: str,
    langs: LanguageProfile,
    deps: DependencyReport,
    struct: StructureMap,
    commands: dict[str, str],
    constitution: str,
) -> str:
    sections = [f"# AGENTS.md -- {name}", ""]
    sections.append("Instructions for AI coding agents (Codex, Claude, etc.).")
    sections.append("")

    sections.append("## Stack")
    sections.append(f"- Primary: {langs.primary_language}")
    if langs.frameworks:
        sections.append(f"- Frameworks: {', '.join(langs.frameworks)}")
    if deps.package_manager != "unknown":
        sections.append(f"- Package manager: {deps.package_manager}")
    sections.append("")

    if commands:
        sections.append("## Commands")
        for cmd_name, cmd in commands.items():
            sections.append(f"- **{cmd_name}**: `{cmd}`")
        sections.append("")

    if struct.top_level_dirs:
        sections.append("## Structure")
        for d in struct.top_level_dirs:
            role = struct.classifications.get(d, "")
            suffix = f" ({role})" if role else ""
            sections.append(f"- `{d}/`{suffix}")
        sections.append("")

    if constitution:
        sections.append("## Project Rules")
        sections.append("")
        sections.append(constitution)
        sections.append("")

    sections.append("## References")
    sections.append("- Specs: `openspec/specs/`")
    sections.append("- Knowledge: `.gg/knowledge/`")
    sections.append("- Constitution: `.gg/constitution.md`")
    sections.append("")

    return "\n".join(sections)


def _build_claude_md(
    name: str,
    langs: LanguageProfile,
    deps: DependencyReport,
    struct: StructureMap,
    commands: dict[str, str],
    constitution: str,
) -> str:
    sections = [
        "# CLAUDE.md",
        "",
        "This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.",
        "",
    ]

    if commands:
        sections.append("## Commands")
        for cmd_name, cmd in commands.items():
            sections.append(f"- **{cmd_name}**: `{cmd}`")
        sections.append("")

    sections.append("## Architecture")
    sections.append(f"- Language: {langs.primary_language}")
    if langs.frameworks:
        sections.append(f"- Frameworks: {', '.join(langs.frameworks)}")
    if struct.is_monorepo:
        sections.append("- Type: Monorepo")
    if struct.top_level_dirs:
        sections.append("- Key directories:")
        for d in struct.top_level_dirs:
            role = struct.classifications.get(d, "")
            suffix = f" -- {role}" if role else ""
            sections.append(f"  - `{d}/`{suffix}")
    sections.append("")

    if constitution:
        sections.append("## Conventions")
        sections.append("")
        sections.append(constitution)
        sections.append("")

    return "\n".join(sections)


def _merge_with_existing(existing: str, generated: str, filename: str) -> str:
    """Preserve user-written sections from existing file, append generated content."""
    marker = "<!-- gg:auto-generated below -->"
    if marker in existing:
        user_part = existing.split(marker)[0].rstrip()
        return f"{user_part}\n\n{marker}\n\n{generated}\n"

    return (
        f"<!-- gg:existing {filename} preserved -->\n"
        f"{existing}\n\n"
        f"{marker}\n\n"
        f"{generated}\n"
    )
