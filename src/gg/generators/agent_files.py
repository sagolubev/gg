from __future__ import annotations

import re
from pathlib import Path

from gg.analyzers.dependencies import DependencyReport
from gg.analyzers.languages import LanguageProfile
from gg.analyzers.structure import StructureMap

MARKER = "<!-- gg:auto-generated below -->"


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

    if existing_agents:
        agents_md = _smart_merge(
            existing_agents, languages, dependencies, structure, commands, constitution_ref,
        )
    else:
        agents_md = _build_agents_md(project_name, languages, dependencies, structure, commands, constitution_ref)
    (root / "AGENTS.md").write_text(agents_md, encoding="utf-8")

    if existing_claude:
        claude_md = _smart_merge(
            existing_claude, languages, dependencies, structure, commands, constitution_ref,
            is_claude=True,
        )
    else:
        claude_md = _build_claude_md(project_name, languages, dependencies, structure, commands, constitution_ref)
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


def _smart_merge(
    existing: str,
    langs: LanguageProfile,
    deps: DependencyReport,
    struct: StructureMap,
    commands: dict[str, str],
    constitution: str,
    is_claude: bool = False,
) -> str:
    """Analyze existing doc, add only what's missing. No duplication."""
    user_part = existing.split(MARKER)[0].rstrip() if MARKER in existing else existing.rstrip()
    existing_lower = user_part.lower()

    gaps: list[str] = []

    missing_commands = _find_missing_commands(existing_lower, commands)
    if missing_commands:
        lines = ["## Additional Commands (detected by gg)"]
        for name, cmd in missing_commands.items():
            lines.append(f"- **{name}**: `{cmd}`")
        gaps.append("\n".join(lines))

    if not _has_section(existing_lower, ["testing", "test instructions", "test commands", "test framework"]):
        test_info = _build_testing_section(deps)
        if test_info:
            gaps.append(test_info)

    if not _has_section(existing_lower, ["grepai", "semantic search"]):
        import shutil
        if shutil.which("grepai"):
            gaps.append(_build_grepai_section())

    if constitution and not _has_section(existing_lower, ["constitution", "project rules"]):
        gaps.append("## GG References\n\n"
                    "- Constitution: `.gg/constitution.md`\n"
                    "- Specs: `openspec/specs/`\n"
                    "- Knowledge: `.gg/knowledge/`")

    if not gaps:
        return user_part + "\n"

    auto_section = "\n\n".join(gaps)
    return f"{user_part}\n\n{MARKER}\n\n{auto_section}\n"


def _find_missing_commands(existing_lower: str, commands: dict[str, str]) -> dict[str, str]:
    missing: dict[str, str] = {}
    for name, cmd in commands.items():
        cmd_pattern = cmd.lower().replace(".", r"\.").split()[0]
        if cmd_pattern not in existing_lower and f"`{cmd.lower()}`" not in existing_lower:
            missing = {**missing, name: cmd}
    return missing


def _has_section(text: str, keywords: list[str]) -> bool:
    for kw in keywords:
        if kw in text:
            return True
    return False


def _build_grepai_section() -> str:
    return """\
## Semantic Code Search (grepai)

This project has grepai configured for semantic code search.
Use it BEFORE reading files to find relevant code efficiently.

### Search by meaning
```
grepai search "authentication logic"     # finds handleUserSession, etc.
grepai search "database connection pool"  # finds pool setup even if named differently
grepai search "error handling pattern"    # finds try/catch conventions
```

### Trace call graphs (before changing a function)
```
grepai trace callers "functionName"   # who calls this function?
grepai trace callees "functionName"   # what does this function call?
```

### When to use grepai vs grep
- **grepai**: when you know WHAT you're looking for conceptually but not the exact name
- **grep/ripgrep**: when you know the exact string, variable name, or pattern

### Rules
- Always run `grepai search` before modifying unfamiliar code areas
- Use `grepai trace callers` before refactoring any public function
- Prefer grepai over reading entire files to save tokens"""


def _build_testing_section(deps: DependencyReport) -> str:
    test_tools = deps.existing_tools.get("test_frameworks", [])
    if not test_tools:
        return ""

    lines = ["## Testing (detected by gg)", ""]
    lines.append(f"Detected test frameworks: {', '.join(test_tools)}")
    lines.append("")

    pm = deps.package_manager
    if "jest" in test_tools or "vitest" in test_tools:
        run = f"{pm} run" if pm != "yarn" else "yarn"
        fw = "vitest" if "vitest" in test_tools else "jest"
        lines.append(f"- Run all tests: `{run} test`")
        lines.append(f"- Run single test: `{run} test -- <pattern>`")
        lines.append(f"- Framework: {fw}")
    elif "pytest" in test_tools:
        lines.append("- Run all tests: `pytest`")
        lines.append("- Run single test: `pytest tests/test_file.py::test_name`")
        lines.append("- With coverage: `pytest --cov`")

    return "\n".join(lines)


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

    test_section = _build_testing_section(deps)
    if test_section:
        sections.append(test_section)
        sections.append("")

    import shutil
    if shutil.which("grepai"):
        sections.append(_build_grepai_section())
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

    test_section = _build_testing_section(deps)
    if test_section:
        sections.append(test_section)
        sections.append("")

    if shutil.which("grepai"):
        sections.append(_build_grepai_section())
        sections.append("")

    if constitution:
        sections.append("## Conventions")
        sections.append("")
        sections.append(constitution)
        sections.append("")

    return "\n".join(sections)
