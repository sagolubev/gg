from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from gg.analyzers.git_history import GitProfile
from gg.analyzers.structure import StructureMap


def build_knowledge(
    *,
    project_path: str | Path,
    git_profile: GitProfile,
    structure: StructureMap,
) -> None:
    root = Path(project_path).resolve()
    knowledge = root / ".gg" / "knowledge"

    _create_dirs(knowledge)
    _write_entities(knowledge / "entities", structure, git_profile)
    _write_fact_registry(knowledge / "fact-registry.md", git_profile)
    _write_resolver(knowledge / "RESOLVER.md")
    _write_init_session(knowledge / "sessions", git_profile, structure)


def _create_dirs(knowledge: Path) -> None:
    for sub in ("entities", "decisions", "sessions"):
        (knowledge / sub).mkdir(parents=True, exist_ok=True)


def _write_entities(entities_dir: Path, structure: StructureMap, git: GitProfile) -> None:
    hot_files_map: dict[str, int] = dict(git.hot_files) if git.hot_files else {}

    for dir_name in structure.top_level_dirs:
        role = structure.classifications.get(dir_name, "unknown")

        relevant_hot = [
            (f, c) for f, c in hot_files_map.items()
            if f.startswith(dir_name + "/") or f.startswith(dir_name + "\\")
        ]
        hot_section = ""
        if relevant_hot:
            sorted_hot = sorted(relevant_hot, key=lambda x: -x[1])[:5]
            hot_lines = "\n".join(f"  - {f}: {c} changes" for f, c in sorted_hot)
            hot_section = f"\n## Hot Files\n\n{hot_lines}\n"

        owner_section = ""
        if git.contributors:
            owner_section = f"\n## Primary Contributors\n\n- {git.contributors[0].name}\n"

        content = (
            f"# {dir_name}\n\n"
            f"Role: {role}\n"
            f"{hot_section}"
            f"{owner_section}"
        )
        (entities_dir / f"{dir_name}.md").write_text(content, encoding="utf-8")


def _write_fact_registry(path: Path, git: GitProfile) -> None:
    lines = ["# Fact Registry", "", "Auto-generated from git history.", ""]

    lines.append("## Contributors")
    lines.append("")
    lines.append("| Name | Commits | Last Active |")
    lines.append("|------|---------|-------------|")
    for c in git.contributors[:15]:
        lines.append(f"| {c.name} | {c.commits} | {c.last_active} |")
    lines.append("")

    if git.hot_files:
        lines.append("## Most Changed Files")
        lines.append("")
        lines.append("| File | Changes |")
        lines.append("|------|---------|")
        for f, count in git.hot_files:
            lines.append(f"| {f} | {count} |")
        lines.append("")

    if git.coupled_files:
        lines.append("## Co-Changed Files (Jaccard >= 0.3)")
        lines.append("")
        lines.append("| File A | File B | Score |")
        lines.append("|--------|--------|-------|")
        for f1, f2, score in git.coupled_files:
            lines.append(f"| {f1} | {f2} | {score:.2f} |")
        lines.append("")

    if git.commit_style:
        lines.append("## Commit Style")
        lines.append("")
        for key, val in git.commit_style.items():
            lines.append(f"- **{key}**: {val}")
        lines.append("")

    if git.monthly_activity:
        lines.append("## Monthly Activity")
        lines.append("")
        lines.append("| Month | Commits |")
        lines.append("|-------|---------|")
        for month, count in git.monthly_activity.items():
            lines.append(f"| {month} | {count} |")
        lines.append("")

    if git.branch_patterns:
        lines.append("## Branch Naming Patterns")
        lines.append("")
        for pattern, count in git.branch_patterns.items():
            lines.append(f"- {pattern}/: {count} branches")
        lines.append("")

    if git.first_commit_date:
        lines.append("## Timeline")
        lines.append("")
        lines.append(f"- First commit: {git.first_commit_date}")
        lines.append(f"- Last commit: {git.last_commit_date}")
        lines.append(f"- Total commits: {git.total_commits}")
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def _write_resolver(path: Path) -> None:
    content = """\
# Entity Resolver

Classification tree for project entities.

## Entity Types

- **module** -- Top-level code module or package
- **service** -- External or internal service dependency
- **library** -- Third-party library with significant usage
- **config** -- Configuration file or directory
- **test** -- Test suite or test utility
- **script** -- Build/deploy/utility script
- **data** -- Data file, migration, or fixture

## Resolution Rules

1. Has its own package.json / pyproject.toml / go.mod? -> **module** (monorepo sub-package)
2. Has API endpoints or serves requests? -> **service**
3. Listed in dependencies and imported in 5+ files? -> **library**
4. Contains only config/env files? -> **config**
5. Lives under tests/ or __tests__/? -> **test**
6. Lives under scripts/ or bin/? -> **script**
7. Contains .json/.csv/.sql data files? -> **data**
8. Default: **module**
"""
    path.write_text(content, encoding="utf-8")


def _write_init_session(sessions_dir: Path, git: GitProfile, structure: StructureMap) -> None:
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "init",
        "data": {
            "total_commits": git.total_commits,
            "contributors_count": len(git.contributors),
            "top_level_dirs": structure.top_level_dirs,
            "is_monorepo": structure.is_monorepo,
        },
    }
    path = sessions_dir / "init.jsonl"
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")
