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
    write_contributor_exemplars(root, git_profile)
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


def rank_contributor_exemplars(git: GitProfile, *, limit: int = 5) -> list[dict]:
    ownership_by_owner: dict[str, list[dict[str, str | float]]] = {}
    for ownership in git.file_ownership:
        ownership_by_owner.setdefault(ownership.primary_owner, []).append(
            {
                "path": ownership.path,
                "ownership_pct": round(ownership.ownership_pct, 2),
            }
        )
    hot_files = {path: count for path, count in git.hot_files}
    ranked: list[dict] = []
    for contributor in git.contributors:
        owned = ownership_by_owner.get(contributor.name, [])
        hot_owned = sum(hot_files.get(str(item["path"]), 0) for item in owned)
        score = contributor.commits + hot_owned * 0.5 + len(owned) * 2
        ranked.append(
            {
                "name": contributor.name,
                "email": contributor.email,
                "commits": contributor.commits,
                "last_active": contributor.last_active,
                "owned_files": owned[:10],
                "hot_owned_changes": hot_owned,
                "score": round(score, 2),
                "reason": _contributor_reason(contributor.commits, owned, hot_owned),
            }
        )
    return sorted(ranked, key=lambda item: (-float(item["score"]), str(item["name"])))[:limit]


def write_contributor_exemplars(project_path: str | Path, git: GitProfile) -> Path:
    root = Path(project_path).resolve()
    knowledge = root / ".gg" / "knowledge"
    knowledge.mkdir(parents=True, exist_ok=True)
    ranked = rank_contributor_exemplars(git)
    local_exemplars = [
        {
            "source": "local-fallback",
            "sha": commit.sha,
            "message": commit.message,
            "date": commit.date,
            "commit_type": commit.commit_type,
            "files_changed": commit.files_changed,
        }
        for commit in git.architectural_commits[:10]
    ]
    payload = {
        "schema_version": 1,
        "source": "local-fallback",
        "contributors": ranked,
        "exemplars": local_exemplars,
    }
    (knowledge / "exemplars.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    lines = ["# Contributor Exemplars", "", "Source: local-fallback", ""]
    lines.append("## Strongest Contributors")
    if not ranked:
        lines.append("")
        lines.append("Insufficient git history for contributor ranking.")
    for item in ranked:
        lines.extend(
            [
                "",
                f"### {item['name']}",
                f"- Score: {item['score']}",
                f"- Commits: {item['commits']}",
                f"- Last active: {item['last_active']}",
                f"- Why: {item['reason']}",
            ]
        )
        owned = item.get("owned_files") or []
        if owned:
            lines.append("- Owned hot files:")
            for owned_file in owned[:5]:
                lines.append(f"  - {owned_file['path']} ({owned_file['ownership_pct']}%)")
    lines.extend(["", "## Good Local Examples"])
    if not local_exemplars:
        lines.append("")
        lines.append("No architectural/refactor commits were detected; use current project constitution as fallback.")
    for exemplar in local_exemplars:
        lines.extend(
            [
                "",
                f"- `{exemplar['sha'][:12]}` {exemplar['message']}",
                f"  - Type: {exemplar['commit_type']}",
                f"  - Date: {exemplar['date']}",
                f"  - Files changed: {exemplar['files_changed']}",
            ]
        )
    path = knowledge / "exemplars.md"
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return path


def _contributor_reason(commits: int, owned: list[dict[str, str | float]], hot_owned: int) -> str:
    reasons = [f"{commits} commits"]
    if owned:
        reasons.append(f"{len(owned)} owned files")
    if hot_owned:
        reasons.append(f"{hot_owned} hot-file changes")
    return ", ".join(reasons)


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
