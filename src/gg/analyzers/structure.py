from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

SKIP_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv", "env",
    ".tox", ".mypy_cache", ".pytest_cache", "dist", "build",
    ".next", ".nuxt", "target", "vendor", ".gg", "openspec",
    ".idea", ".vscode", ".eggs", "*.egg-info",
}

DIR_CLASSIFICATIONS = {
    "src": "source",
    "lib": "source",
    "app": "source",
    "pkg": "source",
    "cmd": "source",
    "internal": "source",
    "components": "source",
    "pages": "source",
    "api": "source",
    "tests": "tests",
    "test": "tests",
    "__tests__": "tests",
    "spec": "tests",
    "e2e": "tests",
    "docs": "docs",
    "doc": "docs",
    "documentation": "docs",
    "config": "config",
    "configs": "config",
    "scripts": "scripts",
    "bin": "scripts",
    "tools": "scripts",
    "data": "data",
    "fixtures": "data",
    "migrations": "data",
    "public": "static",
    "static": "static",
    "assets": "static",
}


@dataclass(frozen=True)
class StructureMap:
    tree: dict[str, list[str]] = field(default_factory=dict)
    classifications: dict[str, str] = field(default_factory=dict)
    is_monorepo: bool = False
    data_patterns: list[str] = field(default_factory=list)
    top_level_dirs: list[str] = field(default_factory=list)

    def to_prompt_context(self) -> str:
        lines = ["## Project Structure"]
        if self.is_monorepo:
            lines.append("Type: Monorepo")
        lines.append(f"Top-level directories: {', '.join(self.top_level_dirs)}")
        if self.classifications:
            lines.append("Directory roles:")
            for dir_name, role in sorted(self.classifications.items()):
                lines.append(f"  - {dir_name}: {role}")
        if self.data_patterns:
            lines.append(f"Data patterns: {', '.join(self.data_patterns)}")
        return "\n".join(lines)


def analyze_structure(project_path: str | Path) -> StructureMap:
    root = Path(project_path).resolve()
    tree: dict[str, list[str]] = {}
    classifications: dict[str, str] = {}
    data_patterns: list[str] = []

    top_level = sorted(
        d.name for d in root.iterdir()
        if d.is_dir() and d.name not in SKIP_DIRS and not d.name.startswith(".")
    )

    _walk_tree(root, root, tree, classifications, depth=0, max_depth=4)

    is_monorepo = _detect_monorepo(root, top_level)
    data_patterns = _detect_data_patterns(root)

    return StructureMap(
        tree=tree,
        classifications=classifications,
        is_monorepo=is_monorepo,
        data_patterns=data_patterns,
        top_level_dirs=top_level,
    )


def _walk_tree(
    root: Path,
    current: Path,
    tree: dict[str, list[str]],
    classifications: dict[str, str],
    depth: int,
    max_depth: int,
) -> None:
    if depth > max_depth:
        return

    rel = str(current.relative_to(root)) if current != root else "."
    children: list[str] = []

    try:
        entries = sorted(current.iterdir())
    except PermissionError:
        return

    for entry in entries:
        if entry.name in SKIP_DIRS or entry.name.startswith("."):
            continue
        if entry.is_dir():
            children = [*children, entry.name + "/"]
            dir_lower = entry.name.lower()
            if dir_lower in DIR_CLASSIFICATIONS:
                classifications[entry.name] = DIR_CLASSIFICATIONS[dir_lower]
            _walk_tree(root, entry, tree, classifications, depth + 1, max_depth)

    if children:
        tree[rel] = children


def _detect_monorepo(root: Path, top_level: list[str]) -> bool:
    if (root / "lerna.json").exists():
        return True
    if (root / "pnpm-workspace.yaml").exists():
        return True
    if "packages" in top_level or "apps" in top_level:
        return True
    pkg_json = root / "package.json"
    if pkg_json.exists():
        import json
        try:
            data = json.loads(pkg_json.read_text())
            if "workspaces" in data:
                return True
        except (json.JSONDecodeError, OSError):
            pass
    return False


def _detect_data_patterns(root: Path) -> list[str]:
    patterns: list[str] = []
    data_files = {
        "*.json": "JSON data files",
        "*.toml": "TOML config files",
        "*.yaml": "YAML config files",
        "*.yml": "YAML config files",
        "*.csv": "CSV data files",
        "*.sql": "SQL scripts",
        "*.db": "SQLite databases",
        "*.sqlite": "SQLite databases",
    }
    for pattern, desc in data_files.items():
        if desc not in patterns:
            matches = list(root.glob(f"**/{pattern}"))
            filtered = [m for m in matches if not any(skip in m.parts for skip in SKIP_DIRS)]
            if len(filtered) > 2:
                patterns = [*patterns, desc]
    return patterns
