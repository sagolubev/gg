from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

KNOWN_LINTERS = {
    "eslint", "prettier", "ruff", "flake8", "pylint", "mypy", "pyright",
    "golangci-lint", "clippy", "rubocop", "stylelint", "biome",
}

KNOWN_TEST_FRAMEWORKS = {
    "jest", "vitest", "mocha", "pytest", "unittest", "nose2",
    "go test", "cargo test", "rspec", "minitest", "playwright",
    "cypress", "testing-library",
}

KNOWN_CI_FILES = {
    ".github/workflows",
    ".gitlab-ci.yml",
    ".circleci",
    "Jenkinsfile",
    ".travis.yml",
    "bitbucket-pipelines.yml",
}


@dataclass(frozen=True)
class DependencyReport:
    package_manager: str
    runtime_deps: dict[str, str] = field(default_factory=dict)
    dev_deps: dict[str, str] = field(default_factory=dict)
    existing_tools: dict[str, list[str]] = field(default_factory=dict)

    def to_prompt_context(self) -> str:
        lines = ["## Dependencies"]
        lines.append(f"Package manager: {self.package_manager}")
        if self.runtime_deps:
            lines.append(f"Runtime dependencies: {len(self.runtime_deps)}")
            for name, ver in list(self.runtime_deps.items())[:20]:
                lines.append(f"  - {name}: {ver}")
            if len(self.runtime_deps) > 20:
                lines.append(f"  ... and {len(self.runtime_deps) - 20} more")
        if self.dev_deps:
            lines.append(f"Dev dependencies: {len(self.dev_deps)}")
            for name, ver in list(self.dev_deps.items())[:10]:
                lines.append(f"  - {name}: {ver}")
        if self.existing_tools:
            lines.append("Detected tooling:")
            for category, tools in self.existing_tools.items():
                lines.append(f"  {category}: {', '.join(tools)}")
        return "\n".join(lines)


def _parse_package_json(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    data = json.loads(path.read_text())
    runtime = data.get("dependencies", {})
    dev = data.get("devDependencies", {})
    return runtime, dev


def _parse_requirements_txt(path: Path) -> dict[str, str]:
    deps: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        match = re.match(r"^([a-zA-Z0-9_.-]+)\s*([><=!~]+.*)?", line)
        if match:
            deps = {**deps, match.group(1): (match.group(2) or "").strip()}
    return deps


def _parse_pyproject_toml(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # type: ignore[no-redef]

    data = tomllib.loads(path.read_text())
    project = data.get("project", {})

    runtime: dict[str, str] = {}
    for dep in project.get("dependencies", []):
        match = re.match(r"^([a-zA-Z0-9_.-]+)\s*(.*)", dep)
        if match:
            runtime = {**runtime, match.group(1): match.group(2).strip()}

    dev: dict[str, str] = {}
    for group_deps in project.get("optional-dependencies", {}).values():
        for dep in group_deps:
            match = re.match(r"^([a-zA-Z0-9_.-]+)\s*(.*)", dep)
            if match:
                dev = {**dev, match.group(1): match.group(2).strip()}

    return runtime, dev


def _parse_go_mod(path: Path) -> dict[str, str]:
    deps: dict[str, str] = {}
    in_require = False
    for line in path.read_text().splitlines():
        line = line.strip()
        if line.startswith("require ("):
            in_require = True
            continue
        if in_require and line == ")":
            in_require = False
            continue
        if in_require:
            parts = line.split()
            if len(parts) >= 2:
                deps = {**deps, parts[0]: parts[1]}
    return deps


def _parse_cargo_toml(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # type: ignore[no-redef]

    data = tomllib.loads(path.read_text())
    runtime: dict[str, str] = {}
    for name, val in data.get("dependencies", {}).items():
        ver = val if isinstance(val, str) else val.get("version", "")
        runtime = {**runtime, name: ver}

    dev: dict[str, str] = {}
    for name, val in data.get("dev-dependencies", {}).items():
        ver = val if isinstance(val, str) else val.get("version", "")
        dev = {**dev, name: ver}

    return runtime, dev


def _detect_tools(
    root: Path, runtime: dict[str, str], dev: dict[str, str],
) -> dict[str, list[str]]:
    all_deps = {**runtime, **dev}
    all_dep_names = {name.lower() for name in all_deps}

    tools: dict[str, list[str]] = {}

    linters = [t for t in KNOWN_LINTERS if t in all_dep_names]
    if (root / ".eslintrc.json").exists() or (root / ".eslintrc.js").exists():
        if "eslint" not in linters:
            linters = [*linters, "eslint"]
    if (root / "ruff.toml").exists() or (root / ".ruff.toml").exists():
        if "ruff" not in linters:
            linters = [*linters, "ruff"]
    if linters:
        tools = {**tools, "linters": linters}

    test_fws = [t for t in KNOWN_TEST_FRAMEWORKS if t in all_dep_names]
    if (root / "jest.config.js").exists() or (root / "jest.config.ts").exists():
        if "jest" not in test_fws:
            test_fws = [*test_fws, "jest"]
    if (root / "vitest.config.ts").exists():
        if "vitest" not in test_fws:
            test_fws = [*test_fws, "vitest"]
    if test_fws:
        tools = {**tools, "test_frameworks": test_fws}

    ci: list[str] = []
    for ci_path in KNOWN_CI_FILES:
        if (root / ci_path).exists():
            ci = [*ci, ci_path]
    if ci:
        tools = {**tools, "ci": ci}

    if (root / ".pre-commit-config.yaml").exists():
        tools = {**tools, "pre_commit": ["pre-commit"]}

    return tools


def analyze_dependencies(project_path: str | Path) -> DependencyReport:
    root = Path(project_path).resolve()
    runtime: dict[str, str] = {}
    dev: dict[str, str] = {}
    pkg_manager = "unknown"

    if (root / "package.json").exists():
        pkg_manager = "npm"
        if (root / "yarn.lock").exists():
            pkg_manager = "yarn"
        elif (root / "pnpm-lock.yaml").exists():
            pkg_manager = "pnpm"
        elif (root / "bun.lockb").exists():
            pkg_manager = "bun"
        runtime, dev = _parse_package_json(root / "package.json")

    elif (root / "pyproject.toml").exists():
        pkg_manager = "pip"
        if (root / "poetry.lock").exists():
            pkg_manager = "poetry"
        elif (root / "uv.lock").exists():
            pkg_manager = "uv"
        elif (root / "pdm.lock").exists():
            pkg_manager = "pdm"
        runtime, dev = _parse_pyproject_toml(root / "pyproject.toml")

    elif (root / "requirements.txt").exists():
        pkg_manager = "pip"
        runtime = _parse_requirements_txt(root / "requirements.txt")

    elif (root / "go.mod").exists():
        pkg_manager = "go"
        runtime = _parse_go_mod(root / "go.mod")

    elif (root / "Cargo.toml").exists():
        pkg_manager = "cargo"
        runtime, dev = _parse_cargo_toml(root / "Cargo.toml")

    existing_tools = _detect_tools(root, runtime, dev)

    return DependencyReport(
        package_manager=pkg_manager,
        runtime_deps=runtime,
        dev_deps=dev,
        existing_tools=existing_tools,
    )
