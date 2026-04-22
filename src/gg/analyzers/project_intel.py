"""Project intelligence: deep regex-based analysis for autonomous agent work.

Scans:
- API routes (tRPC, REST, GraphQL)
- DB schema (Drizzle, Prisma, raw SQL)
- Component tree (React/Vue/Svelte exports)
- Test examples (find representative test files)
- Style exemplars (find "golden" files per code type)
- PR checklist (from detected tools + CI)
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

SKIP_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv", "env",
    "dist", "build", ".next", ".nuxt", "target", "vendor",
    ".gg", "openspec", ".idea", ".vscode",
}

# -- API Inventory --

TRPC_PATTERN = re.compile(
    r"""(?:protectedProcedure|publicProcedure|procedure)\s*"""
    r"""\.(?:input|query|mutation|subscription)""",
)
TRPC_ROUTER_PATTERN = re.compile(r"""(\w+):\s*(?:protectedProcedure|publicProcedure|procedure)""")
ROUTER_FILE_PATTERN = re.compile(r"""(?:createTRPCRouter|router)\s*\(\s*\{""")

REST_PATTERNS = [
    re.compile(r"""@(Get|Post|Put|Delete|Patch)\s*\(\s*['"](/[^'"]*)['"]\s*\)"""),
    re.compile(r"""app\.(get|post|put|delete|patch)\s*\(\s*['"](/[^'"]+)""", re.I),
    re.compile(r"""router\.(get|post|put|delete|patch)\s*\(\s*['"](/[^'"]+)""", re.I),
]


@dataclass(frozen=True)
class ApiEndpoint:
    name: str
    method: str
    file: str
    line: int
    endpoint_type: str  # "trpc", "rest", "graphql"


def scan_api_inventory(project_path: str | Path) -> list[ApiEndpoint]:
    root = Path(project_path).resolve()
    endpoints: list[ApiEndpoint] = []

    for fpath in _walk_code_files(root, extensions={".ts", ".tsx", ".js", ".jsx", ".py", ".go"}):
        try:
            text = fpath.read_text(encoding="utf-8", errors="ignore")
            rel = str(fpath.relative_to(root))

            if ROUTER_FILE_PATTERN.search(text):
                for i, line in enumerate(text.splitlines(), 1):
                    m = TRPC_ROUTER_PATTERN.search(line)
                    if m:
                        endpoints = [*endpoints, ApiEndpoint(
                            name=m.group(1), method="trpc",
                            file=rel, line=i, endpoint_type="trpc",
                        )]

            for pattern in REST_PATTERNS:
                for m in pattern.finditer(text):
                    line_num = text[:m.start()].count("\n") + 1
                    groups = m.groups()
                    if len(groups) == 2:
                        endpoints = [*endpoints, ApiEndpoint(
                            name=groups[1], method=groups[0].upper(),
                            file=rel, line=line_num, endpoint_type="rest",
                        )]
        except OSError:
            continue

    return endpoints


# -- DB Schema Map --

DRIZZLE_TABLE_PATTERN = re.compile(
    r"""(?:pgTable|mysqlTable|sqliteTable)\s*\(\s*['"](\w+)['"]\s*,""",
)
DRIZZLE_COLUMN_PATTERN = re.compile(
    r"""(\w+):\s*(?:text|varchar|integer|serial|boolean|timestamp|uuid|json|bigint|real|numeric)\s*\(""",
)
DRIZZLE_RELATION_PATTERN = re.compile(
    r"""relations\s*\(\s*(\w+)\s*,""",
)

PRISMA_MODEL_PATTERN = re.compile(r"""^model\s+(\w+)\s*\{""", re.M)
PRISMA_FIELD_PATTERN = re.compile(r"""^\s+(\w+)\s+(\w+)""", re.M)


@dataclass(frozen=True)
class DbTable:
    name: str
    file: str
    columns: list[str] = field(default_factory=list)
    relations: list[str] = field(default_factory=list)
    orm: str = ""


def scan_db_schema(project_path: str | Path) -> list[DbTable]:
    root = Path(project_path).resolve()
    tables: list[DbTable] = []

    for fpath in _walk_code_files(root, extensions={".ts", ".js", ".prisma"}):
        try:
            text = fpath.read_text(encoding="utf-8", errors="ignore")
            rel = str(fpath.relative_to(root))

            for m in DRIZZLE_TABLE_PATTERN.finditer(text):
                table_name = m.group(1)
                start = m.end()
                block_end = _find_block_end(text, start)
                block = text[start:block_end]

                cols = [cm.group(1) for cm in DRIZZLE_COLUMN_PATTERN.finditer(block)]
                tables = [*tables, DbTable(
                    name=table_name, file=rel, columns=cols, orm="drizzle",
                )]

            for m in PRISMA_MODEL_PATTERN.finditer(text):
                model_name = m.group(1)
                start = m.end()
                block_end = text.find("}", start)
                block = text[start:block_end] if block_end > 0 else ""

                fields = [fm.group(1) for fm in PRISMA_FIELD_PATTERN.finditer(block)
                          if fm.group(1) not in ("@@", "//")]
                tables = [*tables, DbTable(
                    name=model_name, file=rel, columns=fields, orm="prisma",
                )]
        except OSError:
            continue

    for fpath in _walk_code_files(root, extensions={".ts", ".js"}):
        try:
            text = fpath.read_text(encoding="utf-8", errors="ignore")
            rel = str(fpath.relative_to(root))
            for m in DRIZZLE_RELATION_PATTERN.finditer(text):
                ref_table = m.group(1)
                for i, t in enumerate(tables):
                    if t.name == ref_table or rel == t.file:
                        tables = [
                            *tables[:i],
                            DbTable(
                                name=t.name, file=t.file, columns=t.columns,
                                relations=[*t.relations, ref_table], orm=t.orm,
                            ),
                            *tables[i + 1:],
                        ]
                        break
        except OSError:
            continue

    return tables


# -- Component Tree --

REACT_COMPONENT_PATTERN = re.compile(
    r"""export\s+(?:default\s+)?(?:function|const)\s+([A-Z]\w+)""",
)
VUE_COMPONENT_PATTERN = re.compile(r"""<script[^>]*>\s*""")


@dataclass(frozen=True)
class ComponentInfo:
    name: str
    file: str
    is_default: bool = False


def scan_components(project_path: str | Path) -> list[ComponentInfo]:
    root = Path(project_path).resolve()
    components: list[ComponentInfo] = []

    for fpath in _walk_code_files(root, extensions={".tsx", ".jsx", ".vue", ".svelte"}):
        try:
            text = fpath.read_text(encoding="utf-8", errors="ignore")
            rel = str(fpath.relative_to(root))

            for m in REACT_COMPONENT_PATTERN.finditer(text):
                is_default = "default" in text[max(0, m.start() - 20):m.start()]
                components = [*components, ComponentInfo(
                    name=m.group(1), file=rel, is_default=is_default,
                )]

            if fpath.suffix == ".vue" and "<template" in text:
                name = fpath.stem
                if name[0].isupper() or "-" in name:
                    components = [*components, ComponentInfo(
                        name=name, file=rel, is_default=True,
                    )]
        except OSError:
            continue

    return components


# -- Test Examples --

@dataclass(frozen=True)
class TestExample:
    file: str
    framework: str
    line_count: int
    snippet: str


def scan_test_examples(project_path: str | Path, max_examples: int = 3) -> list[TestExample]:
    root = Path(project_path).resolve()
    test_files: list[tuple[Path, str, int]] = []

    test_patterns = {
        "vitest": re.compile(r"""(?:describe|it|test)\s*\("""),
        "jest": re.compile(r"""(?:describe|it|test)\s*\("""),
        "pytest": re.compile(r"""^(?:def test_|class Test)""", re.M),
        "go_test": re.compile(r"""^func Test\w+\(t \*testing\.T\)""", re.M),
    }

    for fpath in _walk_code_files(root, extensions={".ts", ".tsx", ".js", ".jsx", ".py", ".go"}):
        name = fpath.name
        is_test = (
            name.endswith(".test.ts") or name.endswith(".test.tsx") or
            name.endswith(".test.js") or name.endswith(".spec.ts") or
            name.endswith(".spec.js") or name.startswith("test_") or
            name.endswith("_test.go") or name.endswith("_test.py")
        )
        if not is_test:
            continue
        try:
            text = fpath.read_text(encoding="utf-8", errors="ignore")
            lines = len(text.splitlines())
            if lines < 5 or lines > 300:
                continue

            fw = "unknown"
            for fw_name, pattern in test_patterns.items():
                if pattern.search(text):
                    fw = fw_name
                    break

            test_files = [*test_files, (fpath, fw, lines)]
        except OSError:
            continue

    test_files.sort(key=lambda x: x[2])
    selected = test_files[:max_examples]

    examples: list[TestExample] = []
    for fpath, fw, lines in selected:
        text = fpath.read_text(encoding="utf-8", errors="ignore")
        snippet_lines = text.splitlines()[:50]
        examples = [*examples, TestExample(
            file=str(fpath.relative_to(root)),
            framework=fw,
            line_count=lines,
            snippet="\n".join(snippet_lines),
        )]
    return examples


# -- Style Exemplars --

FILE_TYPE_PATTERNS = {
    "router": [re.compile(r"createTRPCRouter|router\.(get|post)"), "API router"],
    "repository": [re.compile(r"\.findFirst|\.findMany|\.insert|\.update|\.delete|SELECT|INSERT"), "Database repository"],
    "component": [re.compile(r"export\s+(?:default\s+)?function\s+[A-Z].*return\s*\("), "React component"],
    "hook": [re.compile(r"export\s+(?:function|const)\s+use[A-Z]"), "Custom hook"],
    "util": [re.compile(r"export\s+(?:function|const)\s+[a-z]"), "Utility module"],
    "middleware": [re.compile(r"middleware|next\(\)|req,\s*res"), "Middleware"],
    "schema": [re.compile(r"pgTable|mysqlTable|z\.object|z\.string"), "Schema/validation"],
}


@dataclass(frozen=True)
class StyleExemplar:
    file_type: str
    description: str
    file: str
    line_count: int
    preview: str


def scan_style_exemplars(project_path: str | Path) -> list[StyleExemplar]:
    root = Path(project_path).resolve()
    exemplars: list[StyleExemplar] = []
    found_types: set[str] = set()

    candidates: dict[str, list[tuple[Path, int]]] = {t: [] for t in FILE_TYPE_PATTERNS}

    for fpath in _walk_code_files(root, extensions={".ts", ".tsx", ".js", ".jsx", ".py", ".go"}):
        try:
            text = fpath.read_text(encoding="utf-8", errors="ignore")
            lines = len(text.splitlines())
            if lines < 15 or lines > 200:
                continue

            for file_type, (pattern, _desc) in FILE_TYPE_PATTERNS.items():
                if pattern.search(text):
                    candidates[file_type] = [*candidates[file_type], (fpath, lines)]
        except OSError:
            continue

    for file_type, (pattern, desc) in FILE_TYPE_PATTERNS.items():
        type_candidates = candidates[file_type]
        if not type_candidates:
            continue

        type_candidates.sort(key=lambda x: x[1])
        median_idx = len(type_candidates) // 2
        best = type_candidates[median_idx]

        text = best[0].read_text(encoding="utf-8", errors="ignore")
        preview_lines = text.splitlines()[:30]

        exemplars = [*exemplars, StyleExemplar(
            file_type=file_type,
            description=desc,
            file=str(best[0].relative_to(root)),
            line_count=best[1],
            preview="\n".join(preview_lines),
        )]
        found_types = {*found_types, file_type}

    return exemplars


# -- PR Checklist --

def generate_pr_checklist(
    *,
    has_linter: bool,
    has_tests: bool,
    has_ci: bool,
    has_i18n: bool = False,
    has_migrations: bool = False,
    lint_command: str = "",
    test_command: str = "",
) -> str:
    items: list[str] = []
    items.append("- [ ] Code compiles without errors")

    if has_linter:
        cmd = f" (`{lint_command}`)" if lint_command else ""
        items.append(f"- [ ] Linter passes{cmd}")

    if has_tests:
        cmd = f" (`{test_command}`)" if test_command else ""
        items.append(f"- [ ] Tests pass{cmd}")
        items.append("- [ ] New/changed code has test coverage")

    items.append("- [ ] No hardcoded secrets or credentials")
    items.append("- [ ] No console.log / print statements left in code")
    items.append("- [ ] Code follows project constitution (.gg/constitution.md)")

    if has_i18n:
        items.append("- [ ] New user-facing strings are internationalized")

    if has_migrations:
        items.append("- [ ] Database migration created if schema changed")
        items.append("- [ ] Migration tested on development database")

    if has_ci:
        items.append("- [ ] CI pipeline passes")

    items.append("- [ ] PR description explains WHY, not just WHAT")
    items.append("- [ ] Self-review completed before requesting review")

    return "\n".join(items)


# -- Helpers --

def _walk_code_files(root: Path, extensions: set[str]) -> list[Path]:
    files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if os.path.splitext(fname)[1].lower() in extensions:
                files = [*files, Path(dirpath) / fname]
    return files


def _find_block_end(text: str, start: int) -> int:
    depth = 0
    for i in range(start, min(start + 5000, len(text))):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            if depth <= 0:
                return i
            depth -= 1
    return min(start + 2000, len(text))
