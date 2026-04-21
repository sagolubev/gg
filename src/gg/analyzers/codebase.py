"""Fast codebase analysis without AST or LLM.

Extracts project context using regex, file parsing, and heuristics:
- README parsing for description
- Import graph (regex-based)
- TODO/FIXME/HACK markers
- API route detection
- Environment variable scanning
- Doc file freshness
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

SKIP_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv", "env",
    ".tox", ".mypy_cache", ".pytest_cache", "dist", "build",
    ".next", ".nuxt", "target", "vendor", ".gg", "openspec",
    ".idea", ".vscode", ".eggs",
}

ROUTE_PATTERNS = [
    re.compile(r"""@(?:app|router|api)\.(get|post|put|delete|patch)\s*\(\s*['"](/[^'"]+)""", re.I),
    re.compile(r"""router\.(get|post|put|delete|patch)\s*\(\s*['"](/[^'"]+)""", re.I),
    re.compile(r"""@(?:Get|Post|Put|Delete|Patch)Mapping\s*\(\s*['"](/[^'"]+)"""),
    re.compile(r"""@(?:RequestMapping|GetMapping|PostMapping)\s*\(.*?value\s*=\s*['"](/[^'"]+)"""),
    re.compile(r"""app\.(get|post|put|delete|patch)\s*\(\s*['"](/[^'"]+)""", re.I),
]

ENV_PATTERNS = [
    re.compile(r"""process\.env\.([A-Z_][A-Z0-9_]+)"""),
    re.compile(r"""os\.environ(?:\.get)?\s*\(\s*['"]([A-Z_][A-Z0-9_]+)"""),
    re.compile(r"""os\.getenv\s*\(\s*['"]([A-Z_][A-Z0-9_]+)"""),
    re.compile(r"""env\s*\(\s*['"]([A-Z_][A-Z0-9_]+)"""),
    re.compile(r"""ENV\s*\[\s*['"]([A-Z_][A-Z0-9_]+)"""),
]

TODO_PATTERN = re.compile(r"""#\s*(TODO|FIXME|HACK|XXX|BUG|OPTIMIZE)\b[:\s]*(.*)""", re.I)

IMPORT_PATTERNS = [
    re.compile(r"""^import\s+.+?\s+from\s+['"]([^'"./][^'"]+)""", re.M),
    re.compile(r"""^from\s+([a-zA-Z][a-zA-Z0-9_]+)\s+import""", re.M),
    re.compile(r"""(?:require|import)\s*\(\s*['"]([^'"./][^'"]+)"""),
]

STDLIB_MODULES = {
    "__future__", "abc", "ast", "asyncio", "base64", "collections", "contextlib",
    "copy", "csv", "dataclasses", "datetime", "enum", "functools", "hashlib",
    "http", "importlib", "inspect", "io", "itertools", "json", "logging",
    "math", "operator", "os", "pathlib", "pickle", "platform", "pprint",
    "random", "re", "shutil", "signal", "socket", "sqlite3", "string",
    "struct", "subprocess", "sys", "tempfile", "textwrap", "threading",
    "time", "tomllib", "typing", "unittest", "urllib", "uuid", "warnings",
    "xml", "zipfile",
}


def analyze_codebase(project_path: str | Path) -> dict[str, str]:
    """Fast local analysis. Returns description, domains, integrations."""
    root = Path(project_path).resolve()

    description = _extract_description(root)
    domains = _detect_domains(root)
    integrations = _detect_integrations(root)
    todos = scan_todos(root)
    routes = scan_routes(root)
    env_vars = scan_env_vars(root)
    imports = scan_imports(root)

    if not description:
        parts = []
        if domains:
            parts.append(f"Modules: {domains}")
        if integrations:
            parts.append(f"Integrations: {integrations}")
        description = "; ".join(parts) if parts else root.name

    return {
        "description": description,
        "domains": domains,
        "integrations": integrations,
        "todos": todos,
        "routes": routes,
        "env_vars": env_vars,
        "imports": imports,
    }


def _extract_description(root: Path) -> str:
    """Extract project description from README or package files."""
    for readme_name in ("README.md", "README.rst", "README.txt", "README"):
        readme = root / readme_name
        if readme.exists():
            text = readme.read_text(encoding="utf-8", errors="ignore")
            lines = text.strip().splitlines()
            desc_lines: list[str] = []
            past_title = False
            for line in lines:
                if line.startswith("#") and not past_title:
                    past_title = True
                    continue
                if past_title and line.strip():
                    if line.startswith("#") or line.startswith("```") or line.startswith("["):
                        break
                    cleaned = _strip_markdown(line.strip())
                    if cleaned and len(cleaned) > 10:
                        desc_lines = [*desc_lines, cleaned]
                        if len(desc_lines) >= 2:
                            break
            if desc_lines:
                return " ".join(desc_lines)[:200]

    pkg_json = root / "package.json"
    if pkg_json.exists():
        try:
            data = json.loads(pkg_json.read_text())
            if data.get("description"):
                return data["description"][:200]
        except (json.JSONDecodeError, OSError):
            pass

    pyproject = root / "pyproject.toml"
    if pyproject.exists():
        try:
            text = pyproject.read_text()
            m = re.search(r'description\s*=\s*"([^"]+)"', text)
            if m:
                return m.group(1)[:200]
        except OSError:
            pass

    return ""


def _detect_domains(root: Path) -> str:
    """Detect main modules/domains from directory structure."""
    domains: list[str] = []

    src_dirs = [root / "src", root / "lib", root / "app", root / "packages", root / "apps"]
    for src in src_dirs:
        if src.is_dir():
            for child in sorted(src.iterdir()):
                if child.is_dir() and child.name not in SKIP_DIRS and not child.name.startswith("."):
                    domains = [*domains, child.name]

    if not domains:
        for child in sorted(root.iterdir()):
            if child.is_dir() and child.name not in SKIP_DIRS and not child.name.startswith("."):
                has_code = any(child.rglob("*.py")) or any(child.rglob("*.ts")) or any(child.rglob("*.js")) or any(child.rglob("*.go")) or any(child.rglob("*.rs"))
                if has_code:
                    domains = [*domains, child.name]

    return ", ".join(domains[:10])


def _detect_integrations(root: Path) -> str:
    """Detect external integrations from config files and imports."""
    integrations: set[str] = set()

    if (root / "docker-compose.yml").exists() or (root / "docker-compose.yaml").exists():
        integrations = {*integrations, "Docker Compose"}
        dc_path = root / "docker-compose.yml" if (root / "docker-compose.yml").exists() else root / "docker-compose.yaml"
        try:
            text = dc_path.read_text()
            if "postgres" in text.lower():
                integrations = {*integrations, "PostgreSQL"}
            if "redis" in text.lower():
                integrations = {*integrations, "Redis"}
            if "mongo" in text.lower():
                integrations = {*integrations, "MongoDB"}
            if "rabbit" in text.lower():
                integrations = {*integrations, "RabbitMQ"}
            if "kafka" in text.lower():
                integrations = {*integrations, "Kafka"}
            if "elastic" in text.lower():
                integrations = {*integrations, "Elasticsearch"}
            if "mysql" in text.lower():
                integrations = {*integrations, "MySQL"}
        except OSError:
            pass

    if (root / "Dockerfile").exists():
        integrations = {*integrations, "Docker"}

    for env_file in (".env.example", ".env.sample", ".env"):
        path = root / env_file
        if path.exists():
            try:
                text = path.read_text(errors="ignore")
                if "DATABASE_URL" in text or "DB_HOST" in text:
                    integrations = {*integrations, "Database"}
                if "REDIS" in text:
                    integrations = {*integrations, "Redis"}
                if "AWS" in text or "S3" in text:
                    integrations = {*integrations, "AWS"}
                if "STRIPE" in text:
                    integrations = {*integrations, "Stripe"}
                if "SENDGRID" in text or "SMTP" in text or "MAIL" in text:
                    integrations = {*integrations, "Email/SMTP"}
                if "SENTRY" in text:
                    integrations = {*integrations, "Sentry"}
            except OSError:
                pass

    return ", ".join(sorted(integrations)) if integrations else ""


def scan_todos(project_path: str | Path, max_files: int = 200) -> str:
    """Scan for TODO/FIXME/HACK markers in code."""
    root = Path(project_path).resolve()
    todos: list[str] = []
    files_scanned = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if not _is_code_file(fname):
                continue
            if files_scanned >= max_files:
                break
            files_scanned += 1
            fpath = Path(dirpath) / fname
            try:
                text = fpath.read_text(encoding="utf-8", errors="ignore")
                rel = str(fpath.relative_to(root))
                for i, line in enumerate(text.splitlines(), 1):
                    m = TODO_PATTERN.search(line)
                    if m:
                        marker = m.group(1).upper()
                        msg = m.group(2).strip()[:80]
                        todos = [*todos, f"{rel}:{i} {marker}: {msg}"]
            except OSError:
                continue

    return "\n".join(todos[:50])


def scan_routes(project_path: str | Path, max_files: int = 200) -> str:
    """Scan for API route definitions."""
    root = Path(project_path).resolve()
    routes: list[str] = []
    files_scanned = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if not _is_code_file(fname):
                continue
            if files_scanned >= max_files:
                break
            files_scanned += 1
            fpath = Path(dirpath) / fname
            try:
                text = fpath.read_text(encoding="utf-8", errors="ignore")
                rel = str(fpath.relative_to(root))
                for pattern in ROUTE_PATTERNS:
                    for m in pattern.finditer(text):
                        groups = m.groups()
                        if len(groups) == 2:
                            method, path = groups
                            routes = [*routes, f"{method.upper()} {path} ({rel})"]
                        elif len(groups) == 1:
                            routes = [*routes, f"* {groups[0]} ({rel})"]
            except OSError:
                continue

    return "\n".join(sorted(set(routes))[:50])


def scan_env_vars(project_path: str | Path, max_files: int = 200) -> str:
    """Scan for environment variable references in code."""
    root = Path(project_path).resolve()
    env_vars: dict[str, list[str]] = {}
    files_scanned = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if not _is_code_file(fname):
                continue
            if files_scanned >= max_files:
                break
            files_scanned += 1
            fpath = Path(dirpath) / fname
            try:
                text = fpath.read_text(encoding="utf-8", errors="ignore")
                rel = str(fpath.relative_to(root))
                for pattern in ENV_PATTERNS:
                    for m in pattern.finditer(text):
                        var = m.group(1)
                        if var not in env_vars:
                            env_vars[var] = []
                        if rel not in env_vars[var]:
                            env_vars[var] = [*env_vars[var], rel]
            except OSError:
                continue

    lines: list[str] = []
    for var, files in sorted(env_vars.items()):
        lines = [*lines, f"{var} (used in: {', '.join(files[:3])})"]
    return "\n".join(lines[:30])


def scan_imports(project_path: str | Path, max_files: int = 200) -> str:
    """Scan for external package imports to understand dependencies."""
    root = Path(project_path).resolve()
    external_imports: dict[str, int] = {}
    files_scanned = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fname in filenames:
            if not _is_code_file(fname):
                continue
            if files_scanned >= max_files:
                break
            files_scanned += 1
            fpath = Path(dirpath) / fname
            try:
                text = fpath.read_text(encoding="utf-8", errors="ignore")
                for pattern in IMPORT_PATTERNS:
                    for m in pattern.finditer(text):
                        pkg = m.group(1) if len(m.groups()) >= 1 else ""
                        if pkg and not pkg.startswith(".") and not pkg.startswith("/"):
                            pkg_root = pkg.split("/")[0].split(".")[0]
                            if pkg_root and len(pkg_root) > 1 and pkg_root not in STDLIB_MODULES:
                                external_imports = {
                                    **external_imports,
                                    pkg_root: external_imports.get(pkg_root, 0) + 1,
                                }
            except OSError:
                continue

    top = sorted(external_imports.items(), key=lambda x: -x[1])[:20]
    return ", ".join(f"{pkg}({n})" for pkg, n in top)


def _strip_markdown(text: str) -> str:
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"`(.+?)`", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", text)
    text = re.sub(r"[*_~`#>]", "", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _is_code_file(fname: str) -> bool:
    ext = os.path.splitext(fname)[1].lower()
    return ext in {
        ".py", ".js", ".ts", ".tsx", ".jsx", ".go", ".rs", ".rb",
        ".java", ".kt", ".swift", ".c", ".cpp", ".cs", ".php",
        ".scala", ".ex", ".exs", ".vue", ".svelte",
    }
