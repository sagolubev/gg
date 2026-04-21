from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Contributor:
    name: str
    email: str
    commits: int
    last_active: str


@dataclass(frozen=True)
class FileOwnership:
    path: str
    primary_owner: str
    ownership_pct: float
    contributors: list[tuple[str, int]] = field(default_factory=list)


@dataclass(frozen=True)
class ChurnInfo:
    path: str
    change_count: int
    lines_added: int
    lines_removed: int
    churn_ratio: float  # added+removed / total changes -- high = volatile


@dataclass(frozen=True)
class ArchitecturalCommit:
    sha: str
    message: str
    date: str
    files_changed: int
    commit_type: str  # "refactor", "dependency_change", "config_change", "large_feature"


@dataclass(frozen=True)
class DependencyChange:
    date: str
    sha: str
    message: str
    file: str  # package.json, requirements.txt, etc.
    action: str  # "added", "removed", "modified"


@dataclass(frozen=True)
class GitProfile:
    total_commits: int = 0
    first_commit_date: str = ""
    last_commit_date: str = ""
    contributors: list[Contributor] = field(default_factory=list)
    hot_files: list[tuple[str, int]] = field(default_factory=list)
    coupled_files: list[tuple[str, str, float]] = field(default_factory=list)
    commit_style: dict[str, str | int | float] = field(default_factory=dict)
    monthly_activity: dict[str, int] = field(default_factory=dict)
    active_branches: list[str] = field(default_factory=list)
    branch_patterns: dict[str, int] = field(default_factory=dict)
    # New deep analysis fields
    file_ownership: list[FileOwnership] = field(default_factory=list)
    churn_analysis: list[ChurnInfo] = field(default_factory=list)
    dormant_files: list[tuple[str, str]] = field(default_factory=list)  # (path, last_changed_date)
    architectural_commits: list[ArchitecturalCommit] = field(default_factory=list)
    dependency_changes: list[DependencyChange] = field(default_factory=list)
    bus_factor: dict[str, int] = field(default_factory=dict)  # module -> unique contributors
    feature_velocity: dict[str, dict[str, int]] = field(default_factory=dict)  # month -> {feat: N, fix: N}
    work_patterns: dict[str, int] = field(default_factory=dict)  # hour_of_day -> commit count
    risk_scores: list[tuple[str, float]] = field(default_factory=list)  # (path, risk_score)

    def to_prompt_context(self) -> str:
        lines = ["## Git History"]
        lines.append(f"Total commits: {self.total_commits}")
        if self.first_commit_date:
            lines.append(f"History: {self.first_commit_date} -- {self.last_commit_date}")

        if self.contributors:
            lines.append("Top contributors:")
            for c in self.contributors[:10]:
                lines.append(f"  - {c.name}: {c.commits} commits (last: {c.last_active})")

        if self.hot_files:
            lines.append("Most changed files:")
            for path, count in self.hot_files[:15]:
                lines.append(f"  - {path}: {count} changes")

        if self.coupled_files:
            lines.append("Frequently co-changed files:")
            for f1, f2, score in self.coupled_files[:10]:
                lines.append(f"  - {f1} <-> {f2} (Jaccard: {score:.2f})")

        if self.commit_style:
            lines.append("Commit style:")
            for key, val in self.commit_style.items():
                lines.append(f"  {key}: {val}")

        if self.branch_patterns:
            lines.append("Branch naming patterns:")
            for pattern, count in self.branch_patterns.items():
                lines.append(f"  - {pattern}: {count}")

        if self.file_ownership:
            lines.append("Code ownership (top modules):")
            seen_modules: set[str] = set()
            for fo in self.file_ownership:
                module = fo.path.split("/")[0] if "/" in fo.path else fo.path
                if module not in seen_modules:
                    seen_modules = {*seen_modules, module}
                    lines.append(f"  - {fo.path}: {fo.primary_owner} ({fo.ownership_pct:.0f}%)")
                if len(seen_modules) >= 10:
                    break

        if self.bus_factor:
            low_bus = [(m, n) for m, n in self.bus_factor.items() if n <= 1]
            if low_bus:
                lines.append("Bus factor risk (single contributor):")
                for m, n in low_bus[:5]:
                    lines.append(f"  - {m}: {n} contributor(s)")

        if self.risk_scores:
            lines.append("High-risk files (churn + coupling):")
            for path, score in self.risk_scores[:10]:
                lines.append(f"  - {path}: risk={score:.2f}")

        if self.architectural_commits:
            lines.append("Architectural changes:")
            for ac in self.architectural_commits[:5]:
                lines.append(f"  - [{ac.date}] {ac.commit_type}: {ac.message[:60]}")

        if self.dependency_changes:
            lines.append("Dependency history:")
            for dc in self.dependency_changes[:10]:
                lines.append(f"  - [{dc.date}] {dc.action} in {dc.file}: {dc.message[:50]}")

        if self.feature_velocity:
            recent = list(self.feature_velocity.items())[-3:]
            if recent:
                lines.append("Recent feature velocity:")
                for month, counts in recent:
                    feat = counts.get("feat", 0)
                    fix = counts.get("fix", 0)
                    lines.append(f"  - {month}: {feat} features, {fix} fixes")

        return "\n".join(lines)


CONVENTIONAL_RE = re.compile(
    r"^(feat|fix|refactor|docs|test|chore|perf|ci|style|build|revert)(\(.+?\))?!?:\s"
)

NOISE_PATTERNS = {
    "messages.ts", "messages.js", "messages.json", "messages.po",
    "pnpm-lock.yaml", "package-lock.json", "yarn.lock", "bun.lockb",
    "poetry.lock", "Pipfile.lock", "Cargo.lock", "Gemfile.lock", "go.sum",
    "i18n.lock",
}

NOISE_DIRS = {"locales", "locale", "translations", "i18n", "__pycache__", "node_modules"}


def _is_noise_file(path: str) -> bool:
    parts = Path(path).parts
    name = Path(path).name
    if name in NOISE_PATTERNS:
        return True
    if any(p in NOISE_DIRS for p in parts):
        return True
    if name.endswith(".lock"):
        return True
    return False


DEP_FILES = {
    "package.json", "package-lock.json", "yarn.lock", "pnpm-lock.yaml",
    "requirements.txt", "pyproject.toml", "poetry.lock", "Pipfile", "Pipfile.lock",
    "go.mod", "go.sum", "Cargo.toml", "Cargo.lock", "Gemfile", "Gemfile.lock",
    "pubspec.yaml", "pubspec.lock",
}

CONFIG_FILES = {
    "Dockerfile", "docker-compose.yml", "docker-compose.yaml",
    ".github/workflows", ".gitlab-ci.yml", ".circleci",
    "Makefile", "Justfile", "Taskfile.yml",
    "nginx.conf", "terraform", ".env.example",
}


def analyze_git_history(project_path: str | Path, max_commits: int = 500) -> GitProfile:
    root = Path(project_path).resolve()

    try:
        from git import Repo
        repo = Repo(root)
    except Exception:
        return GitProfile()

    if repo.bare or not repo.head.is_valid():
        return GitProfile()

    commits = list(repo.iter_commits(max_count=max_commits))
    if not commits:
        return GitProfile()

    # Existing analysis
    contributors = _extract_contributors(commits)
    hot_files = _extract_hot_files(commits)
    coupled_files = _extract_coupled_files(commits)
    commit_style = _analyze_commit_style(commits)
    monthly = _monthly_activity(commits)
    branches, branch_patterns = _analyze_branches(repo)

    # New deep analysis
    commit_file_map = _build_commit_file_map(commits)
    file_ownership = _analyze_file_ownership(commits, commit_file_map)
    churn = _analyze_churn(commits)
    dormant = _find_dormant_files(commit_file_map, commits[0].committed_datetime)
    arch_commits = _find_architectural_commits(commits, commit_file_map)
    dep_changes = _find_dependency_changes(commits, commit_file_map)
    bus_factor = _calculate_bus_factor(commits, commit_file_map)
    velocity = _feature_velocity(commits)
    work_patterns = _work_patterns(commits)
    risk_scores = _calculate_risk_scores(hot_files, coupled_files, bus_factor, churn)

    first_date = commits[-1].committed_datetime.strftime("%Y-%m-%d")
    last_date = commits[0].committed_datetime.strftime("%Y-%m-%d")

    return GitProfile(
        total_commits=len(commits),
        first_commit_date=first_date,
        last_commit_date=last_date,
        contributors=contributors,
        hot_files=hot_files,
        coupled_files=coupled_files,
        commit_style=commit_style,
        monthly_activity=monthly,
        active_branches=branches,
        branch_patterns=branch_patterns,
        file_ownership=file_ownership,
        churn_analysis=churn,
        dormant_files=dormant,
        architectural_commits=arch_commits,
        dependency_changes=dep_changes,
        bus_factor=bus_factor,
        feature_velocity=velocity,
        work_patterns=work_patterns,
        risk_scores=risk_scores,
    )


# -- Existing helpers (unchanged) --

def _extract_contributors(commits: list) -> list[Contributor]:
    author_data: dict[str, dict] = {}
    for c in commits:
        key = c.author.email or c.author.name
        if key not in author_data:
            author_data[key] = {
                "name": c.author.name,
                "email": c.author.email or "",
                "commits": 0,
                "last_active": c.committed_datetime.strftime("%Y-%m-%d"),
            }
        author_data[key]["commits"] += 1

    result = [
        Contributor(
            name=d["name"], email=d["email"],
            commits=d["commits"], last_active=d["last_active"],
        )
        for d in author_data.values()
    ]
    return sorted(result, key=lambda c: -c.commits)


def _extract_hot_files(commits: list, top_n: int = 20) -> list[tuple[str, int]]:
    file_counts: Counter[str] = Counter()
    for c in commits:
        try:
            if c.parents:
                diff = c.diff(c.parents[0])
            else:
                diff = c.diff(None)
            for d in diff:
                path = d.b_path or d.a_path
                if path and not _is_noise_file(path):
                    file_counts[path] += 1
        except Exception:
            continue
    return file_counts.most_common(top_n)


def _extract_coupled_files(
    commits: list, min_jaccard: float = 0.3, top_n: int = 10,
) -> list[tuple[str, str, float]]:
    file_sets: dict[str, set[int]] = {}

    for idx, c in enumerate(commits):
        try:
            if c.parents:
                diff = c.diff(c.parents[0])
            else:
                diff = c.diff(None)
            files_in_commit = {d.b_path or d.a_path for d in diff if (d.b_path or d.a_path)}
            for f in files_in_commit:
                if f not in file_sets:
                    file_sets[f] = set()
                file_sets[f] = {*file_sets[f], idx}
        except Exception:
            continue

    frequent = {f: s for f, s in file_sets.items() if len(s) >= 3}
    files_list = list(frequent.keys())
    pairs: list[tuple[str, str, float]] = []

    for i in range(len(files_list)):
        for j in range(i + 1, len(files_list)):
            f1, f2 = files_list[i], files_list[j]
            s1, s2 = frequent[f1], frequent[f2]
            intersection = len(s1 & s2)
            union = len(s1 | s2)
            if union > 0:
                jaccard = intersection / union
                if jaccard >= min_jaccard:
                    pairs = [*pairs, (f1, f2, jaccard)]

    return sorted(pairs, key=lambda x: -x[2])[:top_n]


def _analyze_commit_style(commits: list) -> dict[str, str | int | float]:
    total = len(commits)
    conventional_count = sum(1 for c in commits if CONVENTIONAL_RE.match(c.message))
    msg_lengths = [len(c.message.split("\n")[0]) for c in commits]

    type_counts: Counter[str] = Counter()
    for c in commits:
        m = CONVENTIONAL_RE.match(c.message)
        if m:
            type_counts[m.group(1)] += 1

    style: dict[str, str | int | float] = {
        "conventional_commits_pct": round(conventional_count / total * 100, 1) if total else 0,
        "avg_message_length": round(sum(msg_lengths) / len(msg_lengths), 1) if msg_lengths else 0,
    }

    if type_counts:
        top_types = ", ".join(f"{t}({n})" for t, n in type_counts.most_common(5))
        style["top_types"] = top_types

    return style


def _monthly_activity(commits: list) -> dict[str, int]:
    monthly: dict[str, int] = {}
    for c in commits:
        key = c.committed_datetime.strftime("%Y-%m")
        monthly = {**monthly, key: monthly.get(key, 0) + 1}
    return dict(sorted(monthly.items()))


def _analyze_branches(repo) -> tuple[list[str], dict[str, int]]:
    branches: list[str] = []
    patterns: dict[str, int] = {}

    try:
        for ref in repo.references:
            name = ref.name
            if name.startswith("origin/"):
                name = name[7:]
            if name not in branches and name != "HEAD":
                branches = [*branches, name]

            for prefix in ("feature/", "fix/", "bugfix/", "hotfix/", "release/", "chore/", "robot/"):
                if name.startswith(prefix):
                    key = prefix.rstrip("/")
                    patterns = {**patterns, key: patterns.get(key, 0) + 1}
                    break
    except Exception:
        pass

    return branches[:20], patterns


# -- New deep analysis helpers --

def _build_commit_file_map(commits: list) -> dict[str, list[tuple[str, str, str]]]:
    """Map each file to list of (commit_sha, author_name, date)."""
    file_map: dict[str, list[tuple[str, str, str]]] = {}
    for c in commits:
        try:
            if c.parents:
                diff = c.diff(c.parents[0])
            else:
                diff = c.diff(None)
            author = c.author.name
            date = c.committed_datetime.strftime("%Y-%m-%d")
            sha = c.hexsha[:8]
            for d in diff:
                path = d.b_path or d.a_path
                if path:
                    if path not in file_map:
                        file_map[path] = []
                    file_map[path] = [*file_map[path], (sha, author, date)]
        except Exception:
            continue
    return file_map


def _analyze_file_ownership(
    commits: list, file_map: dict[str, list[tuple[str, str, str]]], top_n: int = 30,
) -> list[FileOwnership]:
    results: list[FileOwnership] = []
    sorted_files = sorted(file_map.items(), key=lambda x: -len(x[1]))[:top_n]

    for path, entries in sorted_files:
        author_counts: Counter[str] = Counter()
        for _, author, _ in entries:
            author_counts[author] += 1
        total = sum(author_counts.values())
        if total == 0:
            continue
        top_author, top_count = author_counts.most_common(1)[0]
        results = [
            *results,
            FileOwnership(
                path=path,
                primary_owner=top_author,
                ownership_pct=round(top_count / total * 100, 1),
                contributors=author_counts.most_common(5),
            ),
        ]
    return results


def _analyze_churn(commits: list, top_n: int = 20) -> list[ChurnInfo]:
    file_stats: dict[str, dict[str, int]] = {}  # path -> {changes, added, removed}

    for c in commits:
        try:
            if c.parents:
                diff = c.diff(c.parents[0])
            else:
                diff = c.diff(None)
            for d in diff:
                path = d.b_path or d.a_path
                if not path:
                    continue
                if path not in file_stats:
                    file_stats[path] = {"changes": 0, "added": 0, "removed": 0}

                stats = {**file_stats[path]}
                stats["changes"] = stats["changes"] + 1

                try:
                    if d.diff:
                        diff_text = d.diff.decode("utf-8", errors="ignore") if isinstance(d.diff, bytes) else d.diff
                        for line in diff_text.splitlines():
                            if line.startswith("+") and not line.startswith("+++"):
                                stats["added"] = stats["added"] + 1
                            elif line.startswith("-") and not line.startswith("---"):
                                stats["removed"] = stats["removed"] + 1
                except Exception:
                    pass

                file_stats[path] = stats
        except Exception:
            continue

    results: list[ChurnInfo] = []
    for path, stats in file_stats.items():
        total_lines = stats["added"] + stats["removed"]
        churn_ratio = total_lines / max(stats["changes"], 1)
        results = [
            *results,
            ChurnInfo(
                path=path,
                change_count=stats["changes"],
                lines_added=stats["added"],
                lines_removed=stats["removed"],
                churn_ratio=round(churn_ratio, 1),
            ),
        ]

    return sorted(results, key=lambda x: -(x.change_count * x.churn_ratio))[:top_n]


def _find_dormant_files(
    file_map: dict[str, list[tuple[str, str, str]]],
    latest_date,
    dormant_days: int = 180,
) -> list[tuple[str, str]]:
    from datetime import timedelta

    cutoff = (latest_date - timedelta(days=dormant_days)).strftime("%Y-%m-%d")
    dormant: list[tuple[str, str]] = []

    for path, entries in file_map.items():
        last_date = entries[0][2]  # entries are in commit order (newest first)
        if last_date < cutoff:
            dormant = [*dormant, (path, last_date)]

    return sorted(dormant, key=lambda x: x[1])[:30]


def _find_architectural_commits(
    commits: list, file_map: dict[str, list[tuple[str, str, str]]], min_files: int = 8,
) -> list[ArchitecturalCommit]:
    results: list[ArchitecturalCommit] = []

    for c in commits:
        try:
            if c.parents:
                diff = c.diff(c.parents[0])
            else:
                diff = c.diff(None)
            files = [d.b_path or d.a_path for d in diff if (d.b_path or d.a_path)]
            n_files = len(files)

            msg = c.message.split("\n")[0]
            date = c.committed_datetime.strftime("%Y-%m-%d")
            sha = c.hexsha[:8]

            commit_type = ""
            m = CONVENTIONAL_RE.match(msg)

            if m and m.group(1) == "refactor":
                commit_type = "refactor"
            elif "!" in msg.split(":")[0] if ":" in msg else False:
                commit_type = "breaking_change"
            elif n_files >= min_files:
                commit_type = "large_change"

            dep_touched = any(Path(f).name in DEP_FILES for f in files)
            if dep_touched and not commit_type:
                commit_type = "dependency_change"

            config_touched = any(
                any(cf in f for cf in CONFIG_FILES)
                for f in files
            )
            if config_touched and not commit_type:
                commit_type = "config_change"

            renames = sum(1 for d in diff if d.renamed_file)
            if renames >= 3 and not commit_type:
                commit_type = "restructuring"

            if commit_type:
                results = [
                    *results,
                    ArchitecturalCommit(
                        sha=sha, message=msg, date=date,
                        files_changed=n_files, commit_type=commit_type,
                    ),
                ]
        except Exception:
            continue

    return results[:20]


def _find_dependency_changes(
    commits: list, file_map: dict[str, list[tuple[str, str, str]]],
) -> list[DependencyChange]:
    results: list[DependencyChange] = []

    for c in commits:
        try:
            if c.parents:
                diff = c.diff(c.parents[0])
            else:
                diff = c.diff(None)

            msg = c.message.split("\n")[0]
            date = c.committed_datetime.strftime("%Y-%m-%d")
            sha = c.hexsha[:8]

            for d in diff:
                path = d.b_path or d.a_path
                if path and Path(path).name in DEP_FILES:
                    if d.new_file:
                        action = "added"
                    elif d.deleted_file:
                        action = "removed"
                    else:
                        action = "modified"
                    results = [
                        *results,
                        DependencyChange(
                            date=date, sha=sha, message=msg,
                            file=path, action=action,
                        ),
                    ]
        except Exception:
            continue

    return results[:30]


def _calculate_bus_factor(
    commits: list, file_map: dict[str, list[tuple[str, str, str]]],
) -> dict[str, int]:
    module_authors: dict[str, set[str]] = {}

    for path, entries in file_map.items():
        parts = Path(path).parts
        if len(parts) < 2:
            continue
        module = parts[0]
        if module.startswith(".") or _is_noise_file(path):
            continue
        if module not in module_authors:
            module_authors[module] = set()
        for _, author, _ in entries:
            module_authors[module] = {*module_authors[module], author}

    return {m: len(authors) for m, authors in sorted(module_authors.items())}


def _feature_velocity(commits: list) -> dict[str, dict[str, int]]:
    velocity: dict[str, dict[str, int]] = {}

    for c in commits:
        month = c.committed_datetime.strftime("%Y-%m")
        m = CONVENTIONAL_RE.match(c.message)
        if not m:
            continue
        ctype = m.group(1)
        if month not in velocity:
            velocity[month] = {}
        current = velocity[month]
        velocity[month] = {**current, ctype: current.get(ctype, 0) + 1}

    return dict(sorted(velocity.items()))


def _work_patterns(commits: list) -> dict[str, int]:
    hours: dict[str, int] = {}
    for c in commits:
        hour = str(c.committed_datetime.hour).zfill(2)
        hours = {**hours, hour: hours.get(hour, 0) + 1}
    return dict(sorted(hours.items()))


def _calculate_risk_scores(
    hot_files: list[tuple[str, int]],
    coupled_files: list[tuple[str, str, float]],
    bus_factor: dict[str, int],
    churn: list[ChurnInfo],
    top_n: int = 15,
) -> list[tuple[str, float]]:
    scores: dict[str, float] = {}

    hot_max = max((c for _, c in hot_files), default=1)
    for path, count in hot_files:
        scores[path] = scores.get(path, 0) + (count / hot_max) * 3.0

    for f1, f2, jaccard in coupled_files:
        scores[f1] = scores.get(f1, 0) + jaccard * 2.0
        scores[f2] = scores.get(f2, 0) + jaccard * 2.0

    for ci in churn:
        scores[ci.path] = scores.get(ci.path, 0) + min(ci.churn_ratio / 10.0, 2.0)

    for path in scores:
        module = Path(path).parts[0] if len(Path(path).parts) >= 2 else path
        bf = bus_factor.get(module, 1)
        if bf <= 1:
            scores[path] = scores[path] * 1.5

    return sorted(scores.items(), key=lambda x: -x[1])[:top_n]
