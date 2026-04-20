from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Contributor:
    name: str
    email: str
    commits: int
    last_active: str


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

        return "\n".join(lines)


CONVENTIONAL_RE = re.compile(
    r"^(feat|fix|refactor|docs|test|chore|perf|ci|style|build|revert)(\(.+?\))?!?:\s"
)


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

    contributors = _extract_contributors(commits)
    hot_files = _extract_hot_files(commits)
    coupled_files = _extract_coupled_files(commits)
    commit_style = _analyze_commit_style(commits)
    monthly = _monthly_activity(commits)
    branches, branch_patterns = _analyze_branches(repo)

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
    )


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
                if path:
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
