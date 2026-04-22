from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

LANG_SIGNATURES: dict[str, str] = {
    ".py": "Python",
    ".js": "JavaScript",
    ".ts": "TypeScript",
    ".tsx": "TypeScript",
    ".jsx": "JavaScript",
    ".go": "Go",
    ".rs": "Rust",
    ".rb": "Ruby",
    ".java": "Java",
    ".kt": "Kotlin",
    ".swift": "Swift",
    ".c": "C",
    ".cpp": "C++",
    ".h": "C/C++",
    ".cs": "C#",
    ".php": "PHP",
    ".scala": "Scala",
    ".ex": "Elixir",
    ".exs": "Elixir",
    ".hs": "Haskell",
    ".lua": "Lua",
    ".r": "R",
    ".R": "R",
    ".dart": "Dart",
    ".vue": "Vue",
    ".svelte": "Svelte",
    ".sh": "Shell",
    ".bash": "Shell",
    ".zsh": "Shell",
}

FRAMEWORK_MARKERS: dict[str, str] = {
    "next.config.js": "Next.js",
    "next.config.ts": "Next.js",
    "next.config.mjs": "Next.js",
    "nuxt.config.ts": "Nuxt",
    "nuxt.config.js": "Nuxt",
    "angular.json": "Angular",
    "svelte.config.js": "Svelte",
    "astro.config.mjs": "Astro",
    "remix.config.js": "Remix",
    "vite.config.ts": "Vite",
    "vite.config.js": "Vite",
    "webpack.config.js": "Webpack",
    "manage.py": "Django",
    "settings.py": "Django",
    "app.py": "Flask",
    "Cargo.toml": "Rust/Cargo",
    "go.mod": "Go Modules",
    "Gemfile": "Ruby/Bundler",
    "build.gradle": "Gradle",
    "build.gradle.kts": "Gradle",
    "pom.xml": "Maven",
    "pubspec.yaml": "Flutter/Dart",
    "Package.swift": "Swift Package",
    "docker-compose.yml": "Docker Compose",
    "docker-compose.yaml": "Docker Compose",
    "Dockerfile": "Docker",
}

STYLING_MARKERS: dict[str, str] = {
    "tailwind.config.js": "Tailwind CSS",
    "tailwind.config.ts": "Tailwind CSS",
    "postcss.config.js": "PostCSS",
    ".storybook": "Storybook",
}

SKIP_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv", "env",
    ".tox", ".mypy_cache", ".pytest_cache", "dist", "build",
    ".next", ".nuxt", "target", "vendor", ".gg", "openspec",
}


@dataclass(frozen=True)
class LanguageProfile:
    primary_language: str
    languages: dict[str, int] = field(default_factory=dict)
    frameworks: list[str] = field(default_factory=list)
    styling: list[str] = field(default_factory=list)
    total_files: int = 0

    def to_prompt_context(self) -> str:
        lines = ["## Language Profile"]
        lines.append(f"Primary language: {self.primary_language}")
        lines.append(f"Total source files: {self.total_files}")
        if self.languages:
            lines.append("Languages (by file count):")
            for lang, count in sorted(self.languages.items(), key=lambda x: -x[1]):
                lines.append(f"  - {lang}: {count} files")
        if self.frameworks:
            lines.append(f"Frameworks: {', '.join(self.frameworks)}")
        if self.styling:
            lines.append(f"Styling: {', '.join(self.styling)}")
        return "\n".join(lines)


def analyze_languages(project_path: str | Path) -> LanguageProfile:
    root = Path(project_path).resolve()
    lang_counts: dict[str, int] = {}
    frameworks: list[str] = []
    styling: list[str] = []
    total = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]

        for fname in filenames:
            ext = os.path.splitext(fname)[1]
            if ext in LANG_SIGNATURES:
                lang = LANG_SIGNATURES[ext]
                lang_counts = {**lang_counts, lang: lang_counts.get(lang, 0) + 1}
                total += 1

            if fname in FRAMEWORK_MARKERS:
                fw = FRAMEWORK_MARKERS[fname]
                if fw not in frameworks:
                    frameworks = [*frameworks, fw]

            if fname in STYLING_MARKERS:
                st = STYLING_MARKERS[fname]
                if st not in styling:
                    styling = [*styling, st]

        for dname in dirnames:
            if dname in STYLING_MARKERS:
                st = STYLING_MARKERS[dname]
                if st not in styling:
                    styling = [*styling, st]

    primary = max(lang_counts, key=lang_counts.get, default="Unknown") if lang_counts else "Unknown"

    return LanguageProfile(
        primary_language=primary,
        languages=dict(sorted(lang_counts.items(), key=lambda x: -x[1])),
        frameworks=frameworks,
        styling=styling,
        total_files=total,
    )
