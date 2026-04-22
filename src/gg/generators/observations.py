"""Structured code observations via Codex.

Runs Codex with specific audit prompts for each topic:
- code-quality: testing, linting, CI
- security: auth, input validation, secrets
- error-handling: error patterns, logging, observability
- configurations: env vars, config files, secrets management
"""
from __future__ import annotations

from pathlib import Path

from rich.console import Console

from gg.agents.base import AgentBackend

AUDIT_TOPICS: list[dict[str, str]] = [
    {
        "slug": "code-quality",
        "title": "Code Quality",
        "prompt": (
            "Проанализируй качество кода этого проекта. Ответь структурированно:\n\n"
            "## Testing\n"
            "- Какой test framework используется?\n"
            "- Есть ли тесты? Примерная оценка покрытия.\n"
            "- Какие области не покрыты тестами?\n\n"
            "## Linting & Formatting\n"
            "- Какие линтеры/форматтеры настроены?\n"
            "- Есть ли pre-commit hooks?\n\n"
            "## CI/CD\n"
            "- Есть ли CI pipeline? Что он делает?\n"
            "- Есть ли автоматический деплой?\n\n"
            "## Code Smells\n"
            "- Файлы с подозрительно большим размером\n"
            "- Дублирование кода\n"
            "- Устаревшие зависимости\n\n"
            "Для каждой секции укажи статус: OK / Needs Attention / Critical / Not Analyzed"
        ),
    },
    {
        "slug": "security",
        "title": "Security",
        "prompt": (
            "Проведи аудит безопасности этого проекта. Ответь структурированно:\n\n"
            "## Authentication & Authorization\n"
            "- Как реализована аутентификация?\n"
            "- Есть ли проверка прав доступа?\n\n"
            "## Input Validation\n"
            "- Валидируется ли пользовательский ввод?\n"
            "- Есть ли защита от injection (SQL, XSS, etc.)?\n\n"
            "## Secrets Management\n"
            "- Есть ли захардкоженные секреты в коде?\n"
            "- Как управляются API ключи и credentials?\n"
            "- Есть ли .env.example?\n\n"
            "## Dependencies\n"
            "- Есть ли известные уязвимости в зависимостях?\n\n"
            "Для каждой секции укажи статус: OK / Needs Attention / Critical / Not Analyzed"
        ),
    },
    {
        "slug": "error-handling",
        "title": "Error Handling & Observability",
        "prompt": (
            "Проанализируй обработку ошибок и наблюдаемость проекта:\n\n"
            "## Error Handling Patterns\n"
            "- Как обрабатываются ошибки? (try/catch, error boundaries, etc.)\n"
            "- Есть ли единый подход к ошибкам?\n"
            "- Показываются ли пользователю понятные сообщения?\n\n"
            "## Logging\n"
            "- Какая система логирования?\n"
            "- Достаточно ли логов для диагностики проблем?\n\n"
            "## Monitoring & Observability\n"
            "- Есть ли метрики, трейсинг, APM?\n"
            "- Есть ли health checks?\n\n"
            "Для каждой секции укажи статус: OK / Needs Attention / Critical / Not Analyzed"
        ),
    },
    {
        "slug": "configurations",
        "title": "Configurations & Infrastructure",
        "prompt": (
            "Проанализируй конфигурацию и инфраструктуру проекта:\n\n"
            "## Environment Variables\n"
            "- Какие env vars используются?\n"
            "- Есть ли .env.example с описанием?\n\n"
            "## Config Files\n"
            "- Какие конфиг-файлы есть (yaml, toml, json)?\n"
            "- Разделены ли конфиги по окружениям (dev/staging/prod)?\n\n"
            "## Infrastructure\n"
            "- Docker/docker-compose?\n"
            "- Какие внешние сервисы используются (DB, cache, queues)?\n\n"
            "## Data Storage\n"
            "- Какая база данных?\n"
            "- Есть ли миграции?\n"
            "- Как хранятся файлы/бинарные данные?\n\n"
            "Для каждой секции укажи статус: OK / Needs Attention / Critical / Not Analyzed"
        ),
    },
]


def run_deep_observations(
    *,
    project_path: str | Path,
    agent: AgentBackend,
    console: Console,
) -> int:
    """Run structured code audit via Codex. Returns number of observations written."""
    root = Path(project_path).resolve()
    obs_dir = root / ".gg" / "observations"
    obs_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    total = len(AUDIT_TOPICS)

    for i, topic in enumerate(AUDIT_TOPICS):
        console.print(f"    [{i + 1}/{total}] Auditing: {topic['title']}...")
        try:
            result = agent.generate(topic["prompt"], cwd=str(root))
            if result:
                path = obs_dir / f"{topic['slug']}.md"
                path.write_text(
                    f"# {topic['title']}\n\n{result}\n",
                    encoding="utf-8",
                )
                console.print(f"    [green]  -> {topic['slug']}.md[/green]")
                count += 1
            else:
                console.print(f"    [yellow]  -> empty response, skipped[/yellow]")
        except RuntimeError as e:
            console.print(f"    [yellow]  -> failed: {e}[/yellow]")

    if count > 0:
        _write_observations_index(obs_dir, count)

    return count


def _write_observations_index(obs_dir: Path, count: int) -> None:
    files = sorted(obs_dir.glob("*.md"))
    lines = ["# Code Observations", "", f"{count} audit topics analyzed.", ""]
    for f in files:
        if f.name == "00-index.md":
            continue
        title = f.stem.replace("-", " ").title()
        lines.append(f"- [{title}]({f.name})")
    lines.append("")
    (obs_dir / "00-index.md").write_text("\n".join(lines), encoding="utf-8")
