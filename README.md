# gg -- Agent Orchestrator

Оркестратор AI-агентов: берет задачу из бэклога, прогоняет через пайплайн и доводит до PR.

```
GitHub/GitLab Issue --> gg run --> Research --> Plan --> Implement --> Test --> PR --> Review
                                    |                                           |
                                    +--- knowledge update                       +--- rework if needed
```

## Pipeline

### Full cycle (what `gg run` does)

1. **Pick issue** -- берёт issue из GitHub/GitLab (по приоритету, assignee, labels)
2. **Research** -- Codex анализирует issue, находит релевантные файлы через knowledge base
3. **Plan** -- формирует план изменений с учётом constitution и risk register
4. **Implement** -- Codex пишет код в изолированном worktree
5. **Test** -- запускает тесты, lint, typecheck
6. **PR** -- создаёт pull request с описанием
7. **Review** -- agentic code review по PR checklist
8. **Rework** -- если review/тесты не прошли, возвращается на шаг 3

Каждый шаг записывает события в knowledge system. Агент учится на ошибках.

### What works now

- **`gg init`** -- полная подготовка проекта для автономной работы агента
- **`gg constitution`** -- генерация конституции через Codex
- **`gg knowledge`** -- поиск, rebuild, stats по knowledge base
- **`gg run`**, **`gg issue`**, **`gg review`** -- pipeline (в разработке)

## Install

```bash
pip install git+https://github.com/sagolubev/gg.git
```

Python >= 3.10.

## Prerequisites

**Required:** `git`

**Optional (detected at init, install suggested):**

| Tool | Purpose | Install |
|------|---------|---------|
| `codex` | AI-generated constitution, code implementation | `npm install -g @openai/codex` |
| `gh` | GitHub issues/PRs | `brew install gh` |
| `glab` | GitLab issues/MRs | `brew install glab` |
| `openspec` | Spec-as-code artifacts | `npm install -g openspec` |
| `grepai` | Semantic code search | `brew install yoanbernabeu/tap/grepai` |

## Quick Start

```bash
cd your-project

# Initialize (local analysis + Codex constitution, ~1 min)
gg init

# Or without Codex (purely local, ~30s)
gg init --skip-codex

# See what was generated
cat .gg/constitution.md
cat .gg/knowledge/risk-register.md
cat .gg/knowledge/intel/api-inventory.md
```

## What `gg init` Produces

### For the agent (how to work)

| File | Content |
|------|---------|
| `.gg/constitution.md` | Coding rules specific to this project (Codex or local) |
| `AGENTS.md` | Instructions for Codex (existing preserved, gaps filled) |
| `CLAUDE.md` | Instructions for Claude Code |
| `.gg/goals.md` | Project goals -- edit to guide agent priorities |
| `.gg/knowledge/intel/pr-checklist.md` | What to check before creating PR |
| `.gg/knowledge/intel/style-exemplars.md` | "Golden" files to copy patterns from |
| `.gg/knowledge/intel/test-examples.md` | How tests look in this project |

### For understanding the project (what exists)

| File | Content |
|------|---------|
| `.gg/knowledge/intel/api-inventory.md` | All API endpoints with file:line |
| `.gg/knowledge/intel/db-schema.md` | Tables, columns, relations |
| `.gg/knowledge/intel/components.md` | React/Vue component tree |
| `.gg/knowledge/fact-registry.md` | Contributors, hot files, ownership, velocity |
| `.gg/knowledge/codebase-insights.md` | Env vars, TODO markers, top imports |

### For risk awareness (what to be careful with)

| File | Content |
|------|---------|
| `.gg/knowledge/risk-register.md` | Bus factor, missing tests, high-risk files |
| `.gg/knowledge/decisions/` | Auto-generated ADRs from architectural commits |
| `openspec/` | OpenSpec-compatible project specs |

### Knowledge system (grows automatically)

Event-sourced: every pipeline action writes to JSONL log. Compiled views (markdown) rebuild automatically after PR merge or every 10 events.

```bash
gg knowledge search "authentication"     # search across all knowledge
gg knowledge context "Add OAuth login"   # build agent context for an issue
gg knowledge rebuild                     # full recompile from events + git
gg knowledge stats                       # event counts and types
```

## Commands

```bash
# Project setup
gg init                          # full init (~1 min with Codex)
gg init --skip-codex             # local only (~30s)
gg init --skip-knowledge         # skip knowledge build (large repos)
gg init --deep                   # + Codex audit (security, quality, errors, config)
gg init --debug                  # show Codex output
gg constitution                  # regenerate constitution via Codex

# Knowledge
gg knowledge search <query>      # keyword search
gg knowledge context <title>     # build context for issue
gg knowledge rebuild             # full rebuild
gg knowledge stats               # statistics

# Pipeline (coming soon)
gg run                           # supervisor loop
gg issue <N>                     # process single issue
gg review <N>                    # agentic code review
gg status                        # show active tasks
```

## Platforms

Auto-detected from git remote URL:
- **GitHub** -- issues and PRs via `gh` CLI
- **GitLab** -- issues and MRs via `glab` CLI

## License

MIT
