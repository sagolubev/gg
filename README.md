# gg -- Agent Orchestrator

CLI tool that analyzes a codebase and prepares it for autonomous AI agent work. Takes a raw git repository and produces: project constitution, knowledge base, API inventory, DB schema map, component tree, risk register, and agent instruction files.

## Install

```bash
pip install git+https://github.com/sagolubev/gg.git
```

Requires Python >= 3.10.

## Prerequisites

**Required:**
- `git` -- must be installed and repo initialized

**Optional (detected automatically):**
- `codex` -- OpenAI Codex CLI for AI-generated constitution (`npm install -g @openai/codex`)
- `gh` -- GitHub CLI for issue tracking (`brew install gh`)
- `glab` -- GitLab CLI for issue tracking (`brew install glab`)
- `openspec` -- spec-as-code tool (`npm install -g openspec`)
- `grepai` -- semantic code search (`brew install yoanbernabeu/tap/grepai`)

Missing tools are detected at init and install commands are suggested.

## Quick Start

```bash
cd your-project
gg init                    # full init with Codex constitution
gg init --skip-codex       # local-only, no LLM (fastest)
gg init --debug            # show Codex output
gg init --skip-knowledge   # skip knowledge build (large repos)
```

## What `gg init` Creates

```
your-project/
├── .gg/
│   ├── config.yaml                  # project config (platform, repo, branch)
│   ├── constitution.md              # coding rules (Codex-generated or local)
│   ├── goals.md                     # project goals (edit manually)
│   └── knowledge/
│       ├── fact-registry.md         # contributors, hot files, ownership, velocity
│       ├── risk-register.md         # bus factor, missing tests, high-risk files
│       ├── codebase-insights.md     # env vars, TODO markers, top imports
│       ├── entities/                # per-module profiles
│       ├── decisions/               # auto-generated ADRs from git history
│       ├── sessions/                # event log (JSONL)
│       └── intel/
│           ├── api-inventory.md     # all API endpoints with file:line
│           ├── db-schema.md         # tables, columns, relations
│           ├── components.md        # React/Vue component tree
│           ├── test-examples.md     # representative test files
│           ├── style-exemplars.md   # "golden" files per code type
│           └── pr-checklist.md      # auto-generated PR checklist
├── openspec/                        # OpenSpec-compatible specs
│   ├── config.yaml
│   ├── concept.md
│   └── specs/
├── AGENTS.md                        # instructions for Codex
└── CLAUDE.md                        # instructions for Claude Code
```

## Commands

```bash
gg init                 # initialize project
gg constitution         # regenerate constitution via Codex (2-5 min)
gg knowledge rebuild    # rebuild knowledge from events + git
gg knowledge search Q   # search knowledge base
gg knowledge context T  # build agent context for issue title T
gg knowledge stats      # show knowledge statistics
gg run                  # supervisor loop (coming soon)
gg issue N              # process single issue (coming soon)
gg review N             # agentic code review (coming soon)
```

## How It Works

**Local analysis (~30s):**
- Language/framework detection (regex, file patterns)
- Dependency parsing (package.json, pyproject.toml, go.mod, Cargo.toml)
- Directory structure classification
- Git history: ownership, churn, coupling, velocity, risk scores
- Codebase scan: TODO markers, env vars, API routes, imports
- Project intel: DB schema, component tree, test examples, style exemplars

**Codex constitution (~30s, optional):**
- Minimal context (~300 chars) piped to `codex exec` via stdin
- Read-only sandbox, hooks and MCP disabled for speed
- Generates project-specific coding rules
- Falls back to local constitution if Codex unavailable

**Knowledge system (continuous):**
- Event-sourced: append-only JSONL log is source of truth
- Compiled views: markdown files rebuilt from events + git
- Auto-rebuild after PR merge or every 10 events
- Search: keyword search across entities, facts, decisions, errors

## Architecture

```
src/gg/
├── cli.py              # Click CLI entry point
├── commands/init.py    # gg init orchestration
├── analyzers/          # local codebase analysis (no LLM)
│   ├── languages.py    # language/framework detection
│   ├── dependencies.py # package file parsing
│   ├── structure.py    # directory classification
│   ├── git_history.py  # git log deep analysis
│   ├── codebase.py     # README, TODOs, env vars, imports
│   └── project_intel.py # API, DB, components, tests, exemplars
├── generators/         # artifact generation
│   ├── specs.py        # constitution + openspec (Codex or local)
│   ├── knowledge.py    # legacy knowledge builder
│   └── agent_files.py  # AGENTS.md + CLAUDE.md (smart merge)
├── knowledge/          # knowledge system
│   ├── engine.py       # facade: record events, search, rebuild
│   ├── events.py       # append-only JSONL event log
│   ├── collectors.py   # extract entities/facts/decisions from events
│   ├── compiler.py     # compile markdown artifacts from events + git
│   └── search.py       # keyword search + context builder
├── agents/             # AI agent backends
│   ├── base.py         # AgentBackend ABC
│   └── codex.py        # Codex CLI wrapper (fast + full modes)
├── platforms/          # git platform abstraction
│   ├── base.py         # GitPlatform ABC + auto-detection
│   ├── github.py       # GitHub via gh CLI
│   └── gitlab.py       # GitLab via glab CLI
└── utils/
    ├── system.py       # dependency checks + installer
    └── git_ops.py      # git operations
```

## License

MIT
