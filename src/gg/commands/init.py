from __future__ import annotations

from pathlib import Path

import yaml
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm, Prompt

from gg.analyzers.dependencies import DependencyReport, analyze_dependencies
from gg.analyzers.git_history import GitProfile, analyze_git_history
from gg.analyzers.languages import LanguageProfile, analyze_languages
from gg.analyzers.structure import StructureMap, analyze_structure
from gg.generators.agent_files import generate_agent_files
from gg.generators.specs import UserContext, generate_specs
from gg.knowledge.engine import KnowledgeEngine
from gg.platforms.base import detect_platform
from gg.utils.git_ops import find_repo_root, get_main_branch, get_remote_url, parse_remote_url
from gg.utils.system import run_all_checks


LINTER_SUGGESTIONS: dict[str, dict[str, str]] = {
    "Python": {"tool": "ruff", "install": "pip install ruff", "config": "ruff.toml"},
    "TypeScript": {"tool": "eslint", "install": "npm install -D eslint", "config": ".eslintrc.json"},
    "JavaScript": {"tool": "eslint", "install": "npm install -D eslint", "config": ".eslintrc.json"},
    "Go": {"tool": "golangci-lint", "install": "brew install golangci-lint", "config": ".golangci.yml"},
    "Rust": {"tool": "clippy", "install": "rustup component add clippy", "config": ""},
}

TEST_SUGGESTIONS: dict[str, dict[str, str]] = {
    "Python": {"tool": "pytest", "install": "pip install pytest"},
    "TypeScript": {"tool": "vitest", "install": "npm install -D vitest"},
    "JavaScript": {"tool": "jest", "install": "npm install -D jest"},
    "Go": {"tool": "go test", "install": ""},
    "Rust": {"tool": "cargo test", "install": ""},
}


def run_init(
    *,
    path: str,
    force: bool,
    skip_codex: bool,
    skip_knowledge: bool = False,
    non_interactive: bool,
    deep: bool = False,
    debug: bool = False,
) -> None:
    console = Console()
    project_path = Path(path).resolve()

    console.print(Panel("[bold]gg init[/bold] -- project initialization", style="blue"))

    # 1. System checks
    checks = run_all_checks(offer_install=not non_interactive)

    check_map = {c.name: c for c in checks}
    codex_available = check_map.get("codex", type("", (), {"ok": False})).ok and not skip_codex

    # 2. Find repo root
    repo_root = find_repo_root(project_path)
    if repo_root is None:
        console.print("[red]Not a git repository. Run 'git init' first.[/red]")
        raise SystemExit(1)
    project_path = repo_root

    gg_dir = project_path / ".gg"
    if gg_dir.exists() and not force:
        console.print("[red].gg/ already exists. Use --force to overwrite.[/red]")
        raise SystemExit(1)

    # 3. Detect platform
    platform = _detect_and_confirm_platform(project_path, check_map, non_interactive, console)

    # 4. Run analyzers (local, fast -- no LLM)
    console.print()
    languages, dependencies, structure, git_profile = _run_analyzers(project_path, console)

    # 6. Build context from local analysis (replaces Codex research)
    from gg.analyzers.codebase import analyze_codebase
    codebase_insights = analyze_codebase(project_path)

    user_ctx = UserContext(
        description=codebase_insights.get("description", ""),
        domains=codebase_insights.get("domains", ""),
        integrations=codebase_insights.get("integrations", ""),
    )
    # Codex fast mode: stdin + read-only + MCP disabled = instant response
    from gg.agents.codex import CodexAgent
    agent = CodexAgent(console=console, debug=debug) if codex_available else None

    # Quick Codex description if local one is weak
    if agent and agent.is_available() and len(user_ctx.description) < 30:
        ctx = f"{languages.primary_language} project, {', '.join(languages.frameworks[:3])}, dirs: {', '.join(structure.top_level_dirs[:5])}"
        try:
            desc = agent.generate(
                "Что делает этот проект? Одно предложение, по-русски.",
                context=ctx, timeout=30,
            )
            if desc and len(desc) > 20:
                user_ctx = UserContext(description=desc.strip().split("\n")[0][:200], domains=user_ctx.domains, integrations=user_ctx.integrations)
        except RuntimeError:
            pass

    # 6. Display summary
    _print_summary(
        languages, dependencies, structure, git_profile, console,
        description=user_ctx.description if user_ctx else "",
        domains=user_ctx.domains if user_ctx else "",
        integrations=user_ctx.integrations if user_ctx else "",
    )

    # 7. Create directories
    gg_dir.mkdir(parents=True, exist_ok=True)

    # 8. Generate artifacts
    console.print("\n[bold]Generating artifacts...[/bold]")

    analyzer_context = "\n\n".join([
        languages.to_prompt_context(),
        dependencies.to_prompt_context(),
        structure.to_prompt_context(),
        git_profile.to_prompt_context(),
    ])

    step = 0
    total_steps = (0 if skip_knowledge else 1) + 2  # knowledge + specs + agent_files

    engine = KnowledgeEngine(project_path)

    # 8a. Knowledge system
    if skip_knowledge:
        console.print("  [yellow]Skipping knowledge system (--skip-knowledge)[/yellow]")
    else:
        step += 1
        console.print(f"  [dim][{step}/{total_steps}][/dim] Building knowledge system...")
        console.print("    Analyzing git history for entities...")
        stats = engine.rebuild()
        console.print(f"    Recording init event ({languages.primary_language}, {dependencies.package_manager})...")
        engine.record_init(data={
            "total_commits": git_profile.total_commits,
            "contributors_count": len(git_profile.contributors),
            "top_level_dirs": structure.top_level_dirs,
            "is_monorepo": structure.is_monorepo,
            "primary_language": languages.primary_language,
            "package_manager": dependencies.package_manager,
        })
        console.print(f"  [green]  -> {stats['entities']} entities, {stats['facts']} facts, "
                      f"{stats['events_processed']} events[/green]")

    # 8b. Specs and constitution
    step += 1
    console.print(f"  [dim][{step}/{total_steps}][/dim] Generating specs and constitution...")
    generate_specs(
        project_path=project_path,
        agent=agent,
        analyzer_context=analyzer_context,
        user_ctx=user_ctx,
        interactive=not non_interactive,
        console=console,
    )

    # 8c. Agent instruction files
    step += 1
    console.print(f"  [dim][{step}/{total_steps}][/dim] Generating agent instruction files...")
    constitution_path = gg_dir / "constitution.md"
    generate_agent_files(
        project_path=project_path,
        languages=languages,
        dependencies=dependencies,
        structure=structure,
        constitution_path=constitution_path if constitution_path.exists() else None,
    )
    console.print("  [green]  -> AGENTS.md + CLAUDE.md[/green]")

    # 8d. Deep code audit (optional)
    if deep and agent and agent.is_available():
        from gg.generators.observations import run_deep_observations
        console.print("  [bold]Running deep code audit...[/bold]")
        obs_count = run_deep_observations(
            project_path=project_path, agent=agent, console=console,
        )
        console.print(f"  [green]  -> {obs_count} observations in .gg/observations/[/green]")
    elif deep and not (agent and agent.is_available()):
        console.print("  [yellow]--deep requires Codex, skipping audit[/yellow]")

    # 8f. Create goals file if not exists
    _init_goals(project_path, user_ctx, console)

    # 9. Suggestions
    if not non_interactive:
        console.print()
        _offer_suggestions(project_path, languages, dependencies, console)

    # 10. Write config
    _write_config(project_path, platform, console)
    _write_params(project_path, console)

    # 11. Summary
    _print_final(project_path, console)


def _detect_and_confirm_platform(
    project_path: Path,
    check_map: dict,
    non_interactive: bool,
    console: Console,
) -> str:
    detected = detect_platform(project_path)

    if detected != "unknown":
        console.print(f"\n  Detected platform: [bold]{detected}[/bold]")
        if not non_interactive:
            ok = Confirm.ask(f"  Use {detected}?", default=True)
            if not ok:
                detected = "unknown"

    if detected == "unknown" and not non_interactive:
        choice = Prompt.ask(
            "  Git platform",
            choices=["github", "gitlab"],
            default="github",
            console=console,
        )
        detected = choice

    if detected == "unknown":
        detected = "github"

    cli_tool = "gh" if detected == "github" else "glab"
    check = check_map.get(cli_tool)
    if check and not check.ok:
        console.print(f"  [yellow]Warning: {cli_tool} is not available. Some features will be limited.[/yellow]")

    return detected


def _run_analyzers(
    project_path: Path, console: Console,
) -> tuple[LanguageProfile, DependencyReport, StructureMap, GitProfile]:
    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t1 = progress.add_task("[1/4] Analyzing languages...", total=None)
        languages = analyze_languages(project_path)
        progress.update(t1, description=f"[green][1/4] Languages: {languages.primary_language}[/green]")

        t2 = progress.add_task("[2/4] Parsing dependencies...", total=None)
        dependencies = analyze_dependencies(project_path)
        progress.update(t2, description=f"[green][2/4] Dependencies: {dependencies.package_manager}[/green]")

        t3 = progress.add_task("[3/4] Mapping structure...", total=None)
        structure = analyze_structure(project_path)
        progress.update(t3, description=f"[green][3/4] Structure: {len(structure.top_level_dirs)} top-level dirs[/green]")

        t4 = progress.add_task("[4/4] Analyzing git history...", total=None)
        git_profile = analyze_git_history(project_path)
        progress.update(t4, description=f"[green][4/4] Git: {git_profile.total_commits} commits[/green]")

    return languages, dependencies, structure, git_profile


def _print_summary(
    langs: LanguageProfile,
    deps: DependencyReport,
    struct: StructureMap,
    git: GitProfile,
    console: Console,
    description: str = "",
    domains: str = "",
    integrations: str = "",
) -> None:
    lines = []
    if description:
        lines.append(f"[bold]{description}[/bold]")
        lines.append("")
    lines.append(f"[bold]Language:[/bold] {langs.primary_language} ({langs.total_files} files)")
    if langs.frameworks:
        lines.append(f"[bold]Frameworks:[/bold] {', '.join(langs.frameworks)}")
    lines.append(f"[bold]Package manager:[/bold] {deps.package_manager}")
    if struct.is_monorepo:
        lines.append("[bold]Type:[/bold] Monorepo")
    if domains:
        lines.append(f"[bold]Packages:[/bold] {domains}")
    else:
        lines.append(f"[bold]Dirs:[/bold] {', '.join(struct.top_level_dirs[:8])}")
    if integrations:
        lines.append(f"[bold]Integrations:[/bold] {integrations}")
    lines.append(f"[bold]Git:[/bold] {git.total_commits} commits, {len(git.contributors)} contributors")
    if git.first_commit_date:
        lines.append(f"[bold]History:[/bold] {git.first_commit_date} -- {git.last_commit_date}")

    console.print(Panel("\n".join(lines), title="Analysis Summary", style="cyan"))


def _offer_suggestions(
    project_path: Path,
    langs: LanguageProfile,
    deps: DependencyReport,
    console: Console,
) -> None:
    existing_linters = deps.existing_tools.get("linters", [])
    existing_tests = deps.existing_tools.get("test_frameworks", [])

    primary = langs.primary_language

    if primary in LINTER_SUGGESTIONS and not existing_linters:
        suggestion = LINTER_SUGGESTIONS[primary]
        if Confirm.ask(f"  Add [bold]{suggestion['tool']}[/bold] linter?", default=True):
            console.print(f"    Run: [dim]{suggestion['install']}[/dim]")

    if primary in TEST_SUGGESTIONS and not existing_tests:
        suggestion = TEST_SUGGESTIONS[primary]
        if suggestion["install"] and Confirm.ask(
            f"  Add [bold]{suggestion['tool']}[/bold] test framework?", default=True,
        ):
            console.print(f"    Run: [dim]{suggestion['install']}[/dim]")

    if not (project_path / ".pre-commit-config.yaml").exists():
        if Confirm.ask("  Add [bold]pre-commit[/bold] hooks?", default=False):
            console.print("    Run: [dim]pip install pre-commit && pre-commit install[/dim]")

    if not (project_path / ".github" / "workflows").exists() and not (project_path / ".gitlab-ci.yml").exists():
        if Confirm.ask("  Add CI config template?", default=False):
            console.print("    Will be generated with future [bold]gg ci[/bold] command.")


def _init_goals(project_path: Path, user_ctx: "UserContext | None", console: Console) -> None:
    goals_path = project_path / ".gg" / "goals.md"
    if goals_path.exists():
        console.print(f"  [dim]goals.md already exists ({len(goals_path.read_text())} chars)[/dim]")
        return

    description = user_ctx.description if user_ctx else ""
    content = (
        "# Project Goals\n\n"
        "Define the goals that guide all agent work on this project.\n"
        "The agent reads this file before every task to stay aligned.\n\n"
        "## Business Goals\n\n"
        f"- {description or 'TODO: describe what the project should achieve'}\n\n"
        "## Quality Criteria\n\n"
        "- All changes must pass existing tests\n"
        "- No regressions in core functionality\n"
        "- Code follows project constitution (.gg/constitution.md)\n\n"
        "## Priorities\n\n"
        "- 1. Correctness\n"
        "- 2. Maintainability\n"
        "- 3. Performance\n\n"
        "## Out of Scope\n\n"
        "- TODO: define what the agent should NOT touch\n"
    )
    goals_path.write_text(content, encoding="utf-8")
    console.print("  [green]  -> .gg/goals.md (edit to guide agent priorities)[/green]")


def _init_grepai(project_path: Path, console: Console) -> None:
    import subprocess
    console.print("  Initializing [bold]grepai[/bold] semantic search...")
    try:
        result = subprocess.run(
            ["grepai", "init"],
            capture_output=True, text=True, timeout=30,
            cwd=str(project_path),
        )
        if result.returncode == 0:
            console.print("  [green]  -> grepai index initialized[/green]")
            console.print("  [dim]  Tip: run 'grepai watch' to keep the index updated[/dim]")
        else:
            console.print(f"  [yellow]grepai init failed: {result.stderr.strip()[:100]}[/yellow]")
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        console.print(f"  [yellow]grepai init failed: {e}[/yellow]")


def _write_config(project_path: Path, platform: str, console: Console) -> None:
    remote_url = get_remote_url(project_path)
    owner, repo = parse_remote_url(remote_url)
    main_branch = get_main_branch(project_path)

    config = {
        "version": 1,
        "project": {
            "name": project_path.name,
            "path": str(project_path),
        },
        "git": {
            "remote_url": remote_url,
            "owner": owner,
            "repo": repo,
            "main_branch": main_branch,
        },
        "platform": platform,
        "agent_backend": "codex",
    }

    config_path = project_path / ".gg" / "config.yaml"
    config_path.write_text(
        yaml.dump(config, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_params(project_path: Path, console: Console) -> None:
    params_path = project_path / ".gg" / "params.yaml"
    if params_path.exists():
        console.print("  [dim].gg/params.yaml already exists[/dim]")
        return

    main_branch = get_main_branch(project_path)
    params = {
        "schema_version": 1,
        "project": {
            "default_branch": main_branch,
        },
        "task_system": {
            "platform": "auto",
            "work_label": "gg:in-progress",
            "done_label": "gg:done",
            "blocked_label": "gg:blocked",
        },
        "selection": {
            "include_labels": ["ai-ready"],
            "exclude_labels": ["gg:in-progress", "gg:blocked", "gg:done"],
        },
        "verify": {
            "tests": _default_verify_command(project_path),
            "lint": "",
            "typecheck": "",
            "allow_known_baseline_failures": False,
        },
        "runtime": {
            "agent_backend": "codex",
            "candidates": 1,
            "max_parallel_candidates": 1,
            "max_attempts": 1,
            "repair_candidates": 1,
            "use_sandbox_runtime": True,
            "require_sandbox_runtime": False,
            "candidate_timeout_seconds": 1800,
            "command_timeout_seconds": 600,
            "sandbox_policy": {
                "allowed_domains": [],
                "denied_domains": [],
                "deny_read": ["~/.ssh", ".env"],
                "allow_write": ["."],
                "deny_write": [".env"],
            },
        },
        "audit": {
            "hash_events": False,
            "external_sink": "",
        },
        "git": {
            "author_name": "gg-orchestrator",
            "author_email": "gg-orchestrator@users.noreply.local",
        },
    }
    params_path.write_text(
        yaml.dump(params, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    console.print("  [green]  -> .gg/params.yaml[/green]")


def _default_verify_command(project_path: Path) -> str:
    if (project_path / "pyproject.toml").exists():
        return "pytest"
    if (project_path / "package.json").exists():
        return "npm test"
    return ""


def _print_final(project_path: Path, console: Console) -> None:
    gg = project_path / ".gg"
    openspec = project_path / "openspec"

    created: list[str] = []
    if gg.exists():
        created.append(".gg/config.yaml")
        if (gg / "constitution.md").exists():
            created.append(".gg/constitution.md")
        if (gg / "knowledge").exists():
            created.append(".gg/knowledge/")
    if openspec.exists():
        created.append("openspec/")
    if (project_path / "AGENTS.md").exists():
        created.append("AGENTS.md")
    if (project_path / "CLAUDE.md").exists():
        created.append("CLAUDE.md")

    lines = ["[green bold]Initialization complete![/green bold]", ""]
    lines.append("[bold]Created:[/bold]")
    for f in created:
        lines.append(f"  {f}")
    lines.append("")
    lines.append("[bold]Next steps:[/bold]")
    lines.append("  1. Review .gg/constitution.md and openspec/specs/")
    lines.append("  2. Commit the generated files")
    lines.append("  3. Run [bold]gg run[/bold] to start processing issues")

    console.print(Panel("\n".join(lines), title="gg init", style="green"))
