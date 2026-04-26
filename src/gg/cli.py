from pathlib import Path

import click

from gg import __version__


@click.group()
@click.version_option(version=__version__, prog_name="gg")
def cli():
    """GG -- agent orchestrator: backlog task -> pipeline -> PR."""


@cli.command()
@click.option("--path", type=click.Path(exists=True), default=".", help="Target project path.")
@click.option("--force", is_flag=True, help="Overwrite existing .gg/ directory.")
@click.option("--skip-codex", is_flag=True, help="Skip Codex analysis, use local-only.")
@click.option("--skip-knowledge", is_flag=True, help="Skip knowledge system build (faster for large repos).")
@click.option("--non-interactive", is_flag=True, help="No interactive prompts.")
@click.option("--deep", is_flag=True, help="Run deep code audit (security, quality, error handling).")
@click.option("--debug", is_flag=True, help="Show Codex input/output and verbose logging.")
def init(path, force, skip_codex, skip_knowledge, non_interactive, deep, debug):
    """Initialize project: analyze codebase, generate specs and knowledge."""
    from gg.commands.init import run_init

    run_init(
        path=path, force=force, skip_codex=skip_codex,
        skip_knowledge=skip_knowledge, non_interactive=non_interactive,
        deep=deep, debug=debug,
    )


@cli.group()
def knowledge():
    """Knowledge system management."""


@knowledge.command("rebuild")
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
def knowledge_rebuild(path):
    """Full rebuild of knowledge artifacts from events + git history."""
    from rich.console import Console

    from gg.knowledge.engine import KnowledgeEngine

    console = Console()
    engine = KnowledgeEngine(path)
    stats = engine.rebuild()
    console.print(f"[green]Rebuilt:[/green] {stats['entities']} entities, {stats['facts']} facts, "
                  f"{stats['decisions']} decisions, {stats['events_processed']} events processed")


@knowledge.command("stats")
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
def knowledge_stats(path):
    """Show knowledge system statistics."""
    from rich.console import Console
    from rich.table import Table

    from gg.knowledge.engine import KnowledgeEngine

    console = Console()
    engine = KnowledgeEngine(path)
    events = engine.get_all_events()
    table = Table(title="Knowledge Stats")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Total events", str(len(events)))

    type_counts: dict[str, int] = {}
    for ev in events:
        type_counts = {**type_counts, ev.event_type.value: type_counts.get(ev.event_type.value, 0) + 1}
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        table.add_row(f"  {t}", str(c))

    console.print(table)


@knowledge.command("search")
@click.argument("query")
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--limit", default=10, help="Max results.")
def knowledge_search(query, path, limit):
    """Search knowledge base."""
    from rich.console import Console
    from rich.table import Table

    from gg.knowledge.engine import KnowledgeEngine

    console = Console()
    engine = KnowledgeEngine(path)
    results = engine.search(query, limit=limit)

    if not results:
        console.print("[yellow]No results found.[/yellow]")
        return

    table = Table(title=f"Search: {query}")
    table.add_column("Type", style="bold", width=8)
    table.add_column("Title", width=25)
    table.add_column("Snippet", width=40)
    table.add_column("Score", width=6)

    for r in results:
        table.add_row(r.kind, r.title, r.snippet[:40], f"{r.score:.2f}")

    console.print(table)


@knowledge.command("context")
@click.argument("issue_title")
@click.option("--body", default="", help="Issue body text.")
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
def knowledge_context(issue_title, body, path):
    """Build knowledge context for an issue (used in agent prompts)."""
    from rich.console import Console
    from rich.markdown import Markdown

    from gg.knowledge.engine import KnowledgeEngine

    console = Console()
    engine = KnowledgeEngine(path)
    ctx = engine.context_for_issue(issue_title, body)
    if ctx:
        console.print(Markdown(ctx))
    else:
        console.print("[yellow]No relevant knowledge found.[/yellow]")


@cli.command()
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--debug", is_flag=True, help="Show Codex output.")
def constitution(path, debug):
    """Regenerate constitution using Codex (takes 2-5 min)."""
    from rich.console import Console

    from gg.agents.codex import CodexAgent
    from gg.analyzers.dependencies import analyze_dependencies
    from gg.analyzers.languages import analyze_languages
    from gg.analyzers.structure import analyze_structure

    console = Console()
    agent = CodexAgent(console=console, debug=debug)
    if not agent.is_available():
        console.print("[red]Codex CLI not found.[/red]")
        raise SystemExit(1)

    console.print("[bold]Generating constitution via Codex...[/bold]")
    console.print("  This sends a compact project summary to Codex (read-only, no file access).")
    console.print("  MCP hooks may add startup time. Please wait.\n")

    root = Path(path).resolve()
    compact = "\n\n".join([
        analyze_languages(root).to_prompt_context(),
        analyze_dependencies(root).to_prompt_context(),
        analyze_structure(root).to_prompt_context(),
    ])

    prompt = (
        "На основе контекста проекта выше, сформулируй набор правил (конституцию) "
        "для разработчика. Включи: стек, архитектуру, стилизацию, управление данными, "
        "практики разработки. Формат: markdown с ## секциями."
    )

    import tempfile
    try:
        raw = agent.generate(prompt, cwd=tempfile.gettempdir(), context=compact, timeout=300)
        if raw:
            gg_dir = root / ".gg"
            gg_dir.mkdir(parents=True, exist_ok=True)
            (gg_dir / "constitution.md").write_text(f"# Project Constitution\n\n{raw}\n", encoding="utf-8")
            console.print(f"\n[green]Constitution written to .gg/constitution.md ({len(raw)} chars)[/green]")
        else:
            console.print("[yellow]Codex returned empty response. Local constitution unchanged.[/yellow]")
    except RuntimeError as e:
        console.print(f"[red]Codex failed: {e}[/red]")
        console.print("Local constitution in .gg/constitution.md is still valid.")


def _build_pipeline(path, *, debug: bool = False, profile: str | None = None):
    from gg.orchestrator.pipeline import OrchestratorPipeline

    if not debug:
        return OrchestratorPipeline(path, profile=profile)

    from rich.console import Console

    from gg.agents.codex import CodexAgent

    return OrchestratorPipeline(path, agent=CodexAgent(console=Console(), debug=True), profile=profile)


@cli.command()
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--dry-run", is_flag=True, help="Show the next eligible issue without side effects.")
@click.option("--no-pr", is_flag=True, help="Run locally without creating a pull request.")
@click.option("--batch", "batch_size", default=1, show_default=True, help="Process up to N eligible issues.")
@click.option("--max-attempts", type=int, default=None, help="Override execution/evaluation attempt limit.")
@click.option("--candidates", type=int, default=None, help="Override initial candidate fanout.")
@click.option("--max-parallel-candidates", type=int, default=None, help="Override parallel candidate limit.")
@click.option("--repair-fanout", type=int, default=None, help="Override repair candidate fanout.")
@click.option("--timeout", type=int, default=None, help="Override candidate timeout in seconds.")
@click.option("--base", default=None, help="Override target default branch for publishing.")
@click.option("--debug", is_flag=True, help="Show Codex output and verbose agent progress.")
@click.option("--profile", default=None, help="Apply a named config profile from params.yaml profiles section.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def run(
    path,
    dry_run,
    no_pr,
    batch_size,
    max_attempts,
    candidates,
    max_parallel_candidates,
    repair_fanout,
    timeout,
    base,
    debug,
    profile,
    as_json,
):
    """Supervisor loop: pick issues and orchestrate agents."""
    import json

    from rich.console import Console

    pipeline = _build_pipeline(path, debug=debug, profile=profile).configure_runtime(
        max_attempts=max_attempts,
        candidates=candidates,
        max_parallel_candidates=max_parallel_candidates,
        repair_fanout=repair_fanout,
        timeout=timeout,
        base=base,
    )
    result = (
        pipeline.run_batch(batch_size=batch_size, dry_run=dry_run, no_pr=no_pr)
        if batch_size > 1
        else pipeline.run_next(dry_run=dry_run, no_pr=no_pr)
    )
    if as_json:
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return
    console = Console()
    console.print(f"[bold]{result.get('state')}[/bold] {result.get('message', '')}")
    if result.get("issue"):
        issue_data = result["issue"]
        console.print(f"Next issue: #{issue_data['number']} {issue_data['title']}")
    if result.get("issues"):
        for issue_data in result["issues"]:
            console.print(f"Next issue: #{issue_data['number']} {issue_data['title']}")
    if result.get("results"):
        for item in result["results"]:
            console.print(f"Run: {item.get('run_id')} state={item.get('state')}")
    if result.get("run_id"):
        console.print(f"Run: {result['run_id']}")
    if result.get("pr_url"):
        console.print(f"PR: {result['pr_url']}")


@cli.command()
@click.argument("issue_number", type=int)
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--dry-run", is_flag=True, help="Analyze only; do not call agent or mutate GitHub.")
@click.option("--no-pr", is_flag=True, help="Run locally without creating a pull request.")
@click.option("--max-attempts", type=int, default=None, help="Override execution/evaluation attempt limit.")
@click.option("--candidates", type=int, default=None, help="Override initial candidate fanout.")
@click.option("--max-parallel-candidates", type=int, default=None, help="Override parallel candidate limit.")
@click.option("--repair-fanout", type=int, default=None, help="Override repair candidate fanout.")
@click.option("--timeout", type=int, default=None, help="Override candidate timeout in seconds.")
@click.option("--base", default=None, help="Override target default branch for publishing.")
@click.option("--label", "labels", multiple=True, help="Additional label to apply to the issue.")
@click.option("--debug", is_flag=True, help="Show Codex output and verbose agent progress.")
@click.option("--profile", default=None, help="Apply a named config profile from params.yaml profiles section.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def issue(
    issue_number,
    path,
    dry_run,
    no_pr,
    max_attempts,
    candidates,
    max_parallel_candidates,
    repair_fanout,
    timeout,
    base,
    labels,
    debug,
    profile,
    as_json,
):
    """Process a single GitHub issue."""
    import json

    from rich.console import Console

    result = _build_pipeline(path, debug=debug, profile=profile).configure_runtime(
        max_attempts=max_attempts,
        candidates=candidates,
        max_parallel_candidates=max_parallel_candidates,
        repair_fanout=repair_fanout,
        timeout=timeout,
        base=base,
    ).run_issue(issue_number, dry_run=dry_run, no_pr=no_pr)
    if as_json:
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return
    console = Console()
    console.print(f"[bold]{result.get('state')}[/bold] run={result.get('run_id')}")
    if result.get("pr_url"):
        console.print(f"PR: {result['pr_url']}")
    if result.get("error"):
        console.print(f"[red]{result['error']['code']}:[/red] {result['error']['message']}")


@cli.command()
@click.argument("run_id", required=False)
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def status(run_id, path, as_json):
    """Show status of active tasks."""
    import json

    from rich.console import Console
    from rich.table import Table

    from gg.orchestrator.pipeline import OrchestratorPipeline

    pipeline = OrchestratorPipeline(path)
    rows = pipeline.status()
    if run_id:
        rows = [row for row in rows if row["run_id"] == run_id]
    if as_json:
        click.echo(json.dumps(rows, indent=2, ensure_ascii=False))
        return

    console = Console()
    if not rows:
        console.print("[yellow]No runs found.[/yellow]")
        return
    table = Table(title="gg runs")
    table.add_column("Run")
    table.add_column("Issue")
    table.add_column("State")
    table.add_column("Updated")
    table.add_column("PR")
    for row in rows:
        issue_data = row.get("issue", {})
        table.add_row(
            row["run_id"],
            f"#{issue_data.get('number', '')}",
            row["state"],
            row["updated_at"],
            row.get("pr_url") or "",
        )
    console.print(table)


@cli.command()
@click.argument("run_id", required=False, default=None)
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--dry-run/--execute", default=True, help="Preview cleanup unless --execute is used.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def clean(run_id, path, dry_run, as_json):
    """Prune terminal run metadata. Pass RUN_ID to clean a specific run."""
    import json

    from rich.console import Console

    from gg.orchestrator.pipeline import OrchestratorPipeline

    result = OrchestratorPipeline(path).clean(dry_run=dry_run, run_id=run_id)
    if as_json:
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return
    console = Console()
    mode = "would remove" if dry_run else "removed"
    console.print(f"{mode} {result['count']} runs")
    for run_id in result["runs"]:
        console.print(f"  {run_id}")
    for run_id in result.get("stale_runs", []):
        console.print(f"  stale: {run_id}")


@cli.command()
@click.argument("run_id")
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--reason", default="operator requested cancellation", help="Cancellation reason.")
@click.option("--abandon-worktrees", is_flag=True, help="Remove worktrees even if candidate still running.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def cancel(run_id, path, reason, abandon_worktrees, as_json):
    """Cancel a non-terminal run."""
    import json

    from rich.console import Console

    from gg.orchestrator.pipeline import OrchestratorPipeline

    result = OrchestratorPipeline(path).cancel(
        run_id,
        reason=reason,
        abandon_worktrees=abandon_worktrees,
    )
    if as_json:
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return
    Console().print(f"{result['state']} run={result['run_id']}")


@cli.command()
@click.argument("run_id")
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--no-pr", is_flag=True, help="Resume locally without creating a pull request.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def resume(run_id, path, no_pr, as_json):
    """Resume a run from durable state."""
    import json

    from rich.console import Console

    from gg.orchestrator.pipeline import OrchestratorPipeline

    result = OrchestratorPipeline(path).resume(run_id, no_pr=no_pr)
    if as_json:
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return
    Console().print(f"{result['state']} run={result['run_id']}")


@cli.command()
@click.argument("run_id")
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--no-pr", is_flag=True, help="Retry locally without creating a pull request.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def retry(run_id, path, no_pr, as_json):
    """Retry a recoverable run from durable state."""
    import json

    from rich.console import Console

    from gg.orchestrator.pipeline import OrchestratorPipeline

    result = OrchestratorPipeline(path).retry(run_id, no_pr=no_pr)
    if as_json:
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return
    Console().print(f"{result['state']} run={result['run_id']}")


@cli.command()
@click.argument("run_id")
@click.option("--message", default=None, help="Answer or clarification for a blocked run.")
@click.option("--file", "input_file", type=click.Path(exists=True), default=None, help="Read input from file.")
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def provide(run_id, message, input_file, path, as_json):
    """Provide local input for a Blocked or NeedsInput run."""
    import json

    from rich.console import Console

    from gg.orchestrator.pipeline import OrchestratorPipeline

    if input_file:
        from pathlib import Path as _Path
        message = (message or "") + _Path(input_file).read_text(encoding="utf-8")
    if not message:
        raise click.UsageError("Provide --message or --file")
    result = OrchestratorPipeline(path).provide(run_id, message=message)
    if as_json:
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return
    Console().print(f"{result['state']} run={result['run_id']} accepted={result['accepted']}")


@cli.command()
@click.option("--path", type=click.Path(exists=True), default=".", help="Project path.")
@click.option("--json", "as_json", is_flag=True, help="Print machine-readable JSON.")
def doctor(path, as_json):
    """Check local orchestrator prerequisites."""
    import json

    from rich.console import Console
    from rich.table import Table

    from gg.orchestrator.doctor import run_doctor

    result = run_doctor(path)
    if as_json:
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return
    console = Console()
    table = Table(title=f"gg doctor: {result['status']}")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Message")
    for check in result["checks"]:
        table.add_row(check["name"], check["status"], check["message"])
    console.print(table)


@cli.command()
@click.argument("pr_number", type=int)
def review(pr_number):
    """Run agentic code review on a PR."""
    click.echo(f"Not implemented yet: review PR #{pr_number}")
