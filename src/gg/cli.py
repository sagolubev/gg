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


@cli.command()
def run():
    """Supervisor loop: pick issues and orchestrate agents."""
    click.echo("Not implemented yet.")


@cli.command()
@click.argument("issue_number", type=int)
def issue(issue_number):
    """Process a single GitHub issue."""
    click.echo(f"Not implemented yet: issue #{issue_number}")


@cli.command()
def status():
    """Show status of active tasks."""
    click.echo("Not implemented yet.")


@cli.command()
@click.argument("pr_number", type=int)
def review(pr_number):
    """Run agentic code review on a PR."""
    click.echo(f"Not implemented yet: review PR #{pr_number}")
