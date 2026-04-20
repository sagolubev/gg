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
@click.option("--non-interactive", is_flag=True, help="No interactive prompts.")
def init(path, force, skip_codex, non_interactive):
    """Initialize project: analyze codebase, generate specs and knowledge."""
    from gg.commands.init import run_init

    run_init(path=path, force=force, skip_codex=skip_codex, non_interactive=non_interactive)


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
