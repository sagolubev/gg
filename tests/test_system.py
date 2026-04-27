from unittest.mock import patch

from rich.console import Console

from gg.commands.init import _select_init_backend
from gg.utils.system import (
    CheckResult,
    check_claude,
    check_codex,
    check_git,
    check_glab,
    check_python_version,
)


def test_check_python_version():
    result = check_python_version()
    assert result.ok is True
    assert result.name == "python"
    assert result.required is True


def test_check_git_found():
    result = check_git()
    assert result.name == "git"
    assert result.required is True


@patch("gg.utils.system.shutil.which", return_value=None)
def test_check_codex_not_found(mock_which):
    result = check_codex()
    assert result.ok is False
    assert result.name == "codex"
    assert "npm install" in result.install_hint


@patch("gg.utils.system.shutil.which", return_value=None)
def test_check_claude_not_found(mock_which):
    result = check_claude()
    assert result.ok is False
    assert result.name == "claude"
    assert "@anthropic-ai/claude-code" in result.install_hint


@patch("gg.utils.system.shutil.which", return_value=None)
def test_check_glab_not_found(mock_which):
    result = check_glab()
    assert result.ok is False
    assert result.name == "glab"
    assert result.install_hint != ""


def test_check_result_frozen():
    r = CheckResult("test", True, "ok", required=False)
    try:
        r.name = "changed"  # type: ignore[misc]
        assert False, "Should be frozen"
    except AttributeError:
        pass


def test_select_init_backend_prefers_only_available_backend():
    backend = _select_init_backend(
        requested="auto",
        check_map={
            "codex": CheckResult("codex", False, "missing", required=False),
            "claude": CheckResult("claude", True, "ok", required=False),
        },
        skip_agent=False,
        non_interactive=True,
        console=Console(),
    )

    assert backend == "claude"


def test_select_init_backend_prefers_codex_when_both_available_non_interactive():
    backend = _select_init_backend(
        requested="auto",
        check_map={
            "codex": CheckResult("codex", True, "ok", required=False),
            "claude": CheckResult("claude", True, "ok", required=False),
        },
        skip_agent=False,
        non_interactive=True,
        console=Console(),
    )

    assert backend == "codex"


def test_select_init_backend_returns_local_only_when_skipped():
    backend = _select_init_backend(
        requested="auto",
        check_map={
            "codex": CheckResult("codex", True, "ok", required=False),
            "claude": CheckResult("claude", True, "ok", required=False),
        },
        skip_agent=True,
        non_interactive=True,
        console=Console(),
    )

    assert backend == ""


@patch("gg.commands.init.Prompt.ask", return_value="2")
def test_select_init_backend_uses_numeric_prompt_choice(mock_prompt):
    backend = _select_init_backend(
        requested="auto",
        check_map={
            "codex": CheckResult("codex", True, "ok", required=False),
            "claude": CheckResult("claude", True, "ok", required=False),
        },
        skip_agent=False,
        non_interactive=False,
        console=Console(),
    )

    assert backend == "claude"
    prompt_text = mock_prompt.call_args.kwargs.get("console") and mock_prompt.call_args.args[0]
    assert "1. codex" in prompt_text
    assert "2. claude" in prompt_text
    assert mock_prompt.call_args.kwargs["choices"] == ["1", "2"]
    assert mock_prompt.call_args.kwargs["default"] == "1"
