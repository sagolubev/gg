from __future__ import annotations

from gg.orchestrator.verification import (
    VerificationCommand,
    VerificationRunner,
    required_gate_passes,
)


def test_required_failed_blocks(tmp_path):
    command = VerificationCommand(
        id="required-fail",
        category="test",
        command="python -c 'raise SystemExit(1)'",
        required=True,
    )

    result = VerificationRunner([command], timeout=5).run(tmp_path)[0]

    assert result.status == "failed"
    assert result.required is True
    assert required_gate_passes([result]) is False


def test_advisory_failed_persists_but_required_gate_passes(tmp_path):
    command = VerificationCommand(
        id="advisory-fail",
        category="lint",
        command="python -c 'raise SystemExit(1)'",
        required=False,
    )

    result = VerificationRunner([command], timeout=5).run(tmp_path)[0]

    assert result.status == "failed"
    assert result.required is False
    assert required_gate_passes([result]) is True


def test_output_truncation_metadata(tmp_path):
    command = "python -c \"print('x' * 50)\""

    result = VerificationRunner([command], timeout=5, max_output_chars=10).run(tmp_path)[0]

    assert result.status == "passed"
    assert result.stdout == "xxxxxxxxx\n"
    assert result.stderr == ""
    assert result.truncated is True
    assert result.duration_ms is not None
    assert result.to_dict()["truncated"] is True


def test_flaky_retry_still_works(tmp_path):
    command = (
        "python -c \"from pathlib import Path; "
        "p=Path('attempts.txt'); "
        "n=int(p.read_text() if p.exists() else '0'); "
        "p.write_text(str(n + 1)); "
        "raise SystemExit(1 if n == 0 else 0)\""
    )

    result = VerificationRunner([command], timeout=5, retry_count=1).run(tmp_path)[0]

    assert result.status == "flaky"
    assert result.flaky is True
    assert result.attempts == 2


def test_secret_scan_creates_finding(tmp_path):
    command = VerificationCommand(
        id="secret-scan",
        category="security",
        command="python -c \"print('OPENAI_API_KEY=sk-testsecret000000000000')\"",
        parser="secret-scan",
    )

    result = VerificationRunner([command], timeout=5).run(tmp_path)[0]

    assert result.status == "failed"
    assert result.findings
    assert result.findings[0]["type"] == "secret"
    assert result.findings[0]["parser"] == "secret-scan"
    assert "<redacted>" in result.findings[0]["evidence"]
