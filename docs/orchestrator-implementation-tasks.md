# Orchestrator Implementation Tasks

## P0 Walking Skeleton

- [x] Add durable run state and artifact storage under `.gg/runs/<run_id>/`.
- [x] Add a minimal config loader for `.gg/params.yaml` with safe defaults.
- [x] Add issue analysis that creates a bounded `task-brief.json`.
- [x] Add single-candidate execution in a git worktree.
- [x] Add verification command execution and persisted evidence.
- [x] Wire `gg issue <number>` through issue claim, analysis, one candidate, verification, PR publishing, and final status.
- [x] Wire `gg status` and `gg run --dry-run`.
- [x] Cover the walking skeleton with fake platform/agent tests.

## P1 Target MVP

- [x] Add `SandboxRuntime` wrapper around `sandbox-runtime` / `srt-py`.
- [x] Add bounded fanout via `runtime.candidates` and `runtime.max_parallel_candidates`.
- [x] Add candidate result schemas and evaluator selection.
- [x] Add repair loop with one repair candidate by default.
- [x] Add baseline verification and comparison semantics.
- [x] Add idempotent publishing resume.
- [x] Add `gg resume`, `gg retry`, `gg cancel`, and `gg clean --dry-run`.

## P2 Reliability

- [x] Add SQLite WAL rate-limit store.
- [x] Add CAS-backed context snapshots.
- [x] Add robust worktree cleanup and orphan scan.
- [x] Add richer signal handling and zombie process cleanup.
- [x] Add `gg provide` for local answers to `Blocked` / `NeedsInput`.

## Implemented Run Artifacts / Observability

- [x] Persist `state.json` plus append-only `pipeline.jsonl`, `errors.jsonl`, and `cost.jsonl` under `.gg/runs/<run_id>/`.
- [x] Record state transitions with reasons, candidate status changes, publishing-step changes, and tracked artifact updates in `pipeline.jsonl`.
- [x] Record candidate execution/verification metrics in `cost.jsonl` (durations, changed files, verification outcome, failed commands).
- [x] Redact common token patterns in observability logs before they are written to disk.
- [x] Materialize `artifacts/run-summary.json` as a single redacted snapshot of run status, artifacts, candidates, and log paths.

## P3 Production

- [x] Add plugin interfaces for task systems and executor backends.
- [x] Add audit hashing / external audit sink options.
- [x] Add advanced LFS, supply-chain, and compliance policies.
- [ ] Add daemon/parallel batch mode with shared resource budgets.
