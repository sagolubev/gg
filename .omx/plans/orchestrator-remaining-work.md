# Remaining orchestrator work after `359329f`

## Current evidence snapshot

Observed in the current `feat/implementation` tree at `359329f` (no additional local changes in this worktree):

- **Sandbox wrapper exists, but policy wiring is still stubbed.** `SandboxPolicy` is hard-coded and `CandidateExecutor` never passes project config into it; `params.yaml` exposes only on/off flags, not policy or network/filesystem rules. (`src/gg/orchestrator/sandbox.py`, `src/gg/orchestrator/executor.py`, `src/gg/orchestrator/config.py`, `src/gg/commands/init.py`)
- **Parallel candidate fanout exists, but concurrency control is incomplete.** Candidate batches use `ThreadPoolExecutor`, but there is no early-stop, shared budget coordination, or durable per-run locking around result aggregation / cleanup. (`src/gg/orchestrator/pipeline.py`)
- **Locking is too narrow for end-to-end issue exclusivity.** `run_issue()` holds the per-issue lock only through analysis, then releases it before candidate execution/publishing, so a second process can race the same issue. (`src/gg/orchestrator/pipeline.py`, `src/gg/orchestrator/lock.py`)
- **Rate-limit storage exists but is not integrated.** `RateLimitStore` is implemented and tested, but nothing in pipeline/platform code reads/writes it. (`src/gg/orchestrator/rate_limit.py`; no call sites outside tests)
- **Issue context ingestion is still body-only.** Platform issue fetches exclude comments/timeline data, and `TaskAnalyzer` only summarizes title/body plus knowledge context. (`src/gg/platforms/base.py`, `src/gg/platforms/github.py`, `src/gg/orchestrator/task_analysis.py`)
- **Observability/cost data is still placeholder-level.** `cost.jsonl` writes `total_usd=None` and `token_usage=None`; sandbox/publish side effects are not fully captured as structured artifacts/events. (`src/gg/orchestrator/pipeline.py`, `src/gg/orchestrator/store.py`)
- **Docs overstate a few items.** `docs/orchestrator-implementation-tasks.md` marks sandbox wiring, rate-limit work, richer signal handling, and `gg provide` as complete, but `NeedsInput` is never produced, rate limits are unused, and signal handling is still mostly `KeyboardInterrupt` recovery. (`src/gg/orchestrator/state.py`, `src/gg/orchestrator/pipeline.py`, `src/gg/orchestrator/rate_limit.py`)

## Execution plan

### Phase 1 — Reconcile docs with actual behavior
**Goal:** Stop planning against false positives.

Tasks:
1. Audit `docs/orchestrator-implementation-tasks.md` against current code and downgrade partial items to follow-up tasks.
2. Add a short “implemented vs partial vs not started” note for sandbox wiring, rate limits, signal handling, `gg provide`/`NeedsInput`, and logging/costs.
3. Add regression tests that lock the intended semantics before implementation work expands.

Acceptance criteria:
- Checklist status matches real code paths.
- New tests fail on the known gaps and define the target behavior.

### Phase 2 — Finish sandbox/runtime wiring
**Goal:** Make sandboxing configurable and auditable instead of best-effort.

Tasks:
1. Extend runtime config / `.gg/params.yaml` with sandbox policy fields (network allow/deny, read/write rules, optional mode knobs).
2. Thread resolved sandbox policy from config into `CandidateExecutor` -> `SandboxRuntime.run()`.
3. Persist sandbox settings/result artifacts per candidate so a failed run can be debugged after the fact.
4. Tighten tests around required-sandbox mode, policy serialization, and fallback behavior.

Acceptance criteria:
- A project can change sandbox policy without code edits.
- Candidate artifacts show the effective sandbox config/result used for each run.
- `require_sandbox_runtime` and custom policy cases are covered by tests.

### Phase 3 — Close locking and parallel execution gaps
**Goal:** Make concurrent orchestrator runs safe.

Tasks:
1. Hold issue exclusivity through execution/publish, or introduce a durable claimed-run sentinel checked on resume/retry/run-next.
2. Add store/worktree coordination for candidate result writes, cleanup, and cancellation paths.
3. Teach parallel candidate execution to short-circuit on a winning candidate or hard-fail conditions instead of always waiting for all futures.
4. Add concurrency tests for duplicate issue pickup, cancel-during-fanout, and cleanup races.

Acceptance criteria:
- Two orchestrators cannot execute/publish the same issue concurrently.
- Parallel fanout respects configured limits and cancels/ignores losing work safely.
- Cleanup/resume remains deterministic under concurrent activity.

### Phase 4 — Integrate rate limits and shared budgets
**Goal:** Prevent self-induced throttling across issue polling, comments, labels, PR creation, and retries.

Tasks:
1. Wrap GitHub/GitLab API/CLI touchpoints with rate-limit reads/writes and bucket naming.
2. Record reset windows in `RateLimitStore` and make `run_next` / publish paths back off before making doomed calls.
3. Surface throttle state in run artifacts/state/comment text when a run must wait or block.
4. Add tests for bucket updates, throttle decisions, and resume-after-reset behavior.

Acceptance criteria:
- Platform operations update/read rate-limit buckets.
- The pipeline can block/defer instead of hammering the platform after exhaustion.
- Throttle state is visible in persisted run data.

### Phase 5 — Ingest richer issue context and make `gg provide` real
**Goal:** Give the agent the full discussion, and make the `NeedsInput` path executable.

Tasks:
1. Extend platform issue fetches to collect recent comments / discussion metadata and fold them into `Issue` + task brief artifacts.
2. Capture structured comment/context snapshots so reruns are reproducible.
3. Introduce explicit `NeedsInput` transitions for missing clarifications, and make `gg provide` resume the exact blocked stage.
4. Add tests covering comment ingestion, clarification prompts, and resumed execution after operator input.

Acceptance criteria:
- Task briefs include bounded comment/context history, not just issue body.
- `NeedsInput` is reachable from real execution paths.
- `gg provide` unblocks the run with deterministic artifact trail.

### Phase 6 — Upgrade observability and cost accounting
**Goal:** Make runs explainable and measurable.

Tasks:
1. Replace placeholder cost records with actual token/runtime/API usage fields where available.
2. Log candidate start/finish, lock waits, throttle decisions, sandbox results, publish side effects, and cancellation events as structured events.
3. Add a small run summary artifact that points to winner, loser reasons, verification deltas, and publish outcome.
4. Backfill tests for secret masking and artifact completeness.

Acceptance criteria:
- `cost.jsonl` is no longer mostly `None` placeholders.
- `pipeline.jsonl`/artifacts provide enough data to debug a failed run without re-running it.
- Secret masking still holds across the richer logging surface.

## Recommended implementation order

1. **Phase 1** (doc/test truth)  
2. **Phase 3** (locking/concurrency correctness)  
3. **Phase 2** (sandbox policy wiring)  
4. **Phase 4** (rate-limit integration)  
5. **Phase 5** (comment/context + `NeedsInput`)  
6. **Phase 6** (observability/cost completeness)

Rationale: locking/concurrency bugs can corrupt runs today; sandbox and rate limits are next because they affect safety/stability; richer context and observability become much easier once execution semantics are stable.
