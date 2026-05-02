# GG Diagram Design

Этот документ показывает, как `gg` работает как система: от выбора задачи в трекере до публикации PR, восстановления и отчета по сохраненным артефактам.

## Карта Системы

```mermaid
flowchart LR
    Operator["Оператор / cron / watch mode"] --> CLI["gg CLI"]

    CLI --> Init["gg init"]
    CLI --> Run["gg run / gg issue"]
    CLI --> Recovery["gg resume / retry / provide / cancel"]
    CLI --> ReadOnly["gg status / report / doctor / clean"]
    CLI --> KnowledgeCmd["gg knowledge *"]
    CLI --> Review["gg review"]

    Init --> LocalConfig[".gg/params.yaml<br/>.gg/constitution.md<br/>.gg/knowledge/*"]

    Run --> Pipeline["OrchestratorPipeline"]
    Recovery --> Pipeline
    ReadOnly --> Store

    Pipeline --> Platform["GitHub / GitLab platform adapter"]
    Pipeline --> Store["RunStore<br/>.gg/runs/&lt;run_id&gt;"]
    Pipeline --> Knowledge["KnowledgeEngine<br/>events, search, repair lessons"]
    Pipeline --> Routing["phase routing<br/>analysis / execution / repair / evaluation / final_verification"]

    Routing --> Agents["AgentBackend<br/>Codex / Claude"]
    Pipeline --> Executor["CandidateExecutor"]
    Executor --> Worktrees["isolated git worktrees<br/>.gg-worktrees/*"]
    Executor --> Agents
    Executor --> Sandbox["sandbox-runtime<br/>or direct execution"]
    Executor --> Verify["VerificationRunner<br/>setup / test / lint / typecheck / security / custom"]

    Pipeline --> Evaluator["CandidateEvaluator"]
    Verify --> Evaluator
    Evaluator --> Pipeline

    Pipeline --> Publish["publish winner"]
    Publish --> Platform
    Publish --> IntegrationWT["integration worktree"]
    Publish --> GitRemote["git remote branch / PR"]

    Store --> Report["gg report"]
```

## Основной Запуск

```mermaid
sequenceDiagram
    autonumber
    actor Op as Operator
    participant CLI as gg CLI
    participant P as OrchestratorPipeline
    participant TS as GitHub/GitLab
    participant S as RunStore (.gg/runs)
    participant K as KnowledgeEngine
    participant A as TaskAnalyzer
    participant E as CandidateExecutor
    participant V as VerificationRunner
    participant CE as CandidateEvaluator

    Op->>CLI: gg run / gg issue N
    CLI->>P: build pipeline + runtime overrides
    P->>TS: list/get issue
    P->>S: create state.json
    P->>TS: claim task, move board to in progress
    P->>K: record issue picked
    P->>A: build task brief from issue, comments, inputs, repo context
    A->>S: artifacts/task-brief-vN.json

    alt brief is blocked
        P->>S: state=Blocked, blocked_resume_state=TaskAnalysis
        P->>TS: mark issue blocked
    else brief is ready
        P->>S: state=ReadyForExecution
        P->>P: route execution backend/model/effort
        P->>S: sandbox/resource/baseline preflight artifacts
        P->>E: run candidate batch
        par candidate 1
            E->>S: candidates/candidate-1/agent-handoff.json
            E->>E: create isolated worktree
            E->>E: call Codex/Claude backend
            E->>V: run configured checks
            V->>S: candidates/candidate-1/verification.json
            E->>S: candidates/candidate-1/candidate-result.json + patch.diff
        and candidate N
            E->>S: candidate artifacts
        end
        P->>CE: evaluate candidate records
        CE->>S: artifacts/evaluation.json + candidate-selection.json
    end
```

## Candidate Fanout And Repair Loop

```mermaid
flowchart TD
    Ready["ReadyForExecution"] --> Select["AgentSelection<br/>route execution backend"]
    Select --> Preflight["sandbox + resource + optional baseline checks"]
    Preflight --> Running["AgentRunning"]

    Running --> Fanout{"candidate fanout"}
    Fanout --> C1["candidate-1 worktree"]
    Fanout --> C2["candidate-2 worktree"]
    Fanout --> CN["candidate-N worktree"]

    C1 --> Verify1["verification + policy checks"]
    C2 --> Verify2["verification + policy checks"]
    CN --> VerifyN["verification + policy checks"]

    Verify1 --> Eval["ResultEvaluation"]
    Verify2 --> Eval
    VerifyN --> Eval

    Eval -->|winner| Publish["OutcomePublishing"]
    Eval -->|agent asks for clarification| NeedsInput["NeedsInput"]
    Eval -->|no winner, attempts left| Repair["repair context + repair candidates"]
    Eval -->|no winner, no attempts left| Failure["TerminalFailure"]

    NeedsInput -->|gg provide| Running
    Repair -->|optional high-effort escalation| Running
    Publish --> Complete["Completed"]
```

Правило отбора простое: кандидат должен не только успешно отработать агентом, но и пройти verification gate. Если проверка меняет worktree, ломаются обязательные команды или нарушается policy, кандидат считается failed даже при `status=success` от агента.

## State Machine

```mermaid
stateDiagram-v2
    [*] --> ExternalTaskReady
    ExternalTaskReady --> Claiming
    Claiming --> Queued
    Queued --> RunStarted
    RunStarted --> TaskAnalysis

    TaskAnalysis --> Blocked: missing info / budget / backend blocker
    Blocked --> TaskAnalysis: gg provide / resume
    Blocked --> AgentSelection: resume to execution

    TaskAnalysis --> ReadyForExecution
    ReadyForExecution --> AgentSelection
    AgentSelection --> AgentRunning
    AgentSelection --> Blocked: missing agent / sandbox / disk
    AgentSelection --> TerminalFailure

    AgentRunning --> ResultEvaluation
    AgentRunning --> NeedsInput
    AgentRunning --> TerminalFailure

    ResultEvaluation --> AgentRunning: repair / escalation
    ResultEvaluation --> NeedsInput
    ResultEvaluation --> OutcomePublishing: winner selected
    ResultEvaluation --> TerminalFailure

    NeedsInput --> AgentRunning: gg provide
    NeedsInput --> TerminalFailure

    OutcomePublishing --> Completed
    OutcomePublishing --> TerminalFailure

    Claiming --> Cancelled
    Queued --> Cancelled
    RunStarted --> Cancelled
    TaskAnalysis --> Cancelled
    Blocked --> Cancelled
    ReadyForExecution --> Cancelled
    AgentSelection --> Cancelled
    AgentRunning --> Cancelled
    ResultEvaluation --> Cancelled
    NeedsInput --> Cancelled
    OutcomePublishing --> Cancelled

    Completed --> [*]
    TerminalFailure --> [*]
    Cancelled --> [*]
```

## Publish Flow

```mermaid
sequenceDiagram
    autonumber
    participant P as OrchestratorPipeline
    participant S as RunStore
    participant G as Git
    participant TS as GitHub/GitLab
    participant K as KnowledgeEngine

    P->>S: state=OutcomePublishing
    P->>G: fetch default branch + verify base reachability
    P->>G: prepare integration target and apply winner patch
    P->>G: run integration verification

    alt PR mode
        P->>G: commit integration worktree
        P->>G: push branch
        P->>TS: find or create PR
        P->>TS: move board to in review
        P->>K: record PR created
        P->>S: write run-outcome.json + final-verification.json
        P->>TS: publish outcome comment
        P->>TS: swap work label to in-review label
    else --no-pr mode
        P->>S: write run-outcome.json + final-verification.json
        P->>TS: mark issue done
    end

    P->>G: cleanup integration worktree
    P->>S: state=Completed, publishing_step=completed
```

`OutcomePublishing` intentionally stores `publishing_step`. If the process dies after commit, push, PR creation, or comment publication, `gg resume <run_id>` can continue from the last durable side-effect boundary instead of repeating the whole run.

## Durable Artifacts

```mermaid
flowchart TB
    RunDir[".gg/runs/&lt;run_id&gt;/"] --> State["state.json<br/>current state, attempts, candidates, PR URL"]
    RunDir --> Events["pipeline.jsonl<br/>state transitions, artifact updates, publish steps"]
    RunDir --> Errors["errors.jsonl"]
    RunDir --> Cost["cost.jsonl"]
    RunDir --> Artifacts["artifacts/"]
    RunDir --> Candidates["candidates/&lt;candidate_id&gt;/"]

    Artifacts --> Brief["task-brief-vN.json"]
    Artifacts --> RawIssue["raw-issue-vN.json"]
    Artifacts --> Context["context-snapshot-vN.json"]
    Artifacts --> Preflight["sandbox/resource preflight"]
    Artifacts --> Selection["candidate-selection.json"]
    Artifacts --> Evaluation["evaluation.json"]
    Artifacts --> Final["final-verification.json"]
    Artifacts --> Outcome["run-outcome.json"]
    Artifacts --> Resume["resume-plan-vN.json"]

    Candidates --> Handoff["agent-handoff.json"]
    Candidates --> AgentResult["agent-result.json"]
    Candidates --> CandidateResult["candidate-result.json"]
    Candidates --> Patch["patch.diff"]
    Candidates --> Verification["verification.json"]

    State --> Report["gg report"]
    Events --> Report
    Evaluation --> Report
    Final --> Report
    Cost --> Report
```

## Recovery And Operator Commands

```mermaid
flowchart LR
    Status["gg status"] --> Inspect["inspect active and terminal runs"]
    Report["gg report &lt;run_id&gt;"] --> Inspect

    Inspect --> Blocked{"state?"}
    Blocked -->|Blocked / NeedsInput| Provide["gg provide &lt;run_id&gt; --message ..."]
    Blocked -->|recoverable interrupted run| Resume["gg resume &lt;run_id&gt;"]
    Blocked -->|failed but retryable| Retry["gg retry &lt;run_id&gt;"]
    Blocked -->|should stop| Cancel["gg cancel &lt;run_id&gt;"]
    Blocked -->|terminal and old| Clean["gg clean --dry-run / --execute"]

    Provide --> ResumePath["resume from blocked_resume_state"]
    Resume --> ResumePlan["artifacts/resume-plan-vN.json"]
    Retry --> ResumePlan
    ResumePlan --> Pipeline["continue orchestrator phase"]
```

## Design Invariants

- `state.json` is the source of truth for the current run state.
- All non-trivial decisions write durable artifacts before the next side effect.
- Candidate work happens in isolated worktrees; publish happens through a separate integration target.
- Verification is part of candidate validity, not a postscript.
- Resume is orchestrator-level recovery: it reuses durable artifacts and reruns interrupted candidate work when needed; it does not promise continuation of a live LLM session.
- PR-backed runs move work to review, while `--no-pr` runs can mark the external task done directly.
- Report/status commands are read-only projections over durable state and artifacts.

## Code Map

- `src/gg/cli.py`: command surface and runtime flag wiring.
- `src/gg/orchestrator/pipeline.py`: main state machine, issue claiming, execution, evaluation, publishing, resume/retry/provide/cancel.
- `src/gg/orchestrator/executor.py`: candidate worktree execution, agent handoff/result models, sandbox preflight.
- `src/gg/orchestrator/verification.py`: configured command execution and verification gate summary.
- `src/gg/orchestrator/evaluation.py`: candidate scoring and winner/repair/input decision.
- `src/gg/orchestrator/store.py`: durable run store, artifact writing, event logging, cost aggregation.
- `src/gg/orchestrator/report.py`: read-only report builder from durable artifacts.
- `src/gg/orchestrator/state.py`: allowed task states and transitions.
- `src/gg/orchestrator/config.py`: `.gg/params.yaml` schema and phase routing.
- `src/gg/knowledge/*`: issue history, repair lessons, search, and context generation.
