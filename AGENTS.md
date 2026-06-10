# Crucible Development Agents

This file inherits coding, git hygiene, and notification discipline from
`../nnue/AGENTS.md` and `../enyo/AGENTS.md`. If rules conflict, use the stricter
rule.

## 1. Coding Agent

Owns Crucible tooling, CLI, docs, and focused tests only.

### Responsibilities

- Keep changes narrow and tied to the user's explicit objective.
- Make user-facing workflows simple without hiding advanced capability.
- Prefer repo-owned commands, manifests, examples, and tests over ad hoc shell
  sequences.
- Keep durable tooling changes separate from worker-config changes,
  documentation-only changes, generated runs, and experiments.
- Preserve fast, predictable behavior in worker hot paths: task claiming,
  status reads, worker loops, output sync, and deploy orchestration.

### Rules

- Before editing, read this file and inspect `git status --short --branch`,
  `git diff --name-only`, and the relevant files.
- If unrelated dirty files are present, stop and report the dirty scope before
  adding more changes unless the user explicitly authorizes work in that dirty
  tree.
- Change one concern at a time: CLI/API behavior, worker orchestration,
  manifest schema, docs/help text, worker config, or tests.
- Do not sync to remote hosts until the local diff is scoped, verified, and
  shown.
- Do not run push, cleanup, restore, delete, stash, reset, checkout, or remote sync
  commands as a recovery action without explicit approval.

## 2. Validation Agent

Owns verification only.

### Responsibilities

- Run focused CLI, manifest, worker, and status tests for the changed surface.
- Report exact pass/fail commands and numbers.
- Do not modify source except generated validation outputs explicitly requested
  by the Coding Agent.
- Do not commit.

### Validation Order

1. Validate parser/help/docs changes with focused CLI tests.
2. Validate manifest/worker behavior with focused unit tests.
3. Run the full test suite when code behavior changes.
4. For distributed behavior, prove a small run on the intended coordinator and
   workers before declaring the workflow usable.

Reject immediately if:

- advanced/internal commands are accidentally removed;
- worker output collection or status regressions are detected;
- generated run artifacts are modified as part of a source change.

## 3. Git / Release Agent

Owns git operations only.

### Responsibilities

- Create branches, stage files, commit, rebase, merge, tag, and push only when
  explicitly instructed.
- Enforce clean history and correct commit identity.
- Never resolve source conflicts without handing back to the Coding Agent.

### Rules

- Never commit directly to `main`.
- Never create merge commits unless explicitly requested.
- Never rewrite `main`.
- Never use bot authors or AI co-author trailers.
- Verify commit identity before committing:

```sh
git config user.name "Petter Wahlman"
git config user.email "petter@wahlman.no"
```

- Start every mergeable feature from current `origin/main`.
- The branch creation ahead count must be `0`.
- Before merge, run the contamination check:

```sh
git rev-list --count origin/main..HEAD
git diff --stat origin/main..HEAD
git diff --name-only origin/main..HEAD
```

- Stop if the count or file list contains unrelated feature work, worker-config
  churn, generated runs, experiments, or local machine state.
- Rebase feature branches onto `origin/main`; merge to `main` with `--ff-only`.
- Stage only files that belong to the requested change.
- Keep one feature, fix, or experiment in one logical commit whenever practical.
- Squash local fixup churn before merge.
- Do not commit generated Crucible run artifacts.

## Shared Documentation Rules

- Documentation is part of user-facing CLI changes.
- If a command is meant to simplify the workflow, update top-level help,
  subcommand help, README/runbook examples, and tests for discoverability.
- Keep public docs practical: what to run, where outputs land, how to monitor,
  how to stop, and how to judge success.
- Keep public docs free of machine-specific hostnames, private paths, and
  private operational notes.

## Shared Command Output Rules

- All command output must be visible in an existing `nnue_native` tmux pane.
- Do not create or modify tmux sessions, windows, or panes to satisfy this rule
  unless explicitly requested.
- If no suitable pane exists, stop and ask before running commands.
- Avoid nested shells, heredocs, pagers, and stale tails in tmux panes. If one is
  accidentally created, stop it and leave the pane at a clean prompt.

## Shared Long-Run And Notification Rules

- MANDATORY NOTIFICATION RULE: every long-running Crucible job must be launched
  with completion/failure notifications already attached through
  `--notify-command` or `CRUCIBLE_NOTIFY_COMMAND`. There are no exceptions for
  `start`, `resume`, `retry`, `deploy`, `wait`, or ad hoc distributed runs.
- Polling, watcher scripts, `sleep` loops, tmux tails, or manual status checks
  are not substitutes for launch-time notifications. They may only be used for
  diagnosis after the notification hook is already attached.
- Run long Crucible jobs in tmux and launch distributed jobs from a coordinator
  host that workers can SSH back to.
- Every long-running Crucible job must have completion and failure
  notifications through the repo's notification hook or `--notify-command`.
- Use `notifai.sh`/agent wakeup paths for actionable `done` and `fail` events.
  Do not use raw unauthenticated `curl` calls to agent topics.
- Avoid phase spam. Send concise summaries of status, conclusions, failures,
  rejected lanes, and next actions.
- Every long run must have explicit continuation state: run name, workers,
  success criteria, output paths, next action on success, and next action on
  failure.
- Do not stop at job launch when the next safe monitoring or verification step
  is known.
- Remove temporary tmux windows/panes when the job is done.
