---
name: bugfix-or-hotfix-core-pipeline
description: Workflow command scaffold for bugfix-or-hotfix-core-pipeline in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /bugfix-or-hotfix-core-pipeline

Use this workflow when working on **bugfix-or-hotfix-core-pipeline** in `mework`.

## Goal

Fixes bugs or hotfixes in the core pipeline, often related to grading, recap, or Discord alert logic.

## Common Files

- `tasklets.py`
- `nightly_recap.py`
- `DiscordAlertService.py`
- `orchestrator.py`
- `season_record.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Identify and fix bug in tasklets.py, nightly_recap.py, or DiscordAlertService.py.
- If the bug affects orchestration, update orchestrator.py.
- If the bug is related to season records or recap, update season_record.py.
- Commit and merge fix, often with a descriptive message.

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.