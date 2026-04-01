---
name: bugfix-or-pipeline-fix
description: Workflow command scaffold for bugfix-or-pipeline-fix in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /bugfix-or-pipeline-fix

Use this workflow when working on **bugfix-or-pipeline-fix** in `mework`.

## Goal

Fixes bugs or issues in the pipeline, such as math errors, logic bugs, or incorrect data handling.

## Common Files

- `tasklets.py`
- `calibration_layer.py`
- `DiscordAlertService.py`
- `orchestrator.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Identify and fix the bug in the relevant file(s), commonly tasklets.py, calibration_layer.py, DiscordAlertService.py, or orchestrator.py.
- Commit the fix, often referencing the bug or issue in the commit message.
- Optionally, merge branch or pull request into main.

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.