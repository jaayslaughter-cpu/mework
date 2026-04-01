---
name: bugfix-or-pipeline-fix
description: Workflow command scaffold for bugfix-or-pipeline-fix in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /bugfix-or-pipeline-fix

Use this workflow when working on **bugfix-or-pipeline-fix** in `mework`.

## Goal

Fixes bugs or pipeline issues, often related to grading, recap, math, or date/time logic. These fixes frequently involve tasklets.py and sometimes other pipeline-related files.

## Common Files

- `tasklets.py`
- `DiscordAlertService.py`
- `calibration_layer.py`
- `nightly_recap.py`
- `orchestrator.py`
- `season_record.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Identify the bug or issue and locate the relevant logic in tasklets.py or related files.
- Edit tasklets.py to fix the bug (e.g., grading, recap, math, query filters).
- If the bug affects other modules (e.g., DiscordAlertService.py, calibration_layer.py), update those as well.
- Commit with a message referencing the fix and affected area.

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.