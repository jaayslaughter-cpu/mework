---
name: bugfix-or-pipeline-fix
description: Workflow command scaffold for bugfix-or-pipeline-fix in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /bugfix-or-pipeline-fix

Use this workflow when working on **bugfix-or-pipeline-fix** in `mework`.

## Goal

Fixes bugs or issues in the data pipeline, often related to calculations, data mismatches, or logic errors. May also include hotfixes for production issues.

## Common Files

- `tasklets.py`
- `calibration_layer.py`
- `DiscordAlertService.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Identify and fix the bug in the relevant Python file(s) (commonly tasklets.py, calibration_layer.py, DiscordAlertService.py, etc.)
- If needed, update related files to ensure consistency (e.g., update both calibration_layer.py and tasklets.py for payout logic)
- Commit the fix with a descriptive message (often includes 'Fix', 'Hotfix', or 'bug' in the message)

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.