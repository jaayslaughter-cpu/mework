---
name: feature-phase-development
description: Workflow command scaffold for feature-phase-development in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-phase-development

Use this workflow when working on **feature-phase-development** in `mework`.

## Goal

Implements new features or enhancements to the core logic, usually as part of a named 'Phase' (e.g., Phase 105, Phase 110). Typically involves updating multiple core Python files to add new data signals, features, or logic.

## Common Files

- `fangraphs_layer.py`
- `prop_enrichment_layer.py`
- `tasklets.py`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Edit or add logic in fangraphs_layer.py and/or prop_enrichment_layer.py (if feature relates to enrichment or stats)
- Update tasklets.py to wire up the new feature or support new pipeline steps
- Commit all related files together, often with a 'Phase' number in the commit message

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.