---
name: feature-phase-development
description: Workflow command scaffold for feature-phase-development in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-phase-development

Use this workflow when working on **feature-phase-development** in `mework`.

## Goal

Implements a new feature or phase in the pipeline, often adding new data sources, signals, or enrichment logic.

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

- Modify or add logic in fangraphs_layer.py, prop_enrichment_layer.py, and/or tasklets.py to implement the feature.
- Commit changes with a message referencing the phase or feature.
- Optionally, merge branch or pull request into main.

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.