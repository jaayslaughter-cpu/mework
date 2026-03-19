---
name: add-command-or-workflow-documentation
description: Workflow command scaffold for add-command-or-workflow-documentation in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /add-command-or-workflow-documentation

Use this workflow when working on **add-command-or-workflow-documentation** in `mework`.

## Goal

Adds or updates workflow documentation or command specs for mework ECC, typically as markdown files describing automated or manual workflows.

## Common Files

- `.claude/commands/add-api-service-module.md`
- `.claude/commands/feature-development.md`
- `.claude/commands/database-migration.md`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Create or update a markdown file in .claude/commands/ describing the workflow or command.
- Commit the file with a message referencing the workflow or command name.

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.