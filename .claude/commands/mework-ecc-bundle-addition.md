---
name: mework-ecc-bundle-addition
description: Workflow command scaffold for mework-ecc-bundle-addition in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /mework-ecc-bundle-addition

Use this workflow when working on **mework-ecc-bundle-addition** in `mework`.

## Goal

Adds or updates the mework ECC bundle, including commands, skills, identity, and tool configuration files for the mework agent ecosystem.

## Common Files

- `.claude/commands/bugfix-or-pipeline-fix.md`
- `.claude/commands/feature-phase-development.md`
- `.claude/commands/feature-development.md`
- `.claude/commands/database-migration.md`
- `.claude/identity.json`
- `.claude/ecc-tools.json`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Add or update .claude/commands/*.md files (e.g., bugfix-or-pipeline-fix.md, feature-phase-development.md, feature-development.md, database-migration.md)
- Add or update .claude/identity.json
- Add or update .claude/ecc-tools.json
- Add or update .claude/skills/mework/SKILL.md and/or .agents/skills/mework/SKILL.md
- Optionally add or update .codex/agents/*.toml and .agents/skills/mework/agents/openai.yaml

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.