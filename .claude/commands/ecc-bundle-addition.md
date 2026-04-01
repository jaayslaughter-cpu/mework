---
name: ecc-bundle-addition
description: Workflow command scaffold for ecc-bundle-addition in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /ecc-bundle-addition

Use this workflow when working on **ecc-bundle-addition** in `mework`.

## Goal

Adds or updates the mework ECC bundle, including command definitions, skill manifests, and agent configs.

## Common Files

- `.claude/commands/*.md`
- `.claude/skills/mework/SKILL.md`
- `.agents/skills/mework/SKILL.md`
- `.claude/ecc-tools.json`
- `.claude/identity.json`
- `.codex/agents/*.toml`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Add or update .claude/commands/*.md files for new commands (e.g., feature development, bugfix, migration)
- Add or update .claude/skills/mework/SKILL.md and/or .agents/skills/mework/SKILL.md
- Add or update .claude/ecc-tools.json and .claude/identity.json
- Add or update .codex/agents/*.toml and/or .agents/skills/mework/agents/openai.yaml
- Commit all related files together

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.