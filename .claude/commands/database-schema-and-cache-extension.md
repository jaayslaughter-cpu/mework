---
name: database-schema-and-cache-extension
description: Workflow command scaffold for database-schema-and-cache-extension in mework.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /database-schema-and-cache-extension

Use this workflow when working on **database-schema-and-cache-extension** in `mework`.

## Goal

Adds or modifies database tables, migrations, or cache layers, often to support new features or improve performance.

## Common Files

- `migrations/V*.sql`
- `fangraphs_layer.py`
- `tasklets.py`
- `orchestrator.py`
- `.env.example`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Create or edit migration SQL files in migrations/ (e.g., V27__add_discord_sent.sql, V28__fg_cache.sql)
- Update related Python data access layers (e.g., fangraphs_layer.py for cache, tasklets.py for Discord sent logic)
- Update .env.example if new environment variables are required
- Update orchestrator.py if pipeline wiring is needed

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.