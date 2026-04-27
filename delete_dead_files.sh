#!/bin/bash
# PropIQ dead file cleanup — run from repo root
# These files are confirmed unused in the live pipeline

echo "Deleting dead code files..."

# Old parlay leg builders — superseded by 10-agent system in tasklets.py
rm -f agents/agent_2leg.py && echo "  ✓ agents/agent_2leg.py"
rm -f agents/agent_3leg.py && echo "  ✓ agents/agent_3leg.py"
rm -f agents/agent_5leg.py && echo "  ✓ agents/agent_5leg.py"
rm -f agents/agent_best.py && echo "  ✓ agents/agent_best.py"

# Apify scraper — APIFY_API_KEY removed from Railway, file never imported
rm -f python/apify_scrapers.py && echo "  ✓ python/apify_scrapers.py"

# clv_feedback.py — never imported (clv_feedback_engine.py is the live version)
rm -f clv_feedback.py && echo "  ✓ clv_feedback.py"

# context_modifiers.py — feature engineering for ml_pipeline.py (offline only)
# BullpenFatigueScorer/WeatherParkAdjuster superseded by bvi_layer.py + _WeatherAgent
rm -f context_modifiers.py && echo "  ✓ context_modifiers.py"

echo "Done. $(git status --short 2>/dev/null | wc -l) files changed."
