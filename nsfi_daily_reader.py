"""
nsfi_daily_reader.py
====================
Reads pre-computed NSFI predictions from the mlb-nsfi-model daily JSON files.

The mlb-nsfi-model repo generates two files each day:
    daily_YYYYMMDD.json          — confirmed lineups + game times
    model_predictions_YYYYMMDD.json  — NSFI predictions with EV

Each prediction contains:
    game_id      — "TEX/DET - Top 1"
    half         — "top" | "bot"
    pitcher      — full name
    batting_team — team facing the pitcher
    pitching_team
    model_prob   — P(NSFI = True), i.e. P(no-score first inning)
    implied_prob — market-implied P(NSFI) from DraftKings
    ev           — edge = model_prob - implied_prob
    ev_category  — "strong" | "marginal" | "negative"
    dk_no_odds   — DraftKings American odds for NSFI YES

Usage:
    from nsfi_daily_reader import get_nsfi_predictions, get_nsfi_for_pitcher
    preds = get_nsfi_predictions("/path/to/model_predictions_20260501.json")
    p = get_nsfi_for_pitcher("Jack Flaherty", preds)  # → {"model_prob": 0.29, "ev": -0.02, ...}
"""
from __future__ import annotations

import json
import logging
import os
import unicodedata
import re
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


def _norm(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(s))
    ascii_s = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z ]", "", ascii_s.lower()).strip()


def get_nsfi_predictions(json_path: str) -> list[dict]:
    """Load predictions from a model_predictions_YYYYMMDD.json file."""
    if not os.path.exists(json_path):
        logger.debug("[NSFIReader] File not found: %s", json_path)
        return []
    try:
        with open(json_path) as f:
            data = json.load(f)
        preds = data.get("predictions", [])
        logger.info("[NSFIReader] Loaded %d NSFI predictions from %s",
                    len(preds), os.path.basename(json_path))
        return preds
    except Exception as exc:
        logger.debug("[NSFIReader] Read error: %s", exc)
        return []


def get_nsfi_for_pitcher(
    pitcher_name: str,
    predictions:  list[dict],
) -> Optional[dict]:
    """
    Find a pitcher's NSFI prediction entry by name.
    Returns None if not found. Tries exact match then partial.
    """
    key = _norm(pitcher_name)
    for p in predictions:
        if _norm(p.get("pitcher", "")) == key:
            return p
    # Partial fallback
    for p in predictions:
        pn = _norm(p.get("pitcher", ""))
        if key in pn or pn in key:
            return p
    return None


def find_latest_predictions(repo_root: str) -> Optional[str]:
    """
    Find the most recent model_predictions_YYYYMMDD.json in the repo root.
    Looks back up to 7 days.
    """
    today = date.today()
    for delta in range(7):
        d_obj = date.fromordinal(today.toordinal() - delta)
        fname = f"model_predictions_{d_obj.strftime('%Y%m%d')}.json"
        path  = os.path.join(repo_root, fname)
        if os.path.exists(path):
            return path
    return None


def get_nsfi_ev_category(prediction: dict) -> str:
    """
    Return a human-readable EV tier for the NSFI prediction.
    Mirrors the mlb-nsfi-model ev_category field:
      strong    → ev ≥ 0.07
      marginal  → ev ≥ 0.03
      negative  → ev < 0.03
    """
    ev = float(prediction.get("ev", 0) or 0)
    if ev >= 0.07:
        return "strong"
    if ev >= 0.03:
        return "marginal"
    return "negative"
