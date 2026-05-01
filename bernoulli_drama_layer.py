"""
bernoulli_drama_layer.py
========================
Parses the Bernoullis-on-the-Mound daily markdown files to extract
pitcher variance state (Zen / Drama / Meltdown) for use as a K prop
confidence modifier.

Drama% is the Bernoulli entropy state: a high-Drama pitcher has
unpredictable run-prevention results — good mean stats but high variance
per-outing. For K props this means the expected K rate is less reliable
as a predictor of today's output.

Confidence modifier applied to K prop probability:
    Drama% < 30%:   no change (predictable pitcher)
    Drama% 30–50%:  −1.5pp off model prob (slight caution)
    Drama% 50–65%:  −3.0pp off model prob (high variance)
    Drama% > 65%:   −5.0pp off model prob (very unpredictable)
    Minimum IP:     Only applies for pitchers with ≥ 20 IP season total.

Data source: Bernoullis-on-the-Mound GitHub repo
    https://github.com/Murray2061/Bernoullis-on-the-Mound
    Daily markdown at /YYYY/MM/YYYY-MM-DD.md

Usage:
    from bernoulli_drama_layer import get_drama_penalty, load_bernoulli_rankings
    rankings = load_bernoulli_rankings("/path/to/2026-05-01.md")
    penalty  = get_drama_penalty("Shohei Ohtani", rankings)   # → -5.0
"""
from __future__ import annotations

import logging
import os
import re
import unicodedata
from datetime import date
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

# Minimum IP before Drama penalty is applied (relieves from tiny sample noise)
_MIN_IP_FOR_DRAMA = 20.0

# Drama% → probability penalty (pp = percentage points on 0-100 scale)
_DRAMA_PENALTIES: list[tuple[float, float]] = [
    (65.0, -5.0),   # Drama > 65% → −5pp
    (50.0, -3.0),   # Drama > 50% → −3pp
    (30.0, -1.5),   # Drama > 30% → −1.5pp
    (0.0,   0.0),   # below 30%   → no penalty
]


def _norm_name(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_s = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z ]", "", ascii_s.lower()).strip()


def load_bernoulli_rankings(md_path: str) -> dict[str, dict]:
    """
    Parse a Bernoullis daily markdown file and return a dict keyed by
    normalised pitcher name.

    Returns:
        {
          "jose soriano": {
            "name": "José Soriano",
            "tier": "S",
            "team": "LAA",
            "ip":   42.2,
            "zen":  79.7,
            "drama": 17.8,
            "meltdown": 2.5,
          },
          ...
        }
    """
    if not os.path.exists(md_path):
        logger.debug("[BernoulliDrama] File not found: %s", md_path)
        return {}

    try:
        with open(md_path, encoding="utf-8") as f:
            content = f.read()
    except Exception as exc:
        logger.debug("[BernoulliDrama] Read error: %s", exc)
        return {}

    result: dict[str, dict] = {}

    for line in content.split("\n"):
        if "|" not in line:
            continue
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) < 9:
            continue
        # Skip header and dummy rows
        if parts[0] in ("Rank", "---") or "Dummy" in line or "GHOST" in line:
            continue
        try:
            name     = parts[3].replace("_", " ").strip()
            ip       = float(parts[4])
            zen      = float(parts[6])
            drama    = float(parts[7])
            meltdown = float(parts[8])
            tier     = parts[1]
            team     = parts[2]
        except (ValueError, IndexError):
            continue

        key = _norm_name(name)
        if key:
            result[key] = {
                "name":     name,
                "tier":     tier,
                "team":     team,
                "ip":       ip,
                "zen":      zen,
                "drama":    drama,
                "meltdown": meltdown,
            }

    logger.info("[BernoulliDrama] Loaded %d pitcher rankings from %s",
                len(result), os.path.basename(md_path))
    return result


def get_drama_penalty(
    pitcher_name: str,
    rankings:     dict[str, dict],
) -> tuple[float, str]:
    """
    Return (pp_penalty, note) for a pitcher based on Drama%.

    pp_penalty is on the 0–100 probability scale (e.g. −3.0 means
    subtract 3 percentage points from the model K probability).
    Returns (0.0, "no data") if pitcher not found or IP too low.
    """
    key = _norm_name(pitcher_name)
    entry = rankings.get(key)

    # Try partial match
    if entry is None:
        for k, v in rankings.items():
            if key in k or k in key:
                entry = v
                break

    if entry is None:
        return 0.0, "no bernoulli data"

    if entry["ip"] < _MIN_IP_FOR_DRAMA:
        return 0.0, f"IP too low ({entry['ip']:.1f}) — drama signal suppressed"

    drama = entry["drama"]
    for threshold, penalty in _DRAMA_PENALTIES:
        if drama > threshold:
            note = (f"Drama={drama:.1f}% ({entry['tier']}, {entry['ip']:.1f} IP) "
                    f"→ {penalty:+.1f}pp K-prob adjustment")
            return penalty, note

    return 0.0, f"Drama={drama:.1f}% — no penalty"


def find_latest_md(repo_root: str) -> Optional[str]:
    """
    Find the most recent daily markdown file in the Bernoullis repo.
    Looks for YYYY/MM/YYYY-MM-DD.md pattern.
    """
    today = date.today()
    for delta in range(7):  # look back up to 7 days
        d = date(today.year - (1 if today.month == 1 and delta > 0 else 0),
                 today.month, max(1, today.day - delta))
        candidate = os.path.join(
            repo_root,
            str(d.year),
            f"{d.month:02d}",
            f"{d}.md"
        )
        if os.path.exists(candidate):
            return candidate
    return None
