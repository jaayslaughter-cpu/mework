"""
line_comparator.py — PropIQ Phase 109
=====================================
Compare Underdog Fantasy vs PrizePicks lines for the same player+stat.
Returns the platform with the more favorable line for the model's recommended
direction so Discord alerts always direct to the best available number.

Platform priority (Phase 109):
  1. Underdog Streaks — checked upstream in _make_parlay(); always wins
  2. Underdog — default when UD line is strictly better OR only UD has the prop
  3. PrizePicks — tiebreaker only: chosen when lines are exactly equal on both
                  platforms, OR when prop only exists on PrizePicks

Better line logic:
  - OVER bet:  LOWER line number is better (easier to exceed)
  - UNDER bet: HIGHER line number is better (easier to go under)
  - Equal lines → PrizePicks (tiebreaker; UD Streaks eligibility checked upstream)
"""
from __future__ import annotations

import re
import logging
from typing import Optional

logger = logging.getLogger("propiq.line_comparator")


# ── Name / stat normalisation ────────────────────────────────────────────────

def _name_key(name: str) -> str:
    """Lowercase, strip accents/punctuation, collapse whitespace."""
    s = str(name).lower()
    # Strip common accent characters
    for old, new in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),
                     ("ñ","n"),("ü","u"),("ö","o"),("ä","a")]:
        s = s.replace(old, new)
    return re.sub(r"[^a-z ]", "", s).strip()


def _stat_key(stat: str) -> str:
    """Normalise stat name to underscore_lower."""
    return (
        str(stat).lower()
        .strip()
        .replace("-", "_")
        .replace(" ", "_")
        .replace("+", "_")
    )


# Stat alias map — PrizePicks uses different labels than Underdog
_STAT_ALIASES: dict[str, str] = {
    "pitcher_strikeouts":     "strikeouts",
    "pitcher_strikeout":      "strikeouts",
    "hitter_fantasy_score":   "fantasy_hitter",
    "pitcher_fantasy_score":  "fantasy_pitcher",
    "fantasy_points_hitter":  "fantasy_hitter",
    "fantasy_points_pitcher": "fantasy_pitcher",
    "home_run":               "home_runs",
    "rbi":                    "rbis",
    "stolen_base":            "stolen_bases",
    "earned_run":             "earned_runs",
    "base_on_balls":          "walks",
    "hits___runs___rbis":     "hits_runs_rbis",
    "h_r_rbi":                "hits_runs_rbis",
    "total_base":             "total_bases",
}

def _norm_stat(stat: str) -> str:
    k = _stat_key(stat)
    return _STAT_ALIASES.get(k, k)


# ── Lookup builder ────────────────────────────────────────────────────────────

def build_line_lookup(props: list[dict]) -> dict[tuple[str, str], float]:
    """
    Build a fast lookup {(name_key, stat_norm): line} from a list of prop dicts.

    Works with both Underdog and PrizePicks prop formats since both use
    "player" / "player_name" for the name and "prop_type" / "stat_type" /
    "stat" for the stat name.
    """
    lookup: dict[tuple[str, str], float] = {}
    for p in (props or []):
        pname = (
            p.get("player_name") or p.get("player") or p.get("name") or ""
        ).strip()
        if not pname:
            continue
        stat = (
            p.get("prop_type") or p.get("stat_type") or p.get("stat") or ""
        ).strip()
        if not stat:
            continue
        line = p.get("line")
        if line is None:
            continue
        try:
            line_f = float(line)
        except (TypeError, ValueError):
            continue
        key = (_name_key(pname), _norm_stat(stat))
        if key not in lookup:   # keep first occurrence; fetchers already dedup
            lookup[key] = line_f
    return lookup


# ── Core comparison ───────────────────────────────────────────────────────────

def compare_prop(
    player_name: str,
    stat: str,
    direction: str,          # "OVER" / "UNDER" (case-insensitive)
    ud_lookup: dict,
    pp_lookup: dict,
) -> dict:
    """
    Compare Underdog vs PrizePicks for a given player+stat+direction.

    Platform priority (Phase 109):
      - Underdog Streaks eligibility is checked upstream in _make_parlay().
      - Here: Underdog wins when it has the strictly better line.
      - PrizePicks wins on a true tie (same line on both platforms) — tiebreaker.
      - Only platform available wins by default.

    Returns:
      {
        "platform":  "Underdog" | "PrizePicks",   # better platform
        "line":      float | None,                 # best line available
        "ud_line":   float | None,
        "pp_line":   float | None,
        "note":      str,                          # human-readable
      }
    """
    pkey  = _name_key(player_name)
    skey  = _norm_stat(stat)
    is_over = direction.upper().startswith("O")

    ud_line: Optional[float] = ud_lookup.get((pkey, skey))
    pp_line: Optional[float] = pp_lookup.get((pkey, skey))

    def _res(platform: str, line: Optional[float], note: str) -> dict:
        return {
            "platform": platform,
            "line":     line,
            "ud_line":  ud_line,
            "pp_line":  pp_line,
            "note":     note,
        }

    # ── Both platforms have this prop ────────────────────────────────────────
    if ud_line is not None and pp_line is not None:
        if is_over:
            if pp_line < ud_line:
                diff = round(ud_line - pp_line, 1)
                return _res("PrizePicks", pp_line,
                            f"PrizePicks {pp_line} vs Underdog {ud_line} (PP -{diff} ✅ OVER)")
            elif ud_line < pp_line:
                diff = round(pp_line - ud_line, 1)
                return _res("Underdog", ud_line,
                            f"Underdog {ud_line} vs PrizePicks {pp_line} (UD -{diff} ✅ OVER)")
            else:
                # True tie — PrizePicks is tiebreaker (UD Streaks checked upstream)
                return _res("PrizePicks", ud_line,
                            f"Same line {ud_line} on both → PrizePicks (tiebreaker)")
        else:  # UNDER
            if ud_line > pp_line:
                diff = round(ud_line - pp_line, 1)
                return _res("Underdog", ud_line,
                            f"Underdog {ud_line} vs PrizePicks {pp_line} (UD +{diff} ✅ UNDER)")
            elif pp_line > ud_line:
                diff = round(pp_line - ud_line, 1)
                return _res("PrizePicks", pp_line,
                            f"PrizePicks {pp_line} vs Underdog {ud_line} (PP +{diff} ✅ UNDER)")
            else:
                # True tie — PrizePicks is tiebreaker (UD Streaks checked upstream)
                return _res("PrizePicks", ud_line,
                            f"Same line {ud_line} on both → PrizePicks (tiebreaker)")

    # ── Only one platform has the prop ───────────────────────────────────────
    if ud_line is not None:
        return _res("Underdog",     ud_line,  f"Underdog only ({ud_line}) — not on PrizePicks")
    if pp_line is not None:
        return _res("PrizePicks",   pp_line,  f"PrizePicks only ({pp_line}) — not on Underdog")

    # ── Neither platform has the prop ────────────────────────────────────────
    return _res("Underdog", None, "Not found on either platform — check manually")
