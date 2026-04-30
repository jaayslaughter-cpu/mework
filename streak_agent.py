"""
streak_agent.py
===============
PropIQ — 19th Agent: StreakAgent

Underdog Fantasy "Streaks" format — 11 consecutive correct picks to win.

Entry:
  $10 entry → $10,000 prize

Rules enforced:
  • Confidence gate    : ≥ 7.0/10  (vs. 6.0 for standard parlays)
  • Probability gate   : ≥ 0.62 implied win probability per pick
  • EV gate            : ≥ 8.0% per pick (even-money edge over 50% break-even)
  • Team diversity     : picks 1 & 2 must be from different teams
  • Single pick/day    : one Streaks pick per day maximum
  • Streak window      : 10 calendar days to complete 11 picks
  • In-game allowed    : yes — Underdog accepts full-game total projections
  • Rescues            : supported (player exits early — Underdog grants rescue)
  • Void rules
      - Picks 1–2 void → full streak restart (new streak from pick 1)
      - Picks 3–11 void → replace with next-available pick (pick # preserved)

Pick selection algorithm:
  1. Fetch live Underdog Fantasy MLB props (with team enrichment)
  2. Evaluate each prop using MLB historical base rates (same as dispatcher)
  3. Run all 17 AGENT_CONFIGS filters to count cross-agent "signals"
  4. Score each prop: streak_confidence() = prob_score + ev_bonus + signal_bonus
  5. Filter: conf ≥ 8.0, prob ≥ 0.62, ev_pct ≥ 5.0%
  6. Apply team-diversity rule for picks 1 & 2
  7. Select top-ranked prop; skip day if nothing qualifies (better than a bad pick)

State persistence:
  • Postgres tables: streak_state (one row per active streak),
                     streak_picks (one row per pick)
  • DB connection via POSTGRES_URL env var (same as the rest of the stack)

Discord alerts:
  • Pick announcement  : 8:00 AM PT (before main dispatch window — CronTrigger)
  • Settlement update  : 2 AM alongside nightly_recap.py
  • Streak milestones  : 5/11 and 8/11 celebration pings

Standalone run:
  python streak_agent.py [--date 2026-04-01] [--dry-run] [--entry 10]
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Optional imports — graceful fallback when running outside the full stack
# ---------------------------------------------------------------------------

try:
    from line_comparator import build_line_lookup as _build_ll, compare_prop as _cmp_prop
    _LINE_COMP_AVAILABLE = True
except ImportError:
    _LINE_COMP_AVAILABLE = False

try:
    from DiscordAlertService import discord_alert, MAX_STAKE_USD
    _DISCORD_AVAILABLE = True
except ImportError:
    _DISCORD_AVAILABLE = False
    MAX_STAKE_USD = 20.0

try:
    import psycopg2
    import psycopg2.extras
    _PG_AVAILABLE = True
except ImportError:
    _PG_AVAILABLE = False

# live_dispatcher.py removed (Kill job_dispatch Directive).
# Inline AGENT_CONFIGS for signal counting — 17 active agents with prob-threshold
# proxies matching each agent's core filter logic in tasklets.py.
_DISPATCHER_AVAILABLE = True   # always True — inline configs always available

AGENT_CONFIGS = [
    # H-9 fix: removed 8 phantom agents that only checked implied_prob >= threshold,
    # causing every prop at >= 0.55 prob to count as 8/17 signals automatically.
    # All 17 entries now have differentiated filters using real StreakResult fields.
    # StreakResult fields available: implied_prob, side, ev_pct, prop_type, position, line
    {"name": "EVHunter",             "filter": lambda sr: sr.implied_prob >= 0.55 and sr.ev_pct >= 3.0},
    {"name": "UnderMachine",         "filter": lambda sr: sr.side == "Under" and sr.implied_prob >= 0.55},
    {"name": "UmpireAgent",          "filter": lambda sr: sr.prop_type in ("strikeouts", "pitching_outs") and sr.implied_prob >= 0.57},
    {"name": "F5Agent",              "filter": lambda sr: sr.implied_prob >= 0.60},
    {"name": "FadeAgent",            "filter": lambda sr: sr.side == "Under" and sr.ev_pct >= 5.0},
    {"name": "LineValueAgent",       "filter": lambda sr: sr.implied_prob >= 0.57 and sr.ev_pct >= 4.0},
    {"name": "BullpenAgent",         "filter": lambda sr: sr.prop_type in ("earned_runs", "hits_allowed", "pitching_outs") and sr.implied_prob >= 0.55},
    {"name": "WeatherAgent",         "filter": lambda sr: sr.prop_type in ("strikeouts", "earned_runs") and sr.implied_prob >= 0.57},
    {"name": "MLEdgeAgent",          "filter": lambda sr: sr.implied_prob >= 0.62 and sr.ev_pct >= 6.0},
    {"name": "StackSmithAgent",      "filter": lambda sr: sr.prop_type in ("hits", "hits_runs_rbis", "total_bases") and sr.implied_prob >= 0.58},
    {"name": "ChalkBusterAgent",     "filter": lambda sr: sr.side == "Under" and sr.implied_prob >= 0.58},
    {"name": "CorrelatedParlayAgent","filter": lambda sr: sr.prop_type in ("hits", "total_bases", "rbis", "runs") and sr.implied_prob >= 0.58},
    {"name": "PropCycleAgent",       "filter": lambda sr: sr.ev_pct >= 7.0},
    {"name": "LineupChaseAgent",     "filter": lambda sr: sr.position not in ("SP", "RP") and sr.implied_prob >= 0.57},
    {"name": "LineDriftAgent",       "filter": lambda sr: sr.implied_prob >= 0.60 and sr.ev_pct >= 5.0},
    {"name": "SharpFadeAgent",       "filter": lambda sr: sr.side == "Under" and sr.implied_prob >= 0.60},
    {"name": "UnderDogAgent",        "filter": lambda sr: sr.implied_prob >= 0.62},
]

def fetch_today_schedule(): return []
def normalise_stat(s): return s.lower().strip()
def calc_ev(prob, odds=-110): return (prob - 0.50) / 0.50 * 100  # FIX: Streaks = even money (0.50 break-even), not -110 (0.5238)
def implied_prob_from_odds(odds): return 100.0 / (abs(odds) + 100) if odds < 0 else abs(odds) / (abs(odds) + 100)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("propiq.streak")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STREAK_CONF_MIN    = 6.0    # PR #429: lowered from 7.0 — max achievable at prob=0.62 is ~6.71,
                            # so 7.0 was mathematically impossible (no picks could ever fire).
                            # Do NOT raise above 6.0 until 200+ real graded legs with features_json
                            # AND post-retrain Brier < 0.20 in xgb_model_store.
STREAK_PROB_MIN    = 0.62   # Minimum per-leg win probability for streak picks.
STREAK_EV_MIN      = 8.0    # Minimum edge vs 50% break-even for streak picks.
STREAK_MIN_LINE    = 0.5    # Allow 0.5 lines — high-prob props (earned_runs Over 0.5 = 88%) are valid.
                            # PR #397: STREAK_MIN_LINE = 0.5 (MIN_LINE = 1.0 directive is REVOKED).
STREAK_MIN_SIGNALS = 2      # NEW: at least 2/17 agents must agree before a pick qualifies
STREAK_TOTAL_WINS = 11     # picks needed to win
STREAK_WINDOW_DAYS = 10    # calendar days to complete the streak

# Entry: $10 entry → $10,000 prize
ENTRY_TIERS: dict[int, tuple[float, float]] = {
    1:  (1.0,  1_000.0),
    5:  (5.0,  5_000.0),
    10: (10.0, 10_000.0),
}
DEFAULT_ENTRY = 10   # $10 entry → $10,000 prize

DISCORD_WEBHOOK = os.getenv(
    "DISCORD_WEBHOOK_URL",
    os.getenv("DISCORD_WEBHOOK_URL", ""),
)

# Underdog API
_UD_LINES_URL = "https://api.underdogfantasy.com/beta/v5/over_under_lines"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ESPN box score for settlement
_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb"

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class StreakCandidate:
    """A fully-scored single-leg pick candidate for the Streaks format."""
    player_name: str
    team:        str          # e.g. "NYY", "LAD"
    prop_type:   str          # normalised: hits, strikeouts, etc.
    line:        float
    side:        str          # "Over" or "Under"
    platform:    str          # "Underdog" or "PrizePicks"
    entry_type:  str          # "FLEX" or "STANDARD"
    position:    str          # "SP", "C", "1B", etc.
    implied_prob: float       # 0.0–1.0 estimated win probability
    ev_pct:      float        # expected value %
    confidence:  float        # 1.0–10.0 StreakAgent score
    signal_count: int         # number of AGENT_CONFIGS that approve this pick


# ---------------------------------------------------------------------------
# MLB historical base rates (mirrored from live_dispatcher._evaluate_props)
# ---------------------------------------------------------------------------

# FIX: Base rates aligned with corrected DFS calibration (2024 actual hit rates).
# These are P(Over | line offered by Underdog) — conditional on Underdog's line-setting,
# NOT raw MLB frequencies. Underdog only offers a line when expected outcome ≈ line value.
_BASE_RATES: dict[str, list[tuple[float, float]]] = {
    # Only high-quality, reliable MLB DFS prop types
    # stolen_bases, walks, home_runs removed — low base rates / unreliable for streaks
    "hits":           [(0.5, 0.62), (1.5, 0.38), (2.5, 0.13), (3.5, 0.03)],
    "rbis":           [(0.5, 0.40), (1.5, 0.18), (2.5, 0.07)],
    "runs":           [(0.5, 0.45), (1.5, 0.18), (2.5, 0.06)],
    "total_bases":    [(0.5, 0.64), (1.5, 0.50), (2.5, 0.28), (3.5, 0.12)],
    "hits_runs_rbis": [(0.5, 0.78), (1.5, 0.58), (2.5, 0.40), (3.5, 0.24), (4.5, 0.12)],
    # Strikeouts: line-conditional rates (8.5 line only offered for aces → P(Over)≈40%)
    # FIX: old 8.5 rate was 0.19 (raw MLB avg), corrected to 0.40 (conditional on line)
    # Strikeouts: P(Over | line) conditional on Underdog offering that line.
    # 3.5 line: offered for weaker starters (avg ~4.5 Ks/start) → P(≥4) ≈ 0.65 not 0.72.
    # 0.72 was the rate for quality starters; Underdog only sets 3.5 for fringe arms.
    # Player enrichment layer adjusts further using actual CSW%/FIP per pitcher.
    "strikeouts":     [(3.5, 0.65), (4.5, 0.58), (5.5, 0.50), (6.5, 0.44), (7.5, 0.40), (8.5, 0.37), (9.5, 0.33), (10.5, 0.27)],
    # Earned runs: FIX: 0.5 rate was 0.42 (badly too low), corrected to 0.88 (real ER/start)
    "earned_runs":    [(0.5, 0.88), (1.5, 0.62), (2.5, 0.38), (3.5, 0.20)],
    "fantasy_hitter": [(15.0, 0.55), (20.0, 0.42), (25.0, 0.30), (30.0, 0.20)],
    "fantasy_pitcher":[(30.0, 0.55), (35.0, 0.44), (40.0, 0.33), (45.0, 0.24)],
    "pitching_outs":  [(14.5, 0.58), (17.5, 0.44), (20.5, 0.28)],
    "hits_allowed":   [(3.5, 0.52), (4.5, 0.38), (5.5, 0.25)],
}

_GAME_LINE_RANGES: dict[str, tuple[float, float]] = {
    "hits":           (0.5, 4.5),
    "rbis":           (0.5, 4.5),
    "runs":           (0.5, 3.5),
    "total_bases":    (0.5, 5.5),
    "hits_runs_rbis": (0.5, 8.5),
    "strikeouts":     (1.5, 12.5),
    "earned_runs":    (0.5, 6.5),
    "fantasy_hitter": (5.0, 60.0),
    "fantasy_pitcher":(15.0, 70.0),
}

_STAT_TYPE_MAP: dict[str, str] = {
    # stolen_bases, home_runs, walks removed — not approved prop types
    "strikeouts": "strikeouts", "pitcher strikeouts": "strikeouts", "ks": "strikeouts",
    "hits": "hits",
    "rbis": "rbis", "rbi": "rbis",
    "runs": "runs",
    "total bases": "total_bases", "total_bases": "total_bases",
    "hits+runs+rbis": "hits_runs_rbis", "hits + runs + rbis": "hits_runs_rbis",
    "hitter fantasy score": "fantasy_hitter", "fantasy_points_hitter": "fantasy_hitter",
    "pitcher fantasy score": "fantasy_pitcher", "fantasy_points_pitcher": "fantasy_pitcher",
    "earned runs": "earned_runs", "earned runs allowed": "earned_runs", "earned_runs": "earned_runs",
    "hits allowed": "hits_allowed", "pitching outs": "pitching_outs",
}


def _normalise_stat(raw: str) -> str | None:
    return _STAT_TYPE_MAP.get(raw.strip().lower().replace("-", " "))


def _base_prob(prop_type: str, line: float, side: str) -> float:
    """Interpolate MLB base-rate probability."""
    rates = _BASE_RATES.get(prop_type, [])
    if not rates:
        return 0.50
    xs = [r[0] for r in rates]
    ys = [r[1] for r in rates]
    if line <= xs[0]:
        p_over = ys[0]
    elif line >= xs[-1]:
        p_over = ys[-1]
    else:
        for i in range(len(xs) - 1):
            if xs[i] <= line <= xs[i + 1]:
                t = (line - xs[i]) / (xs[i + 1] - xs[i])
                p_over = ys[i] + t * (ys[i + 1] - ys[i])
                break
        else:
            p_over = 0.50
    return p_over if side == "Over" else (1.0 - p_over)


def _is_game_prop(prop_type: str, line: float) -> bool:
    rng = _GAME_LINE_RANGES.get(prop_type)
    if rng is None:
        return True
    return rng[0] <= line <= rng[1]


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

def streak_confidence(prob: float, ev_pct: float, signal_count: int) -> float:
    """
    Single-leg Streaks confidence  (1.0 – 10.0).

    Formula mirrors build_parlay() in live_dispatcher for consistency:
      prob_score    = (prob – 0.50) / 0.30 × 7.0   → 0–7 over 50%–80%
      ev_bonus      = min(ev_pct / 15.0 × 2.0, 2.0) → 0–2 for 0%–15% EV
      signal_bonus  = min(signal_count × 0.1, 1.0)  → 0–1 for 0–10 agents

    No legs_penalty (single-leg pick).  Gate for StreakAgent: 8.0/10.
    Gate is achievable when:
      prob ≥ 0.76 + ev_pct ≥ 7.5%  (e.g. hits_runs_rbis Over 0.5 = 82%)
      prob ≥ 0.80 + any ev            (dominant Over lines)
    """
    # Prob contribution capped at 5 — prevents high base-rate props (82% hits_runs_rbis 0.5)
    # from dominating the score. Genuine edge (EV + agent signals) carries more weight.
    prob_score   = min((prob - 0.50) / 0.35 * 5.0, 5.0)   # was 0.30/7.0, uncapped
    ev_bonus     = min(ev_pct / 10.0 * 3.0, 3.0)           # was /15 × 2; more EV weight
    signal_bonus = min(signal_count * 0.2, 2.0)             # was × 0.1 cap 1.0; more signal weight
    return round(min(10.0, max(1.0, prob_score + ev_bonus + signal_bonus)), 1)


# ---------------------------------------------------------------------------
# Underdog prop fetch with team enrichment
# ---------------------------------------------------------------------------

def fetch_underdog_props_with_teams() -> list[dict]:
    """
    Fetch Underdog Fantasy MLB props, including team abbreviation.

    Extends the base fetch_underdog_props() to also resolve team from the
    appearance object (appearance.match_id → not available) or from a
    supplemental appearances team_id lookup.

    Falls back to live_dispatcher.fetch_underdog_props() if team is unavailable,
    filling team with "" (team-diversity check skipped gracefully).
    """
    try:
        resp = requests.get(_UD_LINES_URL, headers=_HEADERS, timeout=25)
        if resp.status_code != 200:
            logger.warning("[Streak] Underdog HTTP %d — trying dispatcher fallback", resp.status_code)
            if _DISPATCHER_AVAILABLE:
                base = fetch_underdog_props()
                for p in base:
                    p.setdefault("team", "")
                return base
            return []

        data = resp.json()

        players_map: dict[str, dict]     = {p["id"]: p for p in data.get("players", [])}
        appearances_map: dict[str, dict] = {a["id"]: a for a in data.get("appearances", [])}

        # Build team_id → abbreviation from any 'teams' or 'match_teams' array
        teams_map: dict[str, str] = {}
        for t in data.get("teams", []):
            tid = t.get("id", "")
            abbr = (t.get("abbr") or t.get("abbreviation") or
                    t.get("name", "")[:3].upper())
            if tid:
                teams_map[tid] = abbr

        props: list[dict] = []
        seen: set[str] = set()

        for line in data.get("over_under_lines", []):
            if line.get("status") != "active":
                continue
            if line.get("line_type") != "balanced":  # Phase 116: Pick'em balanced lines only
                continue

            stable_id = line.get("stable_id", line.get("id", ""))
            if stable_id in seen:
                continue

            ou       = line.get("over_under") or {}
            app_stat = ou.get("appearance_stat") or {}
            stat_ud  = app_stat.get("stat", "")
            app_id   = app_stat.get("appearance_id", "")

            if not stat_ud or not app_id:
                continue
            if "inning" in stat_ud.lower():
                continue

            appearance = appearances_map.get(app_id, {})
            player_id  = appearance.get("player_id", "")
            player     = players_map.get(player_id, {})

            if player.get("sport_id") != "MLB":
                continue

            pname = f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
            if not pname:
                continue

            # Resolve team: try appearance.team_id then appearance.team_abbr
            team_id  = appearance.get("team_id", "")
            team_abbr = (teams_map.get(team_id, "")
                         or appearance.get("team_abbr", "")
                         or player.get("team_abbr", "")
                         or "")

            line_val   = float(line.get("stat_value") or 0)
            position   = player.get("position_name", "")
            opts       = line.get("options", [])
            higher_opt = next((o for o in opts if o.get("choice") == "higher"), {})
            entry_type = "STANDARD" if not higher_opt.get("payout_multiplier") else "FLEX"

            seen.add(stable_id)
            props.append({
                "source":      "underdog",
                "player_name": pname,
                "stat_type":   stat_ud,
                "line":        line_val,
                "entry_type":  entry_type,
                "position":    position,
                "team":        team_abbr,
            })

        logger.info("[Streak] Fetched %d MLB lines (with team enrichment)", len(props))
        return props

    except Exception as exc:
        logger.warning("[Streak] Underdog fetch failed: %s", exc)
        if _DISPATCHER_AVAILABLE:
            base = fetch_underdog_props()
            for p in base:
                p.setdefault("team", "")
            return base
        return []


# ---------------------------------------------------------------------------
# Signal counting
# ---------------------------------------------------------------------------

def _count_signals(prop_type: str, side: str, implied_prob: float, position: str = "", prop: dict | None = None) -> int:
    """
    Count how many of the 17 AGENT_CONFIGS would approve this pick.
    Uses the same lambda filters defined in live_dispatcher.AGENT_CONFIGS.
    Returns 0 if dispatcher not available.
    """
    if not AGENT_CONFIGS:
        return 0

    count = 0
    # Build a minimal SelectionResult-like object for filter evaluation
    class _SR:  # noqa: N801
        pass

    sr = _SR()
    sr.side              = side
    sr.prop_type         = prop_type
    sr.implied_prob      = implied_prob
    sr.fantasy_pts_edge  = 0.0
    # FIX PR #388: ev_pct and position were missing — caused 7 agent filters to
    # silently except out, artifically lowering signal counts for every candidate.
    sr.ev_pct   = (implied_prob - 0.50) / 0.50 * 100   # even-money EV
    sr.position = position  # passed from evaluate_props_for_streaks

    for agent in AGENT_CONFIGS:
        try:
            if agent["filter"](sr):
                count += 1
        except Exception:
            pass

    return count


# ---------------------------------------------------------------------------
# Candidate evaluation
# ---------------------------------------------------------------------------

def evaluate_props_for_streaks(raw_props: list[dict]) -> list[StreakCandidate]:
    """
    Score every raw Underdog prop for streak worthiness.
    Returns ALL candidates (not yet filtered by STREAK_CONF_MIN).
    """
    # Group by (player_lower, prop_type) to deduplicate same player/stat
    groups: dict[tuple[str, str], dict] = {}
    for raw in raw_props:
        pname    = raw.get("player_name", "")
        raw_stat = raw.get("stat_type", "")
        line_val = float(raw.get("line") or 0)
        team     = raw.get("team", "")
        etype    = raw.get("entry_type", "FLEX")
        position = raw.get("position", "")

        prop_type = _normalise_stat(raw_stat)
        if not prop_type or prop_type not in _BASE_RATES:
            continue
        if line_val <= 0:
            continue
        if line_val < STREAK_MIN_LINE:          # block trivial 0.5 lines (near-certain base rates)
            continue
        if not _is_game_prop(prop_type, line_val):
            continue

        key = (pname.lower().strip(), prop_type)
        if key not in groups:
            groups[key] = {
                "player_name": pname,
                "team":        team,
                "prop_type":   prop_type,
                "line":        line_val,
                "entry_type":  etype,
                "position":    position,
            }

    candidates: list[StreakCandidate] = []

    for (player_lower, prop_type), info in groups.items():
        line     = info["line"]
        team     = info["team"]
        position = info["position"]
        etype    = info["entry_type"]

        # Evaluate both sides (Over / Under) where base rate is known
        for side in ("Over", "Under"):
            prob = _base_prob(prop_type, line, side)
            if prob < STREAK_PROB_MIN:
                continue

            # EV at DFS standard -110 payout
            ev_pct = (prob - 0.50) / 0.50 * 100   # FIX: Streaks = even money break-even

            signals = _count_signals(prop_type, side, prob, position=position)
            conf    = streak_confidence(prob, ev_pct, signals)

            candidates.append(StreakCandidate(
                player_name  = info["player_name"],
                team         = team,
                prop_type    = prop_type,
                line         = line,
                side         = side,
                platform     = "Underdog",
                entry_type   = etype,
                position     = position,
                implied_prob = prob,
                ev_pct       = round(ev_pct, 2),
                confidence   = conf,
                signal_count = signals,
            ))

    # ── Player-specific enrichment ──────────────────────────────────────────
    # Adjust implied_prob using actual player stats from fangraphs/mlb_stats.
    # Base rates are league averages — this layer makes picks player-specific.
    try:
        from fangraphs_layer import get_pitcher as _fg_pit, get_batter as _fg_bat
        from fangraphs_layer import LEAGUE_DEFAULTS as _FG_DEFAULTS
        _FG_LOADED = True
    except ImportError:
        _FG_LOADED = False

    if _FG_LOADED:
        _LG_K   = _FG_DEFAULTS["pitcher"]["csw_pct"]    # 0.275 league-avg CSW%
        _LG_KP  = _FG_DEFAULTS["pitcher"]["k_bb_pct"]   # 0.130 league-avg K-BB%
        _LG_WRC = _FG_DEFAULTS["batter"]["wrc_plus"]    # 100.0 league-avg wRC+

        for cand in candidates:
            pt   = cand.prop_type
            side = cand.side
            adj  = 0.0  # probability adjustment to apply

            # ── Pitcher K props ──
            if pt == "strikeouts":
                stats = _fg_pit(cand.player_name)
                if stats:
                    csw  = stats.get("csw_pct", _LG_K)
                    kbb  = stats.get("k_bb_pct", _LG_KP)
                    fip  = stats.get("fip", 4.06)
                    # CSW% is the strongest predictor of K outcomes
                    # Each 1pp above league avg = ~+0.8pp win prob
                    csw_delta = (csw - _LG_K) * 0.8
                    # K-BB% quality adjustment
                    kbb_delta = (kbb - _LG_KP) * 0.5
                    # FIP quality: elite <3.50 adds small boost, high >5.00 subtracts
                    fip_delta = max(-0.04, min(0.04, (4.06 - fip) * 0.015))
                    adj = csw_delta + kbb_delta + fip_delta
                    adj = max(-0.12, min(0.12, adj))  # cap at ±12pp

            # ── Pitcher earned_runs / pitching_outs / hits_allowed ──
            elif pt in ("earned_runs", "pitching_outs", "hits_allowed"):
                stats = _fg_pit(cand.player_name)
                if stats:
                    fip  = stats.get("fip", 4.06)
                    era  = stats.get("era", 4.06)
                    # Below-average ERA/FIP → more likely to go deeper (more outs) 
                    # or give up fewer earned runs
                    quality = (4.06 - ((fip + era) / 2)) * 0.02
                    adj = max(-0.08, min(0.08, quality))
                    if side == "Under":
                        adj = -adj  # inverse for Under on ER

            # ── Batter hit/TB/HR props ──
            elif pt in ("hits", "total_bases", "hits_runs_rbis", "rbis", "runs"):
                stats = _fg_bat(cand.player_name)
                if stats:
                    wrc  = stats.get("wrc_plus", _LG_WRC)
                    woba = stats.get("woba", 0.308)
                    xbh  = stats.get("xbh_per_game", 0.50)
                    # wRC+ above 100 → above-average hitter
                    wrc_delta = (wrc - 100.0) * 0.001  # 120 wRC+ → +2pp
                    woba_delta = (woba - 0.308) * 0.15
                    adj = wrc_delta + woba_delta
                    if pt in ("total_bases", "hits_runs_rbis"):
                        # extra-base hit rate adds more weight for TB props
                        adj += (xbh - 0.50) * 0.04
                    adj = max(-0.10, min(0.10, adj))

            if adj != 0.0:
                old_prob = cand.implied_prob
                new_prob = round(max(0.50, min(0.95, old_prob + adj)), 4)
                if new_prob != old_prob:
                    # Recompute EV and confidence with adjusted prob
                    new_ev   = round((new_prob - 0.50) / 0.50 * 100, 2)
                    new_conf = streak_confidence(new_prob, new_ev, cand.signal_count)
                    # Update candidate in-place
                    object.__setattr__(cand, "implied_prob", new_prob)
                    object.__setattr__(cand, "ev_pct",       new_ev)
                    object.__setattr__(cand, "confidence",   new_conf)

    return candidates


# ---------------------------------------------------------------------------
# Pick selection with team-diversity rule
# ---------------------------------------------------------------------------

def select_streak_pick(
    candidates: list[StreakCandidate],
    pick_number: int,
    prior_pick_team: str | None = None,
) -> StreakCandidate | None:
    """
    Choose today's Streaks pick from qualified candidates.

    Rules applied:
      1. Filter: conf ≥ STREAK_CONF_MIN, prob ≥ STREAK_PROB_MIN, ev ≥ STREAK_EV_MIN
      2. Team diversity: if pick_number ≤ 2 and prior_pick_team is set,
         exclude candidates from that same team
      3. Rank: primary = implied_prob desc, tiebreak = confidence desc

    Returns None if no candidate qualifies (system skips the day rather than
    forcing a marginal pick — streak integrity over volume).
    """
    qualified = [
        c for c in candidates
        if c.confidence >= STREAK_CONF_MIN
        and c.implied_prob >= STREAK_PROB_MIN
        and c.ev_pct >= STREAK_EV_MIN
        and c.signal_count >= STREAK_MIN_SIGNALS
    ]

    if not qualified:
        return None

    # Team + player diversity gate for picks 1 & 2
    if pick_number <= 2 and prior_pick_team:
        diverse = [c for c in qualified if c.team.upper() != prior_pick_team.upper()]
        if diverse:
            qualified = diverse
        else:
            # All candidates from same team — skip day rather than relax diversity
            logger.warning(
                "[Streak] Team diversity: all qualified picks from %s — "
                "skipping pick %d today to preserve streak integrity.",
                prior_pick_team, pick_number,
            )
            return None

    # Sort: highest win probability first (streak integrity), confidence as tiebreak
    qualified.sort(key=lambda c: (-c.implied_prob, -c.confidence, -c.signal_count))
    return qualified[0]


def select_start_picks(
    candidates: list[StreakCandidate],
) -> list[StreakCandidate]:
    """
    Select the top 2 picks from different teams for a fresh streak start.
    Rules (Underdog Streaks): picks 1 & 2 must be from different teams.
    Returns list of 2 (ideal), 1 (diversity unavoidable), or 0 if nothing qualifies.
    """
    qualified = [
        c for c in candidates
        if c.confidence >= STREAK_CONF_MIN
        and c.implied_prob >= STREAK_PROB_MIN
        and c.ev_pct >= STREAK_EV_MIN
        and c.signal_count >= STREAK_MIN_SIGNALS
    ]
    if not qualified:
        return []

    # Sort: highest win probability first (both must hit), confidence as tiebreak
    qualified.sort(key=lambda c: (-c.implied_prob, -c.confidence, -c.signal_count))

    pick1 = qualified[0]

    # Underdog Streaks rule: picks 1 & 2 must be from DIFFERENT TEAMS.
    # We also enforce DIFFERENT PLAYERS — same player appearing twice
    # (e.g. Carroll runs Under + Carroll total_bases Under) is invalid
    # because both legs are correlated: if Carroll has a bad day both fail together.
    pick2 = next(
        (c for c in qualified[1:]
         if c.team.upper() != pick1.team.upper()
         and c.player_name.lower() != pick1.player_name.lower()),
        None,
    )

    # Do NOT relax the diversity rule — if no diverse pick exists, skip the day.
    # Sending two correlated picks from the same player or team defeats the purpose
    # of team diversity and inflates the apparent independence of the streak.
    if not pick2:
        logger.info(
            "[Streak] Fresh start: no qualifying pick from a different team/player than %s (%s). "
            "Skipping today — streak will start when a diverse pair is available.",
            pick1.player_name, pick1.team,
        )
        return []
    return [pick1, pick2]


# ---------------------------------------------------------------------------
# Postgres state management
# ---------------------------------------------------------------------------

def _pg_conn():
    """Return a Postgres connection using POSTGRES_URL env var."""
    if not _PG_AVAILABLE:
        raise RuntimeError("psycopg2 not installed")
    url = os.getenv("DATABASE_URL", os.getenv("POSTGRES_URL", ""))  # FIX: DATABASE_URL is primary (matches rest of stack)
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url)


def ensure_streak_tables() -> None:
    """Create streak_state + streak_picks tables if they don't exist.
    Silently skips if psycopg2 unavailable (local dev without Postgres)."""
    if not _PG_AVAILABLE:
        logger.info("[Streak] psycopg2 not available — skipping table setup (local dev mode)")
        return
    if not os.getenv("DATABASE_URL", os.getenv("POSTGRES_URL", "")):
        logger.info("[Streak] No DATABASE_URL — skipping table setup (local dev mode)")
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS streak_state (
        id              SERIAL PRIMARY KEY,
        entry_amount    INTEGER NOT NULL DEFAULT 10,
        current_pick    INTEGER NOT NULL DEFAULT 0,
        wins_in_row     INTEGER NOT NULL DEFAULT 0,
        status          TEXT    NOT NULL DEFAULT 'ACTIVE',
        started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
        last_pick_at    TIMESTAMP,
        notes           TEXT
    );

    CREATE TABLE IF NOT EXISTS streak_picks (
        id              SERIAL PRIMARY KEY,
        streak_id       INTEGER NOT NULL REFERENCES streak_state(id),
        pick_number     INTEGER NOT NULL,
        player_name     TEXT    NOT NULL,
        team            TEXT    NOT NULL DEFAULT '',
        prop_type       TEXT    NOT NULL,
        line            REAL    NOT NULL,
        direction       TEXT    NOT NULL,
        platform        TEXT    NOT NULL DEFAULT 'Underdog',
        confidence      REAL    NOT NULL,
        probability     REAL    NOT NULL,
        ev_pct          REAL    NOT NULL,
        signal_count    INTEGER NOT NULL DEFAULT 0,
        game_date       TEXT    NOT NULL,
        status          TEXT    NOT NULL DEFAULT 'PENDING',
        picked_at       TIMESTAMP NOT NULL DEFAULT NOW(),
        settled_at      TIMESTAMP,
        actual_result   REAL
    );
    """
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        conn.close()
        logger.info("[Streak] Tables ensured.")
    except Exception as e:
        logger.warning("[Streak] ensure_streak_tables error: %s", e)


def get_or_create_active_streak(entry_amount: int = DEFAULT_ENTRY) -> dict | None:
    """
    Return the current ACTIVE streak state dict, or create one if none exists.
    Returns None on DB error or when Postgres is unavailable (local dev).
    """
    if not _PG_AVAILABLE or not os.getenv("DATABASE_URL", os.getenv("POSTGRES_URL", "")):
        logger.info("[Streak] Postgres unavailable — skipping streak state (local dev mode)")
        return None
    try:
        conn = _pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Check for an active streak
            cur.execute(
                "SELECT * FROM streak_state WHERE status = 'ACTIVE' ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                conn.close()
                return dict(row)

            # Check for expired streak (window passed without completion)
            cur.execute(
                """
                SELECT * FROM streak_state
                WHERE status = 'ACTIVE'
                  AND started_at < NOW() - INTERVAL '%s days'
                """,
                (STREAK_WINDOW_DAYS,),
            )
            expired = cur.fetchone()
            if expired:
                cur.execute(
                    "UPDATE streak_state SET status='CASHED', notes='Window expired' WHERE id=%s",
                    (expired["id"],),
                )
                conn.commit()

            # Create new streak
            cur.execute(
                """
                INSERT INTO streak_state (entry_amount, current_pick, wins_in_row, status)
                VALUES (%s, 0, 0, 'ACTIVE')
                RETURNING *
                """,
                (entry_amount,),
            )
            new_row = cur.fetchone()
            conn.commit()
            conn.close()
            logger.info("[Streak] New streak #%d started (entry $%d).", new_row["id"], entry_amount)
            return dict(new_row)
    except Exception as e:
        logger.error("[Streak] get_or_create_active_streak error: %s", e)
        return None


def already_picked_today(streak_id: int, game_date: str) -> bool:
    """Return True if a pick already exists for today's date on this streak."""
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM streak_picks WHERE streak_id=%s AND game_date=%s LIMIT 1",
                (streak_id, game_date),
            )
            exists = cur.fetchone() is not None
        conn.close()
        return exists
    except Exception as e:
        logger.warning("[Streak] already_picked_today error: %s", e)
        return True   # safe default: skip rather than double-pick


def get_prior_pick_team(streak_id: int) -> str | None:
    """Return the team of the last pick in the streak (for team-diversity rule)."""
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT team FROM streak_picks
                WHERE streak_id = %s
                ORDER BY pick_number DESC
                LIMIT 1
                """,
                (streak_id,),
            )
            row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.warning("[Streak] get_prior_pick_team error: %s", e)
        return None


def record_streak_pick(streak_id: int, pick_number: int,
                       pick: StreakCandidate, game_date: str) -> int | None:
    """Insert a new streak pick row and return its id."""
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO streak_picks
                  (streak_id, pick_number, player_name, team, prop_type, line,
                   direction, platform, confidence, probability, ev_pct,
                   signal_count, game_date, status)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
                RETURNING id
                """,
                (
                    streak_id, pick_number,
                    pick.player_name, pick.team, pick.prop_type, pick.line,
                    pick.side, pick.platform,
                    pick.confidence, round(pick.implied_prob, 4), pick.ev_pct,
                    pick.signal_count, game_date,
                ),
            )
            pick_id = cur.fetchone()[0]
            cur.execute(
                """
                UPDATE streak_state
                SET current_pick = %s, last_pick_at = NOW()
                WHERE id = %s
                """,
                (pick_number, streak_id),
            )
        conn.commit()
        conn.close()
        logger.info(
            "[Streak] Recorded pick #%d — %s %s %.1f %s (conf %.1f)",
            pick_number, pick.player_name, pick.prop_type,
            pick.line, pick.side, pick.confidence,
        )
        return pick_id
    except Exception as e:
        logger.error("[Streak] record_streak_pick error: %s", e)
        return None


# ---------------------------------------------------------------------------
# Discord alerts
# ---------------------------------------------------------------------------

_PRIZE_EMOJI = {1: "🥉", 5: "🥈", 10: "🥇"}

_PROP_LABELS: dict[str, str] = {
    "hits":           "Hits",
    "rbis":           "RBIs",
    "runs":           "Runs Scored",
    "total_bases":    "Total Bases",
    "hits_runs_rbis": "H+R+RBI",
    "strikeouts":     "Pitcher Ks",
    "earned_runs":    "Earned Runs",
    "pitching_outs":  "Pitching Outs",
    "hits_allowed":   "Hits Allowed",
    "fantasy_hitter": "Hitter Fantasy Pts",
    "fantasy_pitcher":"Pitcher Fantasy Pts",
}


def _send_webhook(payload: dict) -> bool:
    """Send a raw Discord embed payload to the webhook."""
    try:
        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=15)
        if resp.status_code in (200, 204):
            return True
        logger.warning("[Streak] Discord HTTP %d: %s", resp.status_code, resp.text[:200])
        return False
    except Exception as e:
        logger.warning("[Streak] Discord send error: %s", e)
        return False


def post_pick_alert(
    pick: StreakCandidate,
    pick_number: int,
    wins_in_row: int,
    entry_amount: int,
    season_picks: int,
    season_wins: int,
    line_compare_note: str = "",
) -> None:
    """Post the 8:00 AM PT pick announcement to Discord."""
    stake_usd, prize_usd = ENTRY_TIERS.get(entry_amount, (10.0, 10_000.0))
    remaining  = STREAK_TOTAL_WINS - wins_in_row - 1   # after this pick
    prize_tier = _PRIZE_EMOJI.get(entry_amount, "💰")

    prop_label = _PROP_LABELS.get(pick.prop_type, pick.prop_type.replace("_", " ").title())
    direction  = "HIGHER 📈" if pick.side == "Over" else "LOWER 📉"
    team_str   = f" ({pick.team})" if pick.team else ""

    # Confidence bar
    filled = int(round(pick.confidence))
    conf_bar = "█" * filled + "░" * (10 - filled)

    # Streak progress bar
    prog_filled  = wins_in_row
    prog_bar     = "🟩" * prog_filled + "⬜" * (STREAK_TOTAL_WINS - prog_filled)

    # Win-probability phrasing
    prob_pct = round(pick.implied_prob * 100, 1)

    season_rate = f"{season_wins}/{season_picks}" if season_picks else "0/0"

    # Line comparison note (Phase 92)
    _note_str = f"\n📌 {line_compare_note}" if line_compare_note and "Not found" not in line_compare_note else ""

    embed = {
        "title": f"🔥 StreakAgent — Pick {pick_number}/{STREAK_TOTAL_WINS}",
        "color": 0xF39C12,   # amber — streak in progress
        "fields": [
            {
                "name": f"🎯 {pick.player_name}{team_str}",
                "value": (
                    f"**{direction}  {pick.line}  {prop_label}**\n"
                    f"Platform: **{pick.platform}** | Entry type: `{pick.entry_type}`"
                    f"{_note_str}"
                ),
                "inline": False,
            },
            {
                "name": "📊 Edge",
                "value": (
                    f"Win Prob: **{prob_pct}%** (edge: +{pick.ev_pct:.1f}% vs 50%)\n"
                    f"Streak P(complete): ~**{(pick.implied_prob**STREAK_TOTAL_WINS)*100:.2f}%** per $10\n"
                    f"Signals: **{pick.signal_count}/17** agents agree"
                ),
                "inline": True,
            },
            {
                "name": "🎯 Confidence",
                "value": (
                    f"`{conf_bar}` **{pick.confidence:.1f}/10**\n"
                    f"_(gate: {STREAK_CONF_MIN}/10)_"
                ),
                "inline": True,
            },
            {
                "name": "🔥 Streak Progress",
                "value": (
                    f"{prog_bar}\n"
                    f"**{wins_in_row}/{STREAK_TOTAL_WINS}** complete"
                    f" — need **{remaining}** more after this pick"
                ),
                "inline": False,
            },
            {
                "name": f"{prize_tier} Prize",
                "value": f"**${prize_usd:,.0f}** on a **${stake_usd:.0f}** entry",
                "inline": True,
            },
            {
                "name": "📈 Season Record",
                "value": f"Streak picks: {season_rate} (W/total)",
                "inline": True,
            },
            {
                "name": f"📋 HOW TO ENTER — Pick {pick_number} of 11",
                "value": (
                    f"**Step 1:** Open Underdog Fantasy → tap **Streaks** tab\n"
                    f"**Step 2:** Find your active streak → tap **'Add Pick'**\n"
                    f"**Step 3:** Find the player → select **Higher** or **Lower**\n"
                    f"**Step 4:** Tap **Submit** ✅\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    "⚠️ Picks 3–11 **cannot be cancelled** after submission\n"
                    "🔄 If this pick **ties or voids** → slot auto-replaced with next pick\n"
                    "💡 **Win Now** available anytime to lock in early winnings\n"
                    "⏱️ You have **10 days** after this settles to add the next pick"
                ),
                "inline": False,
            },
        ],
        "footer": {
            "text": (
                f"PropIQ StreakAgent • {datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%b %d %Y %H:%M')} PT • "
                f"Confidence gate ≥ {STREAK_CONF_MIN}/10 • Prob gate ≥ {int(STREAK_PROB_MIN*100)}%"
            )
        },
        "timestamp": datetime.now(ZoneInfo("America/Los_Angeles")).isoformat(),
    }

    payload = {"embeds": [embed]}
    if _send_webhook(payload):
        logger.info("[Streak] Pick alert sent: pick %d/%d", pick_number, STREAK_TOTAL_WINS)


def post_start_picks_alert(
    picks: list[StreakCandidate],
    entry_amount: int,
    season_picks: int,
    season_wins: int,
    notes: list | None = None,
) -> None:
    """Post the combined 2-pick announcement for a fresh streak start."""
    stake_usd, prize_usd = ENTRY_TIERS.get(entry_amount, (10.0, 10_000.0))
    prize_tier  = _PRIZE_EMOJI.get(entry_amount, "💰")
    season_rate = f"{season_wins}/{season_picks}" if season_picks else "0/0"
    notes       = notes or ["", ""]

    fields = []
    for i, pick in enumerate(picks, start=1):
        prop_label = _PROP_LABELS.get(pick.prop_type, pick.prop_type.replace("_", " ").title())
        direction  = "HIGHER 📈" if pick.side == "Over" else "LOWER 📉"
        team_str   = f" ({pick.team})" if pick.team else ""
        note       = notes[i - 1] if i - 1 < len(notes) else ""
        note_str   = f"\n📌 {note}" if note and "Not found" not in note else ""
        filled     = int(round(pick.confidence))
        conf_bar   = "█" * filled + "░" * (10 - filled)
        prob_pct   = round(pick.implied_prob * 100, 1)
        fields.append({
            "name": f"🎯 Pick {i} — {pick.player_name}{team_str}",
            "value": (
                f"**{direction}  {pick.line}  {prop_label}**\n"
                f"Platform: **{pick.platform}** | Entry: `{pick.entry_type}`{note_str}\n"
                f"Win Prob: **{prob_pct}%** | Edge: +{pick.ev_pct:.1f}% vs 50% | "
                f"Signals: **{pick.signal_count}/17**\n"
                f"`{conf_bar}` **{pick.confidence:.1f}/10**"
            ),
            "inline": False,
        })

    fields += [
        {
            "name": f"{prize_tier} Prize",
            "value": f"**${prize_usd:,.0f}** on a **${stake_usd:.0f}** entry",
            "inline": True,
        },
        {
            "name": "📈 Season Record",
            "value": f"Streak picks: {season_rate} (W/total)",
            "inline": True,
        },
        {
            "name": "📋 HOW TO ENTER — Start Your Streak",
            "value": (
                "**Step 1:** Open Underdog Fantasy → tap **Streaks** tab\n"
                "**Step 2:** Tap **'Start New Streak'** → select **$10 entry**\n"
                "**Step 3:** Add **Pick 1** — find the player, tap Higher or Lower\n"
                "**Step 4:** Add **Pick 2** — must be from a **different team**\n"
                "**Step 5:** Tap **Submit** ✅\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "⚠️ **Both picks must win** to advance to Pick 3\n"
                "🔄 If either pick **ties or voids** → full entry **refunded**\n"
                "❌ Picks 1–2 can be **cancelled** shortly after submission\n"
                "⏱️ You have **10 days** after each settled pick to add the next\n"
                "🏆 Win all 11 → **$10,000** payout\n"
                "💡 Max 3 active streaks at a time"
            ),
            "inline": False,
        },
    ]

    embed = {
        "title": "🔥 StreakAgent — FRESH START (Picks 1 & 2)",
        "description": (
            "New streak begins! Picks 1 & 2 are from **different teams**.\n"
            "All 11 must be correct — any wrong pick **auto-resets** to Pick 1."
        ),
        "color":     0x2ECC71,   # green — new streak
        "fields":    fields,
        "footer":    {
            "text": (
                f"PropIQ StreakAgent • "
                f"{datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%b %d %Y %H:%M')} PT • "
                f"Confidence gate ≥ {STREAK_CONF_MIN}/10 • "
                f"Prob gate ≥ {int(STREAK_PROB_MIN * 100)}%"
            )
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if _send_webhook({"embeds": [embed]}):
        logger.info("[Streak] Fresh-start alert sent (%d picks)", len(picks))


def post_settlement_alert(
    pick_number: int,
    player_name: str,
    prop_type: str,
    line: float,
    direction: str,
    actual: float,
    outcome: str,
    wins_in_row: int,
    entry_amount: int,
    streak_status: str,   # ACTIVE / WON / LOST / VOIDED
) -> None:
    """Post the 2 AM settlement result to Discord."""
    stake_usd, prize_usd = ENTRY_TIERS.get(entry_amount, (10.0, 10_000.0))
    prop_label = _PROP_LABELS.get(prop_type, prop_type.replace("_", " ").title())
    direction_label = "Higher 📈" if direction == "Over" else "Lower 📉"

    outcome_emoji = {"WIN": "✅", "LOSS": "❌", "PUSH": "➖", "VOID": "🔄"}.get(outcome, "❓")
    colour = {
        "WIN":  0x2ECC71,   # green
        "LOSS": 0xE74C3C,   # red
        "PUSH": 0x95A5A6,   # grey
        "VOID": 0x3498DB,   # blue
    }.get(outcome, 0x95A5A6)

    # Status-specific messaging
    if streak_status == "WON":
        status_line = f"🏆 **STREAK COMPLETE! {STREAK_TOTAL_WINS}/{STREAK_TOTAL_WINS}** — You won **${prize_usd:,.0f}**! 🎉"
    elif streak_status == "LOST":
        status_line = f"💔 Pick {pick_number} missed — streak **auto-reset**! New streak starts tomorrow with 2 fresh picks."
    elif streak_status == "VOIDED":
        if pick_number <= 2:
            status_line = (
                "🔄 Pick voided on picks 1–2 — **full entry refunded** by Underdog. "
                "Check your account balance. A new streak can be started tomorrow."
            )
        else:
            status_line = (
                f"🔄 Pick {pick_number} voided — this slot is **automatically replaced** "
                "by your next pick. Your streak count is preserved."
            )
    else:
        remaining = STREAK_TOTAL_WINS - wins_in_row
        status_line = f"🔥 Streak active — **{wins_in_row}/{STREAK_TOTAL_WINS}** wins ({remaining} to go)"

    embed = {
        "title": f"{outcome_emoji} Streak Pick {pick_number} — {outcome}",
        "color": colour,
        "fields": [
            {
                "name": f"📋 {player_name}",
                "value": (
                    f"{direction_label} **{line}** {prop_label}\n"
                    f"Actual: **{actual}** | Result: **{outcome}**"
                ),
                "inline": False,
            },
            {
                "name": "🔥 Streak",
                "value": status_line,
                "inline": False,
            },
        ],
        "footer": {
            "text": (
                f"PropIQ StreakAgent Settlement • "
                f"{datetime.now(timezone.utc).strftime('%b %d %Y')} • "
                f"${stake_usd:.0f} entry → ${prize_usd:,.0f} prize"
            )
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    _send_webhook({"embeds": [embed]})


def post_milestone_alert(pick_number: int, wins_in_row: int, entry_amount: int) -> None:
    """Post a milestone celebration at picks 5/11 and 8/11."""
    if wins_in_row not in (5, 8):
        return
    stake_usd, prize_usd = ENTRY_TIERS.get(entry_amount, (10.0, 10_000.0))
    remaining = STREAK_TOTAL_WINS - wins_in_row

    milestone_msg = {
        5: f"🔥🔥🔥 **HALFWAY THERE!** 5/11 wins locked in. {remaining} more to collect **${prize_usd:,.0f}**!",
        8: f"🚨 **3 PICKS AWAY from ${prize_usd:,.0f}!!** 8/11 wins — let's close it out!",
    }

    embed = {
        "title": f"🏆 Streak Milestone — {wins_in_row}/{STREAK_TOTAL_WINS}",
        "color": 0xF1C40F,   # gold
        "description": milestone_msg.get(wins_in_row, ""),
        "footer": {"text": "PropIQ StreakAgent"},
        "timestamp": datetime.now(ZoneInfo("America/Los_Angeles")).isoformat(),
    }
    _send_webhook({"embeds": [embed]})


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------

_PROP_TO_ESPN_STAT: dict[str, str] = {
    "hits":            "hits",
    "rbis":            "RBIs",
    "runs":            "runs",
    "total_bases":     "totalBases",
    "hits_runs_rbis":  "hits",        # composite: H+R+RBI computed manually above
    "strikeouts":      "strikeouts",  # pitcher Ks
    "earned_runs":     "earnedRuns",
    "pitching_outs":   "outsPitched",  # pitcher outs recorded (ESPN label)
    "hits_allowed":    "hits_allowed",  # pitcher stat — hits allowed (distinct from batter hits)
    "walks_allowed":   "walks",
    "hitter_strikeouts": "Strikeouts",  # ESPN stores batter Ks under 'strikeouts' key (capitalized)
    "fantasy_score":   None,            # composite stat — graded manually, no direct ESPN lookup
}


def fetch_espn_boxscore_stats(game_date: str) -> dict[str, dict]:
    """
    Fetch final box score stats from ESPN internal JSON API.
    Returns {player_name_lower: {stat_key: value, ...}}.
    """
    stat_lookup: dict[str, dict] = {}
    try:
        url = f"{_ESPN_BASE}/scoreboard"
        resp = requests.get(url, params={"dates": game_date.replace("-", "")}, timeout=25)
        if resp.status_code != 200:
            return stat_lookup
        events = resp.json().get("events", [])
        for event in events:
            for comp in event.get("competitions", []):
                for team_comp in comp.get("competitors", []):
                    for athlete in team_comp.get("athletes", []):
                        display_name = athlete.get("athlete", {}).get("displayName", "")
                        stats = {}
                        for stat in athlete.get("stats", []):
                            name  = stat.get("name", "")
                            value = stat.get("value")
                            if name and value is not None:
                                try:
                                    stats[name] = float(value)
                                except (TypeError, ValueError):
                                    pass
                        if display_name:
                            stat_lookup[display_name.lower()] = stats
    except Exception as e:
        logger.warning("[Streak] ESPN fetch error: %s", e)
    return stat_lookup


def _grade_pick(
    stat_lookup: dict[str, dict],
    player_name: str,
    prop_type: str,
    line: float,
    direction: str,
) -> tuple[str, float | None]:
    """
    Grade a single streak pick against ESPN box scores.
    Returns (outcome, actual_value):
      outcome: 'WIN' | 'LOSS' | 'PUSH' | 'VOID'
    VOID is returned when: (a) player not in ESPN data (DNP/postponed), or
    (b) player found but specific stat key missing (incomplete box score).
    """
    player_key = player_name.lower()
    stats = stat_lookup.get(player_key)

    # Try last-name fallback
    if not stats:
        last = player_key.split()[-1]
        for key, s in stat_lookup.items():
            if key.endswith(last):
                stats = s
                break

    if not stats:
        # Player not in ESPN data → DNP or postponed → VOID
        return "VOID", None

    # Composite H+R+RBI
    if prop_type == "hits_runs_rbis":
        h   = stats.get("hits",   stats.get("H",   0.0))
        r   = stats.get("runs",   stats.get("R",   0.0))
        rbi = stats.get("RBIs",   stats.get("RBI", 0.0))
        actual = h + r + rbi
    else:
        espn_key = _PROP_TO_ESPN_STAT.get(prop_type, "")
        actual_raw = stats.get(espn_key)
        if actual_raw is None:
            # Stat key missing from box score (incomplete data) → VOID
            return "VOID", None
        actual = float(actual_raw)

    # Grade
    if direction == "Over":
        outcome = "WIN" if actual > line else ("PUSH" if actual == line else "LOSS")
    else:
        outcome = "WIN" if actual < line else ("PUSH" if actual == line else "LOSS")

    return outcome, actual


def settle_streak_picks(game_date: str) -> None:
    """
    Called at 2 AM by the settlement engine.
    Grades all PENDING streak picks for game_date and updates state.
    """
    try:
        conn = _pg_conn()
    except Exception as e:
        logger.warning("[Streak] settle_streak_picks — DB error: %s", e)
        return

    # Fetch all PENDING picks for the date
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT sp.*, ss.entry_amount, ss.wins_in_row, ss.status as streak_status
                FROM streak_picks sp
                JOIN streak_state ss ON ss.id = sp.streak_id
                WHERE sp.game_date = %s AND sp.status = 'PENDING'
                ORDER BY sp.pick_number ASC
                """,
                (game_date,),
            )
            pending = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.warning("[Streak] settle fetch error: %s", e)
        conn.close()
        return

    if not pending:
        logger.info("[Streak] No PENDING picks for %s", game_date)
        conn.close()
        return

    # Fetch ESPN stats — prefer the richer espn_scraper.get_all_player_stats
    # which uses the game summary endpoint + MLB Stats API supplement (doubles, pitching_outs, etc.)
    # Fall back to local fetch_espn_boxscore_stats if import fails.
    stat_lookup: dict = {}
    try:
        from espn_scraper import get_all_player_stats as _espn_full  # noqa: PLC0415
        # get_all_player_stats expects YYYYMMDD format
        _date_fmt = game_date.replace("-", "")
        raw = _espn_full(_date_fmt)
        # Remap: raw is keyed lowercase player name → {hits, runs, rbi, pitching_outs, ...}
        # _grade_pick expects keys matching _PROP_TO_ESPN_STAT labels OR direct stat names
        for _name_lc, _espn in raw.items():
            stat_lookup[_name_lc] = {
                "hits":         _espn.get("hits",          0.0),   # batter hits
                "hits_allowed": _espn.get("hits_allowed",  0.0),   # pitcher hits allowed (separate key)
                "runs":         _espn.get("runs",          0.0),
                "RBIs":         _espn.get("rbis", _espn.get("rbi", 0.0)),
                "totalBases":   _espn.get("total_bases",   0.0),
                "strikeouts":   _espn.get("strikeouts",    0.0),
                "earnedRuns":   _espn.get("earned_runs",   0.0),
                "outsPitched":  _espn.get("pitching_outs", 0.0),
                "walks":        _espn.get("base_on_balls", 0.0),
            }
        logger.info("[Streak] Settlement using espn_scraper stats for %d players", len(stat_lookup))
    except Exception as _esp_err:
        logger.warning("[Streak] espn_scraper import failed — falling back to scoreboard: %s", _esp_err)
        stat_lookup = fetch_espn_boxscore_stats(game_date)

    lost_streaks: set[int] = set()   # streak IDs that auto-reset this batch

    for pick in pending:
        _sid_check = pick["streak_id"]
        if _sid_check in lost_streaks:
            # Streak already reset this batch — skip remaining picks from it
            logger.info(
                "[Streak] Skip pick #%d for streak %d (already auto-reset this batch)",
                pick["pick_number"], _sid_check,
            )
            continue

        outcome, actual = _grade_pick(
            stat_lookup,
            pick["player_name"],
            pick["prop_type"],
            float(pick["line"]),
            pick["direction"],
        )

        if outcome == "VOID":
            # Check if we have ANY ESPN data for the day — if yes, player truly DNP → VOID.
            # If ESPN returned 0 players, data not yet available → leave PENDING.
            if not stat_lookup:
                logger.info("[Streak] No ESPN data yet for %s — leaving PENDING", pick["player_name"])
                continue
            logger.info("[Streak] %s graded VOID (DNP / game postponed)", pick["player_name"])
            # Fall through to VOID handling in state machine below

        streak_id    = pick["streak_id"]
        pick_number  = pick["pick_number"]
        entry_amount = pick.get("entry_amount", DEFAULT_ENTRY)

        try:
            with conn.cursor() as cur:
                # Update the pick row
                cur.execute(
                    """
                    UPDATE streak_picks
                    SET status=%s, actual_result=%s, settled_at=NOW()
                    WHERE id=%s
                    """,
                    (outcome, actual, pick["id"]),
                )

                # Update streak state
                if outcome == "WIN":
                    # Re-read wins_in_row to avoid stale batch reads
                    cur.execute(
                        "SELECT wins_in_row, current_pick FROM streak_state WHERE id=%s FOR UPDATE",
                        (streak_id,),
                    )
                    _fresh = cur.fetchone()
                    new_wins = (_fresh[0] if _fresh else pick["wins_in_row"]) + 1
                    new_current = (_fresh[1] if _fresh else 0) + 1
                    streak_status = "WON" if new_wins >= STREAK_TOTAL_WINS else "ACTIVE"
                    cur.execute(
                        "UPDATE streak_state SET wins_in_row=%s, current_pick=%s, status=%s WHERE id=%s",
                        (new_wins, new_current, streak_status, streak_id),
                    )
                elif outcome == "LOSS":
                    streak_status = "LOST"
                    cur.execute(
                        "UPDATE streak_state SET status='LOST' WHERE id=%s",
                        (streak_id,),
                    )
                    # AUTO-RESET: create a fresh ACTIVE streak immediately
                    cur.execute(
                        """
                        INSERT INTO streak_state (entry_amount, current_pick, wins_in_row, status)
                        VALUES (%s, 0, 0, 'ACTIVE')
                        """,
                        (entry_amount,),
                    )
                    lost_streaks.add(streak_id)
                    logger.info(
                        "[Streak] Auto-reset: new streak created after loss on pick %d",
                        pick_number,
                    )
                elif outcome == "VOID":
                    streak_status = "VOIDED"
                    if pick_number <= 2:
                        # Full restart
                        cur.execute(
                            """
                            UPDATE streak_state
                            SET status='ACTIVE', wins_in_row=0, current_pick=0
                            WHERE id=%s
                            """,
                            (streak_id,),
                        )
                    # picks 3-11: pick_number preserved; next pick resumes at same slot
                elif outcome == "PUSH":
                    streak_status = "ACTIVE"
                    # PUSH = exact line tie. Underdog Streaks: pick slot is voided and
                    # replaced — user picks again for the same slot number.
                    # Mark this pick PUSH so it doesn't count toward wins_in_row,
                    # and reset current_pick so tomorrow's run re-uses this slot.
                    cur.execute(
                        "UPDATE streak_state SET current_pick = GREATEST(0, current_pick - 1) WHERE id=%s",
                        (streak_id,),
                    )
                    logger.info("[Streak] Pick #%d PUSH — slot preserved for re-pick", pick_number)
            conn.commit()
        except Exception as e:
            logger.error("[Streak] DB update error: %s", e)
            continue

        # Fetch updated wins_in_row for alert
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT wins_in_row, status FROM streak_state WHERE id=%s", (streak_id,))
                row = cur.fetchone()
            updated_wins  = row[0] if row else 0
            updated_status = row[1] if row else "ACTIVE"
        except Exception:
            updated_wins  = 0
            updated_status = "ACTIVE"

        post_settlement_alert(
            pick_number  = pick_number,
            player_name  = pick["player_name"],
            prop_type    = pick["prop_type"],
            line         = float(pick["line"]),
            direction    = pick["direction"],
            actual       = actual if actual is not None else 0.0,
            outcome      = outcome,
            wins_in_row  = updated_wins,
            entry_amount = entry_amount,
            streak_status = updated_status,
        )

        if outcome == "WIN":
            post_milestone_alert(pick_number, updated_wins, entry_amount)

        if updated_status in ("WON", "LOST"):
            if updated_status == "WON":
                logger.info("[Streak] 🏆 Streak #%d WON — auto-creating next streak.", streak_id)

    conn.close()


# ---------------------------------------------------------------------------
# Season stats helpers (for footer in pick alert)
# ---------------------------------------------------------------------------

def get_streak_season_stats() -> tuple[int, int]:
    """Return (total_picks, total_wins) from streak_picks this calendar year."""
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            year = datetime.now(ZoneInfo("America/Los_Angeles")).year
            cur.execute(
                """
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) as wins
                FROM streak_picks
                WHERE EXTRACT(YEAR FROM picked_at) = %s
                  AND status != 'PENDING'
                """,
                (year,),
            )
            row = cur.fetchone()
        conn.close()
        total = int(row[0] or 0)
        wins  = int(row[1] or 0)
        return total, wins
    except Exception:
        return 0, 0


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_streak_pick(
    date_str: str | None = None,
    entry_amount: int = DEFAULT_ENTRY,
    dry_run: bool = False,
) -> dict | None:
    """
    Morning run (8:00 AM PT — streak window fires before main dispatch).

    Fetches props → scores → selects best pick → persists → alerts Discord.
    Returns the pick dict, or None if no qualifying pick exists today.
    """
    date = date_str or datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")
    logger.info("[Streak] === StreakAgent run for %s ===", date)

    # Ensure DB tables exist
    ensure_streak_tables()

    # Load or create active streak
    streak = get_or_create_active_streak(entry_amount)
    if not streak:
        logger.warning("[Streak] Could not load/create streak state — aborting.")
        return None

    streak_id    = streak["id"]
    wins_in_row  = streak["wins_in_row"]
    current_pick = streak["current_pick"]
    pick_number  = wins_in_row + 1    # next pick needed

    # is_fresh_start = True when:
    #   A) No picks exist for this streak yet (clean start), OR
    #   B) wins_in_row == 0 AND no picks recorded for TODAY (picks may exist from
    #      prior failed attempts on other dates — e.g. partial insert + Discord crash)
    # This ensures both picks 1 & 2 always go out together on a fresh start day.
    try:
        _fs_conn = _pg_conn()
        with _fs_conn.cursor() as _fc:
            # Count total picks for this streak
            _fc.execute(
                "SELECT COUNT(*) FROM streak_picks WHERE streak_id = %s",
                (streak_id,)
            )
            _pick_count = _fc.fetchone()[0]
            # Count picks recorded for TODAY specifically
            _fc.execute(
                "SELECT COUNT(*) FROM streak_picks WHERE streak_id = %s AND game_date = %s",
                (streak_id, date)
            )
            _today_pick_count = _fc.fetchone()[0]
        _fs_conn.close()
        # Fresh start if: no picks at all, OR no wins yet AND no picks today
        is_fresh_start = (_pick_count == 0) or (wins_in_row == 0 and _today_pick_count == 0)
    except Exception as _fs_err:
        logger.warning("[Streak] is_fresh_start DB check failed: %s — using state columns", _fs_err)
        is_fresh_start = (wins_in_row == 0 and current_pick == 0)

    logger.info(
        "[Streak] State: streak_id=%d wins=%d current_pick=%d pick_number=%d is_fresh_start=%s",
        streak_id, wins_in_row, current_pick, pick_number, is_fresh_start,
    )

    # Already picked today?
    if already_picked_today(streak_id, date):
        logger.info("[Streak] Already picked today (%s) — skipping.", date)
        return None

    # Check for streak completion (shouldn't happen here, but guard)
    if wins_in_row >= STREAK_TOTAL_WINS:
        logger.info("[Streak] Streak #%d already won — a new one should have started.", streak_id)
        return None

    # Fetch today's Underdog props
    raw_props = fetch_underdog_props_with_teams()
    if not raw_props:
        logger.warning("[Streak] No Underdog props available today — skipping.")
        return None

    # Score all props
    candidates = evaluate_props_for_streaks(raw_props)
    logger.info(
        "[Streak] Scored %d candidates (prob≥%.0f%%, ev≥%.0f%%, conf≥%.1f)",
        sum(1 for c in candidates
            if c.confidence >= STREAK_CONF_MIN
            and c.implied_prob >= STREAK_PROB_MIN
            and c.ev_pct >= STREAK_EV_MIN),
        STREAK_PROB_MIN * 100, STREAK_EV_MIN, STREAK_CONF_MIN,
    )

    # ── Pre-fetch both platforms once for line comparison ───────────────────
    _ud_props_lc: list[dict] = []
    _pp_raw_lc:   list[dict] = []
    if _LINE_COMP_AVAILABLE:
        try:
            _ud_props_lc = fetch_underdog_props_with_teams()
            _pp_resp = requests.get(
                "https://partner-api.prizepicks.com/projections",
                params={"league_id": 2, "per_page": 1000, "include": "new_player"},
                headers={"Accept": "application/json",
                         "Referer": "https://app.prizepicks.com/",
                         "User-Agent": "Mozilla/5.0"},
                timeout=15,
            )
            if _pp_resp.status_code == 200:
                _pp_data = _pp_resp.json()
                _pp_pmap = {
                    p["id"]: (
                        p.get("attributes", {}).get("name") or
                        p.get("attributes", {}).get("display_name", "")
                    )
                    for p in _pp_data.get("included", [])
                    if p.get("type") == "new_player"
                }
                for _proj in _pp_data.get("data", []):
                    _a = _proj.get("attributes", {})
                    if str(_a.get("odds_type", "standard") or "standard").lower() not in ("standard", ""):
                        continue
                    if _a.get("adjusted_odds") or _a.get("is_live"):
                        continue
                    _pid = (_proj.get("relationships", {})
                                 .get("new_player", {})
                                 .get("data", {})
                                 .get("id", ""))
                    _pname  = _pp_pmap.get(_pid, "")
                    _line_v = _a.get("line_score")
                    _stat   = _a.get("stat_type", "")
                    if _pname and _line_v is not None and _stat:
                        _pp_raw_lc.append({"player_name": _pname,
                                           "prop_type":   _stat,
                                           "line":        float(_line_v)})
        except Exception as _fe:
            logger.debug("[Streak] Platform pre-fetch error: %s", _fe)

    def _apply_line_comp(p: StreakCandidate) -> str:
        """
        Compare UD vs PP lines for informational note only.
        NEVER switches platform to PrizePicks — Underdog Streaks is UD-only.
        May update p.line to a better UD line if found.
        """
        if not _LINE_COMP_AVAILABLE:
            return ""
        try:
            _ud_lk = _build_ll(_ud_props_lc)
            _pp_lk = _build_ll(_pp_raw_lc)
            _c = _cmp_prop(p.player_name, p.prop_type, p.side, _ud_lk, _pp_lk)
            _n = _c.get("note", "")
            # Streaks = Underdog ONLY. Never switch platform to PrizePicks.
            # PrizePicks line shown as informational note only if it differs from UD.
            if _c.get("platform") == "Underdog" and _c.get("line") is not None:
                # UD has a better/different line — update to the actual UD line
                p.line = _c["line"]
            elif _c.get("platform") == "PrizePicks" and _c.get("line") is not None:
                # PP has a better line but we can't use it for Streaks.
                # Note it for the user but keep platform = Underdog.
                logger.info("[Streak] PP has better line (%s) — staying on UD for Streaks", _n)
                _n = f"UD only (Streaks) — PP line: {_c.get('line', '')}"
            p.platform = "Underdog"   # enforce: Streaks picks are always Underdog
            return _n
        except Exception as _ce:
            logger.debug("[Streak] Line comp error: %s", _ce)
            return ""

    # ── FRESH START: select 2 picks from different teams ────────────────────
    if is_fresh_start:
        start_picks = select_start_picks(candidates)
        if len(start_picks) < 2:
            logger.info(
                "[Streak] Need 2 qualifying picks to start streak — only %d qualify today. "
                "Skipping until more props are available.",
                len(start_picks),
            )
            return None

        notes = [_apply_line_comp(sp) for sp in start_picks]

        for i, sp in enumerate(start_picks, start=1):
            logger.info(
                "[Streak] ✅ Start pick #%d: %s %s %.1f %s | conf=%.1f/10 | "
                "prob=%.1f%% | ev=+%.1f%% | signals=%d",
                i, sp.player_name, sp.prop_type, sp.line, sp.side,
                sp.confidence, sp.implied_prob * 100, sp.ev_pct, sp.signal_count,
            )

        if not dry_run:
            # Clean up any stale PENDING picks from prior failed attempts on OTHER dates
            # (same streak_id, pick_number 1 or 2, but from a different game_date)
            # Without this, record_streak_pick will fail with a duplicate key error
            # if picks 1 & 2 were partially recorded before a crash/redeploy.
            try:
                _clean_conn = _pg_conn()
                with _clean_conn.cursor() as _cc:
                    _cc.execute(
                        """
                        DELETE FROM streak_picks
                        WHERE streak_id = %s
                          AND game_date != %s
                          AND pick_number IN (1, 2)
                          AND status = 'PENDING'
                        """,
                        (streak_id, date),
                    )
                    _deleted = _cc.rowcount
                _clean_conn.commit()
                _clean_conn.close()
                if _deleted:
                    logger.info("[Streak] Cleaned %d stale PENDING picks from prior attempt.", _deleted)
            except Exception as _ce:
                logger.warning("[Streak] Stale pick cleanup failed (non-fatal): %s", _ce)

            for i, sp in enumerate(start_picks, start=1):
                record_streak_pick(streak_id, i, sp, date)
            season_total, season_wins_c = get_streak_season_stats()
            post_start_picks_alert(start_picks, entry_amount, season_total, season_wins_c, notes)
        else:
            for i, sp in enumerate(start_picks, start=1):
                logger.info(
                    "[DRY-RUN] Start pick %d/%d — %s %s %.1f %s | conf=%.1f | %s",
                    i, STREAK_TOTAL_WINS, sp.player_name, sp.prop_type,
                    sp.line, sp.side, sp.confidence, notes[i - 1],
                )

        return {
            "streak_id":   streak_id,
            "fresh_start": True,
            "picks": [
                {
                    "pick_number":        i,
                    "player_name":        sp.player_name,
                    "team":               sp.team,
                    "prop_type":          sp.prop_type,
                    "line":               sp.line,
                    "direction":          sp.side,
                    "platform":           sp.platform,
                    "confidence":         sp.confidence,
                    "probability":        round(sp.implied_prob, 4),
                    "ev_pct":             sp.ev_pct,
                    "signal_count":       sp.signal_count,
                    "game_date":          date,
                    "line_compare_note":  notes[i - 1],
                }
                for i, sp in enumerate(start_picks, start=1)
            ],
        }

    # ── CONTINUING STREAK: 1 pick ────────────────────────────────────────────
    prior_team = get_prior_pick_team(streak_id) if pick_number <= 2 else None

    pick = select_streak_pick(candidates, pick_number, prior_team)
    if not pick:
        logger.info(
            "[Streak] No qualifying pick today (conf≥%.1f/10). Skipping — "
            "better to wait than force a marginal pick.",
            STREAK_CONF_MIN,
        )
        return None

    logger.info(
        "[Streak] ✅ Pick #%d selected: %s %s %.1f %s | conf=%.1f/10 | "
        "prob=%.1f%% | ev=+%.1f%% | signals=%d",
        pick_number, pick.player_name, pick.prop_type, pick.line, pick.side,
        pick.confidence, pick.implied_prob * 100, pick.ev_pct, pick.signal_count,
    )

    _line_comparison_note = _apply_line_comp(pick)
    if _LINE_COMP_AVAILABLE:
        try:
            _ud_props = fetch_underdog_props_with_teams()
            _pp_resp  = requests.get(
                "https://partner-api.prizepicks.com/projections",
                params={"league_id": 2, "per_page": 1000, "include": "new_player"},
                headers={"Accept": "application/json",
                         "Referer": "https://app.prizepicks.com/",
                         "User-Agent": "Mozilla/5.0"},
                timeout=15,
            )
            _pp_raw: list[dict] = []
            if _pp_resp.status_code == 200:
                _pp_data = _pp_resp.json()
                _pp_pmap = {
                    p["id"]: (
                        p.get("attributes", {}).get("name") or
                        p.get("attributes", {}).get("display_name", "")
                    )
                    for p in _pp_data.get("included", [])
                    if p.get("type") == "new_player"
                }
                for _proj in _pp_data.get("data", []):
                    _a = _proj.get("attributes", {})
                    if str(_a.get("odds_type", "standard") or "standard").lower() not in ("standard", ""):
                        continue
                    if _a.get("adjusted_odds") or _a.get("is_live"):
                        continue
                    _pid = (_proj.get("relationships", {})
                                 .get("new_player", {})
                                 .get("data", {})
                                 .get("id", ""))
                    _pname = _pp_pmap.get(_pid, "")
                    _line_v = _a.get("line_score")
                    _stat   = _a.get("stat_type", "")
                    if _pname and _line_v is not None and _stat:
                        _pp_raw.append({"player_name": _pname,
                                        "prop_type":   _stat,
                                        "line":        float(_line_v)})

            _ud_lookup = _build_ll(_ud_props)
            _pp_lookup = _build_ll(_pp_raw)
            _comp = _cmp_prop(pick.player_name, pick.prop_type, pick.side,
                              _ud_lookup, _pp_lookup)
            _line_comparison_note = _comp.get("note", "")

            if _comp.get("platform") == "PrizePicks" and _comp.get("line") is not None:
                # Streaks = Underdog ONLY — never switch to PrizePicks.
                # Log the PP line as informational only.
                logger.info("[Streak] PP has better line (%s) — staying on UD for Streaks",
                            _line_comparison_note)
                _line_comparison_note = f"UD only (Streaks) — PP line: {_comp.get('line', '')}"
            elif _comp.get("platform") == "Underdog" and _comp.get("line") is not None:
                pick.line = _comp["line"]
            pick.platform = "Underdog"   # enforce: Streaks = Underdog only
        except Exception as _lce:
            logger.debug("[Streak] Line comparison error: %s", _lce)

    # Persist to DB
    if not dry_run:
        record_streak_pick(streak_id, pick_number, pick, date)

    # Get season stats for Discord footer
    season_total, season_wins = get_streak_season_stats()

    # Post Discord alert
    if not dry_run:
        post_pick_alert(
            pick              = pick,
            pick_number       = pick_number,
            wins_in_row       = wins_in_row,
            entry_amount      = entry_amount,
            season_picks      = season_total,
            season_wins       = season_wins,
            line_compare_note = _line_comparison_note,
        )
    else:
        logger.info(
            "[DRY-RUN] Would alert: Pick %d/%d — %s %s %.1f %s | conf=%.1f | prob=%.1f%% | %s",
            pick_number, STREAK_TOTAL_WINS,
            pick.player_name, pick.prop_type, pick.line, pick.side,
            pick.confidence, pick.implied_prob * 100, _line_comparison_note,
        )

    return {
        "streak_id":          streak_id,
        "fresh_start":        False,
        "pick_number":        pick_number,
        "player_name":        pick.player_name,
        "team":               pick.team,
        "prop_type":          pick.prop_type,
        "line":               pick.line,
        "direction":          pick.side,
        "platform":           pick.platform,
        "confidence":         pick.confidence,
        "probability":        round(pick.implied_prob, 4),
        "ev_pct":             pick.ev_pct,
        "signal_count":       pick.signal_count,
        "game_date":          date,
        "line_compare_note":  _line_comparison_note,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIQ StreakAgent")
    parser.add_argument("--date",     default=None, help="YYYY-MM-DD (default: today)")
    parser.add_argument("--dry-run",  action="store_true", help="Simulate without DB/Discord")
    parser.add_argument("--settle",   action="store_true", help="Run settlement (2 AM mode)")
    parser.add_argument("--entry",    type=int, choices=[1, 5, 10], default=DEFAULT_ENTRY,
                        help="Entry tier in dollars (1/5/10)")
    args = parser.parse_args()

    if args.settle:
        date = args.date or datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")
        logger.info("[Streak] Running settlement for %s", date)
        settle_streak_picks(date)
    else:
        result = run_streak_pick(
            date_str     = args.date,
            entry_amount = args.entry,
            dry_run      = args.dry_run,
        )
        if result:
            print(json.dumps(result, indent=2))
        else:
            print("No qualifying Streak pick today.")
