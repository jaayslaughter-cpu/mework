"""
streak_agent.py
===============
PropIQ — 19th Agent: StreakAgent

Underdog Fantasy "Streaks" format — 11 consecutive correct picks to win.

Entry tiers:
  $1  entry → $1,000  prize
  $5  entry → $5,000  prize
  $10 entry → $10,000 prize

Rules enforced:
  • Confidence gate    : ≥ 8.0/10  (vs. 7.0 for standard parlays)
  • Probability gate   : ≥ 0.62 implied win probability per pick
  • EV gate            : ≥ 5.0% per pick
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
  3. Run all 18 AGENT_CONFIGS filters to count cross-agent "signals"
  4. Score each prop: streak_confidence() = prob_score + ev_bonus + signal_bonus
  5. Filter: conf ≥ 8.0, prob ≥ 0.62, ev_pct ≥ 5.0%
  6. Apply team-diversity rule for picks 1 & 2
  7. Select top-ranked prop; skip day if nothing qualifies (better than a bad pick)

State persistence:
  • Postgres tables: streak_state (one row per active streak),
                     streak_picks (one row per pick)
  • DB connection via POSTGRES_URL env var (same as the rest of the stack)

Discord alerts:
  • Pick announcement  : 11 AM alongside the main dispatcher
  • Settlement update  : 2 AM alongside nightly_recap.py
  • Streak milestones  : 5/11 and 8/11 celebration pings

Standalone run:
  python streak_agent.py [--date 2026-04-01] [--dry-run] [--entry 1|5|10]
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

# Re-use AGENT_CONFIGS filters from live_dispatcher so signal counting stays
# perfectly in sync with the 18-agent roster without duplicating logic.
try:
    from live_dispatcher import (
        AGENT_CONFIGS,
        fetch_underdog_props,
        fetch_today_schedule,
        normalise_stat,
        PROP_CONFIG,
        calc_ev,
        implied_prob_from_odds,
    )
    _DISPATCHER_AVAILABLE = True
except ImportError:
    _DISPATCHER_AVAILABLE = False
    AGENT_CONFIGS = []

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("propiq.streak")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STREAK_CONF_MIN   = 8.0    # confidence gate (vs. 7.0 for standard parlays)
STREAK_PROB_MIN   = 0.62   # implied win probability floor
STREAK_EV_MIN     = 5.0    # EV % floor
STREAK_TOTAL_WINS = 11     # picks needed to win
STREAK_WINDOW_DAYS = 10    # calendar days to complete the streak

# Entry tiers: entry_key → (stake_usd, prize_usd)
ENTRY_TIERS: dict[int, tuple[float, float]] = {
    1:  (1.0,  1_000.0),
    5:  (5.0,  5_000.0),
    10: (10.0, 10_000.0),
}
DEFAULT_ENTRY = 1   # $1 entry → $1,000 prize

DISCORD_WEBHOOK = os.getenv(
    "DISCORD_WEBHOOK_URL",
    "https://discordapp.com/api/webhooks/1484795164961800374/"
    "jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM",
)

# Underdog API
_UD_LINES_URL = "https://api.underdogfantasy.com/v1/over_under_lines"
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

_BASE_RATES: dict[str, list[tuple[float, float]]] = {
    "hits":           [(0.5, 0.67), (1.5, 0.40), (2.5, 0.19), (3.5, 0.08)],
    "home_runs":      [(0.5, 0.22), (1.5, 0.04)],
    "rbis":           [(0.5, 0.42), (1.5, 0.18), (2.5, 0.07)],
    "runs":           [(0.5, 0.55), (1.5, 0.23), (2.5, 0.09)],
    "total_bases":    [(0.5, 0.70), (1.5, 0.49), (2.5, 0.28), (3.5, 0.14)],
    "stolen_bases":   [(0.5, 0.14), (1.5, 0.03)],
    "hits_runs_rbis": [(0.5, 0.82), (1.5, 0.64), (2.5, 0.44), (3.5, 0.27), (4.5, 0.15)],
    "strikeouts":     [(3.5, 0.74), (4.5, 0.62), (5.5, 0.51), (6.5, 0.40), (7.5, 0.29), (8.5, 0.19)],
    "earned_runs":    [(0.5, 0.42), (1.5, 0.59), (2.5, 0.72), (3.5, 0.82)],
    "fantasy_hitter": [(15.0, 0.58), (20.0, 0.45), (25.0, 0.33), (30.0, 0.22)],
    "fantasy_pitcher":[(30.0, 0.58), (35.0, 0.47), (40.0, 0.36), (45.0, 0.27)],
    "walks":          [(0.5, 0.68), (1.5, 0.42), (2.5, 0.22)],
}

_GAME_LINE_RANGES: dict[str, tuple[float, float]] = {
    "hits":           (0.5, 4.5),
    "home_runs":      (0.5, 2.5),
    "rbis":           (0.5, 4.5),
    "runs":           (0.5, 3.5),
    "total_bases":    (0.5, 5.5),
    "stolen_bases":   (0.5, 2.5),
    "hits_runs_rbis": (0.5, 8.5),
    "strikeouts":     (1.5, 12.5),
    "earned_runs":    (0.5, 6.5),
    "walks":          (0.5, 5.5),
    "fantasy_hitter": (5.0, 60.0),
    "fantasy_pitcher":(15.0, 70.0),
}

_STAT_TYPE_MAP: dict[str, str] = {
    "strikeouts": "strikeouts", "pitcher strikeouts": "strikeouts", "ks": "strikeouts",
    "hits": "hits",
    "home runs": "home_runs", "home_runs": "home_runs",
    "rbis": "rbis", "rbi": "rbis",
    "runs": "runs",
    "total bases": "total_bases", "total_bases": "total_bases",
    "stolen bases": "stolen_bases", "stolen_bases": "stolen_bases",
    "hits+runs+rbis": "hits_runs_rbis", "hits + runs + rbis": "hits_runs_rbis",
    "hitter fantasy score": "fantasy_hitter", "fantasy_points_hitter": "fantasy_hitter",
    "pitcher fantasy score": "fantasy_pitcher", "fantasy_points_pitcher": "fantasy_pitcher",
    "earned runs": "earned_runs", "earned_runs": "earned_runs",
    "walks": "walks",
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
    prob_score   = (prob - 0.50) / 0.30 * 7.0
    ev_bonus     = min(ev_pct / 15.0 * 2.0, 2.0)
    signal_bonus = min(signal_count * 0.1, 1.0)
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

def _count_signals(prop_type: str, side: str, implied_prob: float) -> int:
    """
    Count how many of the 18 AGENT_CONFIGS would approve this pick.
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
    sr.side         = side
    sr.prop_type    = prop_type
    sr.implied_prob = implied_prob
    sr.fantasy_pts_edge = 0.0

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
            ev_pct = (prob - 0.5238) / 0.5238 * 100   # vs. break-even at -110

            signals = _count_signals(prop_type, side, prob)
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
      3. Rank: primary = confidence desc, tiebreak = signal_count desc

    Returns None if no candidate qualifies (system skips the day rather than
    forcing a marginal pick — streak integrity over volume).
    """
    qualified = [
        c for c in candidates
        if c.confidence >= STREAK_CONF_MIN
        and c.implied_prob >= STREAK_PROB_MIN
        and c.ev_pct >= STREAK_EV_MIN
    ]

    if not qualified:
        return None

    # Team diversity gate for picks 1 & 2
    if pick_number <= 2 and prior_pick_team:
        diverse = [c for c in qualified if c.team.upper() != prior_pick_team.upper()]
        if diverse:
            qualified = diverse
        # If nothing passes diversity (all from same team), log + allow anyway
        else:
            logger.warning(
                "[Streak] Team diversity: all qualified picks from %s — "
                "diversity rule relaxed for pick %d",
                prior_pick_team, pick_number,
            )

    # Sort: highest confidence, then highest signal_count as tiebreak
    qualified.sort(key=lambda c: (-c.confidence, -c.signal_count))
    return qualified[0]


# ---------------------------------------------------------------------------
# Postgres state management
# ---------------------------------------------------------------------------

def _pg_conn():
    """Return a Postgres connection using POSTGRES_URL env var."""
    if not _PG_AVAILABLE:
        raise RuntimeError("psycopg2 not installed")
    url = os.getenv("POSTGRES_URL", os.getenv("DATABASE_URL", ""))
    if not url:
        raise RuntimeError("POSTGRES_URL not set")
    return psycopg2.connect(url)


def ensure_streak_tables() -> None:
    """Create streak_state + streak_picks tables if they don't exist.
    Silently skips if psycopg2 unavailable (local dev without Postgres)."""
    if not _PG_AVAILABLE:
        logger.info("[Streak] psycopg2 not available — skipping table setup (local dev mode)")
        return
    if not os.getenv("POSTGRES_URL", os.getenv("DATABASE_URL", "")):
        logger.info("[Streak] No DATABASE_URL — skipping table setup (local dev mode)")
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS streak_state (
        id              SERIAL PRIMARY KEY,
        entry_amount    INTEGER NOT NULL DEFAULT 1,
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
    if not _PG_AVAILABLE or not os.getenv("POSTGRES_URL", os.getenv("DATABASE_URL", "")):
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
    "home_runs":      "Home Runs",
    "rbis":           "RBIs",
    "runs":           "Runs Scored",
    "total_bases":    "Total Bases",
    "stolen_bases":   "Stolen Bases",
    "hits_runs_rbis": "H+R+RBI",
    "strikeouts":     "Pitcher Ks",
    "earned_runs":    "Earned Runs",
    "fantasy_hitter": "Hitter Fantasy Pts",
    "fantasy_pitcher":"Pitcher Fantasy Pts",
    "walks":          "Walks",
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
    """Post the 11 AM pick announcement to Discord."""
    stake_usd, prize_usd = ENTRY_TIERS.get(entry_amount, (1.0, 1_000.0))
    remaining  = STREAK_TOTAL_WINS - wins_in_row - 1   # after this pick
    prize_tier = _PRIZE_EMOJI.get(entry_amount, "💰")

    prop_label = _PROP_LABELS.get(pick.prop_type, pick.prop_type.replace("_", " ").title())
    direction  = "OVER 📈" if pick.side == "Over" else "UNDER 📉"
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
                    f"Win Prob: **{prob_pct}%**\n"
                    f"EV: **+{pick.ev_pct:.1f}%**\n"
                    f"Signals: **{pick.signal_count}/18** agents agree"
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
        ],
        "footer": {
            "text": (
                f"PropIQ StreakAgent • {datetime.now(timezone.utc).strftime('%b %d %Y %H:%M')} UTC • "
                f"Confidence gate ≥ {STREAK_CONF_MIN}/10 • Prob gate ≥ {int(STREAK_PROB_MIN*100)}%"
            )
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    payload = {"embeds": [embed]}
    if _send_webhook(payload):
        logger.info("[Streak] Pick alert sent: pick %d/%d", pick_number, STREAK_TOTAL_WINS)


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
    stake_usd, prize_usd = ENTRY_TIERS.get(entry_amount, (1.0, 1_000.0))
    prop_label = _PROP_LABELS.get(prop_type, prop_type.replace("_", " ").title())

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
        status_line = f"💔 Streak reset after pick {pick_number}. Start fresh tomorrow."
    elif streak_status == "VOIDED":
        status_line = (
            "🔄 Pick voided — streak advances automatically. "
            + ("Full restart (picks 1-2 void)." if pick_number <= 2 else "Next pick replaces this slot.")
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
                    f"{direction} **{line}** {prop_label}\n"
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
    stake_usd, prize_usd = ENTRY_TIERS.get(entry_amount, (1.0, 1_000.0))
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _send_webhook({"embeds": [embed]})


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------

_PROP_TO_ESPN_STAT: dict[str, str] = {
    "hits":           "hits",
    "home_runs":      "homeRuns",
    "rbis":           "RBIs",
    "runs":           "runs",
    "total_bases":    "totalBases",
    "stolen_bases":   "stolenBases",
    "hits_runs_rbis": "hits",        # composite: H+R+RBI computed manually
    "strikeouts":     "strikeouts",  # pitcher Ks
    "earned_runs":    "earnedRuns",
    "walks":          "baseOnBalls",
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
      outcome: 'WIN' | 'LOSS' | 'PUSH' | 'NO_RESULT'
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
        return "NO_RESULT", None

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
            return "NO_RESULT", None
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

    # Fetch ESPN stats once
    stat_lookup = fetch_espn_boxscore_stats(game_date)

    for pick in pending:
        outcome, actual = _grade_pick(
            stat_lookup,
            pick["player_name"],
            pick["prop_type"],
            float(pick["line"]),
            pick["direction"],
        )

        if outcome == "NO_RESULT":
            logger.info("[Streak] No ESPN data yet for %s — leaving PENDING", pick["player_name"])
            continue

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
                    new_wins = pick["wins_in_row"] + 1
                    streak_status = "WON" if new_wins >= STREAK_TOTAL_WINS else "ACTIVE"
                    cur.execute(
                        "UPDATE streak_state SET wins_in_row=%s, status=%s WHERE id=%s",
                        (new_wins, streak_status, streak_id),
                    )
                elif outcome == "LOSS":
                    streak_status = "LOST"
                    cur.execute(
                        "UPDATE streak_state SET status='LOST' WHERE id=%s",
                        (streak_id,),
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
                else:   # PUSH
                    streak_status = "ACTIVE"
                    # Push doesn't advance the win counter but doesn't reset it either
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
            # Auto-create new streak if WON (chain into next streak)
            # LOST: next run of run_streak_pick() will auto-create a fresh one
            if updated_status == "WON":
                logger.info("[Streak] 🏆 Streak #%d WON — auto-creating next streak.", streak_id)
                # New streak auto-created on next morning's dispatcher run

    conn.close()


# ---------------------------------------------------------------------------
# Season stats helpers (for footer in pick alert)
# ---------------------------------------------------------------------------

def get_streak_season_stats() -> tuple[int, int]:
    """Return (total_picks, total_wins) from streak_picks this calendar year."""
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            year = datetime.now(timezone.utc).year
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
    Morning run (11 AM ET alongside main dispatcher).

    Fetches props → scores → selects best pick → persists → alerts Discord.
    Returns the pick dict, or None if no qualifying pick exists today.
    """
    date = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info("[Streak] === StreakAgent run for %s ===", date)

    # Ensure DB tables exist
    ensure_streak_tables()

    # Load or create active streak
    streak = get_or_create_active_streak(entry_amount)
    if not streak:
        logger.warning("[Streak] Could not load/create streak state — aborting.")
        return None

    streak_id   = streak["id"]
    wins_in_row = streak["wins_in_row"]
    pick_number = wins_in_row + 1    # next pick needed

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

    # Team-diversity gate for picks 1 & 2
    prior_team = get_prior_pick_team(streak_id) if pick_number <= 2 else None

    # Select best pick
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

    # ── Phase 92: compare Underdog vs PrizePicks — use better line ──────────
    _line_comparison_note = ""
    if _LINE_COMP_AVAILABLE:
        try:
            # Fetch both platforms fresh for the streak (small overhead, once/day)
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

            # If PrizePicks has a better line, update the pick's platform + line
            if _comp.get("platform") == "PrizePicks" and _comp.get("line") is not None:
                logger.info("[Streak] Better line on PrizePicks: %s (was UD %.1f)",
                            _line_comparison_note, pick.line)
                pick.platform = "PrizePicks"
                pick.line     = _comp["line"]
            elif _comp.get("platform") == "Underdog" and _comp.get("line") is not None:
                pick.line = _comp["line"]   # confirm UD line from live fetch
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
        date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
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
