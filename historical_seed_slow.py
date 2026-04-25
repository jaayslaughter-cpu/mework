"""
historical_seed_slow.py
=======================
Resume-safe, disk-friendly historical seed for XGBoost training data.

Changes vs historical_seed.py:
  - discord_sent = TRUE on every row (so XGBoost can read them)
  - Progress tracked in `seed_progress` Postgres table — fully resume-safe
  - Commits every BATCH_SIZE rows (default 100) instead of per-player
  - 1.5s sleep between players (vs 0.15s) to avoid Postgres temp pressure
  - PLAYERS_PER_RUN cap (default 150) — designed to be called repeatedly

PR #401: Added walks_allowed, hitter_strikeouts, hits_runs_rbis
PR #402: Added fantasy_score (UD + PP formulas, computed from game log stats)

Usage:
    python historical_seed_slow.py              # process next 150 players
    python historical_seed_slow.py --players 50 # process next 50 players
    python historical_seed_slow.py --status     # show progress, don't seed
    python historical_seed_slow.py --reset      # clear progress, start over
"""
from __future__ import annotations

import argparse
import logging
import os
import time
import urllib3
import warnings

import json
import requests
import psycopg2
import psycopg2.extras

warnings.filterwarnings("ignore")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MLBAPI   = "https://statsapi.mlb.com/api/v1"
SEASONS  = [2022, 2023, 2024, 2025]
BATCH_SIZE      = 100   # rows per commit
PLAYERS_PER_RUN = 150   # stop after this many players per invocation
SLEEP_BETWEEN   = 1.5   # seconds between players

PITCHER_LINES = {
    "strikeouts":    5.5,
    "earned_runs":   2.5,
    "pitching_outs": 14.5,
    "walks_allowed": 1.5,   # pitcher BB — baseOnBalls from MLB Stats API gamelog
}
BATTER_LINES = {
    "hits":              0.5,
    "total_bases":       1.5,
    "hitter_strikeouts": 0.5,  # batter Ks — strikeOuts from MLB Stats API gamelog
    "hits_runs_rbis":    2.5,  # H+R+RBI composite
}

# Fantasy score seed lines — league-average defaults used as the line.
# UD pitcher formula: IP×3 + K×3 + QS×5 + W×5 + ER×-3   → avg SP ~18-19 pts
# PP pitcher formula: W×6 + QS×4 + ER×-3 + K×3 + Out×1  → avg SP ~15-16 pts
# UD batter formula:  HR×10+3B×8+2B×5+1B×3+BB×3+HBP×3+RBI×2+R×2+SB×4 → avg ~22 pts
# PP batter formula:  1B×3+2B×5+3B×8+HR×10+R×2+RBI×2+BB×2+HBP×2+SB×5 → avg ~18 pts
FS_PITCHER_UD_LINE = 18.5
FS_PITCHER_PP_LINE = 15.5
FS_BATTER_UD_LINE  = 22.5
FS_BATTER_PP_LINE  = 18.5

MIN_PA = 1
MIN_BF = 3

# ─── Feature vector builder (mirrors _BaseAgent._build_feature_vector) ────────
# Generates the 27-element float list XGBoost trains on.
# Seed rows use league-average defaults for enrichment slots (20-26) since
# we have no pick-time enrichment data for historical games.

_PITCHER_PT = {"strikeouts", "pitching_outs", "earned_runs", "walks_allowed",
               "hits_allowed", "fantasy_pitcher"}
# Must mirror _pt_map in tasklets.py _build_feature_vector — keep in sync
_PT_ENC = {
    "strikeouts":         0.0 / 9,
    "pitcher_strikeouts": 0.0 / 9,
    "hitter_strikeouts":  1.0 / 9,  # distinct bucket from pitcher Ks
    "pitching_outs":      2.0 / 9,
    "home_runs":          3.0 / 9,
    "hits":               4.0 / 9,
    "hits_allowed":       4.0 / 9,
    "rbis":               5.0 / 9,
    "rbi":                5.0 / 9,
    "hits_runs_rbis":     6.0 / 9,
    "total_bases":        7.0 / 9,
    "fantasy_score":      7.0 / 9,
    "walks_allowed":      8.0 / 9,
    "earned_runs":        9.0 / 9,
}

def _clamp(v, lo=0.0, hi=1.0):
    try:
        return max(lo, min(hi, float(v)))
    except Exception:
        return 0.0

def _build_seed_features(prop_type: str, line: float, side: str,
                          model_prob: float = 0.55, ev_pct: float = 3.0,
                          is_pitcher: bool = False) -> list:
    """
    Build a 27-element feature vector for a historical seed row.
    Uses league-average values for all enrichment slots we cannot
    recompute from game-log data alone.
    """
    _side_enc = 0.0 if side == "Over" else 1.0
    _pt_enc   = _PT_ENC.get(prop_type, 5.0 / 9)
    _line_val = _clamp(line / 10.0)
    _mp       = _clamp(model_prob / 100.0 if model_prob > 1 else model_prob)
    _ev       = _clamp((ev_pct + 20) / 40.0)

    if is_pitcher:
        # League-average pitcher slots (FG 2025)
        k_rate       = 0.22           # avg K%
        bb_rate       = 0.08          # avg BB%
        era           = _clamp(4.0 / 9.0)
        whip          = _clamp(1.3 / 3.0)
        shadow_whiff  = 0.275         # avg CSW%
    else:
        # League-average batter slots
        k_rate        = _clamp(100.0 / 200.0)   # wRC+ 100 → 0.5
        bb_rate       = _clamp(0.156 / 0.35)    # ISO avg
        era           = _clamp((0.288 - 0.200) / 0.200)  # BABIP avg
        whip          = _clamp(0.087 / 0.20)    # BB% avg
        shadow_whiff  = _clamp(0.223 / 0.35)    # K% avg

    vec = [
        k_rate, bb_rate, era, whip,           # 0-3  pitcher/batter stats
        shadow_whiff, 0.5 / 1.5,              # 4-5  statcast (zone_mult neutral)
        0.5, 0.28,                            # 6-7  lineup chase neutral, avg o_swing
        _clamp(8.0 / 30.0), _clamp((72 - 32) / 80.0),  # 8-9  weather avg
        0.0,                                  # 10   not spring training
        _mp, _ev, _clamp(0.02 / 3.0),        # 11-13 bet quality
        _line_val, _clamp(52.4 / 100.0),      # 14-15 line, implied prob
        _pt_enc, _side_enc,                   # 16-17 prop meta
        0.25, 0.5,                            # 18-19 brier neutral, sb_line_gap neutral
        0.5,                                  # 20   form_adj neutral
        0.5,                                  # 21   cv_nudge neutral
        0.5,                                  # 22   bayesian_nudge neutral
        0.5,                                  # 23   marcel_adj neutral
        0.5,                                  # 24   predict_plus neutral
        _mp,                                  # 25   ps_prob = model_prob proxy
        0.5,                                  # 26   batting order neutral
    ]
    assert len(vec) == 27
    return [round(v, 6) for v in vec]


# ─── DB ──────────────────────────────────────────────────────────────────────

def _get_conn():
    url = os.environ.get("DATABASE_URL", "").strip().rstrip()
    if not url:
        raise SystemExit("ERROR: DATABASE_URL not set.")
    return psycopg2.connect(url, connect_timeout=15)


def _ensure_progress_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seed_progress (
                season       INTEGER NOT NULL,
                player_type  VARCHAR(10) NOT NULL,   -- 'pitcher' or 'batter'
                player_id    INTEGER NOT NULL,
                player_name  VARCHAR(120),
                done         BOOLEAN DEFAULT FALSE,
                inserted     INTEGER DEFAULT 0,
                processed_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (season, player_type, player_id)
            )
        """)
        # Schema healing — table may exist from prior run without all columns
        for _heal in [
            "ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS done BOOLEAN DEFAULT FALSE",
            "ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS inserted INTEGER DEFAULT 0",
            "ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS player_name VARCHAR(120)",
            "ALTER TABLE seed_progress ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ DEFAULT NOW()",
        ]:
            try:
                cur.execute(_heal)
            except Exception:
                pass
    conn.commit()


def _already_done(conn, season: int, player_type: str, player_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT done FROM seed_progress WHERE season=%s AND player_type=%s AND player_id=%s",
            (season, player_type, player_id)
        )
        row = cur.fetchone()
    return bool(row and row[0])


def _mark_done(conn, season: int, player_type: str, player_id: int, name: str, inserted: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO seed_progress (season, player_type, player_id, player_name, done, inserted)
            VALUES (%s, %s, %s, %s, TRUE, %s)
            ON CONFLICT (season, player_type, player_id)
            DO UPDATE SET done=TRUE, inserted=%s, processed_at=NOW()
        """, (season, player_type, player_id, name, inserted, inserted))
    conn.commit()


def _progress_summary(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT season, player_type,
                   COUNT(*) FILTER (WHERE done) AS done_count,
                   COUNT(*) total,
                   SUM(inserted) FILTER (WHERE done) AS rows_ins
            FROM seed_progress
            GROUP BY season, player_type
            ORDER BY season, player_type
        """)
        rows = cur.fetchall()
    return rows


# ─── Fetch helpers ────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None) -> dict:
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20, verify=False)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    return {}


def get_pitcher_ids(season: int) -> list[tuple[int, str]]:
    data = _get(f"{MLBAPI}/stats", {
        "stats": "season", "group": "pitching", "season": season,
        "gameType": "R", "sportId": 1, "limit": 1000, "playerPool": "All",
    })
    results = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        p = split.get("player", {})
        st = split.get("stat", {})
        if int(st.get("gamesStarted", 0) or 0) >= 5:
            results.append((int(p["id"]), p["fullName"]))
    return results


def get_batter_ids(season: int) -> list[tuple[int, str]]:
    data = _get(f"{MLBAPI}/stats", {
        "stats": "season", "group": "hitting", "season": season,
        "gameType": "R", "sportId": 1, "limit": 1500, "playerPool": "All",
    })
    results = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        p = split.get("player", {})
        st = split.get("stat", {})
        if int(st.get("plateAppearances", 0) or 0) >= 50:
            results.append((int(p["id"]), p["fullName"]))
    return results


def get_pitcher_game_log(player_id: int, season: int) -> list[dict]:
    data = _get(f"{MLBAPI}/people/{player_id}", {
        "hydrate": f"stats(group=pitching,type=gameLog,season={season},gameType=R)"
    })
    people = data.get("people", [])
    if not people:
        return []
    stats = people[0].get("stats", [])
    return stats[0].get("splits", []) if stats else []


def get_batter_game_log(player_id: int, season: int) -> list[dict]:
    data = _get(f"{MLBAPI}/people/{player_id}", {
        "hydrate": f"stats(group=hitting,type=gameLog,season={season},gameType=R)"
    })
    people = data.get("people", [])
    if not people:
        return []
    stats = people[0].get("stats", [])
    return stats[0].get("splits", []) if stats else []


# ─── Row builders ─────────────────────────────────────────────────────────────

def build_pitcher_rows(name: str, splits: list[dict]) -> list[dict]:
    rows = []
    for s in splits:
        st = s.get("stat", {})
        date_str = s.get("date", "")
        if not date_str:
            continue
        if int(st.get("battersFaced", 0) or 0) < MIN_BF:
            continue

        ks   = int(st.get("strikeOuts",   0) or 0)
        er   = int(st.get("earnedRuns",   0) or 0)
        outs = int(st.get("outs",         0) or 0)
        bb   = int(st.get("baseOnBalls",  0) or 0)   # walks allowed
        wins = int(st.get("wins",         0) or 0)
        qs   = 1 if (outs >= 18 and er <= 3) else 0  # quality start: 6+ IP, ≤3 ER
        ip   = outs / 3.0

        # Standard prop rows
        for prop_type, line, actual in [
            ("strikeouts",    PITCHER_LINES["strikeouts"],    ks),
            ("earned_runs",   PITCHER_LINES["earned_runs"],   er),
            ("pitching_outs", PITCHER_LINES["pitching_outs"], outs),
            ("walks_allowed", PITCHER_LINES["walks_allowed"], bb),
        ]:
            for side in ("Over", "Under"):
                outcome = 1 if (actual > line if side == "Over" else actual < line) else 0
                rows.append({
                    "player_name": name, "prop_type": prop_type, "line": line,
                    "side": side, "agent_name": "HistoricalSeed",
                    "status": "WIN" if outcome == 1 else "LOSS",
                    "actual_outcome": outcome, "actual_result": float(actual),
                    "profit_loss": 1.0 if outcome == 1 else -1.0,
                    "model_prob": 55.0, "ev_pct": 3.0,
                    "bet_date": date_str, "platform": "historical",
                    "features_json": json.dumps(_build_seed_features(
                        prop_type, line, side, model_prob=55.0, ev_pct=3.0, is_pitcher=True,
                    )),
                })

        # Fantasy score — Underdog formula: IP×3 + K×3 + QS×5 + W×5 + ER×-3
        ud_fs = round(ip * 3 + ks * 3 + qs * 5 + wins * 5 + er * -3, 2)
        for side in ("Over", "Under"):
            outcome = 1 if (ud_fs > FS_PITCHER_UD_LINE if side == "Over" else ud_fs < FS_PITCHER_UD_LINE) else 0
            rows.append({
                "player_name": name, "prop_type": "fantasy_score",
                "line": FS_PITCHER_UD_LINE,
                "side": side, "agent_name": "HistoricalSeed_UD",
                "status": "WIN" if outcome == 1 else "LOSS",
                "actual_outcome": outcome, "actual_result": ud_fs,
                "profit_loss": 1.0 if outcome == 1 else -1.0,
                "model_prob": 55.0, "ev_pct": 3.0,
                "bet_date": date_str, "platform": "underdog",
                "features_json": json.dumps(_build_seed_features(
                    "fantasy_score", FS_PITCHER_UD_LINE, side, model_prob=55.0, ev_pct=3.0, is_pitcher=True,
                )),
            })

        # Fantasy score — PrizePicks formula: W×6 + QS×4 + ER×-3 + K×3 + Out×1
        pp_fs = round(wins * 6 + qs * 4 + er * -3 + ks * 3 + outs * 1.0, 2)
        for side in ("Over", "Under"):
            outcome = 1 if (pp_fs > FS_PITCHER_PP_LINE if side == "Over" else pp_fs < FS_PITCHER_PP_LINE) else 0
            rows.append({
                "player_name": name, "prop_type": "fantasy_score",
                "line": FS_PITCHER_PP_LINE,
                "side": side, "agent_name": "HistoricalSeed_PP",
                "status": "WIN" if outcome == 1 else "LOSS",
                "actual_outcome": outcome, "actual_result": pp_fs,
                "profit_loss": 1.0 if outcome == 1 else -1.0,
                "model_prob": 55.0, "ev_pct": 3.0,
                "bet_date": date_str, "platform": "prizepicks",
                "features_json": json.dumps(_build_seed_features(
                    "fantasy_score", FS_PITCHER_PP_LINE, side, model_prob=55.0, ev_pct=3.0, is_pitcher=True,
                )),
            })

    return rows


def build_batter_rows(name: str, splits: list[dict]) -> list[dict]:
    rows = []
    for s in splits:
        st = s.get("stat", {})
        date_str = s.get("date", "")
        if not date_str:
            continue
        if int(st.get("plateAppearances", 0) or 0) < MIN_PA:
            continue

        hits    = int(st.get("hits",         0) or 0)
        tb      = int(st.get("totalBases",   0) or 0)
        bk      = int(st.get("strikeOuts",   0) or 0)   # batter strikeouts
        runs    = int(st.get("runs",         0) or 0)
        rbi     = int(st.get("rbi",          0) or 0)
        hr      = int(st.get("homeRuns",     0) or 0)
        doubles = int(st.get("doubles",      0) or 0)
        triples = int(st.get("triples",      0) or 0)
        bb      = int(st.get("baseOnBalls",  0) or 0)
        hbp     = int(st.get("hitByPitch",   0) or 0)
        sb      = int(st.get("stolenBases",  0) or 0)
        singles = max(0, hits - doubles - triples - hr)
        hrbi    = hits + runs + rbi                      # hits_runs_rbis composite

        # Standard prop rows
        for prop_type, line, actual in [
            ("hits",              BATTER_LINES["hits"],              hits),
            ("total_bases",       BATTER_LINES["total_bases"],       tb),
            ("hitter_strikeouts", BATTER_LINES["hitter_strikeouts"], bk),
            ("hits_runs_rbis",    BATTER_LINES["hits_runs_rbis"],    hrbi),
        ]:
            for side in ("Over", "Under"):
                outcome = 1 if (actual > line if side == "Over" else actual < line) else 0
                rows.append({
                    "player_name": name, "prop_type": prop_type, "line": line,
                    "side": side, "agent_name": "HistoricalSeed",
                    "status": "WIN" if outcome == 1 else "LOSS",
                    "actual_outcome": outcome, "actual_result": float(actual),
                    "profit_loss": 1.0 if outcome == 1 else -1.0,
                    "model_prob": 55.0, "ev_pct": 3.0,
                    "bet_date": date_str, "platform": "historical",
                    "features_json": json.dumps(_build_seed_features(
                        prop_type, line, side, model_prob=55.0, ev_pct=3.0, is_pitcher=False,
                    )),
                })

        # Fantasy score — Underdog formula:
        # HR×10 + 3B×8 + 2B×5 + 1B×3 + BB×3 + HBP×3 + RBI×2 + R×2 + SB×4
        ud_fs = round(
            hr * 10 + triples * 8 + doubles * 5 + singles * 3
            + bb * 3 + hbp * 3 + rbi * 2 + runs * 2 + sb * 4,
            2,
        )
        for side in ("Over", "Under"):
            outcome = 1 if (ud_fs > FS_BATTER_UD_LINE if side == "Over" else ud_fs < FS_BATTER_UD_LINE) else 0
            rows.append({
                "player_name": name, "prop_type": "fantasy_score",
                "line": FS_BATTER_UD_LINE,
                "side": side, "agent_name": "HistoricalSeed_UD",
                "status": "WIN" if outcome == 1 else "LOSS",
                "actual_outcome": outcome, "actual_result": ud_fs,
                "profit_loss": 1.0 if outcome == 1 else -1.0,
                "model_prob": 55.0, "ev_pct": 3.0,
                "bet_date": date_str, "platform": "underdog",
                "features_json": json.dumps(_build_seed_features(
                    "fantasy_score", FS_BATTER_UD_LINE, side, model_prob=55.0, ev_pct=3.0, is_pitcher=False,
                )),
            })

        # Fantasy score — PrizePicks formula:
        # 1B×3 + 2B×5 + 3B×8 + HR×10 + R×2 + RBI×2 + BB×2 + HBP×2 + SB×5
        pp_fs = round(
            singles * 3 + doubles * 5 + triples * 8 + hr * 10
            + runs * 2 + rbi * 2 + bb * 2 + hbp * 2 + sb * 5,
            2,
        )
        for side in ("Over", "Under"):
            outcome = 1 if (pp_fs > FS_BATTER_PP_LINE if side == "Over" else pp_fs < FS_BATTER_PP_LINE) else 0
            rows.append({
                "player_name": name, "prop_type": "fantasy_score",
                "line": FS_BATTER_PP_LINE,
                "side": side, "agent_name": "HistoricalSeed_PP",
                "status": "WIN" if outcome == 1 else "LOSS",
                "actual_outcome": outcome, "actual_result": pp_fs,
                "profit_loss": 1.0 if outcome == 1 else -1.0,
                "model_prob": 55.0, "ev_pct": 3.0,
                "bet_date": date_str, "platform": "prizepicks",
                "features_json": json.dumps(_build_seed_features(
                    "fantasy_score", FS_BATTER_PP_LINE, side, model_prob=55.0, ev_pct=3.0, is_pitcher=False,
                )),
            })

    return rows


# ─── Insert with discord_sent=TRUE ───────────────────────────────────────────

INSERT_SQL = """
INSERT INTO bet_ledger (
    player_name, prop_type, line, side,
    agent_name, status, actual_outcome, actual_result,
    profit_loss, model_prob, ev_pct, bet_date, platform, discord_sent,
    features_json, graded_at, lookahead_safe
) VALUES %s
ON CONFLICT DO NOTHING
"""

# Column order matching INSERT_SQL positional slots
_INSERT_COLS = [
    "player_name", "prop_type", "line", "side",
    "agent_name", "status", "actual_outcome", "actual_result",
    "profit_loss", "model_prob", "ev_pct", "bet_date", "platform",
]


def insert_rows_batched(conn, rows: list[dict]) -> int:
    """Insert rows in batches using execute_values — single round-trip per chunk.

    Using execute_values reduces ~1,800 individual Postgres round-trips per
    batter to ~18 (one per BATCH_SIZE=100 chunk), cutting insert time from
    ~124s to ~1s per batter even over the Railway proxy.

    Template has 16 positional %s slots + NOW() literal for graded_at:
      0  player_name
      1  prop_type
      2  line
      3  side
      4  agent_name
      5  status
      6  actual_outcome
      7  actual_result
      8  profit_loss
      9  model_prob
      10 ev_pct
      11 bet_date
      12 platform
      13 discord_sent  (True)
      14 features_json
      NOW() — graded_at literal
      15 lookahead_safe (True)
    """
    # Template: 15 params, then NOW(), then 1 more param = 16 %s total
    TEMPLATE = (
        "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)"
    )
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i:i + BATCH_SIZE]
        tuples = []
        for row in chunk:
            tuples.append((
                row["player_name"],           # 0
                row["prop_type"],             # 1
                row["line"],                  # 2
                row["side"],                  # 3
                row["agent_name"],            # 4
                row["status"],                # 5
                row["actual_outcome"],        # 6
                row["actual_result"],         # 7
                row["profit_loss"],           # 8
                row["model_prob"],            # 9
                row["ev_pct"],                # 10
                row["bet_date"],              # 11
                row["platform"],              # 12
                True,                         # 13 discord_sent
                row.get("features_json"),     # 14 features_json
                # NOW() is literal in template — no slot 15 param
                True,                         # 15 lookahead_safe
            ))
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                INSERT_SQL,
                tuples,
                template=TEMPLATE,
                page_size=BATCH_SIZE,
            )
            inserted += max(cur.rowcount, 0)
        conn.commit()
    return inserted


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--players", type=int, default=PLAYERS_PER_RUN,
                        help="Max players to process this run (default 150)")
    parser.add_argument("--status", action="store_true",
                        help="Print progress summary and exit")
    parser.add_argument("--reset", action="store_true",
                        help="Delete seed_progress rows and HistoricalSeed bet_ledger rows")
    parser.add_argument("--sleep", type=float, default=SLEEP_BETWEEN,
                        help="Seconds between players (default 1.5)")
    args = parser.parse_args()

    conn = _get_conn()
    log.info("Connected to Postgres ✓")
    _ensure_progress_table(conn)

    # ── Status mode ──
    if args.status:
        rows = _progress_summary(conn)
        if not rows:
            log.info("No seed progress yet.")
        else:
            log.info("%-6s %-8s %6s %6s %10s", "Season", "Type", "Done", "Total", "Rows")
            for season, ptype, done, total, ins in rows:
                log.info("%-6s %-8s %6d %6d %10d", season, ptype, done or 0, total or 0, ins or 0)
        conn.close()
        return

    # ── Reset mode ──
    if args.reset:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM seed_progress")
            cur.execute("DELETE FROM bet_ledger WHERE agent_name IN ('HistoricalSeed','HistoricalSeed_UD','HistoricalSeed_PP')")
        conn.commit()
        log.info("Reset complete — seed_progress cleared, HistoricalSeed rows deleted.")
        log.info("Run VACUUM ANALYZE bet_ledger in Railway Postgres console to reclaim disk.")
        conn.close()
        return

    players_processed = 0
    total_inserted    = 0

    for season in SEASONS:
        if players_processed >= args.players:
            break

        # ── Pitchers ──
        try:
            pitchers = get_pitcher_ids(season)
        except Exception as exc:
            log.warning("Could not fetch pitcher list for %d: %s", season, exc)
            pitchers = []

        for pid, name in pitchers:
            if players_processed >= args.players:
                log.info("Reached player cap (%d). Stopping — resume next run.", args.players)
                break
            if _already_done(conn, season, "pitcher", pid):
                continue

            try:
                splits = get_pitcher_game_log(pid, season)
                rows   = build_pitcher_rows(name, splits)
                ins    = insert_rows_batched(conn, rows) if rows else 0
                _mark_done(conn, season, "pitcher", pid, name, ins)
                total_inserted    += ins
                players_processed += 1
                if players_processed % 10 == 0:
                    log.info("[%d/%d] %s (%d) → +%d rows | total so far: %d",
                             players_processed, args.players, name, pid, ins, total_inserted)
            except Exception as exc:
                log.debug("Pitcher %s [%d] skipped: %s", name, pid, exc)

            time.sleep(args.sleep)

        if players_processed >= args.players:
            break

        # ── Batters ──
        try:
            batters = get_batter_ids(season)
        except Exception as exc:
            log.warning("Could not fetch batter list for %d: %s", season, exc)
            batters = []

        for pid, name in batters:
            if players_processed >= args.players:
                log.info("Reached player cap (%d). Stopping — resume next run.", args.players)
                break
            if _already_done(conn, season, "batter", pid):
                continue

            try:
                splits = get_batter_game_log(pid, season)
                rows   = build_batter_rows(name, splits)
                ins    = insert_rows_batched(conn, rows) if rows else 0
                _mark_done(conn, season, "batter", pid, name, ins)
                total_inserted    += ins
                players_processed += 1
                if players_processed % 25 == 0:
                    log.info("[%d/%d] %s (%d) → +%d rows | total so far: %d",
                             players_processed, args.players, name, pid, ins, total_inserted)
            except Exception as exc:
                log.debug("Batter %s [%d] skipped: %s", name, pid, exc)

            time.sleep(args.sleep)

    conn.close()
    log.info("Run complete — players: %d | rows inserted: %d", players_processed, total_inserted)
    log.info("Run again to continue. Use --status to check overall progress.")


if __name__ == "__main__":
    main()
