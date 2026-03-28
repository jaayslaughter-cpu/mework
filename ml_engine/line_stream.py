"""
line_stream.py
==============
Real-time line streaming pipeline.

Runs every 30 minutes, 10 AM - 10 PM ET, via a scheduled trigger.

Three phases per run:

line_stream.py
==============
Real-time line streaming pipeline.

Runs every 30 minutes, 10 AM 	6 PM ET, via a scheduled trigger.

Three phases per run:

  PRE-GAME   Snapshot current PrizePicks + Underdog player prop lines.
             Compare to previous 30-min snapshot 60 detect steam moves
             (line shift 9 0.5 units). Post Discord alert for each move.
             Mark first snapshot of day as opening lines.

  IN-GAME    Fetch live ESPN box scores for games currently in progress.
             Check PENDING parlay leg survival vs. live stats.
             Post informational Discord update (data only 	6 no new bets).
             Mark last pre-game snapshot as closing lines.

  CLV        Once closing lines are recorded, compute CLV for every
             parlay leg sent at 11 AM. Positive CLV = we beat the close.
             Store results in clv_records table in line_stream.db.
             Post CLV report to Discord.

APIs / sources (all free, no new keys):
  - api.prizepicks.com          (no key, same as live_dispatcher.py)
  - api.underdogfantasy.com     (no key, same as live_dispatcher.py)
  - site.api.espn.com           (no key, no quota)
"""

# No in-game bet signals are generated. In-game phase is monitoring only.

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests  # type: ignore

from espn_scraper import get_all_player_stats, get_game_states
from season_record import get_pending_parlays

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DISCORD_WEBHOOK = (
    "https://discordapp.com/api/webhooks/1484795164961800374/"
    "jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM"
)

_DB_PATH = Path("/agent/home/line_stream.db")
_STEAM_THRESHOLD = 0.5   # minimum line movement (units) to fire a steam alert
_REQUEST_SLEEP = 1.2     # seconds between ESPN requests

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# PrizePicks requires its own referer/origin — generic or ESPN referer triggers 403
_PP_HEADERS = {
    **_HEADERS,
    "Referer": "https://app.prizepicks.com/",
    "Origin":  "https://app.prizepicks.com",
}

# Baseball stat types recognised on PrizePicks (must be lowercase)
_PP_MLB_STAT_TYPES: frozenset[str] = frozenset({
    "hits", "home runs", "total bases", "runs", "rbi",
    "hits+runs+rbis", "strikeouts", "earned runs",
    "pitching outs", "walks allowed", "stolen bases",
})

# Prop-type → ESPN stat key for in-game leg survival checks
_PROP_STAT_MAP: dict[str, str] = {
    "hits":           "hits",
    "home runs":      "homeRuns",
    "total bases":    "totalBases",
    "runs":           "runs",
    "rbi":            "rbi",
    "hits+runs+rbis": "_combo_hrr",
    "strikeouts":     "strikeouts",
    "earned runs":    "earnedRuns",
    "pitching outs":  "pitchingOuts",
    "walks allowed":  "baseOnBalls",
    "stolen bases":   "stolenBases",
}


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS line_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    game_date    TEXT    NOT NULL,
    player_name  TEXT    NOT NULL,
    prop_type    TEXT    NOT NULL,
    platform     TEXT    NOT NULL,
    line         REAL    NOT NULL,
    snapshot_ts  TEXT    NOT NULL,
    is_opening   INTEGER NOT NULL DEFAULT 0,
    is_closing   INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ls_lookup
    ON line_snapshots(game_date, player_name, prop_type, platform);

CREATE TABLE IF NOT EXISTS clv_records (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    season_record_id INTEGER,
    game_date        TEXT    NOT NULL,
    player_name      TEXT    NOT NULL,
    prop_type        TEXT    NOT NULL,
    side             TEXT    NOT NULL,
    pick_line        REAL,
    closing_line     REAL,
    clv_pts          REAL,
    beat_close       INTEGER NOT NULL DEFAULT 0,
    recorded_at      TEXT    NOT NULL
);
"""


def _get_db() -> sqlite3.Connection:
    """Open (or create) the line-stream SQLite database."""
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(_DDL)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Prop fetching
# ---------------------------------------------------------------------------

def _fetch_prizepicks() -> list[dict]:
    """Fetch active PrizePicks MLB props. Returns list of prop dicts."""
    try:
        data = None
        for attempt in range(3):
            if attempt:
                time.sleep(2 ** attempt)  # 2s, 4s back-off
            resp = requests.get(
                "https://api.prizepicks.com/projections",
                params={"per_page": 250, "single_stat": True, "league_id": 2},
                headers=_PP_HEADERS,
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                break
            logger.warning("[PP] HTTP %d (attempt %d/3)", resp.status_code, attempt + 1)
        if data is None:
            return []

        player_map: dict[str, str] = {}
        for item in data.get("included", []):
            if item.get("type") == "new_player":
                pid = item["id"]
                name = item.get("attributes", {}).get("display_name", "")
                if name:
                    player_map[pid] = name

        props: list[dict] = []
        for proj in data.get("data", []):
            attrs = proj.get("attributes", {})
            stat_raw = str(attrs.get("stat_type", "") or "").lower()
            if stat_raw not in _PP_MLB_STAT_TYPES or "inning" in stat_raw:
                continue
            line_val = attrs.get("line_score")
            if line_val is None:
                continue
            pid = (
                proj.get("relationships", {})
                    .get("new_player", {})
                    .get("data", {})
                    .get("id", "")
            )
            pname = player_map.get(pid, "")
            if not pname:
                continue
            props.append({
                "player_name": pname,
                "prop_type":   stat_raw,
                "platform":    "prizepicks",
                "line":        float(line_val),
            })

        logger.info("[PP] %d props fetched", len(props))
        return props
    except Exception as exc:
        logger.warning("[PP] fetch failed: %s", exc)
        return []


def _fetch_underdog() -> list[dict]:
    """Fetch active Underdog Fantasy MLB props."""
    try:
        resp = requests.get(
            "https://api.underdogfantasy.com/v1/over_under_lines",
            headers=_HEADERS,
            timeout=20,
        )
        if resp.status_code != 200:
            logger.warning("[UD] HTTP %d", resp.status_code)
            return []

        data = resp.json()
        players_map = {p["id"]: p for p in data.get("players", [])}
        appearances_map = {a["id"]: a for a in data.get("appearances", [])}

        props: list[dict] = []
        seen: set[str] = set()

        for line in data.get("over_under_lines", []):
            if line.get("status") != "active":
                continue
            stable_id = line.get("stable_id", line.get("id", ""))
            if stable_id in seen:
                continue
            seen.add(stable_id)

            ou = line.get("over_under") or {}
            app_stat = ou.get("appearance_stat") or {}
            stat_ud = app_stat.get("stat", "").lower()
            app_id = app_stat.get("appearance_id", "")
            if not stat_ud or not app_id or "inning" in stat_ud:
                continue

            appearance = appearances_map.get(app_id, {})
            player_id = appearance.get("player_id", "")
            player = players_map.get(player_id, {})
            if player.get("sport_id") != "MLB":
                continue

            pname = (
                f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
            )
            if not pname:
                continue

            props.append({
                "player_name": pname,
                "prop_type":   stat_ud,
                "platform":    "underdog",
                "line":        float(line.get("stat_value") or 0),
            })

        logger.info("[UD] %d props fetched", len(props))
        return props
    except Exception as exc:
        logger.warning("[UD] fetch failed: %s", exc)
        return []


def fetch_all_props() -> list[dict]:
    """Return combined PrizePicks + Underdog props."""
    return _fetch_prizepicks() + _fetch_underdog()


# ---------------------------------------------------------------------------
# Snapshot storage
# ---------------------------------------------------------------------------

def store_snapshot(
    conn: sqlite3.Connection,
    date_str: str,
    props: list[dict],
    ts: str,
    is_opening: bool = False,
) -> None:
    """Insert a batch of prop lines at the given UTC timestamp."""
    rows = [
        (
            date_str,
            p["player_name"],
            p["prop_type"],
            p["platform"],
            p["line"],
            ts,
            int(is_opening),
            0,
        )
        for p in props
    ]
    conn.executemany(
        """
        INSERT INTO line_snapshots
            (game_date, player_name, prop_type, platform, line,
             snapshot_ts, is_opening, is_closing)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    logger.info("[DB] Stored %d snapshots (opening=%s) at %s", len(rows), is_opening, ts)


def get_previous_snapshot(
    conn: sqlite3.Connection,
    date_str: str,
    current_ts: str,
) -> dict[tuple, float]:
    """
    Return a dict of (player_name, prop_type, platform) → line
    for the most recent snapshot stored before current_ts.
    """
    row = conn.execute(
        """
        SELECT MAX(snapshot_ts) AS prev_ts
        FROM line_snapshots
        WHERE game_date = ? AND snapshot_ts < ?
        """,
        (date_str, current_ts),
    ).fetchone()
    if not row or not row["prev_ts"]:
        return {}

    prev_ts = row["prev_ts"]
    rows = conn.execute(
        """
        SELECT player_name, prop_type, platform, line
        FROM line_snapshots
        WHERE game_date = ? AND snapshot_ts = ?
        """,
        (date_str, prev_ts),
    ).fetchall()
    return {
        (r["player_name"], r["prop_type"], r["platform"]): r["line"]
        for r in rows
    }


def mark_closing_lines(
    conn: sqlite3.Connection,
    date_str: str,
    current_ts: str,
) -> bool:
    """
    Mark the last pre-game snapshot as closing lines.
    Only acts if no closing lines are recorded yet for today.
    Returns True if closing lines were newly marked.
    """
    already = conn.execute(
        "SELECT COUNT(*) AS cnt FROM line_snapshots WHERE game_date = ? AND is_closing = 1",
        (date_str,),
    ).fetchone()["cnt"]
    if already > 0:
        return False  # already marked

    row = conn.execute(
        """
        SELECT MAX(snapshot_ts) AS prev_ts
        FROM line_snapshots
        WHERE game_date = ? AND snapshot_ts < ?
        """,
        (date_str, current_ts),
    ).fetchone()
    if not row or not row["prev_ts"]:
        return False

    conn.execute(
        """
        UPDATE line_snapshots
        SET is_closing = 1
        WHERE game_date = ? AND snapshot_ts = ?
        """,
        (date_str, row["prev_ts"]),
    )
    conn.commit()
    logger.info("[DB] Closing lines marked at snapshot %s", row["prev_ts"])
    return True


# ---------------------------------------------------------------------------
# Steam move detection
# ---------------------------------------------------------------------------

def detect_steam_moves(
    current_props: list[dict],
    previous_snap: dict[tuple, float],
) -> list[dict]:
    """
    Compare current snapshot to previous snapshot.
    Returns moves where |delta| >= _STEAM_THRESHOLD, sorted by |delta| desc.
    """
    moves: list[dict] = []
    for prop in current_props:
        key = (prop["player_name"], prop["prop_type"], prop["platform"])
        prev_line = previous_snap.get(key)
        if prev_line is None:
            continue
        delta = prop["line"] - prev_line
        if abs(delta) >= _STEAM_THRESHOLD:
            moves.append({
                "player":   prop["player_name"],
                "prop":     prop["prop_type"],
                "platform": prop["platform"],
                "old_line": prev_line,
                "new_line": prop["line"],
                "delta":    round(delta, 2),
            })
    moves.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return moves


# ---------------------------------------------------------------------------
# CLV computation
# ---------------------------------------------------------------------------

def compute_and_store_clv(
    conn: sqlite3.Connection,
    date_str: str,
    parlays: list[dict],
) -> list[dict]:
    """
    Compute closing line value for each parlay leg.

    CLV convention (DFS — no juice, line-based only):
      Under bets: clv_pts = pick_line - closing_line
                  Positive = close moved down (easier to go under), we locked in harder
                  → we beat the close if clv_pts > 0
      Over bets:  clv_pts = closing_line - pick_line
                  Positive = close moved up (harder to go over), we locked in easier
                  → we beat the close if clv_pts > 0

    Stores results in clv_records and returns the list.
    """
    closing_rows = conn.execute(
        """
        SELECT player_name, prop_type, AVG(line) AS line
        FROM line_snapshots
        WHERE game_date = ? AND is_closing = 1
        GROUP BY player_name, prop_type
        """,
        (date_str,),
    ).fetchall()
    closing_map: dict[tuple, float] = {
        (r["player_name"].lower(), r["prop_type"].lower()): r["line"]
        for r in closing_rows
    }

    now_ts = datetime.now(timezone.utc).isoformat()
    results: list[dict] = []

    for parlay in parlays:
        for leg in parlay.get("legs", []):
            pname = (leg.get("player_name") or "").lower()
            ptype = (leg.get("prop_type") or "").lower()
            side = (leg.get("side") or "under").lower()
            pick_line = leg.get("line")
            if pick_line is None:
                continue

            close_line = closing_map.get((pname, ptype))
            if close_line is None:
                continue

            clv_pts = (
                round(pick_line - close_line, 2)
                if side == "under"
                else round(close_line - pick_line, 2)
            )
            beat = 1 if clv_pts > 0 else 0

            conn.execute(
                """
                INSERT INTO clv_records
                    (season_record_id, game_date, player_name, prop_type,
                     side, pick_line, closing_line, clv_pts, beat_close, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    parlay.get("id"),
                    date_str,
                    leg.get("player_name"),
                    leg.get("prop_type"),
                    side,
                    pick_line,
                    close_line,
                    clv_pts,
                    beat,
                    now_ts,
                ),
            )
            results.append({
                "player":     leg.get("player_name"),
                "prop":       leg.get("prop_type"),
                "side":       side,
                "pick_line":  pick_line,
                "close":      close_line,
                "clv_pts":    clv_pts,
                "beat_close": bool(beat),
            })

    conn.commit()
    logger.info("[CLV] Stored %d CLV records for %s", len(results), date_str)
    return results


# ---------------------------------------------------------------------------
# In-game parlay leg survival
# ---------------------------------------------------------------------------

def _name_match(a: str, b: str) -> bool:
    """
    Fuzzy player name match: exact → last-name + first-initial fallback.
    Handles ESPN vs PrizePicks/Underdog name differences.
    """
    a, b = a.lower().strip(), b.lower().strip()
    if a == b:
        return True
    a_parts, b_parts = a.split(), b.split()
    if not a_parts or not b_parts:
        return False
    return a_parts[-1] == b_parts[-1] and a_parts[0][0] == b_parts[0][0]


def check_parlay_legs_live(
    parlays: list[dict],
    player_stats: dict[str, dict],
) -> list[dict]:
    """
    For each PENDING parlay, check current leg survival using live ESPN stats.
    Returns enriched parlays with 'leg_statuses' key added.
    """
    enriched: list[dict] = []

    for parlay in parlays:
        leg_statuses: list[dict] = []

        for leg in parlay.get("legs", []):
            pname = leg.get("player_name", "")
            ptype = (leg.get("prop_type") or "").lower()
            side = (leg.get("side") or "under").lower()
            line = leg.get("line", 0)

            # Fuzzy match player in ESPN stats
            stats: Optional[dict] = None
            for stats_name, s in player_stats.items():
                if _name_match(pname, stats_name):
                    stats = s
                    break

            if stats is None:
                leg_statuses.append({
                    **leg,
                    "current": None,
                    "live_status": "⏳ Not yet in box score",
                })
                continue

            stat_key = _PROP_STAT_MAP.get(ptype)
            if stat_key == "_combo_hrr":
                current = (
                    float(stats.get("hits", 0))
                    + float(stats.get("runs", 0))
                    + float(stats.get("rbi", 0))
                )
            elif stat_key:
                current = float(stats.get(stat_key, 0))
            else:
                leg_statuses.append({
                    **leg,
                    "current": None,
                    "live_status": "⏳ Unsupported prop",
                })
                continue

            if side == "under":
                if current >= line:
                    live_status = f"❌ Busted ({current:.0f} ≥ {line})"
                else:
                    live_status = f"✅ Live ({current:.0f}/{line})"
            else:
                if current > line:
                    live_status = f"✅ Cashed ({current:.0f} > {line})"
                else:
                    live_status = f"📈 Needs {line - current:.1f} more ({current:.0f}/{line})"

            leg_statuses.append({**leg, "current": current, "live_status": live_status})

        enriched.append({**parlay, "leg_statuses": leg_statuses})

    return enriched


# ---------------------------------------------------------------------------
# Discord posting
# ---------------------------------------------------------------------------

def _post_discord(payload: dict) -> bool:
    """POST a Discord embed to the configured webhook."""
    try:
        resp = requests.post(
            DISCORD_WEBHOOK,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        return resp.status_code in (200, 204)
    except Exception as exc:
        logger.warning("[Discord] post failed: %s", exc)
        return False


def post_steam_alert(date_str: str, moves: list[dict]) -> None:
    """Post a steam move Discord alert for detected line movements."""
    if not moves:
        return

    lines: list[str] = []
    for m in moves[:8]:
        arrow = "🔺" if m["delta"] > 0 else "🔻"
        sign = "+" if m["delta"] > 0 else ""
        lines.append(
            f"{arrow} **{m['player']}** {m['prop'].title()} "
            f"({m['platform'].title()}) "
            f"~~{m['old_line']}~~ → **{m['new_line']}** "
            f"({sign}{m['delta']:.1f})"
        )

    _post_discord({
        "embeds": [{
            "title": f"🌊 Steam Alert — {date_str}",
            "description": "\n".join(lines),
            "color": 0xF4A300,
            "footer": {
                "text": f"PropIQ Line Stream · {len(moves)} move(s) detected"
            },
        }]
    })
    logger.info("[Discord] Steam alert posted (%d moves)", len(moves))


def post_ingame_update(
    date_str: str,
    enriched_parlays: list[dict],
    games: list[dict],
) -> None:
    """Post in-game parlay leg status + live scores to Discord."""
    in_progress = [g for g in games if g["status"] == "IN_PROGRESS"]
    if not in_progress or not enriched_parlays:
        return

    score_lines: list[str] = []
    for g in in_progress[:6]:
        parts = g["name"].split(" at ")
        if len(parts) == 2:
            score_lines.append(
                f"{parts[0]} **{g['away_score']}** — **{g['home_score']}** {parts[1]}"
            )

    leg_sections: list[str] = []
    for parlay in enriched_parlays[:10]:
        agent = parlay.get("agent_name", "Unknown")
        statuses = parlay.get("leg_statuses", [])
        if not statuses:
            continue
        lines = [f"**{agent}**"]
        for ls in statuses:
            lines.append(
                f"• {ls.get('live_status', '')} — "
                f"{ls.get('player_name', '')} "
                f"{ls.get('prop_type', '').title()} "
                f"{ls.get('side', '').title()} {ls.get('line', '')}"
            )
        leg_sections.append("\n".join(lines))

    description_parts: list[str] = []
    if score_lines:
        description_parts.append("**🔴 Live Scores**\n" + "\n".join(score_lines))
    if leg_sections:
        description_parts.append(
            "**Active Parlay Legs**\n" + "\n\n".join(leg_sections)
        )

    if not description_parts:
        return

    _post_discord({
        "embeds": [{
            "title": f"⚡ Live Update — {date_str}",
            "description": "\n\n".join(description_parts),
            "color": 0x00B3FF,
            "footer": {
                "text": (
                    "PropIQ Line Stream · In-game monitoring "
                    "(informational only — no new bets generated)"
                )
            },
        }]
    })
    logger.info(
        "[Discord] In-game update posted (%d parlays, %d games live)",
        len(enriched_parlays), len(in_progress),
    )


def post_clv_report(date_str: str, clv_results: list[dict]) -> None:
    """Post CLV summary to Discord once closing lines are recorded."""
    if not clv_results:
        return

    total = len(clv_results)
    beats = sum(1 for c in clv_results if c["beat_close"])
    beat_pct = beats / total * 100 if total else 0
    avg_clv = sum(c["clv_pts"] for c in clv_results) / total if total else 0

    sorted_clv = sorted(clv_results, key=lambda x: abs(x["clv_pts"]), reverse=True)
    lines: list[str] = []
    for c in sorted_clv[:10]:
        icon = "✅" if c["beat_close"] else "❌"
        sign = "+" if c["clv_pts"] >= 0 else ""
        lines.append(
            f"{icon} **{c['player']}** {c['prop'].title()} {c['side'].upper()} "
            f"{c['pick_line']} → close {c['close']} "
            f"({sign}{c['clv_pts']:.2f} CLV)"
        )

    color = 0x00C851 if beat_pct >= 55 else (0xF4A300 if beat_pct >= 45 else 0xE74C3C)
    _post_discord({
        "embeds": [{
            "title": f"📈 CLV Report — {date_str}",
            "description": (
                f"**Beat close on {beats}/{total} legs ({beat_pct:.0f}%) · "
                f"Avg CLV: {'+' if avg_clv >= 0 else ''}{avg_clv:.2f}**\n\n"
                + "\n".join(lines)
            ),
            "color": color,
            "footer": {"text": "PropIQ Line Stream · Closing Line Value Analysis"},
        }]
    })
    logger.info("[Discord] CLV report posted (%d/%d beat close)", beats, total)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _get_game_counts(games):
    mapping = {"SCHEDULED": 0, "IN_PROGRESS": 0, "FINAL": 0}
    for g in games:
        mapping[g["status"]] = mapping.get(g["status"], 0) + 1
    return mapping["SCHEDULED"], mapping["IN_PROGRESS"], mapping["FINAL"]


def _log_game_state(pre, live, final):
    logger.info("[State] %d scheduled | %d in-progress | %d final", pre, live, final)


def _phase1(conn, today):
    props = fetch_all_props()
    is_first = conn.execute(
        "SELECT COUNT(*) AS cnt FROM line_snapshots WHERE game_date = ?",
        (today,),
    ).fetchone()["cnt"] == 0
    return props, is_first


def main() -> None:
    """Main entry point — called every 30 min by the scheduled trigger."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_ts = datetime.now(timezone.utc).isoformat()
    logger.info("=== PropIQ Line Stream | %s ===", now_ts)

    conn = _get_db()

    # ── Phase 0: ESPN game states ───────────────────────────────────────
    games = get_game_states(today)
    pre_count, live_count, final_count = _get_game_counts(games)
    _log_game_state(pre_count, live_count, final_count)

    # ── Phase 1: Pre-game snapshot + steam detection ────────────────────
    if pre_count or live_count:
        props, is_first = _phase1(conn, today)

        previous_snap = get_previous_snapshot(conn, today, now_ts)
        store_snapshot(conn, today, props, now_ts, is_opening=is_first)

        if previous_snap:
            moves = detect_steam_moves(props, previous_snap)
            if moves:
                logger.info("[Steam] %d significant moves detected", len(moves))
                post_steam_alert(today, moves)
            else:
                logger.info("[Steam] No significant moves this window")
        else:
            logger.info("[Steam] No previous snapshot to compare — first run of day")

    # ── Phase 2: In-game leg tracking ──────────────────────────────────
    if live_count > 0:
        # Mark closing lines (first time a game goes IN_PROGRESS)
        newly_marked = mark_closing_lines(conn, today, now_ts)
        if newly_marked:
            logger.info("[Closing] Closing lines marked for %s", today)

        # Live ESPN stats
        espn_date = today.replace("-", "")
        player_stats = get_all_player_stats(espn_date)

        parlays = get_pending_parlays(today)
        if parlays and player_stats:
            enriched = check_parlay_legs_live(parlays, player_stats)
            post_ingame_update(today, enriched, games)
        else:
            logger.info("[InGame] No pending parlays or no ESPN stats — skipping update")

    # ── Phase 3: CLV (runs once, after closing lines appear) ───────────
    closing_count = conn.execute(
        "SELECT COUNT(*) AS cnt FROM line_snapshots WHERE game_date = ? AND is_closing = 1",
        (today,),
    ).fetchone()["cnt"]

    if closing_count > 0:
        already_computed = conn.execute(
            "SELECT COUNT(*) AS cnt FROM clv_records WHERE game_date = ?",
            (today,),
        ).fetchone()["cnt"]

        if already_computed == 0:
            parlays = get_pending_parlays(today)
            if parlays:
                clv_results = compute_and_store_clv(conn, today, parlays)
                post_clv_report(today, clv_results)
            else:
                logger.info("[CLV] No pending parlays found for CLV calc")
        else:
            logger.info("[CLV] Already computed for %s — skipping", today)

    conn.close()
    logger.info("=== PropIQ Line Stream complete ===")


if __name__ == "__main__":
    main()
