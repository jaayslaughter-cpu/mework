"""
discord_backfill.py
===================
Reads PropIQ pick history from a Discord channel and backfills
missing rows into bet_ledger.

HOW TO USE:
1. Export your Discord channel history. Two options:
   a) DiscordChatExporter (recommended): https://github.com/Tyrrrz/DiscordChatExporter
      Run: DiscordChatExporter.Cli export -t YOUR_BOT_TOKEN -c CHANNEL_ID -f Json
      This produces a JSON file — pass it as --input picks_export.json

   b) Manual JSON: paste the raw JSON from Discord's API into a file
      GET https://discord.com/api/v10/channels/{channel_id}/messages?limit=100
      Headers: Authorization: Bot YOUR_TOKEN

2. Run:
   python3 discord_backfill.py --input picks_export.json --dry-run
   python3 discord_backfill.py --input picks_export.json --write

3. Check results:
   SELECT bet_date, agent_name, COUNT(*) FROM bet_ledger
   WHERE created_at IS NULL OR created_at < '2026-04-01'
   GROUP BY bet_date, agent_name ORDER BY bet_date;

WHAT IT DOES:
- Parses each PropIQ embed from Discord (agent name, legs, platform, stake, date)
- Looks up actual game results from ESPN for each leg
- Inserts one row per leg into bet_ledger with status OPEN (grading runs tonight)
- Skips duplicates (same player+prop+date+agent already in bet_ledger)

REQUIREMENTS:
   pip install requests psycopg2-binary python-dotenv
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timezone
from typing import Optional

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("backfill")

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ── Prop type normalisation (mirror _norm_stat from tasklets.py) ──────────────
_NORM_MAP = {
    "strikeouts": "strikeouts", "k": "strikeouts", "ks": "strikeouts",
    "pitcher strikeouts": "strikeouts",
    "total bases": "total_bases", "tb": "total_bases",
    "hits": "hits", "h": "hits",
    "rbis": "rbis", "rbi": "rbis",
    "hits runs rbis": "hits_runs_rbis", "h+r+rbi": "hits_runs_rbis",
    "earned runs": "earned_runs", "er": "earned_runs",
    "pitching outs": "pitching_outs",
    "hits allowed": "hits_allowed",
    "walks allowed": "walks_allowed",
    "runs": "runs", "home runs": "home_runs",
    "fantasy score": "fantasy_score",
}

def _norm_stat(s: str) -> str:
    s = s.lower().strip().replace("_", " ")
    return _NORM_MAP.get(s, s.replace(" ", "_"))


def _norm_side(s: str) -> str:
    s = s.lower().strip()
    if s in ("over", "more", "higher"): return "OVER"
    if s in ("under", "less", "lower"): return "UNDER"
    return s.upper()


# ── Parse DiscordChatExporter JSON format ──────────────────────────────────────
def _parse_dce_export(data: dict) -> list[dict]:
    """
    Parse DiscordChatExporter JSON output.
    Returns list of pick dicts.
    """
    picks = []
    messages = data.get("messages", [])
    log.info("Scanning %d messages from DCE export", len(messages))

    for msg in messages:
        embeds = msg.get("embeds", [])
        timestamp = msg.get("timestamp", "")
        for embed in embeds:
            pick = _parse_embed(embed, timestamp)
            if pick:
                picks.extend(pick)

    return picks


def _parse_raw_messages(messages: list[dict]) -> list[dict]:
    """
    Parse raw Discord API message list.
    Returns list of pick dicts.
    """
    picks = []
    for msg in messages:
        embeds = msg.get("embeds", [])
        timestamp = msg.get("timestamp", "")
        for embed in embeds:
            pick = _parse_embed(embed, timestamp)
            if pick:
                picks.extend(pick)
    return picks


def _parse_embed(embed: dict, timestamp: str) -> Optional[list[dict]]:
    """
    Parse a single Discord embed into a list of leg dicts.
    Returns None if not a PropIQ pick embed.

    PropIQ embed title format:
        "🌱 EVHunter — 3-Leg PrizePicks Slip"
        "⭐ WeatherAgent — 2-Leg Underdog Fantasy — FlexPlay Slip"
        "🔥 StreakAgent — 2-Leg Underdog Streak"

    Fields:
        "Leg 1 — Mike Trout"  →  "Strikeouts Over 1.5\nModel: 62.3% | EV: +4.2%"
        "Leg 2 — Shohei Ohtani"  →  "Hits Under 1.5\nModel: 58.1% | EV: +3.8%"
        "📊 Summary — 3-Leg Slip"  →  "Avg EV: +4.0% | Confidence: ████████░░ 8.0/10 | Stake: $8"
    """
    title = embed.get("title", "") or embed.get("title", "")

    # Must be a PropIQ slip embed
    if not any(kw in title for kw in ["Slip", "Streak", "EVHunter", "UnderMachine",
                                        "WeatherAgent", "UmpireAgent", "F5Agent",
                                        "BullpenAgent", "FadeAgent", "StackSmith",
                                        "ChalkBuster", "SharpFade", "LineDrift",
                                        "LineupChase", "PropCycle", "CorrelatedParlay",
                                        "MLEdge", "LineValue", "SteamAgent"]):
        return None

    # Extract agent name from title
    # Pattern: "🌱 EVHunter — 3-Leg ..."
    agent_match = re.search(r'[🌱🌿⭐🔥👑]\s+(\w+)\s+—', title)
    agent_name = agent_match.group(1) if agent_match else "Unknown"

    # Extract platform
    platform = "underdog" if "underdog" in title.lower() else "prizepicks"

    # Extract entry_type
    entry_type = "STANDARD"
    if "FlexPlay" in title or "Flex" in title:
        entry_type = "FLEXPLAY"
    elif "PowerPlay" in title or "Power" in title:
        entry_type = "STANDARD"

    # Parse timestamp to date
    try:
        if timestamp:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            bet_date = dt.date()
        else:
            bet_date = date.today()
    except Exception:
        bet_date = date.today()

    # Parse stake from Summary field
    stake = 5.0
    ev_pct = 0.0
    confidence = 5.0
    fields = embed.get("fields", [])

    for field in fields:
        fname = field.get("name", "")
        fval  = field.get("value", "")
        if "Summary" in fname or "summary" in fname.lower():
            m_stake = re.search(r'Stake:\s*\*?\*?\$(\d+)', fval)
            m_ev    = re.search(r'Avg EV:\s*\*?\*?\+?([\d.]+)%', fval)
            m_conf  = re.search(r'([\d.]+)/10', fval)
            if m_stake: stake      = float(m_stake.group(1))
            if m_ev:    ev_pct     = float(m_ev.group(1))
            if m_conf:  confidence = float(m_conf.group(1))

    # Parse legs
    legs = []
    for field in fields:
        fname = field.get("name", "")
        fval  = field.get("value", "")

        # Leg field: "Leg 1 — Mike Trout"
        leg_match = re.match(r'Leg\s+\d+\s+—\s+(.+)', fname)
        if not leg_match:
            continue

        player_name = leg_match.group(1).strip()

        # Value: "**Strikeouts Over 1.5** 🏆\nModel: `62.3%`  |  EV: `+4.2%`"
        # Remove markdown
        clean = re.sub(r'\*+|`', '', fval).strip()

        # First line has prop info: "Strikeouts Over 1.5 🏆"
        first_line = clean.split("\n")[0].strip()
        # Remove emojis
        first_line = re.sub(r'[🏆🐶😴🎯]', '', first_line).strip()

        # Parse: prop_type side line
        # e.g. "Strikeouts Over 1.5" or "Total Bases Higher 1.5"
        parts = first_line.rsplit(None, 2)  # split from right, max 2 splits
        if len(parts) < 3:
            # Try to find side word
            side_found = None
            for side_kw in ("Over", "Under", "Higher", "Lower", "More", "Less"):
                if side_kw in first_line:
                    side_found = side_kw
                    break
            if side_found:
                idx = first_line.index(side_found)
                prop_raw = first_line[:idx].strip()
                rest = first_line[idx:].strip().split()
                side_raw = rest[0] if rest else "Over"
                line_str = rest[1] if len(rest) > 1 else "1.5"
            else:
                log.debug("Could not parse leg field: %s | %s", fname, fval)
                continue
        else:
            # Last part is line value, second-to-last is side, rest is prop
            line_str = parts[-1]
            side_raw = parts[-2]
            prop_raw = parts[0] if len(parts) == 3 else " ".join(parts[:-2])

        # Parse model_prob and leg_ev from second line
        model_prob = 50.0
        leg_ev = 0.0
        if "\n" in clean:
            second_line = clean.split("\n")[1] if len(clean.split("\n")) > 1 else ""
            m_model = re.search(r'Model[:\s]+(\d+\.?\d*)%', second_line)
            m_lev   = re.search(r'EV[:\s]+\+?(\d+\.?\d*)%', second_line)
            if m_model: model_prob = float(m_model.group(1))
            if m_lev:   leg_ev     = float(m_lev.group(1))

        try:
            line_val = float(line_str)
        except (ValueError, TypeError):
            line_val = 0.5

        legs.append({
            "player_name":  player_name,
            "prop_type":    _norm_stat(prop_raw),
            "side":         _norm_side(side_raw),
            "line":         line_val,
            "model_prob":   model_prob,
            "ev_pct":       leg_ev,
            "platform":     platform,
            "bet_date":     bet_date,
            "agent_name":   agent_name,
            "entry_type":   entry_type,
            "stake":        stake,
            "confidence":   confidence,
        })

    if not legs:
        log.debug("No legs parsed from embed: %s", title)
        return None

    log.info("Parsed %d legs — %s %s (%s)", len(legs), agent_name, bet_date, platform)
    return legs


# ── ESPN result lookup ────────────────────────────────────────────────────────
def _lookup_result(player: str, prop_type: str, side: str,
                   line: float, game_date: date) -> Optional[tuple[str, float]]:
    """
    Look up actual game result from ESPN.
    Returns (status, actual_value) or None if not found.
    """
    try:
        date_str = game_date.strftime("%Y%m%d")
        url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_str}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
    except Exception as e:
        log.debug("ESPN scoreboard fetch failed for %s: %s", game_date, e)
        return None

    player_lower = player.lower().strip()

    for event in data.get("events", []):
        for comp in event.get("competitions", []):
            for athlete_entry in _get_athlete_stats(comp, date_str):
                name = athlete_entry.get("name", "").lower()
                if player_lower not in name and name not in player_lower:
                    # Try last name match
                    last = player_lower.split()[-1] if player_lower.split() else player_lower
                    if last not in name:
                        continue

                actual = _extract_stat(athlete_entry, prop_type)
                if actual is None:
                    continue

                # Determine WIN/LOSS
                if side == "OVER":
                    status = "WIN" if actual > line else ("PUSH" if actual == line else "LOSS")
                else:
                    status = "WIN" if actual < line else ("PUSH" if actual == line else "LOSS")

                return status, actual

    return None


def _get_athlete_stats(competition: dict, date_str: str) -> list[dict]:
    """Extract athlete stat lines from ESPN competition."""
    athletes = []
    for competitor in competition.get("competitors", []):
        stats = competitor.get("statistics", [])
        for stat in stats:
            athletes.append(stat)
        # Also check roster
        roster = competitor.get("roster", {}).get("entries", [])
        for entry in roster:
            athlete = entry.get("athlete", {})
            stats_val = entry.get("stats", [])
            if athlete and stats_val:
                athletes.append({
                    "name": athlete.get("displayName", ""),
                    "stats": stats_val,
                })
    return athletes


def _extract_stat(athlete: dict, prop_type: str) -> Optional[float]:
    """Extract a specific stat from an ESPN athlete entry."""
    stats = athlete.get("stats", [])
    if not stats:
        return None

    # Map prop_type to ESPN stat name
    _ESPN_MAP = {
        "strikeouts":     ["strikeouts", "SO", "K"],
        "total_bases":    ["totalBases", "TB"],
        "hits":           ["hits", "H"],
        "rbis":           ["RBIs", "RBI", "rbi"],
        "runs":           ["runs", "R"],
        "earned_runs":    ["earnedRuns", "ER"],
        "pitching_outs":  ["outs", "pitchingOuts"],
        "hits_allowed":   ["hitsAllowed", "H"],
        "home_runs":      ["homeRuns", "HR"],
        "hits_runs_rbis": None,  # composite
    }

    targets = _ESPN_MAP.get(prop_type, [prop_type])
    if targets is None:
        # hits_runs_rbis composite
        h = _extract_stat(athlete, "hits") or 0
        r = _extract_stat(athlete, "runs") or 0
        rbi = _extract_stat(athlete, "rbis") or 0
        return h + r + rbi if (h or r or rbi) else None

    for stat_entry in stats:
        label = stat_entry.get("name", "") or stat_entry.get("abbreviation", "")
        if any(t.lower() == label.lower() for t in (targets or [])):
            try:
                return float(stat_entry.get("displayValue", stat_entry.get("value", 0)))
            except (ValueError, TypeError):
                pass

    return None


# ── Database operations ───────────────────────────────────────────────────────
def _pg_conn():
    return psycopg2.connect(DATABASE_URL)


def _check_duplicate(cur, player: str, prop_type: str,
                     side: str, bet_date: date, agent: str) -> bool:
    """Return True if this leg already exists in bet_ledger."""
    cur.execute("""
        SELECT 1 FROM bet_ledger
        WHERE LOWER(player_name) = LOWER(%s)
          AND prop_type = %s
          AND side = %s
          AND bet_date = %s
          AND agent_name = %s
        LIMIT 1
    """, (player, prop_type, side, bet_date, agent))
    return cur.fetchone() is not None


def _insert_leg(cur, leg: dict, status: str,
                actual_value: Optional[float], dry_run: bool) -> bool:
    """Insert a single leg into bet_ledger. Returns True if inserted."""
    if dry_run:
        log.info("  [DRY RUN] Would insert: %s %s %s %s %s  → %s",
                 leg["bet_date"], leg["agent_name"], leg["player_name"],
                 leg["prop_type"], leg["side"], status)
        return True

    actual_outcome = None
    if status == "WIN":   actual_outcome = 1
    elif status == "LOSS": actual_outcome = 0

    profit_loss = None
    if status == "WIN":
        # Simple flat payout estimate (no parlay multiplier — per-leg tracking)
        profit_loss = round(leg["stake"] * 0.85, 2)   # ~85% avg net on DFS platforms
    elif status == "LOSS":
        profit_loss = -round(leg["stake"], 2)
    elif status == "PUSH":
        profit_loss = 0.0

    cur.execute("""
        INSERT INTO bet_ledger
            (player_name, prop_type, line, side, odds_american,
             kelly_units, model_prob, ev_pct, agent_name,
             status, bet_date, platform, features_json,
             units_wagered, mlbam_id, entry_type, discord_sent,
             lookahead_safe, profit_loss, actual_result, actual_outcome,
             graded_at)
        VALUES
            (%s, %s, %s, %s, -110,
             0.05, %s, %s, %s,
             %s, %s, %s, %s,
             %s, NULL, %s, TRUE,
             TRUE, %s, %s, %s,
             %s)
    """, (
        leg["player_name"], leg["prop_type"], leg["line"], leg["side"],
        leg["model_prob"], leg["ev_pct"], leg["agent_name"],
        status, leg["bet_date"], leg["platform"],
        json.dumps({"backfilled": True, "confidence": leg["confidence"]}),
        leg["stake"], leg["entry_type"],
        profit_loss, actual_value, actual_outcome,
        datetime.now(timezone.utc) if status != "OPEN" else None,
    ))
    return True


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Backfill PropIQ bet_ledger from Discord history")
    parser.add_argument("--input",   required=True, help="Path to Discord export JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Parse and show without writing")
    parser.add_argument("--write",   action="store_true", help="Actually write to bet_ledger")
    parser.add_argument("--no-grade", action="store_true",
                        help="Insert as OPEN (grading runs tonight) instead of looking up results")
    args = parser.parse_args()

    if not args.dry_run and not args.write:
        print("Pass --dry-run to preview or --write to commit. Exiting.")
        sys.exit(1)

    dry_run = not args.write

    # Load Discord export
    with open(args.input) as f:
        raw = json.load(f)

    # Auto-detect format
    if isinstance(raw, dict) and "messages" in raw:
        # DiscordChatExporter format
        legs = _parse_dce_export(raw)
    elif isinstance(raw, list):
        # Raw Discord API message list
        legs = _parse_raw_messages(raw)
    else:
        print(f"Unknown format in {args.input}. Expected DCE JSON or raw message list.")
        sys.exit(1)

    log.info("Parsed %d total legs from Discord history", len(legs))

    if not legs:
        log.warning("No pick legs found — check that the export contains PropIQ embeds")
        sys.exit(0)

    # DB connection
    if not dry_run and not DATABASE_URL:
        print("DATABASE_URL not set. Set it in .env or environment.")
        sys.exit(1)

    conn = _pg_conn() if not dry_run else None
    cur  = conn.cursor() if conn else None

    inserted  = 0
    skipped   = 0
    graded    = 0
    no_result = 0

    for leg in legs:
        player    = leg["player_name"]
        prop_type = leg["prop_type"]
        side      = leg["side"]
        bet_date  = leg["bet_date"]
        agent     = leg["agent_name"]

        # Skip duplicates
        if cur and _check_duplicate(cur, player, prop_type, side, bet_date, agent):
            log.debug("SKIP (duplicate): %s %s %s %s", bet_date, agent, player, prop_type)
            skipped += 1
            continue

        # Look up actual result
        if args.no_grade or dry_run:
            status = "OPEN"
            actual_value = None
        else:
            result = _lookup_result(player, prop_type, side, leg["line"], bet_date)
            if result:
                status, actual_value = result
                graded += 1
            else:
                status = "OPEN"
                actual_value = None
                no_result += 1

        if _insert_leg(cur, leg, status, actual_value, dry_run):
            inserted += 1

    if conn:
        conn.commit()
        conn.close()

    print(f"""
=== BACKFILL COMPLETE ===
  Legs parsed:      {len(legs)}
  Inserted:         {inserted}
  Skipped (dupes):  {skipped}
  Graded (ESPN):    {graded}
  OPEN (no result): {no_result}
  Mode:             {'DRY RUN' if dry_run else 'WRITTEN TO DB'}

Next steps:
  1. Check Railway Postgres: SELECT bet_date, COUNT(*) FROM bet_ledger GROUP BY bet_date ORDER BY bet_date DESC
  2. GradingTasklet runs at 2AM PT — will grade any OPEN rows from past dates tonight
  3. After grading: SELECT COUNT(*) FROM bet_ledger WHERE actual_outcome IS NOT NULL
""")


if __name__ == "__main__":
    main()
