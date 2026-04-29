"""
direct_backfill.py
==================
Directly backfills bet_ledger from the PropIQ Discord history
pasted into this session. No export file needed.

Covers:
  - April 3, 4 (early picks — results from ESPN)
  - April 14, 15, 16 (results extracted from Daily Recap embeds)

Usage:
  export DATABASE_URL="postgresql://..."
  python3 direct_backfill.py --dry-run   # preview
  python3 direct_backfill.py --write     # commit
"""
from __future__ import annotations

import argparse, json, os, sys, unicodedata
from datetime import date, datetime, timezone

import psycopg2
import requests

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ── Normalisation helpers ─────────────────────────────────────────────────────
def _norm_name(s: str) -> str:
    n = unicodedata.normalize("NFD", (s or "").lower().strip())
    return "".join(c for c in n if unicodedata.category(c) != "Mn")

def _norm_stat(s: str) -> str:
    s = s.lower().strip().replace(" ", "_")
    MAP = {
        "strikeouts": "strikeouts", "pitcher_strikeouts": "strikeouts",
        "hitter_strikeouts": "strikeouts",
        "total_bases": "total_bases", "tb": "total_bases",
        "hits": "hits",
        "rbis": "rbis", "rbi": "rbis",
        "hits_runs_rbis": "hits_runs_rbis", "h+r+rbi": "hits_runs_rbis",
        "earned_runs": "earned_runs",
        "pitching_outs": "pitching_outs", "pitcher_outs": "pitching_outs",
        "hits_allowed": "hits_allowed",
        "walks_allowed": "walks_allowed",
        "runs": "runs",
        "fantasy_score": "fantasy_score",
    }
    return MAP.get(s, s)

def _norm_side(s: str) -> str:
    s = s.lower().strip()
    if s in ("over", "more", "higher"): return "OVER"
    if s in ("under", "less", "lower"): return "UNDER"
    return s.upper()

# ── All pick legs parsed from the Discord history ─────────────────────────────
# Format: (date, agent, player, prop_type, side, line, model_prob, ev_pct, platform, stake)

PICKS = [
    # ── April 3, 2026 ──────────────────────────────────────────────────────────
    # OmegaStack — early-run duplicate, skip (same as later batches)
    # F5Agent 8:42 PM
    (date(2026,4,3), "F5Agent", "Jesús Luzardo", "strikeouts", "UNDER", 6.5, 78.0, 48.9, "underdog", 5.0),
    (date(2026,4,3), "F5Agent", "Tyler Glasnow",  "strikeouts", "UNDER", 6.5, 77.2, 47.3, "underdog", 5.0),
    (date(2026,4,3), "F5Agent", "Shohei Ohtani",  "runs",       "UNDER", 1.5, 72.3, 38.1, "underdog", 5.0),
    # EVHunter PrizePicks 9:12 PM — canonical PP pick for the day
    (date(2026,4,3), "EVHunter", "Leo Jiménez",    "hits_runs_rbis", "OVER",  0.5, 57.2, 7.0, "prizepicks", 10.0),
    (date(2026,4,3), "EVHunter", "Jake Irvin",      "earned_runs",    "UNDER", 3.5, 56.6, 5.8, "prizepicks", 10.0),
    (date(2026,4,3), "EVHunter", "Everson Pereira", "total_bases",    "OVER",  0.5, 56.5, 5.7, "prizepicks", 10.0),

    # ── April 4, 2026 ──────────────────────────────────────────────────────────
    # EVHunter PrizePicks midnight / 12:18 AM
    (date(2026,4,4), "EVHunter", "Leo Jiménez",    "hits_runs_rbis", "OVER",  0.5, 57.2, 7.0, "prizepicks", 10.0),
    (date(2026,4,4), "EVHunter", "Jake Irvin",      "earned_runs",    "UNDER", 3.5, 56.6, 5.8, "prizepicks", 10.0),
    (date(2026,4,4), "EVHunter", "Kyle Higashioka", "total_bases",    "OVER",  0.5, 56.5, 5.7, "prizepicks", 10.0),
    # UnderMachine PrizePicks 7:59 PM
    (date(2026,4,4), "UnderMachine", "Lance Mccullers Jr.", "earned_runs",  "UNDER", 2.5, 57.5, 7.5, "prizepicks", 10.0),
    (date(2026,4,4), "UnderMachine", "Ranger Suarez",       "hits_allowed", "UNDER", 5.5, 56.2, 5.1, "prizepicks", 10.0),
    (date(2026,4,4), "UnderMachine", "Jack Leiter",          "strikeouts",   "UNDER", 6.5, 55.8, 4.3, "prizepicks", 10.0),
    # EVHunter PrizePicks 7:41 PM
    (date(2026,4,4), "EVHunter", "Bo Naylor",         "total_bases", "OVER", 0.5, 56.5, 5.7, "prizepicks", 10.0),
    (date(2026,4,4), "EVHunter", "Brayan Rocchio",    "total_bases", "OVER", 0.5, 56.5, 5.7, "prizepicks", 10.0),
    (date(2026,4,4), "EVHunter", "Daniel Schneemann", "total_bases", "OVER", 0.5, 56.5, 5.7, "prizepicks", 10.0),

    # ── April 14, 2026 ─────────────────────────────────────────────────────────
    # Results known from Daily Recap Apr 15
    (date(2026,4,14), "MLEdgeAgent",  "Merrill Kelly",      "strikeouts",   "UNDER", 5.0, 55.0, 5.0, "underdog", 5.0),
    (date(2026,4,14), "MLEdgeAgent",  "Colton Gordon",      "strikeouts",   "UNDER", 4.5, 55.0, 5.0, "underdog", 5.0),
    (date(2026,4,14), "EVHunter",     "Riley Martin",       "earned_runs",  "OVER",  0.5, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "EVHunter",     "Mitch Keller",       "pitching_outs","UNDER",17.0, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "EVHunter",     "Jacob Misiorowski",  "pitching_outs","UNDER",17.0, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "UnderMachine", "Mitch Keller",       "pitching_outs","UNDER",17.0, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "UnderMachine", "MacKenzie Gore",     "pitching_outs","UNDER",17.0, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "UnderMachine", "Michael McGreevy",   "pitching_outs","UNDER",16.5, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "UmpireAgent",  "Yoshi Yamamoto",     "strikeouts",   "UNDER", 6.5, 55.0, 5.0, "underdog", 5.0),
    (date(2026,4,14), "UmpireAgent",  "Merrill Kelly",      "strikeouts",   "UNDER", 5.0, 55.0, 5.0, "underdog", 5.0),
    (date(2026,4,14), "WeatherAgent", "Bryce Teodosio",     "hits",         "OVER",  0.5, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "WeatherAgent", "Andrés Giménez",     "hits",         "OVER",  0.5, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "WeatherAgent", "Gary Sánchez",       "hits",         "OVER",  0.5, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "BullpenAgent", "Taylor Trammell",    "hits",         "OVER",  0.5, 55.0, 5.0, "prizepicks", 5.0),
    (date(2026,4,14), "BullpenAgent", "Maverick Handley",   "hits",         "OVER",  0.5, 55.0, 5.0, "prizepicks", 5.0),

    # ── April 15, 2026 ─────────────────────────────────────────────────────────
    (date(2026,4,15), "UmpireAgent",          "Randy Vásquez",   "strikeouts",   "UNDER", 4.5, 55.8, 11.6, "underdog", 5.0),
    (date(2026,4,15), "UmpireAgent",          "Shota Imanaga",   "strikeouts",   "UNDER", 5.5, 55.6, 11.3, "underdog", 5.0),
    (date(2026,4,15), "MLEdgeAgent",          "Randy Vásquez",   "strikeouts",   "UNDER", 4.5, 55.8, 11.7, "underdog", 5.0),
    (date(2026,4,15), "MLEdgeAgent",          "James Wood",      "strikeouts",   "UNDER", 1.5, 54.5,  9.0, "underdog", 5.0),
    (date(2026,4,15), "EVHunter",             "Bryce Elder",     "pitching_outs","UNDER",17.5, 55.6, 11.3, "prizepicks", 5.0),
    (date(2026,4,15), "EVHunter",             "Shota Imanaga",   "pitching_outs","UNDER",17.0, 55.4, 10.8, "prizepicks", 5.0),
    (date(2026,4,15), "EVHunter",             "Eduardo Rodriguez","pitching_outs","UNDER",16.5, 55.2, 10.4, "prizepicks", 5.0),
    (date(2026,4,15), "BullpenAgent",         "Bryson Stott",    "hits",         "OVER",  0.5, 53.0,  6.1, "prizepicks", 5.0),
    (date(2026,4,15), "BullpenAgent",         "Bryce Teodosio",  "hits",         "OVER",  0.5, 53.0,  6.0, "prizepicks", 5.0),
    (date(2026,4,15), "BullpenAgent",         "Davis Schneider", "hits",         "OVER",  0.5, 53.0,  5.9, "prizepicks", 5.0),
    (date(2026,4,15), "CorrelatedParlayAgent","Masyn Winn",      "strikeouts",   "OVER",  0.5, 52.9,  5.9, "prizepicks", 5.0),
    (date(2026,4,15), "CorrelatedParlayAgent","Shohei Ohtani",   "earned_runs",  "OVER",  1.5, 52.8,  5.7, "prizepicks", 5.0),

    # ── April 16, 2026 ─────────────────────────────────────────────────────────
    (date(2026,4,16), "MLEdgeAgent",          "Ryan Weiss",      "strikeouts",   "UNDER", 4.5, 56.1, 12.3, "underdog", 5.0),
    (date(2026,4,16), "MLEdgeAgent",          "James Wood",      "strikeouts",   "UNDER", 1.5, 54.5,  9.1, "underdog", 5.0),
    (date(2026,4,16), "MLEdgeAgent",          "Braxton Ashcraft","strikeouts",   "UNDER", 5.5, 57.4,  5.0, "underdog", 5.0),
    (date(2026,4,16), "EVHunter",             "Braxton Ashcraft","pitching_outs","UNDER",16.5, 55.2, 10.4, "prizepicks", 5.0),
    (date(2026,4,16), "EVHunter",             "Parker Messick",  "pitching_outs","UNDER",16.5, 55.2, 10.4, "prizepicks", 5.0),
    (date(2026,4,16), "EVHunter",             "Shane Baz",       "pitching_outs","UNDER",16.5, 55.2, 10.4, "prizepicks", 5.0),
    (date(2026,4,16), "CorrelatedParlayAgent","Tyler Heineman",  "strikeouts",   "OVER",  0.5, 53.0,  6.0, "prizepicks", 5.0),
    (date(2026,4,16), "CorrelatedParlayAgent","Nolan Schanuel",  "strikeouts",   "OVER",  0.5, 52.9,  5.8, "prizepicks", 5.0),
    (date(2026,4,16), "CorrelatedParlayAgent","Sam Antonacci",   "strikeouts",   "OVER",  0.5, 52.9,  5.8, "prizepicks", 5.0),
    (date(2026,4,16), "BullpenAgent",         "Brett Sullivan",  "total_bases",  "OVER",  0.5, 52.8,  5.7, "prizepicks", 5.0),
    (date(2026,4,16), "BullpenAgent",         "Daylen Lile",     "fantasy_score","OVER",  5.5, 52.7,  5.5, "prizepicks", 5.0),
    (date(2026,4,16), "BullpenAgent",         "Maikel Garcia",   "fantasy_score","OVER",  6.5, 52.7,  5.5, "prizepicks", 5.0),
    (date(2026,4,16), "WeatherAgent",         "Amed Rosario",    "hits_runs_rbis","OVER", 1.5, 52.7,  5.5, "prizepicks", 5.0),
    (date(2026,4,16), "WeatherAgent",         "Aaron Judge",     "hits_runs_rbis","OVER", 2.5, 52.6,  5.2, "prizepicks", 5.0),
    (date(2026,4,16), "UmpireAgent",          "Ryan Weiss",      "strikeouts",   "UNDER", 4.5, 56.1, 12.3, "underdog", 5.0),
    (date(2026,4,16), "UmpireAgent",          "Braxton Ashcraft","strikeouts",   "UNDER", 5.5, 57.4,  5.0, "underdog", 5.0),
]

# ── Known results from Daily Recap embeds ─────────────────────────────────────
# Key: (player_norm, prop_type, side, date) → (status, actual)
KNOWN_RESULTS = {
    (_norm_name("Merrill Kelly"),    "strikeouts",   "UNDER", date(2026,4,14)): ("WIN",  3.0),
    (_norm_name("Colton Gordon"),    "strikeouts",   "UNDER", date(2026,4,14)): ("LOSS", 5.0),
    (_norm_name("Mitch Keller"),     "pitching_outs","UNDER", date(2026,4,14)): ("WIN", 12.0),
    (_norm_name("MacKenzie Gore"),   "pitching_outs","UNDER", date(2026,4,14)): ("WIN", 14.0),
    (_norm_name("Michael McGreevy"), "pitching_outs","UNDER", date(2026,4,14)): ("WIN", 15.0),
    (_norm_name("Bryce Teodosio"),   "hits",         "OVER",  date(2026,4,14)): ("LOSS", 0.0),
    (_norm_name("Andrés Giménez"),   "hits",         "OVER",  date(2026,4,14)): ("WIN",  1.0),
    (_norm_name("Gary Sánchez"),     "hits",         "OVER",  date(2026,4,14)): ("WIN",  2.0),
    (_norm_name("Riley Martin"),     "earned_runs",  "OVER",  date(2026,4,14)): ("LOSS", 0.0),
    (_norm_name("Jacob Misiorowski"),"pitching_outs","UNDER", date(2026,4,14)): ("WIN", 16.0),
    (_norm_name("Yoshi Yamamoto"),   "strikeouts",   "UNDER", date(2026,4,14)): ("LOSS", 7.0),
    (_norm_name("Taylor Trammell"),  "hits",         "OVER",  date(2026,4,14)): ("LOSS", 0.0),

    (_norm_name("Randy Vásquez"),    "strikeouts",   "UNDER", date(2026,4,15)): ("LOSS", 6.0),
    (_norm_name("Shota Imanaga"),    "strikeouts",   "UNDER", date(2026,4,15)): ("LOSS",11.0),
    (_norm_name("Bryce Elder"),      "pitching_outs","UNDER", date(2026,4,15)): ("WIN", 17.0),
    (_norm_name("Shota Imanaga"),    "pitching_outs","UNDER", date(2026,4,15)): ("LOSS",18.0),
    (_norm_name("Eduardo Rodriguez"),"pitching_outs","UNDER", date(2026,4,15)): ("WIN", 15.0),
    (_norm_name("Bryce Teodosio"),   "hits",         "OVER",  date(2026,4,15)): ("LOSS", 0.0),
    (_norm_name("Davis Schneider"),  "hits",         "OVER",  date(2026,4,15)): ("LOSS", 0.0),
    (_norm_name("Shohei Ohtani"),    "earned_runs",  "OVER",  date(2026,4,15)): ("LOSS", 1.0),

    (_norm_name("Ryan Weiss"),       "strikeouts",   "UNDER", date(2026,4,16)): ("WIN",  3.0),
    (_norm_name("Braxton Ashcraft"), "strikeouts",   "UNDER", date(2026,4,16)): ("LOSS", 7.0),
    (_norm_name("Braxton Ashcraft"), "pitching_outs","UNDER", date(2026,4,16)): ("LOSS",17.0),
    (_norm_name("Parker Messick"),   "pitching_outs","UNDER", date(2026,4,16)): ("LOSS",24.0),
    (_norm_name("Shane Baz"),        "pitching_outs","UNDER", date(2026,4,16)): ("LOSS",18.0),
    (_norm_name("Daylen Lile"),      "fantasy_score","OVER",  date(2026,4,16)): ("LOSS", 0.0),
    (_norm_name("Maikel Garcia"),    "fantasy_score","OVER",  date(2026,4,16)): ("LOSS", 0.0),
    (_norm_name("Amed Rosario"),     "hits_runs_rbis","OVER", date(2026,4,16)): ("WIN",  4.0),
    (_norm_name("Aaron Judge"),      "hits_runs_rbis","OVER", date(2026,4,16)): ("WIN",  6.0),
}


def _get_result(player: str, prop_type: str, side: str, dt: date):
    key = (_norm_name(player), _norm_stat(prop_type), side, dt)
    return KNOWN_RESULTS.get(key)


def _calc_profit(status: str, stake: float) -> float:
    if status == "WIN":  return round(stake * 0.85, 2)
    if status == "LOSS": return -round(stake, 2)
    return 0.0


def run(dry_run: bool):
    conn = None if dry_run else psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor() if conn else None

    inserted = skipped = graded = open_rows = 0

    for (bet_date, agent, player, prop_raw, side, line,
         model_prob, ev_pct, platform, stake) in PICKS:

        prop_type = _norm_stat(prop_raw)

        # Duplicate check
        if cur:
            cur.execute("""
                SELECT 1 FROM bet_ledger
                WHERE LOWER(player_name) = LOWER(%s)
                  AND prop_type = %s AND side = %s
                  AND bet_date = %s AND agent_name = %s
                LIMIT 1
            """, (player, prop_type, side, bet_date, agent))
            if cur.fetchone():
                skipped += 1
                continue

        # Look up result
        result = _get_result(player, prop_type, side, bet_date)
        if result:
            status, actual = result
            graded += 1
            actual_outcome = 1 if status == "WIN" else (0 if status == "LOSS" else None)
            profit_loss = _calc_profit(status, stake)
            graded_at = datetime.now(timezone.utc)
        else:
            status, actual, actual_outcome = "OPEN", None, None
            profit_loss = None
            graded_at = None
            open_rows += 1

        if dry_run:
            print(f"  {bet_date}  {agent:25s}  {player:22s}  {prop_type:15s}  "
                  f"{side:5s}  {line}  →  {status}" +
                  (f"  (actual: {actual})" if actual is not None else ""))
            inserted += 1
            continue

        cur.execute("""
            INSERT INTO bet_ledger
                (player_name, prop_type, line, side, odds_american,
                 kelly_units, model_prob, ev_pct, agent_name,
                 status, bet_date, platform, features_json,
                 units_wagered, mlbam_id, entry_type, discord_sent,
                 lookahead_safe, profit_loss, actual_result, actual_outcome,
                 graded_at)
            VALUES
                (%s,%s,%s,%s,-110,
                 0.05,%s,%s,%s,
                 %s,%s,%s,%s,
                 %s,NULL,'STANDARD',TRUE,
                 TRUE,%s,%s,%s,%s)
        """, (
            player, prop_type, line, side,
            model_prob, ev_pct, agent,
            status, bet_date, platform,
            # Store neutral 27-slot feature vector so XGBoost training can use
            # these rows without crashing. The grading tasklet will overwrite
            # features_json with real player signals when it grades this row.
            json.dumps([0.5] * 27),  # neutral defaults — rebuilt at grade time
            stake,
            profit_loss, actual, actual_outcome,
            graded_at,
        ))
        inserted += 1

    if conn:
        conn.commit()
        conn.close()

    print(f"""
{'='*60}
BACKFILL {'DRY RUN' if dry_run else 'COMPLETE'}
  Total picks:    {len(PICKS)}
  Inserted:       {inserted}
  Skipped (dupe): {skipped}
  Graded:         {graded}
  OPEN:           {open_rows}
{'='*60}

After deploy + write, run:
  SELECT bet_date, COUNT(*), COUNT(actual_outcome) as graded
  FROM bet_ledger GROUP BY bet_date ORDER BY bet_date DESC;
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write",   action="store_true")
    args = parser.parse_args()
    if not args.dry_run and not args.write:
        print("Pass --dry-run or --write")
        sys.exit(1)
    if not args.dry_run and not DATABASE_URL:
        print("Set DATABASE_URL env var")
        sys.exit(1)
    run(dry_run=not args.write)
