"""
nsfi_layer.py
=============
PropIQ — No Strikeout First Inning (NSFI) Monte Carlo simulation layer.

Adapted from mlb-nsfi-model (github.com/austinlmcconnell/mlb-nsfi-model)
  model.py — simulate_half_inning(), PARK_FACTORS, load_model_data()

What NSFI is:
    DraftKings and FanDuel offer a daily "No Strikeout First Inning" Yes/No
    market per half-inning. The "No" outcome pays out if the starting pitcher
    records zero strikeouts in the 1st inning. This model predicts P(NSFI)
    using Monte Carlo simulation of 1st-inning plate appearances.

How it works:
    1. Fetch today's lineups and probable pitchers from MLB Stats API (free).
    2. Load pitcher K/BB/hit rates and batter K/BB/hit rates from FanGraphs
       CSV exports (already cached in fangraphs_layer.py).
    3. For each game, simulate 10,000 half-innings using:
         - Platoon splits (vs LHH/RHH, vs LHP/RHP)
         - Year-weighted stats (40% prior year + 60% current season)
         - Park factors (all 30 parks × 8 hit types × 2 handedness)
         - Combined rate via Odds Ratio / Log5 method (csf = odds_ratio(batter, pitcher, lg_avg))
    4. P(NSFI) = fraction of simulated half-innings with zero strikeouts.
    5. Compare P(NSFI) to DraftKings implied probability — bet when edge > 3%.

Data sources (all free, no key):
    statsapi.mlb.com  — lineups, probable pitchers, handedness
    fangraphs_layer.py — K%, BB%, wRC+, BABIP (already in your pipeline)

Integration:
    Call simulate_game_nsfi(game_data, fangraphs_hub) from DataHub
    or from a dedicated NSFIAgent in tasklets.py.
    Results stored in hub["physics"]["nsfi"] list.
"""

from __future__ import annotations

import logging
import time
import requests
import numpy as np
from typing import Optional

logger = logging.getLogger("propiq.nsfi")

_MLBAPI = "https://statsapi.mlb.com/api/v1"
_HEADERS = {"User-Agent": "Mozilla/5.0 (PropIQ/2.0)"}

# ---------------------------------------------------------------------------
# 2024 MLB league averages (per PA)
# Source: Baseball Reference 2024 season totals
# ---------------------------------------------------------------------------

MLB_AVGS = {
    "K_Rate":  0.223,   # FIX: FG 2025 (was 0.224)
    "BB_Rate": 0.086,   # FIX: FG 2025 (was 0.085)
    "1B_Rate": 0.143,
    "2B_Rate": 0.047,
    "3B_Rate": 0.004,
    "HR_Rate": 0.032,   # FIX: 2024 actual (was 0.030)
}

# ---------------------------------------------------------------------------
# Park factors — all 30 MLB parks
# Adapted directly from mlb-nsfi-model/model.py PARK_FACTORS dict
# Format: {ballpark_key: {hit_type_handedness: multiplier}}
# hit_type: 1B, 2B, 3B, HR | handedness: LH (left-hand batter), RH (right-hand batter)
# ---------------------------------------------------------------------------

PARK_FACTORS: dict[str, dict[str, float]] = {
    "Angels":        {"1B_LH":0.95,"1B_RH":0.96,"2B_LH":0.91,"2B_RH":1.02,"3B_LH":0.55,"3B_RH":0.95,"HR_LH":1.29,"HR_RH":1.02},
    "Diamondbacks":  {"1B_LH":1.05,"1B_RH":0.99,"2B_LH":1.01,"2B_RH":0.95,"3B_LH":2.39,"3B_RH":1.52,"HR_LH":0.97,"HR_RH":0.87},
    "Orioles":       {"1B_LH":0.99,"1B_RH":1.00,"2B_LH":1.01,"2B_RH":0.87,"3B_LH":0.90,"3B_RH":0.65,"HR_LH":1.11,"HR_RH":1.20},
    "Red Sox":       {"1B_LH":0.97,"1B_RH":0.99,"2B_LH":1.59,"2B_RH":1.25,"3B_LH":1.19,"3B_RH":1.21,"HR_LH":0.82,"HR_RH":0.97},
    "Cubs":          {"1B_LH":1.03,"1B_RH":0.99,"2B_LH":0.98,"2B_RH":1.01,"3B_LH":1.18,"3B_RH":1.56,"HR_LH":0.83,"HR_RH":0.98},
    "White Sox":     {"1B_LH":0.95,"1B_RH":1.03,"2B_LH":0.72,"2B_RH":0.91,"3B_LH":0.84,"3B_RH":0.31,"HR_LH":1.15,"HR_RH":1.12},
    "Reds":          {"1B_LH":0.99,"1B_RH":0.93,"2B_LH":0.92,"2B_RH":1.08,"3B_LH":0.79,"3B_RH":0.63,"HR_LH":1.35,"HR_RH":1.30},
    "Guardians":     {"1B_LH":0.99,"1B_RH":1.00,"2B_LH":1.13,"2B_RH":1.02,"3B_LH":0.85,"3B_RH":0.88,"HR_LH":1.08,"HR_RH":0.98},
    "Rockies":       {"1B_LH":1.15,"1B_RH":1.19,"2B_LH":1.12,"2B_RH":1.43,"3B_LH":1.91,"3B_RH":2.17,"HR_LH":1.22,"HR_RH":1.21},
    "Tigers":        {"1B_LH":0.98,"1B_RH":1.06,"2B_LH":0.83,"2B_RH":1.09,"3B_LH":1.69,"3B_RH":1.85,"HR_LH":0.88,"HR_RH":0.97},
    "Astros":        {"1B_LH":0.98,"1B_RH":1.01,"2B_LH":0.91,"2B_RH":0.87,"3B_LH":1.27,"3B_RH":0.61,"HR_LH":1.05,"HR_RH":1.10},
    "Royals":        {"1B_LH":1.15,"1B_RH":1.03,"2B_LH":1.22,"2B_RH":1.07,"3B_LH":1.17,"3B_RH":1.28,"HR_LH":0.76,"HR_RH":0.84},
    "Dodgers":       {"1B_LH":0.96,"1B_RH":0.99,"2B_LH":1.06,"2B_RH":0.92,"3B_LH":0.24,"3B_RH":0.50,"HR_LH":1.04,"HR_RH":1.21},
    "Marlins":       {"1B_LH":0.91,"1B_RH":1.09,"2B_LH":0.90,"2B_RH":1.04,"3B_LH":1.25,"3B_RH":0.99,"HR_LH":0.77,"HR_RH":0.72},
    "Brewers":       {"1B_LH":0.96,"1B_RH":0.96,"2B_LH":0.91,"2B_RH":0.92,"3B_LH":0.82,"3B_RH":0.92,"HR_LH":1.08,"HR_RH":1.14},
    "Twins":         {"1B_LH":1.03,"1B_RH":0.94,"2B_LH":1.03,"2B_RH":1.22,"3B_LH":1.40,"3B_RH":0.73,"HR_LH":0.89,"HR_RH":0.86},
    "Mets":          {"1B_LH":1.01,"1B_RH":0.86,"2B_LH":0.74,"2B_RH":0.88,"3B_LH":0.62,"3B_RH":0.70,"HR_LH":0.98,"HR_RH":1.07},
    "Yankees":       {"1B_LH":1.06,"1B_RH":1.05,"2B_LH":0.89,"2B_RH":0.85,"3B_LH":0.53,"3B_RH":1.36,"HR_LH":1.09,"HR_RH":1.02},
    "Athletics":     {"1B_LH":1.00,"1B_RH":1.00,"2B_LH":1.00,"2B_RH":1.00,"3B_LH":1.00,"3B_RH":1.00,"HR_LH":1.00,"HR_RH":1.00},
    "Phillies":      {"1B_LH":0.97,"1B_RH":0.98,"2B_LH":0.98,"2B_RH":0.88,"3B_LH":1.10,"3B_RH":0.99,"HR_LH":1.17,"HR_RH":1.22},
    "Pirates":       {"1B_LH":0.97,"1B_RH":0.95,"2B_LH":1.27,"2B_RH":1.10,"3B_LH":0.75,"3B_RH":0.83,"HR_LH":0.93,"HR_RH":0.79},
    "Padres":        {"1B_LH":0.95,"1B_RH":0.93,"2B_LH":1.07,"2B_RH":0.96,"3B_LH":0.76,"3B_RH":0.71,"HR_LH":0.92,"HR_RH":0.98},
    "Giants":        {"1B_LH":0.97,"1B_RH":1.05,"2B_LH":1.05,"2B_RH":0.94,"3B_LH":1.66,"3B_RH":1.19,"HR_LH":0.73,"HR_RH":0.79},
    "Mariners":      {"1B_LH":1.01,"1B_RH":0.95,"2B_LH":0.86,"2B_RH":0.83,"3B_LH":0.50,"3B_RH":0.75,"HR_LH":0.89,"HR_RH":1.04},
    "Cardinals":     {"1B_LH":1.01,"1B_RH":1.05,"2B_LH":0.89,"2B_RH":0.89,"3B_LH":0.75,"3B_RH":1.10,"HR_LH":0.92,"HR_RH":0.84},
    "Rays":          {"1B_LH":0.97,"1B_RH":0.96,"2B_LH":0.85,"2B_RH":1.01,"3B_LH":1.32,"3B_RH":1.22,"HR_LH":0.94,"HR_RH":0.86},
    "Rangers":       {"1B_LH":1.04,"1B_RH":1.00,"2B_LH":1.01,"2B_RH":0.96,"3B_LH":1.01,"3B_RH":0.98,"HR_LH":0.95,"HR_RH":0.96},
    "Blue Jays":     {"1B_LH":0.95,"1B_RH":0.92,"2B_LH":0.99,"2B_RH":1.02,"3B_LH":0.86,"3B_RH":1.03,"HR_LH":1.21,"HR_RH":1.12},
    "Nationals":     {"1B_LH":1.01,"1B_RH":1.00,"2B_LH":1.30,"2B_RH":1.04,"3B_LH":0.85,"3B_RH":0.83,"HR_LH":1.14,"HR_RH":1.09},
    "Braves":        {"1B_LH":0.99,"1B_RH":1.09,"2B_LH":1.04,"2B_RH":1.03,"3B_LH":0.69,"3B_RH":0.91,"HR_LH":0.90,"HR_RH":0.93},
}

# Team name → park key
TEAM_TO_PARK: dict[str, str] = {
    "Los Angeles Angels": "Angels",       "Arizona Diamondbacks": "Diamondbacks",
    "Baltimore Orioles":  "Orioles",      "Boston Red Sox":       "Red Sox",
    "Chicago Cubs":       "Cubs",         "Chicago White Sox":    "White Sox",
    "Cincinnati Reds":    "Reds",         "Cleveland Guardians":  "Guardians",
    "Colorado Rockies":   "Rockies",      "Detroit Tigers":       "Tigers",
    "Houston Astros":     "Astros",       "Kansas City Royals":   "Royals",
    "Los Angeles Dodgers":"Dodgers",      "Miami Marlins":        "Marlins",
    "Milwaukee Brewers":  "Brewers",      "Minnesota Twins":      "Twins",
    "New York Mets":      "Mets",         "New York Yankees":     "Yankees",
    "Oakland Athletics":  "Athletics",    "Sacramento Athletics": "Athletics",
    "Philadelphia Phillies":"Phillies",   "Pittsburgh Pirates":   "Pirates",
    "San Diego Padres":   "Padres",       "San Francisco Giants": "Giants",
    "Seattle Mariners":   "Mariners",     "St. Louis Cardinals":  "Cardinals",
    "Tampa Bay Rays":     "Rays",         "Texas Rangers":        "Rangers",
    "Toronto Blue Jays":  "Blue Jays",    "Washington Nationals": "Nationals",
    "Atlanta Braves":     "Braves",
}


# ---------------------------------------------------------------------------
# MLB Stats API helpers (free, no key)
# ---------------------------------------------------------------------------

def _mlb_get(path: str, params: dict | None = None) -> dict:
    try:
        resp = requests.get(
            f"{_MLBAPI}{path}", params=params or {},
            headers=_HEADERS, timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.debug("[NSFI] MLB API %s failed: %s", path, exc)
        return {}


def fetch_lineups_and_pitchers(date_str: str) -> list[dict]:
    """
    Fetch today's lineups and probable pitchers from MLB Stats API.
    Returns list of game dicts with home/away lineup + pitcher info.
    """
    data = _mlb_get("/schedule", {
        "sportId": 1, "date": date_str,
        "hydrate": "lineups,probablePitcher,team,venue",
        "gameType": "R",
    })

    # Collect all player IDs for batch handedness resolution
    all_ids: list[int] = []
    raw_games = []
    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            raw_games.append(g)
            for side in ("home", "away"):
                prob = g["teams"][side].get("probablePitcher")
                if prob:
                    all_ids.append(prob["id"])
                for p in g.get("lineups", {}).get(f"{side}Players", []):
                    all_ids.append(p["id"])

    # Batch resolve handedness (batSide, pitchHand) in chunks of 150
    hand: dict[int, dict] = {}
    for i in range(0, len(all_ids), 150):
        chunk = all_ids[i:i+150]
        resp = _mlb_get("/people", {"personIds": ",".join(str(x) for x in chunk)})
        for person in resp.get("people", []):
            hand[person["id"]] = {
                "batSide":    person.get("batSide", {}).get("code", "R"),
                "pitchHand":  person.get("pitchHand", {}).get("code", "R"),
                "fullName":   person.get("fullName", ""),
            }

    games = []
    for g in raw_games:
        home_name = g["teams"]["home"]["team"]["name"]
        away_name = g["teams"]["away"]["team"]["name"]
        lineups   = g.get("lineups", {})
        home_lp   = lineups.get("homePlayers", [])
        away_lp   = lineups.get("awayPlayers", [])

        def _pitcher(side: str) -> dict:
            prob = g["teams"][side].get("probablePitcher")
            if not prob:
                return {"name": "TBD", "id": None, "pitchHand": "R"}
            h = hand.get(prob["id"], {})
            return {
                "name":      prob.get("fullName", "TBD"),
                "id":        prob["id"],
                "pitchHand": h.get("pitchHand", "R"),
            }

        def _lineup(players: list) -> list[dict]:
            return [
                {
                    "name":    hand.get(p["id"], {}).get("fullName", p.get("fullName", "")),
                    "id":      p["id"],
                    "batSide": hand.get(p["id"], {}).get("batSide", "R"),
                }
                for p in players
            ]

        games.append({
            "game_id":        g.get("gamePk"),
            "home_team":      home_name,
            "away_team":      away_name,
            "ballpark":       TEAM_TO_PARK.get(home_name, ""),
            "lineup_complete": len(home_lp) == 9 and len(away_lp) == 9,
            "home_pitcher":   _pitcher("home"),
            "away_pitcher":   _pitcher("away"),
            "home_lineup":    _lineup(home_lp),
            "away_lineup":    _lineup(away_lp),
        })

    return games


def _get_pitcher_rates(pitcher_name: str, fg_data: dict) -> dict:
    """
    Fetch pitcher K/BB/hit rates from FanGraphs hub data.
    Falls back to MLB averages if not found.
    fg_data: hub["physics"]["fangraphs"] or similar dict keyed by player name.
    """
    avgs = MLB_AVGS
    if pitcher_name and fg_data:
        pname_lower = pitcher_name.lower()
        for key, stats in fg_data.items():
            if pname_lower in key.lower():
                k_pct = float(stats.get("k_pct", avgs["K_Rate"]) or avgs["K_Rate"])
                bb_pct = float(stats.get("bb_pct", avgs["BB_Rate"]) or avgs["BB_Rate"])
                return {
                    "Year_K%_LHH":   k_pct,  "Season_K%_LHH":   k_pct,
                    "Year_K%_RHH":   k_pct,  "Season_K%_RHH":   k_pct,
                    "Year_BB_Rate_LHH": bb_pct, "Season_BB_Rate_LHH": bb_pct,
                    "Year_BB_Rate_RHH": bb_pct, "Season_BB_Rate_RHH": bb_pct,
                    "Year_Opp_1B_LHH": avgs["1B_Rate"], "Season_Opp_1B_LHH": avgs["1B_Rate"],
                    "Year_Opp_1B_RHH": avgs["1B_Rate"], "Season_Opp_1B_RHH": avgs["1B_Rate"],
                    "Year_2B_Rate_LHH": avgs["2B_Rate"], "Season_2B_Rate_LHH": avgs["2B_Rate"],
                    "Year_2B_Rate_RHH": avgs["2B_Rate"], "Season_2B_Rate_RHH": avgs["2B_Rate"],
                    "Year_3B_Rate_LHH": avgs["3B_Rate"], "Season_3B_Rate_LHH": avgs["3B_Rate"],
                    "Year_3B_Rate_RHH": avgs["3B_Rate"], "Season_3B_Rate_RHH": avgs["3B_Rate"],
                    "Year_HR_Rate_LHH": avgs["HR_Rate"], "Season_HR_Rate_LHH": avgs["HR_Rate"],
                    "Year_HR_Rate_RHH": avgs["HR_Rate"], "Season_HR_Rate_RHH": avgs["HR_Rate"],
                }

    # League average fallback
    return {
        "Year_K%_LHH": avgs["K_Rate"],   "Season_K%_LHH": avgs["K_Rate"],
        "Year_K%_RHH": avgs["K_Rate"],   "Season_K%_RHH": avgs["K_Rate"],
        "Year_BB_Rate_LHH": avgs["BB_Rate"], "Season_BB_Rate_LHH": avgs["BB_Rate"],
        "Year_BB_Rate_RHH": avgs["BB_Rate"], "Season_BB_Rate_RHH": avgs["BB_Rate"],
        "Year_Opp_1B_LHH": avgs["1B_Rate"], "Season_Opp_1B_LHH": avgs["1B_Rate"],
        "Year_Opp_1B_RHH": avgs["1B_Rate"], "Season_Opp_1B_RHH": avgs["1B_Rate"],
        "Year_2B_Rate_LHH": avgs["2B_Rate"], "Season_2B_Rate_LHH": avgs["2B_Rate"],
        "Year_2B_Rate_RHH": avgs["2B_Rate"], "Season_2B_Rate_RHH": avgs["2B_Rate"],
        "Year_3B_Rate_LHH": avgs["3B_Rate"], "Season_3B_Rate_LHH": avgs["3B_Rate"],
        "Year_3B_Rate_RHH": avgs["3B_Rate"], "Season_3B_Rate_RHH": avgs["3B_Rate"],
        "Year_HR_Rate_LHH": avgs["HR_Rate"], "Season_HR_Rate_LHH": avgs["HR_Rate"],
        "Year_HR_Rate_RHH": avgs["HR_Rate"], "Season_HR_Rate_RHH": avgs["HR_Rate"],
    }


def _get_batter_rates(batter_name: str, batter_side: str, fg_data: dict) -> dict:
    """
    Fetch batter K/BB/hit rates from FanGraphs hub data.
    Falls back to MLB averages if not found.
    """
    avgs = MLB_AVGS
    if batter_name and fg_data:
        bname_lower = batter_name.lower()
        for key, stats in fg_data.items():
            if bname_lower in key.lower():
                k_pct  = float(stats.get("k_pct",  avgs["K_Rate"])  or avgs["K_Rate"])
                bb_pct = float(stats.get("bb_pct", avgs["BB_Rate"]) or avgs["BB_Rate"])
                return {
                    "Year_K%_LHP":   k_pct,  "Season_K%_LHP":   k_pct,
                    "Year_K%_RHP":   k_pct,  "Season_K%_RHP":   k_pct,
                    "Year_BB_Rate_LHP": bb_pct, "Season_BB_Rate_LHP": bb_pct,
                    "Year_BB_Rate_RHP": bb_pct, "Season_BB_Rate_RHP": bb_pct,
                    "Year_1B_Rate_LHP": avgs["1B_Rate"], "Season_1B_Rate_LHP": avgs["1B_Rate"],
                    "Year_1B_Rate_RHP": avgs["1B_Rate"], "Season_1B_Rate_RHP": avgs["1B_Rate"],
                    "Year_2B_Rate_LHP": avgs["2B_Rate"], "Season_2B_Rate_LHP": avgs["2B_Rate"],
                    "Year_2B_Rate_RHP": avgs["2B_Rate"], "Season_2B_Rate_RHP": avgs["2B_Rate"],
                    "Year_3B_Rate_LHP": avgs["3B_Rate"], "Season_3B_Rate_LHP": avgs["3B_Rate"],
                    "Year_3B_Rate_RHP": avgs["3B_Rate"], "Season_3B_Rate_RHP": avgs["3B_Rate"],
                    "Year_HR_Rate_LHP": avgs["HR_Rate"], "Season_HR_Rate_LHP": avgs["HR_Rate"],
                    "Year_HR_Rate_RHP": avgs["HR_Rate"], "Season_HR_Rate_RHP": avgs["HR_Rate"],
                }

    return {
        "Year_K%_LHP": avgs["K_Rate"],   "Season_K%_LHP": avgs["K_Rate"],
        "Year_K%_RHP": avgs["K_Rate"],   "Season_K%_RHP": avgs["K_Rate"],
        "Year_BB_Rate_LHP": avgs["BB_Rate"], "Season_BB_Rate_LHP": avgs["BB_Rate"],
        "Year_BB_Rate_RHP": avgs["BB_Rate"], "Season_BB_Rate_RHP": avgs["BB_Rate"],
        "Year_1B_Rate_LHP": avgs["1B_Rate"], "Season_1B_Rate_LHP": avgs["1B_Rate"],
        "Year_1B_Rate_RHP": avgs["1B_Rate"], "Season_1B_Rate_RHP": avgs["1B_Rate"],
        "Year_2B_Rate_LHP": avgs["2B_Rate"], "Season_2B_Rate_LHP": avgs["2B_Rate"],
        "Year_2B_Rate_RHP": avgs["2B_Rate"], "Season_2B_Rate_RHP": avgs["2B_Rate"],
        "Year_3B_Rate_LHP": avgs["3B_Rate"], "Season_3B_Rate_LHP": avgs["3B_Rate"],
        "Year_3B_Rate_RHP": avgs["3B_Rate"], "Season_3B_Rate_RHP": avgs["3B_Rate"],
        "Year_HR_Rate_LHP": avgs["HR_Rate"], "Season_HR_Rate_LHP": avgs["HR_Rate"],
        "Year_HR_Rate_RHP": avgs["HR_Rate"], "Season_HR_Rate_RHP": avgs["HR_Rate"],
    }


# ---------------------------------------------------------------------------
# Core: Monte Carlo half-inning simulation
# Adapted from mlb-nsfi-model/model.py simulate_half_inning()
# ---------------------------------------------------------------------------

def simulate_half_inning(
    pitcher_name: str,
    pitcher_hand: str,
    lineup: list[dict],       # [{"name": str, "batSide": "L"/"R"/"S"}]
    ballpark: str,
    batting_team: str,
    pitching_team: str,
    fg_data: dict | None = None,
    n_sims: int = 10_000,
) -> dict:
    """
    Run n Monte Carlo simulations of a single half-inning (1st inning).

    For each simulation, plate appearances are generated until 3 outs.
    Each PA outcome is drawn from the combined pitcher+batter rate,
    weighted 40% prior year / 60% current season, park-adjusted.

    Returns:
        {
          p_nsfi:    probability of NO strikeouts (the "No" NSFI bet),
          p_no_hits: probability of no hits (no-no through 1),
          p_under4:  probability that ≤ 3 batters come up (quick inning),
          n_sims:    simulations run,
        }

    Adapted from mlb-nsfi-model/model.py simulate_half_inning() by
    austinlmcconnell. Key changes:
      - Removed pandas dependency (pure numpy + dicts)
      - FanGraphs data loaded from PropIQ's existing fangraphs_layer.py
      - Park factor application matches original notebook logic exactly
    """
    if fg_data is None:
        fg_data = {}

    avgs    = MLB_AVGS
    avg_k   = avgs["K_Rate"]
    avg_bb  = avgs["BB_Rate"]
    avg_1b  = avgs["1B_Rate"]
    avg_2b  = avgs["2B_Rate"]
    avg_3b  = avgs["3B_Rate"]
    avg_hr  = avgs["HR_Rate"]

    bp_key = TEAM_TO_PARK.get(batting_team,  ballpark)
    tp_key = TEAM_TO_PARK.get(pitching_team, ballpark)
    pk_key = TEAM_TO_PARK.get(batting_team,  ballpark)  # ballpark = home team's park
    pf = PARK_FACTORS

    # Helper: weighted season + year stat
    def wsf(year_val: float, season_val: float, yw: float = 0.4, sw: float = 0.6) -> float:
        return year_val * yw + season_val * sw

    # Helper: Odds Ratio matchup combination (Log5 method)
    # More accurate than simple averaging — accounts for interaction between
    # high-K pitcher vs low-K batter (and vice versa) relative to league avg.
    # Formula: (p*b/L) / ((p*b/L) + ((1-p)*(1-b)/(1-L)))
    # Source: Bill James Log5 / PropMatchupEngine odds_ratio
    def csf(bv: float, pv: float, avg: float) -> float:
        bv = avg if (bv is None or bv != bv or bv <= 0) else bv   # nan/zero guard
        pv = avg if (pv is None or pv != pv or pv <= 0) else pv
        avg = max(avg, 1e-6)
        # Clamp to sane rate range to avoid division by zero at extremes
        bv  = min(max(bv,  0.001), 0.999)
        pv  = min(max(pv,  0.001), 0.999)
        avg = min(max(avg, 0.001), 0.999)
        num = (pv * bv) / avg
        den = num + ((1 - pv) * (1 - bv)) / (1 - avg)
        return num / den if den > 0 else avg

    # Pitcher stats
    pstats = _get_pitcher_rates(pitcher_name, fg_data)

    # Batter stats + precomputed per-batter PA probability tuples
    batter_stats: list[tuple] = []
    lineup_slice = lineup[:9] if len(lineup) >= 9 else lineup
    if len(lineup_slice) < 9:
        # Pad with league average batters if lineup is incomplete
        pad = [{"name": f"Avg_Batter_{i}", "batSide": "R"}
               for i in range(9 - len(lineup_slice))]
        lineup_slice = lineup_slice + pad

    for batter in lineup_slice:
        bh       = batter.get("batSide", "R")
        bname    = batter.get("name", "")
        bstats   = _get_batter_rates(bname, bh, fg_data)

        # Pitcher suffix (which platoon column to use for pitcher stats)
        if bh == "S":
            ps = "LHH" if pitcher_hand == "R" else "RHH"
        else:
            ps = "LHH" if bh == "L" else "RHH"

        # Batter suffix (which platoon column to use for batter stats)
        if bh == "S":
            bs = "RHP" if pitcher_hand == "L" else "LHP"
        else:
            bs = "LHP" if pitcher_hand == "L" else "RHP"

        p_k  = wsf(pstats.get(f"Year_K%_{ps}", avg_k),        pstats.get(f"Season_K%_{ps}", avg_k))
        p_bb = wsf(pstats.get(f"Year_BB_Rate_{ps}", avg_bb),   pstats.get(f"Season_BB_Rate_{ps}", avg_bb))
        p_1b = wsf(pstats.get(f"Year_Opp_1B_{ps}", avg_1b),    pstats.get(f"Season_Opp_1B_{ps}", avg_1b))
        p_2b = wsf(pstats.get(f"Year_2B_Rate_{ps}", avg_2b),   pstats.get(f"Season_2B_Rate_{ps}", avg_2b))
        p_3b = wsf(pstats.get(f"Year_3B_Rate_{ps}", avg_3b),   pstats.get(f"Season_3B_Rate_{ps}", avg_3b))
        p_hr = wsf(pstats.get(f"Year_HR_Rate_{ps}", avg_hr),   pstats.get(f"Season_HR_Rate_{ps}", avg_hr))

        b_k  = wsf(bstats.get(f"Year_K%_{bs}", avg_k),         bstats.get(f"Season_K%_{bs}", avg_k))
        b_bb = wsf(bstats.get(f"Year_BB_Rate_{bs}", avg_bb),    bstats.get(f"Season_BB_Rate_{bs}", avg_bb))
        b_1b = wsf(bstats.get(f"Year_1B_Rate_{bs}", avg_1b),    bstats.get(f"Season_1B_Rate_{bs}", avg_1b))
        b_2b = wsf(bstats.get(f"Year_2B_Rate_{bs}", avg_2b),    bstats.get(f"Season_2B_Rate_{bs}", avg_2b))
        b_3b = wsf(bstats.get(f"Year_3B_Rate_{bs}", avg_3b),    bstats.get(f"Season_3B_Rate_{bs}", avg_3b))
        b_hr = wsf(bstats.get(f"Year_HR_Rate_{bs}", avg_hr),    bstats.get(f"Season_HR_Rate_{bs}", avg_hr))

        # Park factor adjustments — matches mlb-nsfi-model logic exactly
        hand_key = "RH" if (bh == "R" or (bh == "S" and pitcher_hand == "L")) else "LH"
        if pk_key in pf:
            if pk_key == bp_key:         # home batter
                for ht, bv_ref, pv_ref in [
                    ("1B", "b_1b", "p_1b"), ("2B", "b_2b", "p_2b"),
                    ("3B", "b_3b", "p_3b"), ("HR", "b_hr", "p_hr"),
                ]:
                    factor = pf[pk_key].get(f"{ht}_{hand_key}", 1.0)
                    adj    = 0.5 * factor + 0.5
                    if ht == "1B":   b_1b *= adj
                    elif ht == "2B": b_2b *= adj
                    elif ht == "3B": b_3b *= adj
                    elif ht == "HR": b_hr *= adj
                if tp_key in pf:
                    for ht, bv_ref, pv_ref in [
                        ("1B", "b_1b", "p_1b"), ("2B", "b_2b", "p_2b"),
                        ("3B", "b_3b", "p_3b"), ("HR", "b_hr", "p_hr"),
                    ]:
                        bp_f = pf[pk_key].get(f"{ht}_{hand_key}", 1.0)
                        tp_f = pf[tp_key].get(f"{ht}_{hand_key}", 1.0)
                        adj  = 1.0 + (tp_f - bp_f) * 0.5
                        if ht == "1B":   p_1b *= adj
                        elif ht == "2B": p_2b *= adj
                        elif ht == "3B": p_3b *= adj
                        elif ht == "HR": p_hr *= adj
            else:                        # away batter
                if bp_key in pf:
                    for ht in ["1B", "2B", "3B", "HR"]:
                        bp_f = pf[pk_key].get(f"{ht}_{hand_key}", 1.0)
                        tb_f = pf[bp_key].get(f"{ht}_{hand_key}", 1.0)
                        adj  = 1.0 + (tb_f - bp_f) * 0.5
                        if ht == "1B":   b_1b *= adj
                        elif ht == "2B": b_2b *= adj
                        elif ht == "3B": b_3b *= adj
                        elif ht == "HR": b_hr *= adj
                for ht in ["1B", "2B", "3B", "HR"]:
                    factor = pf[pk_key].get(f"{ht}_{hand_key}", 1.0)
                    adj    = 0.5 * factor + 0.5
                    if ht == "1B":   p_1b *= adj
                    elif ht == "2B": p_2b *= adj
                    elif ht == "3B": p_3b *= adj
                    elif ht == "HR": p_hr *= adj

        ck  = csf(b_k,  p_k,  avg_k)
        cbb = csf(b_bb, p_bb, avg_bb)
        c1b = csf(b_1b, p_1b, avg_1b)
        c2b = csf(b_2b, p_2b, avg_2b)
        c3b = csf(b_3b, p_3b, avg_3b)
        chr_ = csf(b_hr, p_hr, avg_hr)

        # Normalise: K + BB + in_play = 1.0
        cip = max(0.0, 1.0 - ck - cbb)
        total = ck + cbb + cip
        if total > 0 and abs(total - 1.0) > 1e-6:
            ck  /= total
            cbb /= total
            cip = max(0.0, 1.0 - ck - cbb)

        if ck == 0 and cbb == 0 and cip == 0:
            ck = avg_k; cbb = avg_bb; cip = 1.0 - ck - cbb

        batter_stats.append((ck, cbb, cip, c1b, c2b, c3b, chr_))

    # ── Monte Carlo simulation ────────────────────────────────────────────
    rng    = np.random.default_rng()
    no_k   = 0
    no_h   = 0
    under4 = 0

    for _ in range(n_sims):
        outs = ks = hits = batters = 0
        bi   = 0

        while outs < 3:
            batters += 1
            ck, cbb, cip, c1b, c2b, c3b, chr_ = batter_stats[bi % 9]
            roll = rng.random()

            if roll < ck:
                ks   += 1
                outs += 1
            elif roll < ck + cbb:
                pass  # walk, no outs, no hits
            else:
                # Ball in play — determine hit type
                if cip > 0:
                    ip = (roll - ck - cbb) / cip
                    h1  = c1b / cip
                    h2  = (c1b + c2b) / cip
                    h3  = (c1b + c2b + c3b) / cip
                    hr_ = (c1b + c2b + c3b + chr_) / cip
                else:
                    ip = 1.0; h1 = h2 = h3 = hr_ = 0.0

                if ip < h1 or ip < h2 or ip < h3 or ip < hr_:
                    hits += 1
                else:
                    outs += 1  # field out

            bi += 1

        if ks    == 0: no_k   += 1
        if hits  == 0: no_h   += 1
        if batters <= 3: under4 += 1

    return {
        "p_nsfi":    round(no_k    / n_sims, 4),
        "p_no_hits": round(no_h    / n_sims, 4),
        "p_under4":  round(under4  / n_sims, 4),
        "n_sims":    n_sims,
    }


# ---------------------------------------------------------------------------
# Public API — fetch and simulate all games today
# ---------------------------------------------------------------------------

def fetch_nsfi_predictions_today(
    fg_data: dict | None = None,
    n_sims: int = 5_000,
    date_str: str | None = None,
) -> list[dict]:
    """
    Fetch today's MLB games and run NSFI Monte Carlo for every half-inning.

    Args:
        fg_data:  Optional FanGraphs player stats dict keyed by player name.
                  Pass hub["physics"].get("fangraphs", {}) from your DataHub.
                  Without it the model uses league average rates only.
        n_sims:   Simulations per half-inning (5,000 = fast, 10,000 = precise).
        date_str: Date in YYYY-MM-DD format. Defaults to today.

    Returns list of half-inning prediction dicts:
        [{
            game_id, home_team, away_team, ballpark,
            inning_half: "top" or "bot",
            batting_team, pitching_team,
            pitcher_name, pitcher_hand,
            lineup_complete: bool,
            p_nsfi:    float,   # P(no strikeout 1st inning) — target the "No" bet
            p_no_hits: float,   # P(no hits 1st inning)
            p_under4:  float,   # P(≤ 3 batters face pitcher)
            n_sims:    int,
        }]

    Call from DataHub physics group:
        from nsfi_layer import fetch_nsfi_predictions_today
        nsfi_results = fetch_nsfi_predictions_today(fg_data=hub.get("fangraphs",{}))
        hub["physics"]["nsfi"] = nsfi_results
    """
    import datetime
    if date_str is None:
        date_str = datetime.date.today().isoformat()

    try:
        games = fetch_lineups_and_pitchers(date_str)
    except Exception as exc:
        logger.warning("[NSFI] Failed to fetch lineups: %s", exc)
        return []

    if not games:
        logger.info("[NSFI] No games found for %s", date_str)
        return []

    logger.info("[NSFI] Simulating %d games × 2 half-innings × %d sims", len(games), n_sims)
    results = []

    for game in games:
        home    = game["home_team"]
        away    = game["away_team"]
        park    = game["ballpark"]
        g_id    = game["game_id"]
        lc      = game["lineup_complete"]

        for inning_half, batting, pitching, pitcher, lineup in [
            ("top", away, home, game["home_pitcher"], game["away_lineup"]),
            ("bot", home, away, game["away_pitcher"], game["home_lineup"]),
        ]:
            try:
                sim = simulate_half_inning(
                    pitcher_name=pitcher["name"],
                    pitcher_hand=pitcher["pitchHand"],
                    lineup=lineup,
                    ballpark=park,
                    batting_team=batting,
                    pitching_team=pitching,
                    fg_data=fg_data or {},
                    n_sims=n_sims,
                )
            except Exception as exc:
                logger.warning("[NSFI] Sim failed %s %s %s: %s",
                               g_id, inning_half, pitcher["name"], exc)
                continue

            results.append({
                "game_id":        g_id,
                "home_team":      home,
                "away_team":      away,
                "ballpark":       park,
                "inning_half":    inning_half,
                "batting_team":   batting,
                "pitching_team":  pitching,
                "pitcher_name":   pitcher["name"],
                "pitcher_hand":   pitcher["pitchHand"],
                "lineup_complete": lc,
                **sim,
            })

    logger.info(
        "[NSFI] Complete: %d half-innings. Avg P(NSFI)=%.3f",
        len(results),
        sum(r["p_nsfi"] for r in results) / max(len(results), 1),
    )
    return results


if __name__ == "__main__":
    import datetime
    print(f"NSFI Layer smoke test — {datetime.date.today()}")
    print("Running with league-average stats (no FanGraphs data)...")

    # Quick test: single half-inning, 1000 sims
    result = simulate_half_inning(
        pitcher_name="Test Pitcher",
        pitcher_hand="R",
        lineup=[
            {"name": f"Batter {i}", "batSide": "R" if i % 2 == 0 else "L"}
            for i in range(9)
        ],
        ballpark="Yankees",
        batting_team="Boston Red Sox",
        pitching_team="New York Yankees",
        fg_data={},
        n_sims=1000,
    )
    print(f"\nSingle half-inning (1000 sims, Yankees park, RHP vs avg lineup):")
    print(f"  P(NSFI - No strikeout): {result['p_nsfi']:.3f}")
    print(f"  P(No hits):             {result['p_no_hits']:.3f}")
    print(f"  P(≤3 batters):          {result['p_under4']:.3f}")
    print(f"\nLeague avg K rate: {MLB_AVGS['K_Rate']:.3f}")
    print(f"Expected P(NSFI) ≈ (1 - {MLB_AVGS['K_Rate']:.3f})^3 ≈ {(1 - MLB_AVGS['K_Rate'])**3:.3f}")
