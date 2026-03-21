"""
Agent 7: Grading Agent
-----------------------
Strategy: Post-game settlement. Fetches boxscore results from MLB Stats API
and SportsData.io, grades all pending bets, updates agent stats + calibration.
"""
from __future__ import annotations
import logging
import os
import requests
from datetime import date
from .base_agent import BaseAgent, BetSlip

logger = logging.getLogger("propiq.agent.grading")

SPORTSDATA_KEY = os.getenv("SPORTSDATA_API_KEY", "")
MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
SPORTSDATA_BASE = "https://api.sportsdata.io/v3/mlb"


class GradingAgent(BaseAgent):
    name = "grading"
    strategy = "Boxscore Settlement"
    max_legs = 0   # Grading agent doesn't place bets
    min_legs = 0
    ev_threshold = 0.0

    def analyze(self, hub_data: dict) -> list[BetSlip]:
        """Grading agent doesn't generate bets — it settles them."""
        return []

    def grade_all_pending(self, game_date: str | None = None) -> dict:
        """
        Fetch results for game_date (YYYY-MM-DD) and settle all pending bets.
        Returns settlement summary.
        """
        target_date = game_date or date.today().isoformat()
        logger.info(f"[grading] Grading pending bets for {target_date}")

        # Fetch all pending bets
        pending = self.db.get_pending_bets()
        if not pending:
            logger.info("[grading] No pending bets to grade.")
            return {"graded": 0, "wins": 0, "losses": 0, "pushes": 0}

        # Fetch actual results
        results = self._fetch_boxscores(target_date)
        if not results:
            logger.warning(f"[grading] No boxscores found for {target_date}")
            return {"graded": 0, "wins": 0, "losses": 0, "pushes": 0}

        summary = {"graded": 0, "wins": 0, "losses": 0, "pushes": 0, "errors": 0}

        for bet in pending:
            try:
                # Parse legs from JSON
                import json
                legs_raw = json.loads(bet.get("legs_json", "[]"))
                all_legs_settled = True
                bet_outcome = "win"

                for leg_data in legs_raw:
                    player = leg_data.get("player", "")
                    prop_type = leg_data.get("prop_type", "")
                    line = float(leg_data.get("line", 0))
                    direction = leg_data.get("direction", "over")

                    # Look up actual result
                    actual = results.get(f"{player}|{prop_type}")
                    if actual is None:
                        all_legs_settled = False
                        break

                    # Grade the leg
                    if direction == "over":
                        if actual > line:
                            leg_outcome = "win"
                        elif actual == line:
                            leg_outcome = "push"
                        else:
                            leg_outcome = "loss"
                    else:  # under
                        if actual < line:
                            leg_outcome = "win"
                        elif actual == line:
                            leg_outcome = "push"
                        else:
                            leg_outcome = "loss"

                    if leg_outcome == "loss":
                        bet_outcome = "loss"
                        break
                    elif leg_outcome == "push" and bet_outcome != "loss":
                        bet_outcome = "push"

                if not all_legs_settled:
                    continue  # Skip — results not available yet

                # Calculate profit
                combined_odds = float(bet.get("combined_odds", 1.0))
                stake = float(bet.get("stake_units", 1.0))
                if bet_outcome == "win":
                    profit = round(stake * (combined_odds - 1), 4)
                    summary["wins"] += 1
                elif bet_outcome == "push":
                    profit = 0.0
                    summary["pushes"] += 1
                else:
                    profit = -stake
                    summary["losses"] += 1

                self.db.settle_bet(bet["bet_id"], bet_outcome, profit)
                summary["graded"] += 1

            except Exception as e:
                logger.error(f"[grading] Error grading bet {bet.get('bet_id')}: {e}")
                summary["errors"] += 1

        # Update all agent stats after grading
        agent_names = set(b["agent_name"] for b in pending if b["agent_name"])
        for agent_name in agent_names:
            self.db.update_agent_stats(agent_name)

        logger.info(
            f"[grading] Settled {summary['graded']} bets — "
            f"W:{summary['wins']} L:{summary['losses']} P:{summary['pushes']}"
        )
        return summary

    def _fetch_boxscores(self, game_date: str) -> dict:
        """Returns dict of {player|prop_type: actual_value}."""
        results = {}

        # --- MLB Stats API ---
        try:
            resp = requests.get(
                f"{MLB_STATS_BASE}/schedule",
                params={"sportId": 1, "date": game_date, "hydrate": "boxscore"},
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                for date_entry in data.get("dates", []):
                    for game in date_entry.get("games", []):
                        self._parse_mlb_boxscore(game, results)
        except Exception as e:
            logger.warning(f"[grading] MLB Stats API error: {e}")

        # --- SportsData.io fallback ---
        if not results:
            try:
                resp = requests.get(
                    f"{SPORTSDATA_BASE}/stats/json/PlayerGameStatsByDate/{game_date}",
                    headers={"Ocp-Apim-Subscription-Key": SPORTSDATA_KEY},
                    timeout=15
                )
                if resp.status_code == 200:
                    for stat in resp.json():
                        player = stat.get("Name", "")
                        if not player:
                            continue
                        results[f"{player}|pitcher_strikeouts"] = stat.get("PitchingStrikeouts", 0)
                        results[f"{player}|batter_strikeouts"] = stat.get("Strikeouts", 0)
                        results[f"{player}|batter_hits"] = stat.get("Hits", 0)
                        results[f"{player}|batter_home_runs"] = stat.get("HomeRuns", 0)
                        results[f"{player}|batter_total_bases"] = stat.get("TotalBases", 0)
                        results[f"{player}|batter_runs_batted_in"] = stat.get("RunsBattedIn", 0)
            except Exception as e:
                logger.warning(f"[grading] SportsData.io boxscore error: {e}")

        logger.info(f"[grading] Loaded {len(results)} stat lines from boxscores")
        return results

    def _parse_mlb_boxscore(self, game: dict, results: dict):
        """Parse MLB Stats API boxscore into results dict."""
        try:
            boxscore = game.get("boxscore", {})
            for side in ("home", "away"):
                team_data = boxscore.get("teams", {}).get(side, {})
                players = team_data.get("players", {})
                for _, player_data in players.items():
                    name = player_data.get("person", {}).get("fullName", "")
                    if not name:
                        continue
                    stats = player_data.get("stats", {})
                    pitching = stats.get("pitching", {})
                    batting = stats.get("batting", {})
                    if pitching:
                        results[f"{name}|pitcher_strikeouts"] = int(pitching.get("strikeOuts", 0))
                    if batting:
                        results[f"{name}|batter_hits"] = int(batting.get("hits", 0))
                        results[f"{name}|batter_home_runs"] = int(batting.get("homeRuns", 0))
                        results[f"{name}|batter_total_bases"] = int(batting.get("totalBases", 0))
                        results[f"{name}|batter_strikeouts"] = int(batting.get("strikeOuts", 0))
                        results[f"{name}|batter_runs_batted_in"] = int(batting.get("rbi", 0))
        except Exception as e:
            logger.debug(f"[grading] Boxscore parse error: {e}")
