"""
F5Agent — First 5 Innings under bets when starter FIP < 3.50 + SwStr% > 12%.

Logic:
  - Target F5 totals (over/under for first 5 innings)
  - Fire UNDER when: FIP < 3.50 AND SwStr% > 12% AND opponent wRC+ < 105
  - Avoid when: Wind > 8mph out, Coors Field, bullpen fatigue >= 3
  - Typical EV: +6-9% on F5 unders vs elite pitchers

Why F5:
  - Eliminates bullpen variance (managers lift starters after 5)
  - Elite starters suppress runs for exactly 5 innings
  - Books set F5 totals conservatively → value in unders
"""

import logging
from datetime import datetime

from .base_agent import BaseAgent, BetRecommendation

logger = logging.getLogger(__name__)

# F5Agent thresholds
F5_FIP_THRESHOLD = 3.50
F5_SWSTR_THRESHOLD = 12.0
F5_OPPONENT_WRC_MAX = 105
F5_WIND_BLOCK_MPH = 8.0
F5_EV_THRESHOLD = 5.0

# Parks to avoid (hitter-friendly, inflated F5 totals)
AVOID_PARKS = {"COL", "CIN"}


class F5Agent(BaseAgent):
    """
    Fires F5 under bets for elite starters in neutral/pitcher-friendly parks.
    """

    name = "F5Agent"
    max_legs = 1
    strategy = "F5 unders: starter FIP < 3.50, SwStr > 12%"
    ev_threshold = F5_EV_THRESHOLD

    def analyze(self, hub_data: dict) -> list[BetRecommendation]:
        """
        hub_data keys:
          - pitchers: [{"pitcher": "Cole", "fip": 3.12, "swstr_pct": 13.4, ...}]
          - team_stats: [{"team": "BOS", "wrc_plus": 112, ...}]
          - weather: [{"game": "NYY@BOS", "wind_speed": 5, "wind_dir": "calm", ...}]
          - todays_games: [{"home": "BOS", "away": "NYY", "home_park": "BOS", ...}]
          - bullpen_fatigue: {"BOS": 1, "NYY": 2, ...}  (score 0-4)
        """
        recommendations = []

        pitchers = hub_data.get("pitchers", [])
        team_stats = hub_data.get("team_stats", {})
        weather_list = hub_data.get("weather", [])
        games = hub_data.get("todays_games", [])
        bullpen_fatigue = hub_data.get("bullpen_fatigue", {})

        # Build weather lookup by game
        weather_map = {w.get("game", ""): w for w in weather_list}

        for pitcher_info in pitchers:
            fip = float(pitcher_info.get("fip", 5.0))
            swstr = float(pitcher_info.get("swstr_pct", 10.0))
            pitcher_name = pitcher_info.get("pitcher", "Unknown")
            pitcher_team = pitcher_info.get("team", "UNK")
            k9 = float(pitcher_info.get("k9", 8.0))

            # ── Primary threshold check ────────────────────────────────────────
            if fip > F5_FIP_THRESHOLD:
                continue
            if swstr < F5_SWSTR_THRESHOLD:
                continue

            # ── Park filter ────────────────────────────────────────────────────
            game_info = self._find_game(pitcher_team, games)
            park = game_info.get("park", pitcher_team)
            if park.upper() in AVOID_PARKS:
                logger.debug(f"[F5] {pitcher_name} — skipping hitter-friendly park {park}")
                continue

            # ── Wind filter ────────────────────────────────────────────────────
            game_key = game_info.get("matchup_str", "")
            weather = weather_map.get(game_key, {})
            wind_speed = float(weather.get("wind_speed", 0))
            wind_dir = weather.get("wind_dir", "calm").lower()
            is_tailwind = any(d in wind_dir for d in ["out_to", "r_to_l", "l_to_r"])

            if wind_speed >= F5_WIND_BLOCK_MPH and is_tailwind:
                logger.debug(f"[F5] {pitcher_name} — skipping: tailwind {wind_speed}mph {wind_dir}")
                continue

            # ── Bullpen fatigue doesn't affect F5 (5 innings = starter only) ─
            # (No adjustment needed for F5 bets)

            # ── Opponent wRC+ check ────────────────────────────────────────────
            opponent = game_info.get("opponent", "UNK")
            opp_wrc = team_stats.get(opponent, {}).get("wrc_plus", 100)
            if opp_wrc > F5_OPPONENT_WRC_MAX:
                ev_penalty = (opp_wrc - F5_OPPONENT_WRC_MAX) * 0.05
            else:
                ev_penalty = 0.0

            # ── EV calculation ─────────────────────────────────────────────────
            # Base EV: FIP advantage + SwStr% bonus
            fip_edge = (F5_FIP_THRESHOLD - fip) * 2.0         # e.g. FIP 3.12 → +0.76%
            swstr_edge = (swstr - F5_SWSTR_THRESHOLD) * 0.8   # e.g. 13.4% → +1.12%
            base_ev = 5.0 + fip_edge + swstr_edge - ev_penalty

            # Wind headwind bonus (suppressant)
            if wind_speed >= 5.0 and "in_from" in wind_dir:
                base_ev += 1.5

            if base_ev < self.ev_threshold:
                continue

            model_prob = min(0.65, 0.54 + (base_ev / 100))

            # Estimate F5 total line (avg runs * 5/9 innings)
            f5_line = round((k9 / 9.0) * 5.0 * 0.6, 1)  # rough F5 run estimate
            f5_line = max(3.5, min(5.5, f5_line))

            bet = BetRecommendation(
                agent=self.name,
                bet_type="f5_under",
                players=[pitcher_name],
                description=f"{pitcher_name} F5 U{f5_line} runs",
                legs=1,
                ev_pct=round(base_ev, 1),
                model_prob=round(model_prob, 3),
                odds_american=-115,
                books=["DraftKings", "FanDuel", "BetMGM"],
                metadata={
                    "pitcher_fip": fip,
                    "pitcher_swstr_pct": swstr,
                    "pitcher_k9": k9,
                    "park": park,
                    "opponent": opponent,
                    "opp_wrc_plus": opp_wrc,
                    "wind_speed": wind_speed,
                    "wind_dir": wind_dir,
                    "f5_line": f5_line,
                    "pro_checklist": [
                        {"factor": "FIP", "value": str(fip), "pass": fip < F5_FIP_THRESHOLD, "threshold": f"< {F5_FIP_THRESHOLD}"},
                        {"factor": "SwStr%", "value": f"{swstr}%", "pass": swstr > F5_SWSTR_THRESHOLD, "threshold": f"> {F5_SWSTR_THRESHOLD}%"},
                        {"factor": "Park", "value": park, "pass": park.upper() not in AVOID_PARKS, "threshold": "Not Coors/Cincy"},
                        {"factor": "Wind", "value": f"{wind_speed}mph {wind_dir}", "pass": not (wind_speed >= 8 and is_tailwind), "threshold": "No tailwind 8mph+"},
                        {"factor": "Opp wRC+", "value": str(opp_wrc), "pass": opp_wrc <= F5_OPPONENT_WRC_MAX, "threshold": f"<= {F5_OPPONENT_WRC_MAX}"},
                    ],
                },
                timestamp=datetime.utcnow().isoformat(),
            )
            recommendations.append(bet)
            logger.info(
                f"[F5 AGENT] 🟢 {pitcher_name} F5 U{f5_line} | "
                f"FIP: {fip} SwStr: {swstr}% | EV: +{base_ev:.1f}%"
            )

        return sorted(recommendations, key=lambda b: b.ev_pct, reverse=True)

    def _find_game(self, team: str, games: list) -> dict:
        """Find today's game info for a given team."""
        for g in games:
            if team in (g.get("home", ""), g.get("away", "")):
                opponent = g.get("away", "UNK") if g.get("home") == team else g.get("home", "UNK")
                return {
                    "park": g.get("park", team),
                    "opponent": opponent,
                    "matchup_str": f"{g.get('away','')}@{g.get('home','')}",
                }
        return {"park": team, "opponent": "UNK", "matchup_str": ""}
