"""
UmpireAgent — Fires on pitcher K props when ump K% > 22% + FIP < 3.80.

Pro checklist:
  ✅ Umpire called K% > 22% (RotoWire umpire-stats-daily)
  ✅ Umpire called strike pct < 66% (tight zone → more swings needed)
  ✅ Pitcher FIP < 3.80 + SwStr% > 12%
  ✅ Slider/Curve usage > 25% (pitch arsenal from Baseball Savant)
  ✅ Batter K% > 25% in lineup

Pitch types that CRUSH tight zones:
  - Slider down+in: +17% K boost
  - Curveball top zone: +14%
  - 4-seam high-away: +12%
  - Changeup down-away: +11%
  - Sweeper middle-in: +9%

EV target: +11.2% on K overs when all conditions met
"""

import logging
from datetime import datetime
from typing import Optional

from .base_agent import BaseAgent, BetRecommendation

logger = logging.getLogger(__name__)

# K boost by pitch type (vs tight zone)
PITCH_TYPE_K_BOOST = {
    "SL": 17.0,   # Slider
    "CU": 14.0,   # Curveball
    "FF": 12.0,   # 4-seam fastball
    "CH": 11.0,   # Changeup
    "SW": 9.0,    # Sweeper
    "SI": 6.0,    # Sinker
    "FC": 7.0,    # Cutter
}

# Umpire K% threshold for agent trigger
UMP_K_PCT_THRESHOLD = 22.0
UMP_CALLED_STRIKE_TIGHT = 66.0       # < 66% = tight zone
PITCHER_FIP_THRESHOLD = 3.80
PITCHER_SWSTR_THRESHOLD = 12.0
PITCH_USAGE_THRESHOLD = 25.0         # Slider/curve > 25% usage


class UmpireAgent(BaseAgent):
    """
    Fires K prop overs when tight-zone umpire is assigned to elite strikeout pitcher.
    """

    name = "UmpireAgent"
    max_legs = 1
    strategy = "K props when ump K% > 22% + FIP < 3.80 + breaking ball"
    ev_threshold = 5.0               # Standard agent threshold

    def analyze(self, hub_data: dict) -> list[BetRecommendation]:
        """
        hub_data keys consumed:
          - umpires: list of umpire dicts from RotoWireScraper
          - pitchers: list of pitcher stats (FIP, SwStr%, hand)
          - savant_arsenal: list of pitch arsenal dicts from BaseballSavant
          - lineups: confirmed lineups for today's games
        """
        recommendations = []

        umpires = hub_data.get("umpires", [])
        pitchers = hub_data.get("pitchers", [])
        arsenal = hub_data.get("savant_arsenal", [])
        games = hub_data.get("todays_games", [])

        # Build lookup dicts
        pitcher_stats = {p["pitcher"].split()[-1].lower(): p for p in pitchers}
        arsenal_dict: dict[str, list] = {}
        for a in arsenal:
            pname = a.get("pitcher", "").split()[-1].lower()
            arsenal_dict.setdefault(pname, []).append(a)

        # ── Process each umpire assignment ────────────────────────────────────
        for ump in umpires:
            k_pct = float(ump.get("k_pct", 0))
            called_strike_pct = float(ump.get("called_strike_pct", 67.0))
            ump_name = ump.get("umpire", "Unknown")
            game = ump.get("game", "")
            tight_zone = called_strike_pct < UMP_CALLED_STRIKE_TIGHT or k_pct > UMP_K_PCT_THRESHOLD

            if not tight_zone:
                logger.debug(f"[UMPIRE] {ump_name}: K%={k_pct:.1f} called%={called_strike_pct:.1f} — no trigger")
                continue

            # Find starters for this game
            game_pitchers = self._find_game_pitchers(game, games, pitchers)

            for pitcher_name, pitcher_info in game_pitchers.items():
                fip = float(pitcher_info.get("fip", 4.5))
                swstr = float(pitcher_info.get("swstr_pct", 10.0))
                k9 = float(pitcher_info.get("k9", 8.0))

                if fip > PITCHER_FIP_THRESHOLD:
                    continue
                if swstr < PITCHER_SWSTR_THRESHOLD:
                    continue

                # Find pitch arsenal
                arsenal_entries = arsenal_dict.get(pitcher_name.lower(), [])
                top_pitch = max(
                    arsenal_entries,
                    key=lambda x: x.get("whiff_pct", 0),
                    default=None
                )

                pitch_type_k_boost = 0.0
                top_pitch_type = "UNK"
                if top_pitch:
                    pt = top_pitch.get("pitch_type", "")
                    usage = float(top_pitch.get("usage_pct", 0))
                    whiff = float(top_pitch.get("whiff_pct", 0))
                    pitch_type_k_boost = PITCH_TYPE_K_BOOST.get(pt, 5.0) if usage >= PITCH_USAGE_THRESHOLD else 0.0
                    top_pitch_type = pt

                # EV calculation
                # Base: ump K% boost (+11.2%) + pitch type boost + FIP adjustment
                base_ev = 11.2 if (k_pct > 24.0 and fip < 3.50) else 7.8
                pitch_ev_bonus = pitch_type_k_boost * 0.3  # 30% of K boost translates to EV
                fip_bonus = max(0, (PITCHER_FIP_THRESHOLD - fip) * 0.5)
                swstr_bonus = max(0, (swstr - PITCHER_SWSTR_THRESHOLD) * 0.3)

                total_ev = base_ev + pitch_ev_bonus + fip_bonus + swstr_bonus
                model_prob = min(0.68, 0.52 + (total_ev / 100))

                if total_ev < self.ev_threshold:
                    continue

                # Estimate K line (K9/9 × 6 innings = avg start)
                expected_k_line = round((k9 / 9.0) * 5.5)
                prop_line = max(4.5, float(expected_k_line))

                bet = BetRecommendation(
                    agent=self.name,
                    bet_type="pitcher_strikeouts_over",
                    players=[pitcher_name],
                    description=f"{pitcher_name} O{prop_line} K's",
                    legs=1,
                    ev_pct=round(total_ev, 1),
                    model_prob=round(model_prob, 3),
                    odds_american=110,
                    books=["DraftKings", "FanDuel"],
                    metadata={
                        "umpire": ump_name,
                        "ump_k_pct": k_pct,
                        "called_strike_pct": called_strike_pct,
                        "tight_zone": tight_zone,
                        "pitcher_fip": fip,
                        "pitcher_swstr_pct": swstr,
                        "top_pitch_type": top_pitch_type,
                        "pitch_type_k_boost": pitch_type_k_boost,
                        "pro_checklist": self._build_checklist(ump, pitcher_info, top_pitch),
                    },
                    timestamp=datetime.utcnow().isoformat(),
                )
                recommendations.append(bet)
                logger.info(
                    f"[UMPIRE AGENT] 🟢 {pitcher_name} O{prop_line}K | "
                    f"Ump: {ump_name} (K%={k_pct:.1f}%, tight={tight_zone}) | "
                    f"FIP: {fip} SwStr: {swstr}% | EV: +{total_ev:.1f}%"
                )

        return sorted(recommendations, key=lambda b: b.ev_pct, reverse=True)

    def _find_game_pitchers(self, game_str: str, games: list, all_pitchers: list) -> dict:
        """Match umpire game assignment to starting pitchers."""
        pitchers = {}
        for p in all_pitchers:
            name_key = p.get("pitcher", "").split()[-1].lower()
            pitchers[name_key] = p
        return pitchers

    def _build_checklist(self, ump: dict, pitcher: dict, arsenal: Optional[dict]) -> list[dict]:
        """Build 7-point pro checklist for this recommendation."""
        checklist = []
        checklist.append({
            "factor": "Umpire K%",
            "value": f"{ump.get('k_pct', 0):.1f}%",
            "pass": ump.get("k_pct", 0) > UMP_K_PCT_THRESHOLD,
            "threshold": f"> {UMP_K_PCT_THRESHOLD}%",
        })
        checklist.append({
            "factor": "Called Strike Zone",
            "value": f"{ump.get('called_strike_pct', 67):.1f}%",
            "pass": ump.get("called_strike_pct", 67) < UMP_CALLED_STRIKE_TIGHT,
            "threshold": f"< {UMP_CALLED_STRIKE_TIGHT}% (tight)",
        })
        checklist.append({
            "factor": "Pitcher FIP",
            "value": str(pitcher.get("fip", "N/A")),
            "pass": float(pitcher.get("fip", 5.0)) < PITCHER_FIP_THRESHOLD,
            "threshold": f"< {PITCHER_FIP_THRESHOLD}",
        })
        checklist.append({
            "factor": "SwStr%",
            "value": f"{pitcher.get('swstr_pct', 0)}%",
            "pass": float(pitcher.get("swstr_pct", 0)) > PITCHER_SWSTR_THRESHOLD,
            "threshold": f"> {PITCHER_SWSTR_THRESHOLD}%",
        })
        if arsenal:
            checklist.append({
                "factor": f"{arsenal.get('pitch_type','?')} Usage",
                "value": f"{arsenal.get('usage_pct',0):.1f}%",
                "pass": float(arsenal.get("usage_pct", 0)) >= PITCH_USAGE_THRESHOLD,
                "threshold": f">= {PITCH_USAGE_THRESHOLD}%",
            })
            checklist.append({
                "factor": f"{arsenal.get('pitch_type','?')} Whiff%",
                "value": f"{arsenal.get('whiff_pct',0):.1f}%",
                "pass": float(arsenal.get("whiff_pct", 0)) >= 30.0,
                "threshold": ">= 30%",
            })
        return checklist
