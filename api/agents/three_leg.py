"""
Agent 3: 3-Leg Correlated Props
---------------------------------
Strategy: EXACTLY 3 legs — correlated player props from the same game.
Edge: 8-12% via same-game correlation (e.g., lineup stacks, pitcher strikeout + batter K props).
"""
from __future__ import annotations
import itertools
import logging
from .base_agent import BaseAgent, BetSlip, Leg

logger = logging.getLogger("propiq.agent.three_leg")

CORRELATION_MAP = {
    # Positive correlations (stack)
    ("pitcher_strikeouts", "batter_strikeouts"): 0.65,
    ("pitcher_strikeouts", "batter_total_bases"): -0.30,
    ("batter_hits", "batter_runs_batted_in"): 0.55,
    ("batter_hits", "batter_total_bases"): 0.70,
    ("batter_home_runs", "batter_runs_batted_in"): 0.80,
    ("batter_home_runs", "batter_total_bases"): 0.85,
}


def _correlation_bonus(legs: list[Leg]) -> float:
    """Estimate correlation adjustment for a set of legs from the same game."""
    if len(legs) != 3:
        return 1.0
    bonus = 1.0
    for a, b in itertools.combinations(legs, 2):
        key = tuple(sorted([a.prop_type, b.prop_type]))
        corr = CORRELATION_MAP.get(key, 0.0)
        bonus += abs(corr) * 0.03
    return round(bonus, 4)


class ThreeLeg(BaseAgent):
    name = "three_leg"
    strategy = "Correlated 3-Leg"
    max_legs = 3
    min_legs = 3
    ev_threshold = 0.08

    def analyze(self, hub_data: dict) -> list[BetSlip]:
        props: list[dict] = hub_data.get("player_props", [])
        predictions: dict = hub_data.get("model_predictions", {})

        by_game: dict[str, list[dict]] = {}
        for prop in props:
            gid = prop.get("game_id", "unknown")
            by_game.setdefault(gid, []).append(prop)

        slips: list[BetSlip] = []

        for game_id, game_props in by_game.items():
            candidate_legs: list[Leg] = []
            for prop in game_props:
                for direction in ("over", "under"):
                    american = prop.get(f"{direction}_odds")
                    if not american:
                        continue
                    decimal = self.american_to_decimal(int(american))
                    if decimal < 1.50:
                        continue

                    book_prob = self.decimal_to_prob(decimal)
                    line = prop.get("line", 0.0)
                    player = prop.get("player_name", "")
                    prop_type = prop.get("prop_type", "")
                    book = prop.get("bookmaker", "draftkings")

                    key = f"{player}|{prop_type}|{line}|{direction}"
                    model_prob = predictions.get(key, {}).get("calibrated_prob")
                    if model_prob is None or model_prob < 0.55:
                        continue

                    candidate_legs.append(Leg(
                        player=player, prop_type=prop_type, line=line,
                        direction=direction, book=book,
                        american_odds=int(american), decimal_odds=decimal,
                        book_prob=book_prob, model_prob=model_prob,
                        edge=round(model_prob - book_prob, 4),
                    ))

            if len(candidate_legs) < 3:
                continue

            candidate_legs.sort(key=lambda x: x.model_prob, reverse=True)
            top = candidate_legs[:6]

            for combo in itertools.combinations(top, 3):
                legs = list(combo)
                seen = set()
                valid = True
                for leg in legs:
                    key = (leg.player, leg.prop_type, leg.direction)
                    if key in seen:
                        valid = False
                        break
                    seen.add(key)
                if not valid:
                    continue

                combined_dec = self.parlay_odds(legs)
                combined_prob = (
                    legs[0].model_prob *
                    legs[1].model_prob *
                    legs[2].model_prob *
                    _correlation_bonus(legs)
                )
                ev = self.calculate_ev(combined_prob, combined_dec)

                if ev >= self.ev_threshold:
                    slips.append(BetSlip(
                        agent_name=self.name,
                        strategy="Correlated 3-Leg",
                        legs=legs,
                        stake_units=0.5,
                        combined_odds=combined_dec,
                        expected_value=ev,
                        confidence=combined_prob,
                        metadata={
                            "game_id": game_id,
                            "correlation_bonus": _correlation_bonus(legs),
                            "source": "three_leg_correlated",
                        },
                    ))

        slips.sort(key=lambda x: x.expected_value, reverse=True)
        top_slips = slips[:5]
        logger.info(f"[three_leg] {len(slips)} candidate combos → {len(top_slips)} slips filed")
        return top_slips
