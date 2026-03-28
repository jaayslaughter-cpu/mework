"""
Agent 4: Parlay Agent
---------------------
Strategy: 2–4 leg parlays on game outcomes (moneylines + run totals).
ROI target: 2–3% via game model edge + line shopping.
Books: CA offshore (DK, FD, BetMGM, bet365).
"""
from __future__ import annotations
import itertools
import logging
from .base_agent import BaseAgent, BetSlip, Leg

logger = logging.getLogger("propiq.agent.parlay")

BOOKS_PRIORITY = ["draftkings", "fanduel", "betmgm", "bet365"]
MAX_AMERICAN_ODDS_PER_LEG = 250   # Cap any single leg at +250


class ParlayAgent(BaseAgent):
    name = "parlay"
    strategy = "Game Outcome Parlay"
    max_legs = 4
    min_legs = 2
    ev_threshold = 0.02   # 2% — parlays are inherently juice-heavy

    def analyze(self, hub_data: dict) -> list[BetSlip]:
        game_odds: list[dict] = hub_data.get("game_odds", [])
        game_predictions: dict = hub_data.get("game_predictions", {})

        # Build moneyline legs
        ml_legs: list[Leg] = []

        for game in game_odds:
            game_id = game.get("game_id", "")
            for side in ("home", "away"):
                ml_american = game.get(f"{side}_ml_odds")
                if not ml_american:
                    continue
                ml_american = int(ml_american)
                if abs(ml_american) > MAX_AMERICAN_ODDS_PER_LEG and ml_american > 0:
                    continue

                decimal = self.american_to_decimal(ml_american)
                book_prob = self.decimal_to_prob(decimal)
                team = game.get(f"{side}_team", "")
                book = game.get("bookmaker", "draftkings").lower()

                if book not in BOOKS_PRIORITY:
                    continue

                # Game prediction from model
                model_data = game_predictions.get(game_id, {})
                raw_key = f"{side}_win_prob"
                model_prob = model_data.get(raw_key)
                if model_prob is None:
                    model_prob = book_prob + 0.01  # Minimal edge assumption

                ev = self.calculate_ev(model_prob, decimal)
                if ev < 0:
                    continue

                ml_legs.append(Leg(
                    player=team,
                    prop_type="moneyline",
                    line=0.0,
                    direction=side,
                    book=book,
                    american_odds=ml_american,
                    decimal_odds=decimal,
                    book_prob=book_prob,
                    model_prob=model_prob,
                    edge=round(model_prob - book_prob, 4),
                ))

        # Also include run total legs
        for game in game_odds:
            for direction in ("over", "under"):
                total_odds = game.get(f"total_{direction}_odds")
                if not total_odds:
                    continue
                decimal = self.american_to_decimal(int(total_odds))
                if decimal < 1.80:
                    continue
                book_prob = self.decimal_to_prob(decimal)
                game_id = game.get("game_id", "")
                line = game.get("total_line", 8.5)
                book = game.get("bookmaker", "draftkings").lower()

                model_data = game_predictions.get(game_id, {})
                model_prob = model_data.get(f"total_{direction}_prob")
                if model_prob is None:
                    model_prob = book_prob + 0.01

                ev = self.calculate_ev(model_prob, decimal)
                if ev < 0:
                    continue

                ml_legs.append(Leg(
                    player=f"{game.get('away_team', '')} @ {game.get('home_team', '')}",
                    prop_type="total_runs",
                    line=line,
                    direction=direction,
                    book=book,
                    american_odds=int(total_odds),
                    decimal_odds=decimal,
                    book_prob=book_prob,
                    model_prob=model_prob,
                    edge=round(model_prob - book_prob, 4),
                ))

        # Sort by edge
        ml_legs.sort(key=lambda x: x.edge, reverse=True)

        slips: list[BetSlip] = []
        top = ml_legs[:8]

        # 2-leg parlays
        for a, b in itertools.combinations(top[:6], 2):
            combined_dec = self.parlay_odds([a, b])
            combined_prob = a.model_prob * b.model_prob
            ev = self.calculate_ev(combined_prob, combined_dec)
            if ev >= self.ev_threshold:
                slips.append(BetSlip(
                    agent_name=self.name, strategy="2-Leg ML Parlay",
                    legs=[a, b], stake_units=1.0,
                    combined_odds=combined_dec, expected_value=ev,
                    confidence=combined_prob,
                    metadata={"legs": 2}
                ))

        # 3-leg parlays (top 3 legs only)
        for combo in itertools.combinations(top[:5], 3):
            legs = list(combo)
            combined_dec = self.parlay_odds(legs)
            combined_prob = legs[0].model_prob * legs[1].model_prob * legs[2].model_prob
            ev = self.calculate_ev(combined_prob, combined_dec)
            if ev >= self.ev_threshold:
                slips.append(BetSlip(
                    agent_name=self.name, strategy="3-Leg ML Parlay",
                    legs=legs, stake_units=0.5,
                    combined_odds=combined_dec, expected_value=ev,
                    confidence=combined_prob,
                    metadata={"legs": 3}
                ))

        # 4-leg parlays (very selective)
        if len(top) >= 4:
            best4 = top[:4]
            combined_dec = self.parlay_odds(best4)
            combined_prob = 1.0
            for l in best4:
                combined_prob *= l.model_prob
            ev = self.calculate_ev(combined_prob, combined_dec)
            if ev >= 0.04:  # Stricter for 4-leggers
                slips.append(BetSlip(
                    agent_name=self.name, strategy="4-Leg ML Parlay",
                    legs=best4, stake_units=0.25,
                    combined_odds=combined_dec, expected_value=ev,
                    confidence=combined_prob,
                    metadata={"legs": 4}
                ))

        slips.sort(key=lambda x: x.expected_value, reverse=True)
        logger.info("[parlay] %s legs -> %s parlay slips", len(ml_legs), len(slips))
        return slips[:8]
