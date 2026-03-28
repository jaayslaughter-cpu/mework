"""
Agent 2: Under Machine
-----------------------
Strategy: Bet UNDER total strikeouts / total bases in pitching duels where
both starters have ERA < 3.50. Leverages pitcher dominance + park factors.
Win rate target: 58%+  |  Legs: 1–3.
"""
from __future__ import annotations
import logging
from .base_agent import BaseAgent, BetSlip, Leg

logger = logging.getLogger("propiq.agent.under_machine")

ERA_THRESHOLD = 3.50
WIN_RATE_TARGET = 0.58
TARGET_PROPS = {"pitcher_strikeouts", "batter_strikeouts", "batter_total_bases", "total_runs"}


class UnderMachine(BaseAgent):
    name = "under_machine"
    strategy = "Under ERA Duel"
    max_legs = 3
    min_legs = 1
    ev_threshold = 0.04   # Slightly lower — structure edge compensates

    def analyze(self, hub_data: dict) -> list[BetSlip]:
        games: list[dict] = hub_data.get("games_today", [])
        props: list[dict] = hub_data.get("player_props", [])
        pitcher_stats: dict = hub_data.get("pitcher_stats", {})
        predictions: dict = hub_data.get("model_predictions", {})

        # Step 1: Find qualifying duels (both starters ERA < 3.50)
        qualifying_games: list[str] = []
        for game in games:
            home_pitcher = game.get("home_pitcher", "")
            away_pitcher = game.get("away_pitcher", "")
            home_era = pitcher_stats.get(home_pitcher, {}).get("era", 9.99)
            away_era = pitcher_stats.get(away_pitcher, {}).get("era", 9.99)
            if home_era < ERA_THRESHOLD and away_era < ERA_THRESHOLD:
                qualifying_games.append(game.get("game_id", ""))
                logger.info(
                    "[under_machine] Qualifying duel: %s (%.2f ERA) vs %s (%.2f ERA)",
                    away_pitcher, away_era, home_pitcher, home_era
                )

        if not qualifying_games:
            logger.info("[under_machine] No qualifying ERA duels today.")
            return []

        # Step 2: Find UNDER props in qualifying games
        under_legs: list[Leg] = []

        for prop in props:
            game_id = prop.get("game_id", "")
            if game_id not in qualifying_games:
                continue

            prop_type = prop.get("prop_type", "")
            if prop_type not in TARGET_PROPS:
                continue

            under_odds = prop.get("under_odds")
            if not under_odds:
                continue

            decimal = self.american_to_decimal(int(under_odds))
            if decimal < 1.60:   # Under chalk isn't +EV enough
                continue

            book_prob = self.decimal_to_prob(decimal)
            line = prop.get("line", 0.0)
            player = prop.get("player_name", "")
            book = prop.get("bookmaker", "draftkings")

            # Model prob: start from historical under-rate for ERA duels
            key = f"{player}|{prop_type}|{line}|under"
            model_prob = predictions.get(key, {}).get("calibrated_prob")
            if model_prob is None:
                # Structural edge: ERA duels suppress scoring by ~8-12%
                model_prob = min(book_prob + 0.08, 0.78)

            ev = self.calculate_ev(model_prob, decimal)
            if ev < self.ev_threshold:
                continue

            under_legs.append(Leg(
                player=player,
                prop_type=prop_type,
                line=line,
                direction="under",
                book=book,
                american_odds=int(under_odds),
                decimal_odds=decimal,
                book_prob=book_prob,
                model_prob=model_prob,
                edge=round(model_prob - book_prob, 4),
            ))

        under_legs.sort(key=lambda x: x.edge, reverse=True)

        slips: list[BetSlip] = []

        # Singles
        for leg in under_legs[:3]:
            ev = self.calculate_ev(leg.model_prob, leg.decimal_odds)
            kelly = self.kelly_fraction(leg.model_prob, leg.decimal_odds)
            slips.append(BetSlip(
                agent_name=self.name,
                strategy="Under Single (ERA Duel)",
                legs=[leg],
                stake_units=max(0.5, min(kelly * 8, 2.0)),
                combined_odds=leg.decimal_odds,
                expected_value=ev,
                confidence=leg.model_prob,
                metadata={"qualifying_games": qualifying_games, "era_threshold": ERA_THRESHOLD}
            ))

        # 2-leg under parlay (same game correlation)
        if len(under_legs) >= 2:
            a, b = under_legs[0], under_legs[1]
            # Correlation bonus: same-game unders are positively correlated
            # Adjust combined prob upward slightly
            combined_prob = min(a.model_prob * b.model_prob * 1.05, 0.99)
            combined_dec = self.parlay_odds([a, b])
            ev = self.calculate_ev(combined_prob, combined_dec)
            if ev >= self.ev_threshold:
                slips.append(BetSlip(
                    agent_name=self.name,
                    strategy="Under 2-Leg (ERA Duel)",
                    legs=[a, b],
                    stake_units=0.5,
                    combined_odds=combined_dec,
                    expected_value=ev,
                    confidence=combined_prob,
                    metadata={"correlation": "positive_same_game_under"}
                ))

        # 3-leg under parlay
        if len(under_legs) >= 3:
            a, b, c = under_legs[0], under_legs[1], under_legs[2]
            combined_prob = min(a.model_prob * b.model_prob * c.model_prob * 1.08, 0.99)
            combined_dec = self.parlay_odds([a, b, c])
            ev = self.calculate_ev(combined_prob, combined_dec)
            if ev >= self.ev_threshold:
                slips.append(BetSlip(
                    agent_name=self.name,
                    strategy="Under 3-Leg (ERA Duel)",
                    legs=[a, b, c],
                    stake_units=0.25,
                    combined_odds=combined_dec,
                    expected_value=ev,
                    confidence=combined_prob,
                    metadata={"correlation": "3_game_under_parlay"}
                ))

        logger.info(
            "[under_machine] %s qualifying duels, %s under legs → %s slips",
            len(qualifying_games),
            len(under_legs),
            len(slips),
        )
        return slips
