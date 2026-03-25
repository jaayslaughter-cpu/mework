"""
Agent 1: +EV Hunter
-------------------
Strategy: Scan all player props for model_prob > book_prob by >5%.
Legs: 1–3 (takes highest EV singles first, then correlated pairs/triples).
Threshold: EV > 5%  |  Capital: scales with performance.
Books: DraftKings, FanDuel, BetMGM, bet365.
"""
from __future__ import annotations
import logging
from .base_agent import BaseAgent, BetSlip, Leg

logger = logging.getLogger("propiq.agent.ev_hunter")

TOP_BOOKS = {"draftkings", "fanduel", "betmgm", "bet365"}
EV_THRESHOLD = 0.05      # 5%
MAX_SINGLE_ODDS = 3.50   # Don't chase > +250 longshots
MIN_SINGLE_ODDS = 1.40   # Floor to avoid -300+ chalk


class EVHunter(BaseAgent):
    name = "ev_hunter"
    strategy = "+EV"
    max_legs = 3
    min_legs = 1
    ev_threshold = EV_THRESHOLD

    def analyze(self, hub_data: dict) -> list[BetSlip]:
        props: list[dict] = hub_data.get("player_props", [])
        predictions: dict = hub_data.get("model_predictions", {})

        ev_legs: list[Leg] = []

        for prop in props:
            player = prop.get("player_name", "")
            prop_type = prop.get("prop_type", "")
            book = prop.get("bookmaker", "").lower()

            if book not in TOP_BOOKS:
                continue

            for direction in ("over", "under"):
                american = prop.get(f"{direction}_odds")
                if not american:
                    continue
                decimal = self.american_to_decimal(int(american))
                if not (MIN_SINGLE_ODDS <= decimal <= MAX_SINGLE_ODDS):
                    continue

                book_prob = self.decimal_to_prob(decimal)
                line = prop.get("line", 0.0)

                # Pull model prediction
                key = f"{player}|{prop_type}|{line}|{direction}"
                model_prob = predictions.get(key, {}).get("calibrated_prob")
                if model_prob is None:
                    # Fallback: naive 2% edge on book_prob
                    model_prob = book_prob + 0.02
                    if model_prob < 0.50:
                        continue

                ev = self.calculate_ev(model_prob, decimal)
                if ev < EV_THRESHOLD:
                    continue

                ev_legs.append(Leg(
                    player=player,
                    prop_type=prop_type,
                    line=line,
                    direction=direction,
                    book=book,
                    american_odds=int(american),
                    decimal_odds=decimal,
                    book_prob=book_prob,
                    model_prob=model_prob,
                    edge=round(model_prob - book_prob, 4),
                ))

        # Sort by EV descending
        ev_legs.sort(key=lambda x: x.edge, reverse=True)

        slips: list[BetSlip] = []

        # --- Singles (top 5) ---
        for leg in ev_legs[:5]:
            ev = self.calculate_ev(leg.model_prob, leg.decimal_odds)
            kelly = self.kelly_fraction(leg.model_prob, leg.decimal_odds)
            slips.append(BetSlip(
                agent_name=self.name,
                strategy="+EV Single",
                legs=[leg],
                stake_units=max(0.5, min(kelly * 10, 3.0)),   # 0.5–3u
                combined_odds=leg.decimal_odds,
                expected_value=ev,
                confidence=leg.model_prob,
                metadata={"source": "ev_hunter_single"}
            ))

        # --- 2-leg combos (same game preferred) ---
        for i in range(min(3, len(ev_legs))):
            for j in range(i + 1, min(6, len(ev_legs))):
                a, b = ev_legs[i], ev_legs[j]
                combined_dec = self.parlay_odds([a, b])
                combined_prob = a.model_prob * b.model_prob
                ev = self.calculate_ev(combined_prob, combined_dec)
                if ev >= EV_THRESHOLD:
                    slips.append(BetSlip(
                        agent_name=self.name,
                        strategy="+EV 2-Leg",
                        legs=[a, b],
                        stake_units=0.5,
                        combined_odds=combined_dec,
                        expected_value=ev,
                        confidence=combined_prob,
                        metadata={"source": "ev_hunter_2leg"}
                    ))

        # --- 3-leg combos (high confidence only) ---
        top3 = [l for l in ev_legs[:4] if l.model_prob >= 0.65]
        if len(top3) >= 3:
            a, b, c = top3[0], top3[1], top3[2]
            combined_dec = self.parlay_odds([a, b, c])
            combined_prob = a.model_prob * b.model_prob * c.model_prob
            ev = self.calculate_ev(combined_prob, combined_dec)
            if ev >= EV_THRESHOLD:
                slips.append(BetSlip(
                    agent_name=self.name,
                    strategy="+EV 3-Leg",
                    legs=[a, b, c],
                    stake_units=0.25,
                    combined_odds=combined_dec,
                    expected_value=ev,
                    confidence=combined_prob,
                    metadata={"source": "ev_hunter_3leg"}
                ))

        logger.info("[ev_hunter] Found %d +EV legs → %d slips", len(ev_legs), len(slips))
        return slips
