"""
Agent 5: Live Betting Agent
----------------------------
Strategy: Detect in-play line movements > 5%. Fade sharp steam or follow
depending on game state. Single leg only. Edge: 5–8%.
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timedelta
from .base_agent import BaseAgent, BetSlip, Leg

logger = logging.getLogger("propiq.agent.live")

MOVEMENT_THRESHOLD = 0.05   # 5% probability shift triggers action
STALENESS_SECONDS = 120     # Ignore snapshots older than 2 minutes


class LiveAgent(BaseAgent):
    name = "live"
    strategy = "Live Line Movement"
    max_legs = 1
    min_legs = 1
    ev_threshold = 0.05

    def __init__(self):
        super().__init__()
        self._line_history: dict[str, list[dict]] = {}  # prop_key → list of snapshots

    def _update_history(self, props: list[dict]):
        now = time.time()
        for prop in props:
            key = self._prop_key(prop)
            for direction in ("over", "under"):
                american = prop.get(f"{direction}_odds")
                if not american:
                    continue
                dk = f"{key}|{direction}"
                if dk not in self._line_history:
                    self._line_history[dk] = []
                self._line_history[dk].append({
                    "ts": now,
                    "decimal": self.american_to_decimal(int(american)),
                    "american": int(american),
                })
                # Keep last 20 snapshots
                self._line_history[dk] = self._line_history[dk][-20:]

    @staticmethod
    def _prop_key(prop: dict) -> str:
        return f"{prop.get('player_name','')}|{prop.get('prop_type','')}|{prop.get('line', 0)}"

    def _detect_movement(self) -> list[tuple[str, str, float]]:
        """Returns list of (prop_key, direction, delta_prob) above threshold."""
        movements = []
        now = time.time()
        for dk, snapshots in self._line_history.items():
            if len(snapshots) < 2:
                continue
            oldest = snapshots[0]
            newest = snapshots[-1]
            if now - newest["ts"] > STALENESS_SECONDS:
                continue
            old_prob = 1 / oldest["decimal"] if oldest["decimal"] > 0 else 0
            new_prob = 1 / newest["decimal"] if newest["decimal"] > 0 else 0
            delta = new_prob - old_prob
            if abs(delta) >= MOVEMENT_THRESHOLD:
                movements.append((dk, delta))
        return movements

    def analyze(self, hub_data: dict) -> list[BetSlip]:
        props: list[dict] = hub_data.get("live_props", hub_data.get("player_props", []))
        predictions: dict = hub_data.get("model_predictions", {})

        # Update line history
        self._update_history(props)

        # Detect movements
        movements = self._detect_movement()
        if not movements:
            logger.info("[live] No significant line movements detected.")
            return []

        # Build props lookup
        props_by_key: dict[str, dict] = {}
        for prop in props:
            for direction in ("over", "under"):
                k = f"{self._prop_key(prop)}|{direction}"
                props_by_key[k] = {**prop, "_direction": direction}

        slips: list[BetSlip] = []

        for dk, delta in sorted(movements, key=lambda x: abs(x[1]), reverse=True)[:5]:
            prop_data = props_by_key.get(dk)
            if not prop_data:
                continue

            direction = prop_data["_direction"]
            american = prop_data.get(f"{direction}_odds")
            if not american:
                continue

            decimal = self.american_to_decimal(int(american))
            book_prob = self.decimal_to_prob(decimal)
            player = prop_data.get("player_name", "")
            prop_type = prop_data.get("prop_type", "")
            line = prop_data.get("line", 0.0)
            book = prop_data.get("bookmaker", "draftkings")

            # Sharp money moving a line → fade if it became chalk, follow if it got longer
            # Positive delta = line shortened (sharps bet it) → FADE (bet opposite)
            # Negative delta = line lengthened (public faded) → FOLLOW (value emerged)
            action_direction = direction
            if delta > 0:   # Line shortened → value may be on opposite side
                action_direction = "under" if direction == "over" else "over"
                american_action = prop_data.get(f"{action_direction}_odds")
                if not american_action:
                    continue
                decimal = self.american_to_decimal(int(american_action))
                book_prob = self.decimal_to_prob(decimal)

            key = f"{player}|{prop_type}|{line}|{action_direction}"
            model_prob = predictions.get(key, {}).get("calibrated_prob")
            if model_prob is None:
                # Movement itself is the signal — estimate edge from delta magnitude
                model_prob = book_prob + abs(delta) * 0.8

            ev = self.calculate_ev(model_prob, decimal)
            if ev < self.ev_threshold:
                continue

            slips.append(BetSlip(
                agent_name=self.name,
                strategy=f"Live {'Follow' if delta < 0 else 'Fade'} — {abs(delta):.1%} move",
                legs=[Leg(
                    player=player, prop_type=prop_type, line=line,
                    direction=action_direction, book=book,
                    american_odds=int(american), decimal_odds=decimal,
                    book_prob=book_prob, model_prob=model_prob,
                    edge=round(model_prob - book_prob, 4),
                )],
                stake_units=0.5,
                combined_odds=decimal,
                expected_value=ev,
                confidence=model_prob,
                metadata={
                    "movement_delta": delta,
                    "action": "fade" if delta > 0 else "follow",
                    "snapshots": len(self._line_history.get(dk, []))
                }
            ))

        logger.info(f"[live] {len(movements)} movements → {len(slips)} live slips")
        return slips
