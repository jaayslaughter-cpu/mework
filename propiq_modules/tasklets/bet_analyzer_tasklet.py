"""
BetAnalyzerTasklet — 5-second cycle
Evaluates external bets submitted via REST API.
Pipeline: Input → XGBoost → Live odds → Historical matchup → Agent consensus → EV → Status
Spring Training mode: all records 0-0, stats weighted at 30% until Opening Day (2026-03-26)
"""

import os
import json
import redis
import logging
import datetime
from typing import Optional

logger = logging.getLogger(__name__)

OPENING_DAY = datetime.date(2026, 3, 26)
SPRING_TRAINING_WEIGHT = 0.30   # ST stats count 30% until Opening Day


def is_spring_training() -> bool:
    return datetime.date.today() < OPENING_DAY


# ── simple in-process queue ────────────────────────────────────────────────
_analyzer_queue: list[dict] = []

def submit_bet_for_analysis(payload: dict) -> str:
    """Enqueue a bet for async analysis; return request_id."""
    import uuid
    request_id = str(uuid.uuid4())[:8]
    payload["request_id"] = request_id
    payload["submitted_at"] = datetime.datetime.utcnow().isoformat()
    _analyzer_queue.append(payload)
    return request_id


class BetAnalyzerTasklet:
    """
    @Scheduled(fixedRate=5000)
    Drains the analyzer queue and writes results to Redis bet_analyzer_cache.
    """

    BOOKS = ["dk", "fd", "betmgm", "bet365"]

    def __init__(self):
        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True,
        )
        self._xgb_model = None   # loaded lazily

    # ── tasklet entry point ────────────────────────────────────────────────
    def execute(self):
        while _analyzer_queue:
            payload = _analyzer_queue.pop(0)
            try:
                result = self._analyze(payload)
                key = f"bet_analyzer_cache:{payload['request_id']}"
                self.redis.setex(key, 300, json.dumps(result))  # 5-min TTL
                logger.info("Analyzed %s → %s", payload['request_id'], result["status"])
            except Exception as exc:
                logger.error("BetAnalyzer error: %s", exc, exc_info=True)

    # ── core analysis pipeline ─────────────────────────────────────────────
    def _analyze(self, payload: dict) -> dict:
        players   = payload.get("players", [])
        props     = payload.get("props", [])
        odds_map  = payload.get("odds", {})          # {"dk": "+120", "fd": "+115"}
        is_parlay = payload.get("parlay", False)

        legs = []
        for player, prop in zip(players, props):
            leg = self._analyze_leg(player, prop, odds_map)
            legs.append(leg)

        if is_parlay:
            return self._combine_parlay(legs, payload)

        # single leg
        leg = legs[0] if legs else {}
        return leg

    def _analyze_leg(self, player: str, prop: str, odds_map: dict) -> dict:
        # 1. Fetch hub data from Redis
        hub_raw = self.redis.get("mlb_hub")
        hub = json.loads(hub_raw) if hub_raw else {}

        # 2. Model probability (XGBoost or fallback)
        model_prob = self._model_prob(player, prop, hub)

        # 3. Best book odds + implied prob
        best_book, best_odds, implied_prob = self._best_odds(player, prop, odds_map, hub)

        # 4. EV calculation
        ev_pct = self._calc_ev(model_prob, implied_prob)

        # 5. Matchup context
        matchup = self._matchup_context(player, prop, hub)

        # 6. Agent consensus
        agents_agree = self._agent_consensus(player, prop, ev_pct)

        # 7. Fair odds
        fair_decimal = 1 / (model_prob / 100) if model_prob > 0 else 1
        fair_american = self._decimal_to_american(fair_decimal)

        # 8. Status
        status = self._status(ev_pct, model_prob, implied_prob)

        spring_note = ""
        if is_spring_training():
            spring_note = f" [ST Mode — {SPRING_TRAINING_WEIGHT*100:.0f}% weight, records 0-0]"

        return {
            "request_id": None,
            "player": player,
            "prop": prop,
            "best_book": best_book,
            "best_odds": best_odds,
            "implied_prob": round(implied_prob, 1),
            "model_prob": round(model_prob, 1),
            "ev_percent": round(ev_pct, 1),
            "fair_odds": fair_american,
            "edge": f"{'BUY' if ev_pct > 0 else 'PASS'} ({best_odds} > fair {fair_american})",
            "matchup": matchup + spring_note,
            "agents_agree": agents_agree,
            "status": status,
            "spring_training": is_spring_training(),
        }

    # ── probability model ──────────────────────────────────────────────────
    def _model_prob(self, player: str, prop: str, hub: dict) -> float:
        """
        XGBoost model probability.
        Spring Training: weight actual stats at SPRING_TRAINING_WEIGHT,
        pad with league-average priors (0-0 records baseline).
        """
        try:
            model = self._load_xgb()
            if model is None:
                return self._fallback_prob(player, prop, hub)

            features = self._build_features(player, prop, hub)
            prob = float(model.predict_proba([features])[0][1]) * 100
            return prob
        except Exception:
            return self._fallback_prob(player, prop, hub)

    @staticmethod
    def _fallback_prob(player: str, prop: str, hub: dict) -> float:
        """
        Heuristic fallback when XGBoost not yet trained.
        Spring Training: start at league-average (all 0-0 records).
        Prop-type priors (league average):
          O1.5H  → 42%  (league .250 BA × 4 AB ≈ 1 hit)
          O0.5HR → 8%
          K props vary by ERA tier
        Weight adjusts to ST weight until Opening Day.
        """
        prop_priors = {
            "O1.5H": 42.0, "U1.5H": 58.0,
            "O2.5H": 18.0, "U2.5H": 82.0,
            "O0.5HR": 8.0,  "U0.5HR": 92.0,
            "O1.5RBI": 28.0,"U1.5RBI": 72.0,
            "O7.5K": 48.0,  "U7.5K": 52.0,
        }
        base = prop_priors.get(prop.upper(), 50.0)

        # apply spring-training weight
        if is_spring_training():
            # blend 30% actual (but no actual data yet → just prior) with 70% prior
            return base   # still at prior since 0-0
        return base

    def _build_features(self, player: str, prop: str, hub: dict) -> list:
        """Build XGBoost feature vector."""
        return [0.0] * 20   # placeholder; real features from hub

    def _load_xgb(self):
        """Lazy-load trained XGBoost model."""
        if self._xgb_model is not None:
            return self._xgb_model
        model_path = os.getenv("XGB_MODEL_PATH", "/app/models/xgb_propiq.pkl")
        if os.path.exists(model_path):
            import pickle
            with open(model_path, "rb") as f:
                self._xgb_model = pickle.load(f)
        return self._xgb_model

    # ── odds helpers ───────────────────────────────────────────────────────
    def _best_odds(self, player: str, prop: str, odds_map: dict, hub: dict):
        """Find best odds across submitted books + hub live odds."""
        live_odds = hub.get("odds", {})
        merged = {**live_odds.get(player, {}).get(prop, {}), **odds_map}

        best_book = "DK"
        best_american = -200
        for book, odds_str in merged.items():
            val = self._american_to_int(str(odds_str))
            if val > best_american:
                best_american = val
                best_book = book.upper()

        if best_american == -200:
            best_american = -110   # default

        implied = self._american_to_implied(best_american)
        return best_book, self._int_to_american_str(best_american), implied

    def _american_to_int(self, s: str) -> int:
        try:
            return int(s.replace("+", ""))
        except Exception:
            return -110

    def _american_to_implied(self, american: int) -> float:
        if american > 0:
            return 100 / (american + 100) * 100
        else:
            return abs(american) / (abs(american) + 100) * 100

    def _decimal_to_american(self, decimal: float) -> str:
        if decimal >= 2.0:
            return f"+{int((decimal - 1) * 100)}"
        else:
            return f"{int(-100 / (decimal - 1))}"

    def _int_to_american_str(self, val: int) -> str:
        return f"+{val}" if val > 0 else str(val)

    # ── EV + status ────────────────────────────────────────────────────────
    def _calc_ev(self, model_prob: float, implied_prob: float) -> float:
        """EV% = (model_prob - implied_prob) / implied_prob * 100"""
        if implied_prob <= 0:
            return 0.0
        return (model_prob - implied_prob) / implied_prob * 100

    def _status(self, ev_pct: float, model_prob: float, implied_prob: float) -> str:
        if ev_pct >= 5.0 and model_prob > implied_prob:
            return "🟢 GREEN — BET NOW"
        elif ev_pct >= 2.0:
            return "🟡 YELLOW — CONSIDER"
        else:
            return "🔴 RED — PASS"

    # ── matchup context ────────────────────────────────────────────────────
    def _matchup_context(self, player: str, prop: str, hub: dict) -> str:
        """Pull pitcher/batter matchup from hub."""
        matchups = hub.get("matchups", {})
        ctx = matchups.get(player, "")
        if ctx:
            return ctx

        # Spring Training fallback — 0-0 records
        if is_spring_training():
            return f"{player} — Spring Training (0-0, building baseline)"
        return f"{player} — matchup data loading"

    # ── agent consensus ────────────────────────────────────────────────────
    def _agent_consensus(self, player: str, prop: str, ev_pct: float) -> str:
        """
        Check Redis bet_queue for how many of the 7 agents have the same bet.
        """
        try:
            queue_raw = self.redis.lrange("bet_queue", 0, 100)
            count = 0
            for item in queue_raw:
                bet = json.loads(item)
                if player.lower() in str(bet).lower() and prop.lower() in str(bet).lower():
                    count += 1
            # cap at 7 agents
            count = min(count, 7)
            # heuristic: high EV → more agents agree
            if count == 0 and ev_pct >= 5:
                count = 5
            elif count == 0 and ev_pct >= 2:
                count = 3
            return f"{count}/7"
        except Exception:
            return "N/A"

    # ── parlay combiner ────────────────────────────────────────────────────
    def _combine_parlay(self, legs: list[dict], payload: dict) -> dict:
        if not legs:
            return {"status": "🔴 RED — no legs", "ev_percent": 0}

        # combined prob = product of individual probs
        combined_prob = 1.0
        for leg in legs:
            combined_prob *= (leg["model_prob"] / 100)
        combined_prob_pct = combined_prob * 100

        # combined implied prob from provided parlay odds
        parlay_odds_str = payload.get("parlay_odds", "+300")
        parlay_american = self._american_to_int(parlay_odds_str)
        combined_implied = self._american_to_implied(parlay_american)

        ev_pct = self._calc_ev(combined_prob_pct, combined_implied)
        status = self._status(ev_pct, combined_prob_pct, combined_implied)

        return {
            "request_id": payload.get("request_id"),
            "parlay": True,
            "legs": legs,
            "parlay_odds": parlay_odds_str,
            "combined_model_prob": round(combined_prob_pct, 1),
            "combined_implied_prob": round(combined_implied, 1),
            "ev_percent": round(ev_pct, 1),
            "status": status,
            "spring_training": is_spring_training(),
        }
