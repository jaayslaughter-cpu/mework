"""
FadeAgent — Contrarian plays when public >70% on one side.

Sharp money indicators used:
  1. Public bet% > 70% AND bet volume > 5,000 → fade the public
  2. RLM (Reverse Line Movement): line moves against public direction → pro money
  3. Money% vs Bet%: if bet% 72% but money% only 54% → sharps on other side
  4. Steam moves: sudden 10¢+ line move against public = sharp trigger

Production thresholds (per FadeAgent spec):
  RULE 1: MAX EDGE (>75% public + tight umpire K% < 20%)
    → Bet pitcher K overs on faded team | EV +3.8%
  RULE 2: RELIABLE (>70% public + hitter umpire K% > 24%)
    → Bet opposite side | EV +2.9%
  RULE 3: SHARP CONFIRM (<40% public + accurate ump >93%)
    → Bet public side (sharp money) | EV +2.1%
"""

import logging
from datetime import datetime

from .base_agent import BaseAgent, BetRecommendation

logger = logging.getLogger(__name__)

# FadeAgent thresholds
FADE_PUBLIC_THRESHOLD = 70.0       # >70% public → fade
MAX_EDGE_THRESHOLD = 75.0          # >75% = MAX EDGE tier
SHARP_CONFIRM_MAX_PUBLIC = 40.0    # <40% on your side = sharp money
SHARP_CONFIRM_UMP_ACCURACY = 93.0  # Accurate ump confirms sharp
MIN_VOLUME_TRIGGER = 5000          # Minimum bet volume for reliability
EV_BOOST_PER_PCT = 0.03            # +0.03% EV per % above 70%


class FadeAgent(BaseAgent):
    """
    Contrarian agent. Fades public consensus using Action Network data.
    Cross-references umpire quality for MAX EDGE situations.
    """

    name = "FadeAgent"
    max_legs = 1
    strategy = "Public >70% → opposite side with RLM/sharp money confirmation"
    ev_threshold = 2.0  # Lower threshold (contrarian plays need less edge)

    def analyze(self, hub_data: dict) -> list[BetRecommendation]:
        """
        hub_data keys:
          - public_betting: list from ActionNetworkScraper
          - line_movement: list from ActionNetworkScraper
          - umpires: list from RotoWireScraper (for MAX EDGE combos)
          - odds: current odds from TheOddsAPI
        """
        recommendations = []

        public_data = hub_data.get("public_betting", [])
        line_movement = hub_data.get("line_movement", [])
        umpires = hub_data.get("umpires", [])
        current_odds = hub_data.get("odds", {})

        # Build umpire lookup by game
        ump_map = {u.get("game", ""): u for u in umpires}

        # Build line movement lookup by game
        rlm_map = {}
        for lm in line_movement:
            game = lm.get("game", "")
            rlm_map[game] = lm

        for game_data in public_data:
            away_team = game_data.get("away_team", "UNK")
            home_team = game_data.get("home_team", "UNK")
            game_key = game_data.get("game", f"{away_team}@{home_team}")

            away_public = float(game_data.get("away_public_bets_pct", 50.0))
            home_public = float(game_data.get("home_public_bets_pct", 50.0))
            away_money = float(game_data.get("away_public_money_pct", 50.0))
            home_money = float(game_data.get("home_public_money_pct", 50.0))
            bet_volume = int(game_data.get("bet_volume", 0))
            rlm = game_data.get("rlm_detected", False)

            # ── Volume check ──────────────────────────────────────────────────
            if bet_volume > 0 and bet_volume < MIN_VOLUME_TRIGGER:
                logger.debug(f"[FADE] {game_key}: volume {bet_volume} < {MIN_VOLUME_TRIGGER} — skip")
                continue

            # Determine heavy public side
            public_pct = max(away_public, home_public)
            public_side = away_team if away_public > home_public else home_team
            fade_side = home_team if public_side == away_team else away_team

            # Money divergence check (bets% vs money%)
            if public_side == away_team:
                money_divergence = abs(away_public - away_money)
            else:
                money_divergence = abs(home_public - home_money)

            sharp_money_confirmed = money_divergence > 10.0  # 10%+ divergence = sharps

            # ── RULE 1: MAX EDGE (>75% + tight umpire) ────────────────────────
            ump = ump_map.get(game_key, {})
            ump_k_pct = float(ump.get("k_pct", 21.0))
            ump_accuracy = float(ump.get("accuracy", 90.0))
            tight_ump = ump_k_pct < 20.0

            if public_pct > MAX_EDGE_THRESHOLD and tight_ump and bet_volume >= MIN_VOLUME_TRIGGER:
                ev_boost = (public_pct - 70.0) * EV_BOOST_PER_PCT + 3.8
                bet = self._build_bet(
                    agent_rule="MAX_EDGE",
                    fade_side=fade_side,
                    public_side=public_side,
                    public_pct=public_pct,
                    ev_pct=ev_boost,
                    game=game_key,
                    ump=ump,
                    odds=current_odds.get(fade_side, 115),
                    bet_type="moneyline",
                    description=f"FADE {public_side} → {fade_side} ML (MAX EDGE: {public_pct:.0f}% public + tight ump)",
                    game_data=game_data,
                )
                if bet:
                    recommendations.append(bet)
                continue

            # ── RULE 2: RELIABLE (>70% public + hitter umpire K% > 24%) ──────
            hitter_ump = ump_k_pct > 24.0
            if public_pct > FADE_PUBLIC_THRESHOLD and hitter_ump:
                ev_boost = (public_pct - 70.0) * EV_BOOST_PER_PCT + 2.9
                bet = self._build_bet(
                    agent_rule="RELIABLE",
                    fade_side=fade_side,
                    public_side=public_side,
                    public_pct=public_pct,
                    ev_pct=ev_boost,
                    game=game_key,
                    ump=ump,
                    odds=current_odds.get(fade_side, 115),
                    bet_type="moneyline",
                    description=f"FADE {public_side} → {fade_side} ML (RELIABLE: {public_pct:.0f}% public + hitter ump)",
                    game_data=game_data,
                )
                if bet:
                    recommendations.append(bet)
                continue

            # ── RULE 3: SHARP CONFIRM (<40% public + accurate ump) ────────────
            min_public = min(away_public, home_public)
            if min_public < SHARP_CONFIRM_MAX_PUBLIC and ump_accuracy > SHARP_CONFIRM_UMP_ACCURACY:
                sharp_side = away_team if away_public < home_public else home_team
                ev_boost = 2.1 + (sharp_money_confirmed * 0.8) + (rlm * 1.2)
                bet = self._build_bet(
                    agent_rule="SHARP_CONFIRM",
                    fade_side=sharp_side,
                    public_side=public_side,
                    public_pct=min_public,
                    ev_pct=ev_boost,
                    game=game_key,
                    ump=ump,
                    odds=current_odds.get(sharp_side, -105),
                    bet_type="moneyline",
                    description=f"SHARP MONEY → {sharp_side} ML ({min_public:.0f}% public + accurate ump)",
                    game_data=game_data,
                )
                if bet:
                    recommendations.append(bet)

        return sorted(recommendations, key=lambda b: b.ev_pct, reverse=True)

    def _build_bet(
        self, agent_rule: str, fade_side: str, public_side: str,
        public_pct: float, ev_pct: float, game: str,
        ump: dict, odds: int, bet_type: str, description: str,
        game_data: dict,
    ) -> BetRecommendation | None:

        if ev_pct < self.ev_threshold:
            return None

        model_prob = min(0.60, 0.50 + ev_pct / 100)

        logger.info(
            f"[FADE AGENT] 🔄 {agent_rule} | {description} | "
            f"Volume: {game_data.get('bet_volume',0):,} | EV: +{ev_pct:.1f}%"
        )

        return BetRecommendation(
            agent=self.name,
            bet_type=bet_type,
            players=[fade_side],
            description=description,
            legs=1,
            ev_pct=round(ev_pct, 1),
            model_prob=round(model_prob, 3),
            odds_american=odds if isinstance(odds, int) else 115,
            books=["DraftKings", "FanDuel", "BetMGM", "bet365"],
            metadata={
                "rule": agent_rule,
                "public_side": public_side,
                "public_pct": public_pct,
                "bet_volume": game_data.get("bet_volume", 0),
                "sharp_money": game_data.get("sharp_money_trigger", False),
                "rlm": game_data.get("rlm_detected", False),
                "umpire": ump.get("umpire", "Unknown"),
                "ump_k_pct": ump.get("k_pct", 21.0),
                "ump_accuracy": ump.get("accuracy", 90.0),
                "pro_checklist": [
                    {"factor": "Public %",    "value": f"{public_pct:.0f}%", "pass": public_pct > 70 or public_pct < 40, "threshold": ">70% or <40%"},
                    {"factor": "Bet Volume",  "value": str(game_data.get("bet_volume", 0)), "pass": game_data.get("bet_volume", 0) >= MIN_VOLUME_TRIGGER, "threshold": f">= {MIN_VOLUME_TRIGGER:,}"},
                    {"factor": "Sharp Money", "value": str(game_data.get("sharp_money_trigger", False)), "pass": game_data.get("sharp_money_trigger", False), "threshold": "Money% divergence > 10%"},
                    {"factor": "RLM",         "value": str(game_data.get("rlm_detected", False)), "pass": game_data.get("rlm_detected", False), "threshold": "Line moved vs public"},
                    {"factor": "Ump Quality", "value": f"{ump.get('accuracy', 90):.1f}%", "pass": ump.get("accuracy", 90) > 90, "threshold": "> 90% accurate"},
                ],
            },
            timestamp=datetime.utcnow().isoformat(),
        )
