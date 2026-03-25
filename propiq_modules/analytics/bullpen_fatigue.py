"""
BullpenFatigueScorer — 0-4 fatigue score for MLB bullpens.

Score breakdown:
  +1  Three or more relievers pitched last night (high usage)
  +1  Team bullpen pitched 8+ innings in last 2 games
  +1  Total bullpen pitch count L3 games > 150 pitches
  +1  One or more relievers with 0 days rest after 20+ pitch outing

Thresholds from production logic:
  0-1: Fresh → Starter gets less rope, hit props NEUTRAL
  2  : Moderate → small hit prop boost (+2%)
  3  : Tired → hit prop boost (+4%), starter K/9 bump
  4  : Exhausted → hit props +7%, total overs, late-inning props +10%
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RelieverData:
    name: str
    pitches_last_outing: int = 0
    pitches_l3: int = 0             # Pitches last 3 games
    days_rest: int = 1
    innings_l2: float = 0.0
    high_leverage_appearances: int = 0


@dataclass
class BullpenFatigueResult:
    score: int = 0                  # 0-4 scale
    label: str = "Fresh"
    hit_prop_boost_pct: float = 0.0
    starter_k_boost_pct: float = 0.0
    total_over_boost_pct: float = 0.0
    late_inning_boost_pct: float = 0.0
    zero_rest_relievers: int = 0
    triggers: list = field(default_factory=list)


SCORE_LABELS = {0: "Fresh", 1: "Normal", 2: "Moderate", 3: "Tired", 4: "Exhausted"}
HIT_BOOST_TABLE = {0: 0.0, 1: 0.0, 2: 2.0, 3: 4.0, 4: 7.0}
K_BOOST_TABLE = {0: 0.0, 1: 0.0, 2: 0.0, 3: 3.0, 4: 5.0}
TOTAL_BOOST_TABLE = {0: 0.0, 1: 0.0, 2: 1.0, 3: 3.0, 4: 5.0}
LATE_INNING_BOOST_TABLE = {0: 0.0, 1: 0.0, 2: 2.0, 3: 5.0, 4: 10.0}


class BullpenFatigueScorer:

    @staticmethod
    def score(relievers: list[RelieverData], team_innings_l2: float = 0.0) -> BullpenFatigueResult:
        """
        Compute fatigue score 0-4.
        relievers: list of RelieverData objects for the bullpen
        team_innings_l2: total bullpen innings pitched in last 2 games
        """
        result = BullpenFatigueResult()
        score = 0

        # ── Trigger 1: 3+ relievers pitched last night ────────────────────────
        pitched_last_night = sum(1 for r in relievers if r.pitches_last_outing > 0)
        if pitched_last_night >= 3:
            score += 1
            result.triggers.append(f"T1: {pitched_last_night} relievers pitched last night")

        # ── Trigger 2: 8+ innings in last 2 games ─────────────────────────────
        if team_innings_l2 >= 8.0:
            score += 1
            result.triggers.append(f"T2: {team_innings_l2:.1f} bullpen innings L2 games")

        # ── Trigger 3: Total pitch count L3 > 150 ─────────────────────────────
        total_pc_l3 = sum(r.pitches_l3 for r in relievers)
        if total_pc_l3 > 150:
            score += 1
            result.triggers.append(f"T3: {total_pc_l3} total bullpen pitches L3")

        # ── Trigger 4: Zero rest after 20+ pitch outing ───────────────────────
        zero_rest_relievers = [
            r for r in relievers
            if r.days_rest == 0 and r.pitches_last_outing >= 20
        ]
        result.zero_rest_relievers = len(zero_rest_relievers)
        if zero_rest_relievers:
            score += 1
            names = ", ".join(r.name for r in zero_rest_relievers[:3])
            result.triggers.append(f"T4: {len(zero_rest_relievers)} zero-rest relievers ({names})")

        score = min(score, 4)
        result.score = score
        result.label = SCORE_LABELS[score]
        result.hit_prop_boost_pct = HIT_BOOST_TABLE[score]
        result.starter_k_boost_pct = K_BOOST_TABLE[score]
        result.total_over_boost_pct = TOTAL_BOOST_TABLE[score]
        result.late_inning_boost_pct = LATE_INNING_BOOST_TABLE[score]

        if score >= 3:
            logger.info(f"[FATIGUE ALERT] Score {score}/4 — {result.label}: {result.triggers}")

        return result

    @staticmethod
    def is_fatigued(relievers: list[RelieverData], team_innings_l2: float = 0.0) -> bool:
        """Quick check: is bullpen tired (score >= 3)?"""
        result = BullpenFatigueScorer.score(relievers, team_innings_l2)
        return result.score >= 3

    @staticmethod
    def starter_extension_prob(
        reliever_fatigue_result: BullpenFatigueResult,
        starter_k9: float,
        starter_ip_projection: float,
    ) -> dict:
        """
        When bullpen is tired (score >= 3), estimate probability manager
        extends starter longer + adjust prop projections accordingly.
        """
        k_adj_pct = 0.0
        outs_adj_pct = 0.0
        extension_prob = 0.0

        if reliever_fatigue_result.score >= 3:
            if starter_k9 >= 9.0:
                k_adj_pct = 4.0  # 4% bump
                extension_prob += 0.15
            if starter_k9 >= 11.0:
                k_adj_pct = 6.0
                extension_prob += 0.10

            if starter_ip_projection >= 5.0:
                outs_adj_pct = 6.0  # Manager keeps him in
                extension_prob += 0.20
            if starter_ip_projection >= 6.0:
                outs_adj_pct = 8.0
                extension_prob += 0.10

        return {
            "extension_probability": round(min(extension_prob, 0.60), 2),
            "k_projection_adj_pct": k_adj_pct,
            "outs_projection_adj_pct": outs_adj_pct,
            "recommend": k_adj_pct > 0 or outs_adj_pct > 0,
        }

    @staticmethod
    def individual_reliever_fatigued(reliever: RelieverData) -> bool:
        """
        Individual reliever fatigue check.
        Returns True if this specific reliever should be avoided.
        """
        return (
            (reliever.pitches_l3 >= 50 and reliever.days_rest == 0) or
            (reliever.pitches_last_outing >= 30 and reliever.days_rest <= 1) or
            (reliever.high_leverage_appearances >= 4 and reliever.days_rest == 0)
        )

    @staticmethod
    def under_threshold_summary(
        result: BullpenFatigueResult,
        team: str,
        opponent: str = "",
    ) -> str:
        """
        Human-readable summary for DataHubTasklet → Redis.
        """
        bars = "█" * result.score + "░" * (4 - result.score)
        color = "🔴" if result.score >= 3 else "🟡" if result.score >= 2 else "🟢"
        summary = (
            f"{color} {team} Bullpen Fatigue [{bars}] {result.score}/4 ({result.label})"
            f" | Hit +{result.hit_prop_boost_pct:.0f}%"
            f" | Starter K +{result.starter_k_boost_pct:.0f}%"
        )
        if result.triggers:
            summary += " | " + " · ".join(result.triggers[:2])
        return summary
