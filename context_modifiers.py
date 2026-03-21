"""context_modifiers.py — PropIQ Analytics Context Modifier Layer

Three feature-generation classes that calculate environmental multipliers
for a given day's MLB slate.  Their combined output is a Pandas DataFrame
indexed by game_id that is merged directly into the XGBoost ML Engine's
feature matrix before inference.

These classes do NOT place bets or evaluate edges.  They are pure
feature engineering utilities consumed by ml_pipeline.py.

Modifiers:
    BullpenFatigueScorer  – Rolling pitch-count fatigue index (0–4 scale)
    WeatherParkAdjuster   – Park factor × temperature × wind multiplier
    UmpireRunEnvironment  – Strike-zone tightness K/BB rate modifiers
    ModifierOrchestrator  – Aggregates all three into one daily DataFrame
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Modifier 1 — BullpenFatigueScorer
# ---------------------------------------------------------------------------

class BullpenFatigueScorer:
    """Calculates a normalised bullpen fatigue index from recent pitching logs.

    Fatigue Index Scale (0.0 – 4.0):
        0.0 – Fully rested: no appearances in 2+ days, low pitch count
        1.0 – Normal workload: one appearance in 3 days, < 30 pitches
        2.0 – Moderate fatigue: 2 appearances in 3 days or > 40 total pitches
        3.0 – Heavy fatigue: 3 consecutive days or > 60 pitches in 3 days
        4.0 – Exhausted: 3 consecutive days + > 80 pitches (near arm limit)

    A higher fatigue index raises the expected offensive value of opposing
    batters (tired relievers yield more runs) and is merged as a feature
    into the ML Engine's inference step.

    Formula:
        base_score  = min(pitch_count_3d / PITCH_LIMIT_3D, 1.0)
                      Ratio of 3-day pitch count to the exhaustion threshold.
        rest_penalty = max(0, 1 - avg_rest_days / REST_FULL)
                      Penalises arms that haven't had adequate recovery.
        fatigue_raw  = (base_score + rest_penalty) / 2.0
        fatigue_idx  = fatigue_raw * 4.0            (scale to 0–4)

    Constants:
        PITCH_LIMIT_3D = 90  pitches — 3-day budget before exhaustion risk
        PITCH_LIMIT_5D = 150 pitches — 5-day extended load budget
        REST_FULL      = 2   days   — minimum rest to consider arm "fresh"
    """

    PITCH_LIMIT_3D: float = 90.0
    PITCH_LIMIT_5D: float = 150.0
    REST_FULL: float = 2.0

    def score(
        self,
        pitching_logs: pd.DataFrame,
        target_date: date,
    ) -> pd.DataFrame:
        """Calculate the fatigue index for every team in the pitching log.

        Args:
            pitching_logs: DataFrame with required columns:
                - ``team_id``     (str)   — team abbreviation
                - ``pitcher_id``  (str)   — unique pitcher identifier
                - ``game_date``   (str/datetime) — date of the appearance
                - ``pitch_count`` (int)   — pitches thrown in that appearance
                - ``rest_days``   (int)   — days since pitcher's last appearance
            target_date: The game-slate date to generate modifiers for.
                         Only appearances strictly before this date count.

        Returns:
            DataFrame with columns:
                - ``team_id``         (str)
                - ``pitch_count_3d``  (float) — 3-day bullpen pitch total
                - ``pitch_count_5d``  (float) — 5-day bullpen pitch total
                - ``avg_rest_days``   (float) — average rest across used arms
                - ``fatigue_index``   (float) — normalised score 0–4
        """
        df = pitching_logs.copy()
        df["game_date"] = pd.to_datetime(df["game_date"]).dt.date

        cutoff_3d = target_date - timedelta(days=3)
        cutoff_5d = target_date - timedelta(days=5)

        recent_3d = df[(df["game_date"] >= cutoff_3d) & (df["game_date"] < target_date)]
        recent_5d = df[(df["game_date"] >= cutoff_5d) & (df["game_date"] < target_date)]

        grp_3d = (
            recent_3d.groupby("team_id")["pitch_count"]
            .sum()
            .rename("pitch_count_3d")
        )
        grp_5d = (
            recent_5d.groupby("team_id")["pitch_count"]
            .sum()
            .rename("pitch_count_5d")
        )
        grp_rest = (
            recent_3d.groupby("team_id")["rest_days"]
            .mean()
            .rename("avg_rest_days")
        )

        summary = pd.concat([grp_3d, grp_5d, grp_rest], axis=1).fillna(0.0)
        summary["fatigue_index"] = summary.apply(
            lambda row: self._compute_fatigue(
                pitch_count_3d=row["pitch_count_3d"],
                avg_rest_days=row["avg_rest_days"],
            ),
            axis=1,
        )

        logger.debug(
            "BullpenFatigueScorer: scored %d teams for %s", len(summary), target_date
        )
        return summary.reset_index()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_fatigue(self, pitch_count_3d: float, avg_rest_days: float) -> float:
        """Apply the fatigue formula and return a clamped 0–4 index.

        Args:
            pitch_count_3d: Total bullpen pitches over the last 3 days.
            avg_rest_days:  Mean rest days across pitchers with recent appearances.

        Returns:
            Fatigue index clamped to [0.0, 4.0].
        """
        base_score = min(pitch_count_3d / self.PITCH_LIMIT_3D, 1.0)
        rest_penalty = max(0.0, 1.0 - avg_rest_days / self.REST_FULL)
        fatigue_raw = (base_score + rest_penalty) / 2.0
        return round(min(fatigue_raw * 4.0, 4.0), 3)


# ---------------------------------------------------------------------------
# Modifier 2 — WeatherParkAdjuster
# ---------------------------------------------------------------------------

@dataclass
class GameEnvironment:
    """Environmental inputs for a single game.

    Attributes:
        game_id:          Unique game identifier.
        team_id:          Home team abbreviation.
        stadium:          Stadium name (for logging/display).
        park_factor:      Park run-scoring multiplier. 1.0 = league neutral;
                          > 1.0 = hitter-friendly; < 1.0 = pitcher-friendly.
        temperature_f:    Game-time temperature in degrees Fahrenheit.
        wind_speed_mph:   Wind speed in miles per hour.
        wind_direction:   Descriptive direction: "out", "in", "cross", "calm",
                          or "variable".
        is_dome:          True if the stadium is fully enclosed (weather
                          modifiers are bypassed; only park_factor applies).
    """

    game_id: str
    team_id: str
    stadium: str
    park_factor: float
    temperature_f: float
    wind_speed_mph: float
    wind_direction: str
    is_dome: bool = False


class WeatherParkAdjuster:
    """Calculates a composite run-environment multiplier for each game.

    Component multipliers:
        m_park  = park_factor
                  Provided directly from Park Factor databases (e.g., 1.08 for
                  Coors Field, 0.94 for T-Mobile Park).

        m_temp  = 1 + (temperature_f − TEMP_BASELINE) × TEMP_COEFF
                  Each °F above the 72°F baseline adds ~0.2% to expected run
                  scoring; colder temperatures reduce it proportionally.
                  Warm air is less dense → balls carry farther.

        m_wind  = 1 + wind_speed_mph × WIND_COEFF × wind_sign
                  Wind direction determines the sign:
                      "out"  → +1.0  (ball aided, multiplier > 1)
                      "in"   → −1.0  (ball suppressed, multiplier < 1)
                      "cross" / "calm" / "variable" → 0.0 (neutral)
                  Each mph of "out" wind adds ~0.5% to expected scoring.

        run_environment_multiplier = m_park × m_temp × m_wind

    Dome handling:
        When ``is_dome=True``, temperature and wind modifiers are set to 1.0
        so only the park factor applies.

    Practical examples:
        Wrigley Field, 88°F, 15 mph out:
            m_park=1.08, m_temp=1.032, m_wind=1.075 → total ≈ 1.198
        T-Mobile Park, 52°F, 12 mph in:
            m_park=0.94, m_temp=0.960, m_wind=0.940 → total ≈ 0.848
        Tropicana Field (dome):
            m_park=0.96, m_temp=1.0, m_wind=1.0    → total = 0.960

    Constants:
        TEMP_BASELINE = 72.0  (°F — approximate MLB season average)
        TEMP_COEFF    = 0.002 (0.2% per °F)
        WIND_COEFF    = 0.005 (0.5% per mph)
    """

    TEMP_BASELINE: float = 72.0
    TEMP_COEFF: float = 0.002
    WIND_COEFF: float = 0.005

    _WIND_SIGN: Dict[str, float] = {
        "out": 1.0,
        "in": -1.0,
        "cross": 0.0,
        "calm": 0.0,
        "variable": 0.0,
    }

    def adjust(self, environments: List[GameEnvironment]) -> pd.DataFrame:
        """Calculate run-environment multipliers for a list of games.

        Args:
            environments: :class:`GameEnvironment` instances for today's slate.

        Returns:
            DataFrame with columns:
                - ``game_id``
                - ``team_id``
                - ``stadium``
                - ``m_park``
                - ``m_temp``
                - ``m_wind``
                - ``run_environment_multiplier``
        """
        records = []
        for env in environments:
            m_park, m_temp, m_wind, m_total = self._compute(env)
            records.append({
                "game_id": env.game_id,
                "team_id": env.team_id,
                "stadium": env.stadium,
                "m_park": round(m_park, 4),
                "m_temp": round(m_temp, 4),
                "m_wind": round(m_wind, 4),
                "run_environment_multiplier": round(m_total, 4),
            })
        df = pd.DataFrame(records)
        logger.debug("WeatherParkAdjuster: adjusted %d games.", len(df))
        return df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute(
        self, env: GameEnvironment
    ) -> Tuple[float, float, float, float]:
        """Compute all three component multipliers for one game.

        Args:
            env: Single game environment record.

        Returns:
            Tuple of (m_park, m_temp, m_wind, m_total).
        """
        m_park = env.park_factor

        if env.is_dome:
            m_temp = 1.0
            m_wind = 1.0
        else:
            m_temp = 1.0 + (env.temperature_f - self.TEMP_BASELINE) * self.TEMP_COEFF
            wind_sign = self._WIND_SIGN.get(env.wind_direction.lower(), 0.0)
            m_wind = 1.0 + env.wind_speed_mph * self.WIND_COEFF * wind_sign

        m_total = m_park * m_temp * m_wind
        return m_park, m_temp, m_wind, m_total


# ---------------------------------------------------------------------------
# Modifier 3 — UmpireRunEnvironment
# ---------------------------------------------------------------------------

@dataclass
class UmpireProfile:
    """Historical performance metrics for a single home plate umpire.

    All rate comparisons are expressed as deltas relative to MLB average
    (e.g., +0.08 means 8% above league average).

    Attributes:
        umpire_id:        Unique umpire identifier.
        name:             Full name for display.
        k_rate_vs_avg:    Strikeout-rate delta vs MLB average (e.g., +0.08).
        bb_rate_vs_avg:   Walk-rate delta vs MLB average (e.g., -0.03).
        run_rate_vs_avg:  Run-rate delta vs MLB average (e.g., -0.05).
    """

    umpire_id: str
    name: str
    k_rate_vs_avg: float
    bb_rate_vs_avg: float
    run_rate_vs_avg: float


class UmpireRunEnvironment:
    """Generates K-rate, walk-rate, and run-environment modifiers per game.

    Formula:
        k_rate_modifier    = 1 + umpire.k_rate_vs_avg
        walk_rate_modifier = 1 + umpire.bb_rate_vs_avg
        run_env_modifier   = 1 + umpire.run_rate_vs_avg

    Interpretation examples:
        Tight-zone umpire (k_rate_vs_avg = +0.12):
            → k_rate_modifier = 1.12 → pitcher K props get an upward boost
            → batter Under strikeout props get a downward push (pitched strike,
              not ball four → harder to draw walks)

        Wide-zone umpire (k_rate_vs_avg = −0.10):
            → k_rate_modifier = 0.90 → suppresses pitcher K Overs
            → more walks → higher run environment → Over run totals favoured

    The three modifiers are output as float columns merged into the XGBoost
    feature matrix before any inference is run.
    """

    def score(
        self,
        assignments: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        """Generate umpire modifier rows for a list of game assignments.

        Args:
            assignments: List of dicts, each containing:
                - ``game_id``       (str)
                - ``home_team_id``  (str)
                - ``away_team_id``  (str)
                - ``umpire``        (:class:`UmpireProfile`)

        Returns:
            DataFrame with columns:
                - ``game_id``
                - ``umpire_id``
                - ``umpire_name``
                - ``k_rate_modifier``
                - ``walk_rate_modifier``
                - ``run_env_modifier``
        """
        records = []
        for asgn in assignments:
            ump: UmpireProfile = asgn["umpire"]
            records.append({
                "game_id": asgn["game_id"],
                "umpire_id": ump.umpire_id,
                "umpire_name": ump.name,
                "k_rate_modifier": round(1.0 + ump.k_rate_vs_avg, 4),
                "walk_rate_modifier": round(1.0 + ump.bb_rate_vs_avg, 4),
                "run_env_modifier": round(1.0 + ump.run_rate_vs_avg, 4),
            })
        df = pd.DataFrame(records)
        logger.debug("UmpireRunEnvironment: scored %d game assignments.", len(df))
        return df


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class ModifierOrchestrator:
    """Aggregates all three daily modifiers into a single merged DataFrame.

    The resulting DataFrame is indexed by ``game_id`` with ``team_id``
    retained as a column so the ML Engine can join on either key.

    Neutral fill:
        Any game missing modifier data (e.g., umpire not yet assigned) has
        its modifier columns filled with 1.0, which is the multiplicative
        identity — no adjustment applied.

    Usage::

        orchestrator = ModifierOrchestrator()
        modifiers_df = orchestrator.generate_daily_modifiers(
            target_date=date.today(),
            pitching_logs=pitching_df,
            environments=game_environments,
            ump_assignments=ump_data,
        )
        # Merge into XGBoost feature matrix
        features_df = features_df.merge(modifiers_df, on="game_id", how="left")
    """

    def __init__(self) -> None:
        self._fatigue = BullpenFatigueScorer()
        self._weather = WeatherParkAdjuster()
        self._umpire = UmpireRunEnvironment()

    def generate_daily_modifiers(
        self,
        target_date: date,
        pitching_logs: pd.DataFrame,
        environments: List[GameEnvironment],
        ump_assignments: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        """Run all three modifiers and join results into a single DataFrame.

        Steps:
            1. Score bullpen fatigue per team.
            2. Calculate weather/park multiplier per game.
            3. Score umpire run environment per game.
            4. Join on game_id and team_id with left joins (missing = 1.0).

        Args:
            target_date:     Calendar date for today's slate.
            pitching_logs:   Raw bullpen appearance log DataFrame.
            environments:    List of :class:`GameEnvironment` objects.
            ump_assignments: List of umpire assignment dicts.

        Returns:
            Merged DataFrame with columns:
                game_id, team_id, fatigue_index, pitch_count_3d,
                avg_rest_days, run_environment_multiplier, m_park, m_temp,
                m_wind, k_rate_modifier, walk_rate_modifier, run_env_modifier,
                modifier_date.
        """
        logger.info(
            "ModifierOrchestrator: generating modifiers for %s", target_date
        )

        fatigue_df = self._fatigue.score(pitching_logs, target_date)
        weather_df = self._weather.adjust(environments)
        umpire_df = self._umpire.score(ump_assignments)

        # Build the game → team mapping from environments
        base = pd.DataFrame([
            {"game_id": e.game_id, "team_id": e.team_id}
            for e in environments
        ])

        # 1. Join fatigue on team_id
        merged = base.merge(
            fatigue_df[["team_id", "fatigue_index", "pitch_count_3d", "avg_rest_days"]],
            on="team_id",
            how="left",
        )

        # 2. Join weather on game_id
        merged = merged.merge(
            weather_df[[
                "game_id", "run_environment_multiplier",
                "m_park", "m_temp", "m_wind",
            ]],
            on="game_id",
            how="left",
        )

        # 3. Join umpire on game_id
        merged = merged.merge(
            umpire_df[[
                "game_id", "k_rate_modifier",
                "walk_rate_modifier", "run_env_modifier",
            ]],
            on="game_id",
            how="left",
        )

        # Fill missing modifier values with 1.0 (multiplicative identity)
        modifier_cols = [
            "fatigue_index", "pitch_count_3d", "avg_rest_days",
            "run_environment_multiplier", "m_park", "m_temp", "m_wind",
            "k_rate_modifier", "walk_rate_modifier", "run_env_modifier",
        ]
        for col in modifier_cols:
            if col not in merged.columns:
                merged[col] = 1.0
        merged[modifier_cols] = merged[modifier_cols].fillna(1.0)

        merged["modifier_date"] = target_date.isoformat()

        logger.info(
            "ModifierOrchestrator: produced %d rows × %d cols for %s",
            len(merged), len(merged.columns), target_date,
        )
        return merged
