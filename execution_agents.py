"""execution_agents.py — PropIQ Analytics Execution Tier

Four independent execution agents that consume ML probability outputs and
Ten independent execution agents that consume ML probability outputs and
Market Scanner alerts from RabbitMQ, filter by their specific strategy
criteria, generate Underdog Fantasy slip combinations, validate expected
value via the UnderdogMathEngine, and publish profitable slips back to the
broker for Discord delivery.

Classes:
    BaseSlipBuilder   – Combination generation, correlation filter, EV validation
    EVHunter          – Generalist: top-EV props regardless of type or side
    UnderMachine      – Specialist: all-Under contrarian slips only
    F5Agent           – First-5-innings props only (ignores bullpen data)
    MLEdgeAgent       – Pure ML calibrated probability slips (no market scanner)
    SlipPublisher     – RabbitMQ publisher to alerts.discord.slips
    ExecutionSquad    – Orchestrates all four agents on a shared consumer loop
    UmpireAgent       – Umpire K-rate and run-environment tendencies
    FadeAgent         – Contrarian fades against public consensus
    LineValueAgent    – Sharp consensus gap plays (no-vig line value)
    BullpenAgent      – Bullpen fatigue and rest-pattern exploitation
    WeatherAgent      – Wind, temperature, and park-factor adjustments
    SteamAgent        – Sharp line-movement velocity signals
    SlipPublisher     – RabbitMQ publisher to alerts.discord.slips
    ExecutionSquad    – Orchestrates all ten agents on a shared consumer loop

RabbitMQ:
    Inbound:  alerts.market_edges, mlb.projections.*
    Outbound: alerts.discord.slips
"""

from __future__ import annotations

import itertools
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pika
import pika.exceptions
from underdog_math_engine import SlipEvaluation, UnderdogMathEngine  # noqa: E402
from odds_math import MIN_EV_THRESHOLD, calculate_no_vig_ev  # noqa: E402
from underdog_math_engine import SlipEvaluation, UnderdogMathEngine  # noqa: E402
from apify_scrapers import DataEnricher  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------
SlipPayload = Dict[str, Any]
PropData = Dict[str, Any]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PropEdge:
    """A single flagged prop opportunity with its associated edge metadata.

    Attributes:
        player_id:    Unique player identifier.
        player_name:  Display name (e.g., "Aaron Judge").
        prop_type:    Stat category (e.g., "total_bases", "strikeouts").
        line:         Underdog Fantasy numeric projection line.
        side:         "Over" or "Under".
        true_prob:    Calibrated probability from ML Engine or market scanner.
        edge_pct:     Probability gap vs Underdog's implied (~0.535).
        source:       Origin of the edge signal:
                      "ml_engine" | "linevalue" | "steam" | "fade".
        is_f5:        True if the prop is scoped to the First 5 Innings.
        kelly_fraction: Pre-calculated fractional Kelly (populated downstream).
    """

    player_id: str
    player_name: str
    prop_type: str
    line: float
    side: str
    true_prob: float
    edge_pct: float
    source: str
    is_f5: bool = False
    kelly_fraction: float = 0.0

    # --- Sportsbook odds (required for true no-vig EV calculation) ---
    # Defaults to standard -110/-110 juice when live odds are unavailable.
    odds_over: float = -110.0
    odds_under: float = -110.0

    # --- UmpireAgent metadata ---
    # Called strike percentage for the assigned home-plate umpire (0.0–1.0).
    # League average is ~32.5%.  Values above/below drive directional signals.
    umpire_cs_pct: float = 0.0

    # --- FadeAgent metadata ---
    # ticket_pct: share of bets on this side as a decimal (e.g. 0.80 = 80%).
    # money_pct:  share of handle (dollar volume) on this side.
    # A large ticket_pct vs small money_pct indicates sharp money opposing
    # the public — the primary FadeAgent trigger.
    ticket_pct: float = 0.0
    money_pct: float = 0.0

    # --- BullpenAgent metadata ---
    # Normalised fatigue score from BullpenFatigueScorer (0.0 = fresh, 1.0 = exhausted).
    # Scores >= 0.70 place a bullpen in the red zone.
    fatigue_index: float = 0.0

    # --- WeatherAgent metadata ---
    # wind_speed:     Miles per hour at game time.
    # wind_direction: Direction relative to the field, e.g. "out", "in",
    #                 "out-to-cf", "in-from-cf", "crosswind".
    wind_speed: float = 0.0
    wind_direction: str = ""

    # --- SteamAgent metadata ---
    # steam_velocity:   Odds-point movement per minute (e.g. 2.5 = 2.5 pts/min).
    # steam_book_count: Number of sportsbooks that moved in the same direction
    #                   within the same 60-second window.  >= 3 books = real steam.
    steam_velocity: float = 0.0
    steam_book_count: int = 0

    # --- ArsenalAgent metadata ---
    # pitcher_arsenal_json: JSON-encoded dict mapping pitch_type label to
    #   {"usage_rate": float, "stuff_plus": float}.
    #   usage_rate is 0.0–1.0 (e.g. 0.40 = 40% slider usage).
    #   stuff_plus > 100 = above-average raw stuff on that pitch.
    # batter_whiff_json: JSON-encoded dict mapping pitch_type to whiff_rate
    #   (0.0–1.0, e.g. 0.32 = 32% swing-and-miss on breaking balls).
    pitcher_arsenal_json: str = "{}"
    batter_whiff_json: str = "{}"

    # --- PlatoonAgent metadata ---
    # wRC+ (Weighted Runs Created Plus) split by handedness of opposing pitcher.
    # 100 = league average. >100 = above average, <70 = severe weakness.
    wrc_plus_vl: float = 100.0       # batter wRC+ vs Left-Handed Pitching
    wrc_plus_vr: float = 100.0       # batter wRC+ vs Right-Handed Pitching
    wrc_plus_overall: float = 100.0  # season-long baseline (book's pricing anchor)
    batter_handedness: str = ""      # "L" | "R" | "S" (switch)
    pitcher_handedness: str = ""     # "L" | "R"
    pa_starter: float = 2.5          # projected PAs against today's starter
    pa_total: float = 4.0            # total expected PAs for the game
    p_lhp_bullpen: float = 0.30      # probability of facing an LHP reliever
    p_rhp_bullpen: float = 0.70      # probability of facing an RHP reliever
    # pinch_hit_risk (γ): 0.0–1.0 probability of late-game substitution.
    # Set to >0 when wrc_plus_vl < 70 and the bullpen is LHP-heavy.
    pinch_hit_risk: float = 0.0

    # --- CatcherAgent metadata ---
    # catcher_framing_runs: Statcast framing runs above average per 7,000 pitches.
    #   > 0 = net strike gains; > 2.0 = elite framer tier.
    # catcher_pop_time: Seconds from catch to tag at 2B. < 1.85 = elite arm.
    # pitcher_time_to_plate: Pitcher delivery time (seconds). > 1.4 = slow,
    #   giving baserunners a significant jump advantage.
    catcher_framing_runs: float = 0.0
    catcher_pop_time: float = 1.90
    pitcher_time_to_plate: float = 1.30

    # --- LineupAgent metadata ---
    # lineup_position: Confirmed batting order spot (1 = leadoff, 9 = last).
    # team_total_runs: Implied team run total derived from the game O/U + moneyline.
    # pa_average: Batter's rolling 14-day PA mean — the book's likely volume anchor.
    lineup_position: int = 5
    team_total_runs: float = 4.5
    pa_average: float = 3.8

    # --- GetawayAgent metadata ---
    # hours_rest: Hours elapsed between the team's last game and today's first pitch.
    # time_zone_change: Timezone shift in hours (e.g. 3 for East Coast → West Coast).
    # previous_game_innings: Innings played in the most recent game.
    #   > 9 = extra innings fatigue flag.
    hours_rest: float = 24.0
    time_zone_change: int = 0
    previous_game_innings: int = 9


# ---------------------------------------------------------------------------
# Slip Publisher
# ---------------------------------------------------------------------------

class SlipPublisher:
    """Publishes finalised slip payloads to ``alerts.discord.slips``."""

    EXCHANGE = "propiq_events"
    ROUTING_KEY = "alerts.discord.slips"

    def __init__(self, amqp_url: Optional[str] = None) -> None:
        self._amqp_url = amqp_url
        self._enricher: DataEnricher = DataEnricher()
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Any = None

    def connect(self) -> None:
        """Open a RabbitMQ connection and declare the topic exchange."""
        if not self._amqp_url:
            logger.warning("SlipPublisher: no AMQP URL — mock mode active.")
            return
        try:
            params = pika.URLParameters(self._amqp_url)
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=self.EXCHANGE,
                exchange_type="topic",
                durable=True,
            )
        except pika.exceptions.AMQPConnectionError as exc:
            logger.error("SlipPublisher: connection failed: %s — mock mode.", exc)

    def publish(self, payload: SlipPayload) -> None:
        """Publish a single slip payload to RabbitMQ.

        Falls back to a logged mock when the channel is unavailable.

        Args:
            payload: Standardised slip dict from any execution agent.
        """
        body = json.dumps(payload)
        if self._channel is None:
            logger.info("[MOCK SLIP] %s → %s", self.ROUTING_KEY, body)
            return
        self._channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            body=body.encode(),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        logger.info(
            "Slip published | agent=%s | legs=%d | ev=%.2f%%",
            payload.get("agent_name"),
            len(payload.get("legs", [])),
            payload.get("total_ev", 0.0) * 100,
        )

    def close(self) -> None:
        """Gracefully close the RabbitMQ connection."""
        if self._connection and not self._connection.is_closed:
            self._connection.close()


# ---------------------------------------------------------------------------
# Base Slip Builder
# ---------------------------------------------------------------------------

class BaseSlipBuilder(ABC):
    """Abstract base class providing shared slip-building infrastructure.

    All four execution agents inherit from this class and override only
    :meth:`agent_name` and :meth:`filter_props`.

    Shared capabilities:
        - :meth:`generate_combinations` — itertools.combinations 3/4/5-leg
        - :meth:`validate_correlation`  — correlation + duplicate guard
        - :meth:`build_and_publish_slips` — full filter → evaluate → publish pipeline

    Args:
        amqp_url: AMQP connection string (``None`` → mock mode).
        min_ev:   Minimum total EV threshold to publish a slip. Default 0.0.
    """

    MIN_LEGS: int = 3
    MAX_LEGS: int = 5
    MAX_POOL_SIZE: int = 20   # cap before combination count explodes

    #: Hard gate — slip is only published when the average per-leg no-vig EV
    #: (model_prob / true_no_vig_prob - 1) exceeds this threshold.
    #: Sourced from :data:`odds_math.MIN_EV_THRESHOLD` (3 %).
    MIN_NO_VIG_EV: float = MIN_EV_THRESHOLD

    def __init__(
        self,
        amqp_url: Optional[str] = None,
        min_ev: float = 0.0,
    ) -> None:
        self.min_ev = min_ev
        self.math_engine = UnderdogMathEngine()
        self.publisher = SlipPublisher(amqp_url=amqp_url)
        self.publisher.connect()

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def agent_name(self) -> str:
        """Human-readable agent identifier (used in payloads and logs)."""

    @abstractmethod
    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Apply agent-specific prop selection criteria.

        Args:
            props: Full pool of flagged props from all inbound sources.

        Returns:
            Filtered subset appropriate for this agent's strategy.
        """

    # ------------------------------------------------------------------
    # Public pipeline
    # ------------------------------------------------------------------

    def build_and_publish_slips(self, props: List[PropEdge]) -> List[SlipPayload]:
        """End-to-end pipeline: filter → combine → validate → evaluate → publish.

        Args:
            props: Raw pool of incoming :class:`PropEdge` objects.

        Returns:
            List of published slip payloads (EV > ``min_ev``).
        """
        filtered = self.filter_props(props)
        filtered = sorted(filtered, key=lambda p: p.edge_pct, reverse=True)
        filtered = filtered[: self.MAX_POOL_SIZE]

        published: List[SlipPayload] = []
        for combo in self.generate_combinations(filtered):
            if not self.validate_correlation(combo):
                continue
            leg_probs = [leg.true_prob for leg in combo]
            try:
                slip_eval: SlipEvaluation = self.math_engine.evaluate_slip(leg_probs)
            except ValueError as exc:
                logger.debug("%s: evaluate_slip skipped — %s", self.agent_name, exc)
                continue
            if slip_eval.total_ev <= self.min_ev:
                continue
            payload = self._format_payload(list(combo), slip_eval)
            self.publisher.publish(payload)
            published.append(payload)

        best_ev = max((p["total_ev"] for p in published), default=0.0)
        logger.info(
            "%s: %d props → %d slips published (best EV: %.2f%%)",

            # ------------------------------------------------------------------
            # No-vig EV gate — compare XGBoost model probability against the
            # true (vig-stripped) market probability for each leg.
            # Formula per leg: EV% = (model_prob / true_no_vig_prob) - 1
            # Gate: average per-leg EV must exceed MIN_NO_VIG_EV (3%).
            # This ensures every published slip has a genuine mathematical
            # edge versus the sharp market, not just the Underdog Pick'em line.
            # ------------------------------------------------------------------
            leg_evs: List[float] = [
                calculate_no_vig_ev(
                    leg.true_prob, leg.odds_over, leg.odds_under, leg.side
                )
                for leg in combo
            ]
            avg_leg_ev: float = sum(leg_evs) / len(leg_evs) if leg_evs else 0.0
            if avg_leg_ev < self.MIN_NO_VIG_EV:
                logger.debug(
                    "%s: combo rejected — avg no-vig EV %.2f%% < %.0f%% gate.",
                    self.agent_name,
                    avg_leg_ev * 100,
                    self.MIN_NO_VIG_EV * 100,
                )
                continue

            payload = self._format_payload(list(combo), slip_eval, leg_evs, avg_leg_ev)
            self.publisher.publish(payload)
            published.append(payload)

        best_ev = max((p["avg_leg_ev"] for p in published), default=0.0)
        logger.info(
            "%s: %d props → %d slips published (best no-vig EV: %.2f%%)",
            self.agent_name, len(filtered), len(published), best_ev * 100,
        )
        return published

    def generate_combinations(
        self,
        props: List[PropEdge],
    ) -> Iterator[Tuple[PropEdge, ...]]:
        """Yield all valid 3-leg, 4-leg, and 5-leg prop combinations.

        Iterates from the minimum (3) to the maximum (5) number of legs.
        Callers should call :meth:`validate_correlation` on each yielded
        tuple before evaluating EV.

        Args:
            props: Filtered and sorted prop list (already capped at
                   ``MAX_POOL_SIZE``).

        Yields:
            Tuples of :class:`PropEdge` objects, one per slip candidate.
        """
        for n in range(self.MIN_LEGS, self.MAX_LEGS + 1):
            yield from itertools.combinations(props, n)

    def validate_correlation(self, combo: Tuple[PropEdge, ...]) -> bool:
        """Screen out slip combinations with invalid correlations.

        Rules enforced:
            1. **Duplicate player** — the same player_id cannot appear twice.
            2. **Pitcher K Over + Batter TB/Hits Under** — these props are
               positively correlated: more strikeouts means fewer balls in
               play, which directly reduces total bases and hits.  Combining
               them in a slip overstates the true combined probability.

        Note:
            In production the duplicate-player rule is supplemented by a
            game_id check (all legs from the same game are flagged as
            highly correlated).  That check requires a game_id field on
            PropEdge and is marked TODO below.

        Args:
            combo: Candidate slip combination tuple.

        Returns:
            ``True`` if the combination passes all checks, ``False`` otherwise.
        """
        seen_players: set = set()
        has_pitcher_over_k = False
        has_batter_under_contact = False

        for leg in combo:
            # Rule 1: no duplicate players
            if leg.player_id in seen_players:
                return False
            seen_players.add(leg.player_id)

            # Rule 2: collect pitcher K Over and batter contact Under flags
            if leg.prop_type.lower() in ("strikeouts", "ks") and leg.side == "Over":
                has_pitcher_over_k = True
            if (
                leg.prop_type.lower() in ("total_bases", "hits", "singles")
                and leg.side == "Under"
            ):
                has_batter_under_contact = True

        if has_pitcher_over_k and has_batter_under_contact:
            logger.debug(
                "%s: correlation filter rejected combo (K-Over + TB/Hits-Under).",
                self.agent_name,
            )
            return False

        # TODO: add game_id-based correlation check when PropEdge carries game_id
        # game_id correlation: block if ≥4 legs from the same game.
        # Same-game parlays carry high leg-to-leg correlation that understates
        # true combined probability, especially when all props belong to the
        # same pitcher / offensive lineup.
        populated_game_ids = [leg.game_id for leg in combo if leg.game_id]
        if populated_game_ids:
            from collections import Counter  # noqa: PLC0415
            counts = Counter(populated_game_ids)
            if max(counts.values()) >= 4:
                logger.debug(
                    "%s: correlation filter rejected combo "
                    "(≥4 legs from same game_id '%s').",
                    self.agent_name,
                    max(counts, key=counts.__getitem__),
                )
                return False

        return True

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _format_payload(
        self,
        legs: List[PropEdge],
        slip_eval: SlipEvaluation,
        leg_evs: List[float],
        avg_leg_ev: float,
    ) -> SlipPayload:
        """Serialise a validated slip into the standard Discord-bound JSON.

        Args:
            legs:      Validated list of :class:`PropEdge` objects.
            slip_eval: :class:`SlipEvaluation` from
                       :meth:`UnderdogMathEngine.evaluate_slip`.
            legs:        Validated list of :class:`PropEdge` objects.
            slip_eval:   :class:`SlipEvaluation` from
                         :meth:`UnderdogMathEngine.evaluate_slip`.
            leg_evs:     Per-leg no-vig EV values (parallel list to ``legs``).
                         Calculated as ``(model_prob / true_no_vig_prob) - 1``.
            avg_leg_ev:  Mean of ``leg_evs`` — the Discord headline EV figure.

        Returns:
            Slip payload conforming to the Discord dispatcher schema.
            The ``recommended_entry_type`` field ("FLEX" or "STANDARD") drives
            the badge rendered by the Discord dispatcher.
            The ``no_vig_ev`` field on each leg and ``avg_leg_ev`` on the slip
            surface the true mathematical edge against the sharp market.
        """
        return {
            "agent_name": self.agent_name,
            # Human-readable verdict from the math engine
            # e.g. "🛡️ INSURED (FLEX) — 3-leg +6.2% EV"
            "slip_type": slip_eval.verdict,
            "recommended_entry_type": slip_eval.recommended_entry_type,
            "recommended_multiplier": slip_eval.recommended_multiplier,
            "legs": [
                {
                    "player": leg.player_name,
                    "prop": leg.prop_type,
                    "line": leg.line,
                    "side": leg.side,
                    "true_prob": round(leg.true_prob, 4),
                    "edge_pct": round(leg.edge_pct, 4),
                }
                for leg in legs
            ],
            # EV breakdown for Discord embed
            "total_ev": slip_eval.total_ev,
            "flex_ev": slip_eval.flex_ev,
            "standard_ev": slip_eval.standard_ev,
            # Probability breakdown
                    # No-vig EV for this leg: (model_prob / true_no_vig_prob) - 1
                    # This is the number shown on the Discord message per leg.
                    "no_vig_ev": round(ev, 4),
                }
                for leg, ev in zip(legs, leg_evs)
            ],
            # --- No-vig EV summary (Discord headline) ---
            # avg_leg_ev is the primary figure shown to the user.
            # It represents the average edge vs the sharp market across all legs.
            "avg_leg_ev": round(avg_leg_ev, 4),
            # --- Underdog payout EV breakdown ---
            "total_ev": slip_eval.total_ev,
            "flex_ev": slip_eval.flex_ev,
            "standard_ev": slip_eval.standard_ev,
            # --- Probability breakdown ---
            "p_all_correct": slip_eval.p_all_correct,
            "p_one_loss": slip_eval.p_one_loss,
            "p_two_loss": slip_eval.p_two_loss,
            # ½ Kelly, capped at 10 % — sourced from the math engine
            "recommended_unit_size": slip_eval.recommended_unit_size,
            "timestamp": datetime.utcnow().isoformat(),
        }


# ---------------------------------------------------------------------------
# Agent 1 — EVHunter (Generalist)
# ---------------------------------------------------------------------------

class EVHunter(BaseSlipBuilder):
    """Builds maximum-EV slips regardless of prop type, side, or source.

    Subscribes to both ML Engine calibrated probability alerts and all
    Market Scanner (LineValue, Steam, Fade) alerts.  Selects the top-N
    props by edge percentage across the entire MLB slate and constructs
    every valid 3/4/5-leg combination.
    Market Scanner (LineValue, Steam, Fade) alerts.  Also ingests
    market_fusion CLV edges and dislocation-scored props from the
    multi-provider OddsFetcher pipeline.

    Prop ranking uses a composite score:
        composite = edge_pct + (dislocation_score * DISLOCATION_WEIGHT)

    This ensures props with confirmed Pinnacle/soft-book gaps are
    ranked above props with the same raw edge but no sharp consensus.

    Strategy:
        Pure expected-value maximisation — no filters on Over/Under bias,
        prop category, or signal source.  The EVHunter's job is to find
        the single most profitable combination available on any given day.
    """

    TOP_N: int = 10
    TOP_N: int              = 10
    DISLOCATION_WEIGHT: float = 0.5   # weight applied to dislocation_score bonus
    CLV_SOURCES: frozenset  = frozenset({
        "market_fusion", "linevalue", "dislocation", "arbitrage",
    })

    @property
    def agent_name(self) -> str:
        return "EVHunter"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Select the top-N props by edge percentage.

        Args:
            props: Full unfiltered prop pool.

        Returns:
            Top :attr:`TOP_N` props ranked by ``edge_pct`` descending,
            requiring positive edge.
        """
        eligible = [p for p in props if p.edge_pct > 0]
        eligible.sort(key=lambda p: p.edge_pct, reverse=True)
    def _composite_score(self, p: "PropEdge") -> float:
        """Composite ranking score blending edge_pct + CLV dislocation bonus."""
        dis_score = getattr(p, "dislocation_score", 0.0) or 0.0
        return p.edge_pct + (dis_score * self.DISLOCATION_WEIGHT)

    def filter_props(self, props: List["PropEdge"]) -> List["PropEdge"]:
        """
        Select the top-N props by composite EV score.

        Promotes props from CLV-sourced feeds (market_fusion, linevalue,
        dislocation) with a dislocation_score bonus on top of raw edge_pct.

        Args:
            props: Full unfiltered prop pool from all inbound sources.

        Returns:
            Top :attr:`TOP_N` props ranked by composite score descending,
            requiring positive edge.
        """
        eligible = [p for p in props if p.edge_pct > 0]
        eligible.sort(key=self._composite_score, reverse=True)
        return eligible[: self.TOP_N]


# ---------------------------------------------------------------------------
# Agent 1b — ArbitrageAgent (True Cross-Book Arbitrage)
# ---------------------------------------------------------------------------

class ArbitrageAgent(BaseSlipBuilder):
    """
    Identifies and builds slips from true cross-book arbitrage opportunities.

    Consumes PropEdges with ``source == "arbitrage"`` from the
    MarketFusionEngine.arbitrage_scan() pipeline.  These are props where
    the best available Over on one book + best Under on another book
    combine to a total implied probability < 1.0 — a guaranteed edge
    regardless of outcome.

    Filters:
        - source == "arbitrage"
        - arb_margin ≥ ARB_GATE (0.5 % guaranteed return)
        - min_providers ≥ 2 (confirmed cross-book, not a data artefact)

    Slip construction:
        - MAX_LEGS = 3  (keeps arb slips tight; wider slips dilute the edge)
        - Builds Over legs only; the guaranteed margin is captured on the
          over side where the dislocation is largest.
    """

    MAX_LEGS: int  = 3
    ARB_GATE: float = 0.005   # 0.5 % minimum guaranteed margin

    @property
    def agent_name(self) -> str:
        return "ArbitrageAgent"

    def filter_props(self, props: List["PropEdge"]) -> List["PropEdge"]:
        """
        Filter to confirmed arbitrage edges with positive margin.

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            Arbitrage props sorted by ``arb_margin`` descending.
        """
        arb_props = [
            p for p in props
            if getattr(p, "source", "") == "arbitrage"
            and getattr(p, "arb_margin", 0.0) >= self.ARB_GATE
            and len(getattr(p, "providers_sampled", [])) >= 2
        ]
        arb_props.sort(
            key=lambda p: getattr(p, "arb_margin", 0.0),
            reverse=True,
        )
        return arb_props


# ---------------------------------------------------------------------------
# Agent 2 — UnderMachine (All-Under Specialist)
# ---------------------------------------------------------------------------

class UnderMachine(BaseSlipBuilder):
    """Builds all-Under contrarian slips to exploit public Over bias.

    The general betting public overwhelmingly favours Overs on player props,
    particularly for high-profile players.  This systematic bias inflates
    Over prices and leaves the Under side undervalued at Underdog Fantasy.

    Strategy:
        Strict Under filter — any prop with ``side == "Over"`` is discarded
        regardless of its edge percentage.  The resulting all-Under slips
        offer diversified exposure to the structural market inefficiency.
    """

    @property
    def agent_name(self) -> str:
        return "UnderMachine"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain only Under-side props with a positive edge.

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            Under props sorted by ``edge_pct`` descending.
        """
        under_props = [
            p for p in props
            if p.side.lower() == "under" and p.edge_pct > 0
        ]
        under_props.sort(key=lambda p: p.edge_pct, reverse=True)
        return under_props


# ---------------------------------------------------------------------------
# Agent 3 — F5Agent (First-5-Innings Specialist)
# ---------------------------------------------------------------------------

class F5Agent(BaseSlipBuilder):
    """Builds slips exclusively from First-5-Innings scoped props.

    F5 props are isolated to the starting pitcher matchup, eliminating the
    variance introduced by late-game bullpen usage.  This produces a cleaner
    signal because the starting pitching matchup is known well in advance and
    the stat line is completed after 5 innings regardless of the final score.

    Strategy:
        Accepts only props where ``is_f5 == True``.  Bullpen fatigue data is
        irrelevant and its correlation check is bypassed in the overridden
        :meth:`validate_correlation`.
    """

    @property
    def agent_name(self) -> str:
        return "F5Agent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain only F5-flagged props with positive edge.

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            F5 props sorted by ``edge_pct`` descending.
        """
        f5_props = [p for p in props if p.is_f5 and p.edge_pct > 0]
        f5_props.sort(key=lambda p: p.edge_pct, reverse=True)
        return f5_props

    def validate_correlation(self, combo: Tuple[PropEdge, ...]) -> bool:
        """F5-specific override: skip pitcher K / batter contact check.

        F5 props do not involve relief pitchers, so the standard
        K-Over + TB-Under correlation is not applicable.  Only the
        duplicate-player guard is enforced.

        Args:
            combo: Candidate F5 slip combination.

        Returns:
            ``True`` if no player appears twice, ``False`` otherwise.
        """
        seen = {leg.player_id for leg in combo}
        return len(seen) == len(combo)


# ---------------------------------------------------------------------------
# Agent 4 — MLEdgeAgent (Pure Quant)
# ---------------------------------------------------------------------------

class MLEdgeAgent(BaseSlipBuilder):
    """Builds slips based solely on XGBoost calibrated probability output.

    Ignores all Market Scanner alerts (LineValue, Steam, Fade) entirely.
    Trusts the ML Engine's probability estimates even when sharp books
    disagree.  This agent is designed to capture statistical alpha that
    exists in inefficiencies the sharp market has not yet incorporated.

    Strategy:
        Only ``source == "ml_engine"`` props are considered.  An additional
        minimum probability threshold (:attr:`ML_MIN_PROB`) ensures only
        predictions where the model has high confidence are included.
    """

    ML_MIN_PROB: float = 0.55   # Minimum calibrated probability to consider

    @property
    def agent_name(self) -> str:
        return "MLEdgeAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain only high-confidence ML Engine props.

        Filters to:
            1. ``source == "ml_engine"``
            2. ``true_prob >= ML_MIN_PROB``
            3. Positive ``edge_pct``

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            ML-only props sorted by ``edge_pct`` descending.
        """
        ml_props = [
            p for p in props
            if p.source == "ml_engine"
            and p.true_prob >= self.ML_MIN_PROB
            and p.edge_pct > 0
        ]
        ml_props.sort(key=lambda p: p.edge_pct, reverse=True)
        return ml_props


# ---------------------------------------------------------------------------
# Execution Squad — Shared RabbitMQ Consumer
# ---------------------------------------------------------------------------

class ExecutionSquad:
    """Orchestrates all four agents on a shared RabbitMQ consumer loop.
# Agent 5 — UmpireAgent (Umpire Tendencies)
# ---------------------------------------------------------------------------

class UmpireAgent(BaseSlipBuilder):
    """Builds slips based on umpire K-rate and run-environment tendencies.

    Umpires materially affect game outcomes through their strike-zone bias.
    A tight-zone umpire inflates pitcher strikeout numbers and suppresses
    run scoring; a wide-zone umpire does the opposite.  The Context Modifier
    tier publishes ``k_rate_modifier`` and ``run_environment_multiplier``
    features that are baked into ML Engine probability outputs for umpire-
    sensitive prop types.

    Strategy:
        Selects props from the umpire context signal (``source == "umpire"``)
        plus any ML Engine props whose type is directly umpire-sensitive:
        strikeouts, walks, and earned runs.  Requires ``true_prob >= 0.54``
        to avoid marginal umpire signals.
    """

    UMPIRE_PROP_TYPES: frozenset = frozenset({
        "strikeouts", "ks", "walks", "bb", "earned_runs", "er",
    })
    MIN_PROB: float = 0.54
    PITCHER_ZONE_PROPS: frozenset = frozenset({"strikeouts", "ks"})
    HITTER_ZONE_PROPS: frozenset = frozenset({"walks", "bb"})
    TOTAL_PROPS: frozenset = frozenset({"earned_runs", "er", "total", "runs_scored"})
    MIN_PROB: float = 0.54
    #: League average called-strike rate (~2024 MLB season).
    LEAGUE_AVG_CS_PCT: float = 0.325
    #: Umpire CS% must be this much ABOVE league avg to qualify as pitcher zone.
    CS_PITCHER_THRESHOLD: float = 0.020
    #: Umpire CS% must be this much BELOW league avg to qualify as hitter zone.
    CS_HITTER_THRESHOLD: float = 0.020

    @property
    def agent_name(self) -> str:
        return "UmpireAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain umpire-source props and umpire-sensitive ML props.

        Criteria:
            1. ``source == "umpire"`` — explicit umpire context signal.
            2. ``source == "ml_engine"`` + ``prop_type`` is strikeout, walk,
               or earned-run — umpire environment already baked into the
               XGBoost probability.
            3. ``true_prob >= MIN_PROB`` and positive ``edge_pct``.
        """Retain umpire-relevant props with directional zone validation.

        Strategy mirrors the Environment Dictator spec:

        * **Pitcher's umpire** (CS% > league avg + threshold):
          Flag ``Over`` Strikeouts and ``Under`` Earned Runs / game total.
          A tight zone drives more Ks and suppresses run scoring.

        * **Hitter's umpire** (CS% < league avg - threshold):
          Flag ``Over`` Walks and ``Over`` Earned Runs / game total.
          A permissive zone inflates walks and run environment.

        * **No CS% data** (``umpire_cs_pct == 0.0``):
          Accept any explicitly tagged ``source == "umpire"`` prop —
          the scanner upstream has already validated the signal.

        Criteria:
            1. Source is ``"umpire"`` or ML on umpire-sensitive prop type.
            2. ``true_prob >= MIN_PROB``.
            3. Directional match between umpire zone and prop side.

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            Umpire-relevant props sorted by ``edge_pct`` descending.
        """
        eligible = [
            p for p in props
            if p.edge_pct > 0
            and p.true_prob >= self.MIN_PROB
            and (
                p.source == "umpire"
                or (
                    p.source == "ml_engine"
                    and p.prop_type.lower() in self.UMPIRE_PROP_TYPES
                )
            )
        ]
        eligible: List[PropEdge] = []
        for p in props:
            if p.edge_pct <= 0 or p.true_prob < self.MIN_PROB:
                continue
            is_umpire_src = p.source == "umpire"
            is_ml_umpire = (
                p.source == "ml_engine"
                and p.prop_type.lower() in self.UMPIRE_PROP_TYPES
            )
            if not (is_umpire_src or is_ml_umpire):
                continue

            prop_lower = p.prop_type.lower()
            cs_delta = p.umpire_cs_pct - self.LEAGUE_AVG_CS_PCT

            if p.umpire_cs_pct == 0.0:
                # No CS% data from upstream — accept source-tagged umpire props
                if is_umpire_src:
                    eligible.append(p)
            elif cs_delta >= self.CS_PITCHER_THRESHOLD:
                # Pitcher's zone: Over Ks are valuable; Under totals/ER are valuable
                if prop_lower in self.PITCHER_ZONE_PROPS and p.side == "Over":
                    eligible.append(p)
                elif prop_lower in self.TOTAL_PROPS and p.side == "Under":
                    eligible.append(p)
            elif cs_delta <= -self.CS_HITTER_THRESHOLD:
                # Hitter's zone: Over BBs are valuable; Over totals/ER are valuable
                if prop_lower in self.HITTER_ZONE_PROPS and p.side == "Over":
                    eligible.append(p)
                elif prop_lower in self.TOTAL_PROPS and p.side == "Over":
                    eligible.append(p)
            # Props outside zone threshold are rejected

        eligible.sort(key=lambda p: p.edge_pct, reverse=True)
        return eligible


# ---------------------------------------------------------------------------
# Agent 6 — FadeAgent (Contrarian Public Fades)
# ---------------------------------------------------------------------------

class FadeAgent(BaseSlipBuilder):
    """Builds contrarian slips by fading heavily backed public selections.

    The FadeScanner in Tier 2 identifies props where public betting
    percentage diverges significantly from sharp consensus probability.
    Heavy public action inflates the Over price and creates value on the
    Under side.  FadeAgent harvests these structural mispricings.

    Strategy:
        Only ``source == "fade"`` props — explicit FadeScanner output.
        Accepts a slightly lower probability floor (``0.52``) because
        contrarian edges are inherently smaller but highly uncorrelated
        with other agents' slips, providing portfolio diversification.
    """

    MIN_PROB: float = 0.52
    #: Minimum discrepancy between public ticket% and sharp money% to trigger.
    #: e.g. 80% tickets / 40% money = 0.40 delta — sharp fade signal.
    FADE_SIGNAL_THRESHOLD: float = 0.40

    @property
    def agent_name(self) -> str:
        return "FadeAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain only FadeScanner contrarian props.
        """Retain contrarian props where public tickets diverge from sharp money.

        Core contrarian gauge logic:
            The general betting public overwhelmingly favours Overs on star
            players.  This inflates the Over price.  When retail ticket count
            is high but actual dollar volume (sharp money) is low on the same
            side, large well-capitalised bettors are taking the other side.

        Signal definition:
            ``fade_signal = ticket_pct - money_pct``
            e.g. 80% tickets, 40% money → delta = 0.40 → sharp fade confirmed.

        Gate:
            Only props where ``fade_signal >= FADE_SIGNAL_THRESHOLD`` (40%)
            are accepted.  If no ticket/money data is available
            (``ticket_pct == money_pct == 0.0``), the explicit ``source``
            tag from the FadeScanner is trusted directly.

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            Fade props with ``edge_pct > 0`` and ``true_prob >= 0.52``,
            sorted by ``edge_pct`` descending.
        """
        fade_props = [
            p for p in props
            if p.source == "fade"
            and p.edge_pct > 0
            and p.true_prob >= self.MIN_PROB
        ]
        fade_props.sort(key=lambda p: p.edge_pct, reverse=True)
            Contrarian fade props sorted by ``fade_signal`` descending
            (strongest divergence first).
        """
        fade_props: List[PropEdge] = []
        for p in props:
            if p.source != "fade" or p.edge_pct <= 0 or p.true_prob < self.MIN_PROB:
                continue
            fade_signal = p.ticket_pct - p.money_pct
            if fade_signal >= self.FADE_SIGNAL_THRESHOLD:
                fade_props.append(p)
            elif p.ticket_pct == 0.0 and p.money_pct == 0.0:
                # No ticket/money data from upstream — trust explicit source tag
                fade_props.append(p)
        fade_props.sort(key=lambda p: (p.ticket_pct - p.money_pct), reverse=True)
        return fade_props


# ---------------------------------------------------------------------------
# Agent 7 — LineValueAgent (Sharp Consensus Gap)
# ---------------------------------------------------------------------------

class LineValueAgent(BaseSlipBuilder):
    """Builds slips from no-vig sharp consensus vs Underdog Fantasy gaps.

    The LineValueScanner compares Underdog Fantasy's implied probability
    (approximately 53.5% on a Pick'em) against the no-vig consensus
    probability derived from multiple sharp sportsbooks.  A gap of ≥3%
    represents a structural mispricing that persists over large sample sizes.

    Strategy:
        Only ``source == "linevalue"`` props with an explicit minimum
        gap threshold (:attr:`MIN_EDGE_PCT`).  This is the most direct
        mathematical edge available — pure price discovery, no narrative.
    """

    MIN_EDGE_PCT: float = 0.03   # 3 % minimum no-vig gap

    @property
    def agent_name(self) -> str:
        return "LineValueAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain LineValueScanner props with a minimum 3% edge gap.

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            LineValue props sorted by ``edge_pct`` descending.
        """
        lv_props = [
            p for p in props
            if p.source == "linevalue"
            and p.edge_pct >= self.MIN_EDGE_PCT
        ]
        lv_props.sort(key=lambda p: p.edge_pct, reverse=True)
        return lv_props


# ---------------------------------------------------------------------------
# Agent 8 — BullpenAgent (Bullpen Fatigue & Rest Patterns)
# ---------------------------------------------------------------------------

class BullpenAgent(BaseSlipBuilder):
    """Builds slips exploiting bullpen fatigue and rest-pattern signals.

    The BullpenFatigueScorer in Tier 3 (Context Modifiers) measures relief
    pitcher workloads over the prior 3-7 days and generates a ``fatigue_index``
    feature that depresses run-prevention ability for tired bullpens.  This
    creates value on Over props for batters facing fatigued relievers in the
    late innings.

    Strategy:
        Selects ``source == "bullpen"`` props — signals explicitly tagged by
        the BullpenFatigueScorer — plus ML Engine props on run-production
        categories where fatigue modifiers meaningfully inflate the probability.
        Excludes all F5-scoped props (bullpen data is irrelevant for F5).
    """

    BULLPEN_SENSITIVE_PROPS: frozenset = frozenset({
        "total_bases", "rbi", "runs_scored", "hits", "home_runs",
        "earned_runs", "hits_allowed",
    })
    MIN_PROB: float = 0.54
    #: Normalised fatigue score threshold for the "red zone".
    #: Scores >= 0.70 indicate the top bullpen arms are overworked.
    FATIGUE_RED_ZONE: float = 0.70

    @property
    def agent_name(self) -> str:
        return "BullpenAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain bullpen-source props and bullpen-sensitive full-game ML props.

        Criteria:
            1. ``source == "bullpen"`` — explicit fatigue signal.
            2. ``source == "ml_engine"`` + prop_type is bullpen-sensitive
               + ``is_f5 == False`` (full-game props only).
            3. ``true_prob >= MIN_PROB`` and positive ``edge_pct``.
        """Retain props where bullpen fatigue creates late-inning value.

        Rest & Usage Tracker logic:
            Tracks pitches thrown in the last 3-5 days, consecutive days
            worked, and high-leverage innings.  A ``fatigue_index`` of 0.70+
            places the bullpen in the red zone — run prevention ability drops
            significantly, creating value on opposing batter Over props and
            team total Overs in the late innings.

        Gate:
            * ``source == "bullpen"`` props with ``fatigue_index >= 0.70`` OR
              no fatigue data (upstream scanner already validated the signal).
            * ML Engine props on run-production categories where fatigue
              modifiers meaningfully inflate the probability, excluding all
              F5-scoped props (bullpen data irrelevant for First-5-innings).

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            Bullpen-relevant props sorted by ``edge_pct`` descending.
        """
        eligible = [
            p for p in props
            if p.edge_pct > 0
            and p.true_prob >= self.MIN_PROB
            and not p.is_f5
            and (
                p.source == "bullpen"
                or (
                    p.source == "ml_engine"
                    and p.prop_type.lower() in self.BULLPEN_SENSITIVE_PROPS
                )
            )
        ]
        eligible.sort(key=lambda p: p.edge_pct, reverse=True)
            Bullpen-relevant props sorted by ``fatigue_index`` descending
            (most fatigued bullpen first), then by ``edge_pct``.
        """
        eligible: List[PropEdge] = []
        for p in props:
            if p.edge_pct <= 0 or p.true_prob < self.MIN_PROB or p.is_f5:
                continue
            is_bullpen_src = p.source == "bullpen"
            is_ml_bullpen = (
                p.source == "ml_engine"
                and p.prop_type.lower() in self.BULLPEN_SENSITIVE_PROPS
            )
            if not (is_bullpen_src or is_ml_bullpen):
                continue
            # Red-zone gate: fatigue_index must be high OR data unavailable
            in_red_zone = p.fatigue_index >= self.FATIGUE_RED_ZONE
            no_fatigue_data = p.fatigue_index == 0.0
            if in_red_zone or no_fatigue_data:
                eligible.append(p)
        # Sort by fatigue severity first — the most overworked bullpens produce
        # the strongest signal; break ties by edge_pct
        eligible.sort(key=lambda p: (p.fatigue_index, p.edge_pct), reverse=True)
        return eligible


# ---------------------------------------------------------------------------
# Agent 9 — WeatherAgent (Wind / Temp / Park Factor)
# ---------------------------------------------------------------------------

class WeatherAgent(BaseSlipBuilder):
    """Builds slips driven by wind, temperature, and park-factor adjustments.

    The WeatherParkAdjuster in Tier 3 (Context Modifiers) ingests Rotowire
    weather data and park dimensions to produce a ``run_environment_multiplier``
    feature.  Stadiums with strong wind-out conditions at high temperatures
    substantially inflate home-run and total-base probabilities.  The inverse
    (wind in, cold, pitcher-friendly park) suppresses scoring.

    Strategy:
        Targets ``source == "weather"`` props plus ML Engine props on
        power/contact categories where park and weather effects are greatest.
        WeatherAgent builds both Over (wind-out) and Under (wind-in) slips,
        trusting the ML calibration to reflect the directional environment.
    """

    WEATHER_SENSITIVE_PROPS: frozenset = frozenset({
        "home_runs", "total_bases", "hits", "runs_scored",
        "singles", "doubles", "xbh",
    })
    MIN_PROB: float = 0.54
    POWER_PROPS: frozenset = frozenset({"home_runs", "total_bases", "xbh", "doubles"})
    MIN_PROB: float = 0.54
    #: Wind speed (MPH) that triggers strong wind adjustments.
    STRONG_WIND_MPH: float = 15.0
    #: Wind speed (MPH) minimum to include moderate wind-tagged props.
    MODERATE_WIND_MPH: float = 10.0
    WIND_OUT_DIRS: frozenset = frozenset({"out", "out-to-cf", "out-lf", "out-rf", "out-center"})
    WIND_IN_DIRS: frozenset = frozenset({"in", "in-from-cf", "in-lf", "in-rf", "in-center"})

    @property
    def agent_name(self) -> str:
        return "WeatherAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain weather-source props and weather-sensitive ML props.

        Criteria:
            1. ``source == "weather"`` — explicit park/weather signal.
            2. ``source == "ml_engine"`` + prop_type is weather-sensitive.
            3. ``true_prob >= MIN_PROB`` and positive ``edge_pct``.
        """Retain props where atmospheric conditions create measurable edge.

        Atmospheric Adjuster logic:
            Air density, temperature, and wind direction relative to stadium
            orientation materially affect carry distance.  A 15 mph wind
            blowing straight out to center field at Wrigley Field turns
            routine flyouts into home runs — a direct upgrade to all batter
            total-bases and home-run props.  Wind blowing in does the inverse.

        Directional gates:
            * **Wind Out >= 15 mph**: ``Over`` HR, TB, XBH, Doubles upgraded.
            * **Wind In  >= 15 mph**: ``Under`` HR, TB, XBH downgraded (value on Under).
            * **Crosswind / no direction**: trust ML calibration; include if
              prop is weather-sensitive and wind > 10 mph.
            * **No wind data** (``wind_speed == 0.0``): accept only explicit
              ``source == "weather"`` tags from the upstream scanner.

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            Weather-relevant props sorted by ``edge_pct`` descending.
        """
        eligible = [
            p for p in props
            if p.edge_pct > 0
            and p.true_prob >= self.MIN_PROB
            and (
                p.source == "weather"
                or (
                    p.source == "ml_engine"
                    and p.prop_type.lower() in self.WEATHER_SENSITIVE_PROPS
                )
            )
        ]
        eligible.sort(key=lambda p: p.edge_pct, reverse=True)
            Weather-relevant props sorted by wind speed descending,
            then by ``edge_pct``.
        """
        eligible: List[PropEdge] = []
        for p in props:
            if p.edge_pct <= 0 or p.true_prob < self.MIN_PROB:
                continue
            is_weather_src = p.source == "weather"
            is_ml_weather = (
                p.source == "ml_engine"
                and p.prop_type.lower() in self.WEATHER_SENSITIVE_PROPS
            )
            if not (is_weather_src or is_ml_weather):
                continue

            prop_lower = p.prop_type.lower()
            wind_dir = p.wind_direction.lower().replace(" ", "-")
            is_out = wind_dir in self.WIND_OUT_DIRS
            is_in = wind_dir in self.WIND_IN_DIRS

            if p.wind_speed >= self.STRONG_WIND_MPH:
                if is_out and prop_lower in self.POWER_PROPS and p.side == "Over":
                    # Strong wind out: Over HR/TB/XBH are directly upgraded
                    eligible.append(p)
                elif is_in and prop_lower in self.POWER_PROPS and p.side == "Under":
                    # Strong wind in: Under HR/TB/XBH carry significant suppression
                    eligible.append(p)
                elif not (is_out or is_in):
                    # Crosswind or unspecified — trust ML calibration
                    eligible.append(p)
            elif p.wind_speed >= self.MODERATE_WIND_MPH and is_weather_src:
                # Moderate wind with explicit weather scanner tag
                eligible.append(p)
            elif p.wind_speed == 0.0 and is_weather_src:
                # No wind data — upstream scanner already validated the signal
                eligible.append(p)

        eligible.sort(key=lambda p: (p.wind_speed, p.edge_pct), reverse=True)
        return eligible


# ---------------------------------------------------------------------------
# Agent 10 — SteamAgent (Sharp Line-Movement Velocity)
# ---------------------------------------------------------------------------

class SteamAgent(BaseSlipBuilder):
    """Builds slips based on sharp line-movement velocity signals.

    The SteamScanner in Tier 2 monitors real-time line movement across
    multiple sportsbooks and flags props where rapid movement (steam) indicates
    coordinated sharp-money action.  Steam moves are among the most reliable
    short-term signals in sports betting because they reflect well-capitalised
    bettors with strong information edges.

    Strategy:
        Only ``source == "steam"`` props with an edge gap of at least
        :attr:`MIN_EDGE_PCT` (4%).  The tighter edge floor versus other agents
        is intentional — steam moves are only worth following when the residual
        gap after line adjustment remains meaningful.  Slips are kept to 3-leg
        maximum to preserve speed-to-market before the line fully corrects.
    """

    MIN_EDGE_PCT: float = 0.04   # 4% floor — only follow strong steam
    MAX_LEGS: int = 3             # Move fast before the line corrects
    #: Odds-point movement per minute that qualifies as steam velocity.
    #: e.g. 2.5 = the line moved 2.5 points in 60 seconds.
    STEAM_VELOCITY_THRESHOLD: float = 2.0
    #: Minimum number of sportsbooks that must move in the same direction
    #: within a 60-second window to confirm a coordinated steam move.
    STEAM_BOOK_THRESHOLD: int = 3

    @property
    def agent_name(self) -> str:
        return "SteamAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Retain only SteamScanner props with a 4%+ edge gap.
        """Retain props where rapid uniform line movement signals sharp action.

        Velocity Tracker logic:
            Steam = sudden, uniform line movement across the entire marketplace
            within a 60-second window.  The SteamScanner monitors the *rate of
            change* (velocity) of odds across multiple sportsbooks.  If a prop
            moves >= 2.0 odds points per minute AND at least 3 books move in
            the same direction simultaneously, a well-capitalised betting
            syndicate has dropped a large wager, forcing books to adjust.

        Action:
            * **Chase strategy**: find a slower book that hasn't updated yet
              and take the old price — this is the primary SteamAgent use case.
            * **Signal upgrade**: high steam velocity also increases confidence
              in the ML model's probability estimate for that prop.

        Gate:
            ``steam_velocity >= 2.0 OR steam_book_count >= 3`` — either
            condition is sufficient.  If no velocity data is available
            (``steam_velocity == 0.0`` and ``steam_book_count == 0``), the
            explicit ``source == "steam"`` tag is trusted directly.

        Args:
            props: Full prop pool from all inbound sources.

        Returns:
            Steam props sorted by ``edge_pct`` descending.
        """
        steam_props = [
            p for p in props
            if p.source == "steam"
            and p.edge_pct >= self.MIN_EDGE_PCT
        ]
        steam_props.sort(key=lambda p: p.edge_pct, reverse=True)
            Steam props sorted by velocity descending (fastest-moving first),
            then by book count for tie-breaking.  Max 3 legs enforced at
            combination stage.
        """
        steam_props: List[PropEdge] = []
        for p in props:
            if p.source != "steam" or p.edge_pct < self.MIN_EDGE_PCT:
                continue
            has_velocity = p.steam_velocity >= self.STEAM_VELOCITY_THRESHOLD
            has_consensus = p.steam_book_count >= self.STEAM_BOOK_THRESHOLD
            no_data = p.steam_velocity == 0.0 and p.steam_book_count == 0
            if has_velocity or has_consensus or no_data:
                steam_props.append(p)
        # Sort by velocity first — chase the fastest-moving lines
        steam_props.sort(
            key=lambda p: (p.steam_velocity, p.steam_book_count), reverse=True
        )
        return steam_props


# ---------------------------------------------------------------------------
# Execution Squad — Shared RabbitMQ Consumer
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Agent 11 — ArsenalAgent (Pitch-Type Matchup Specialist)
# ---------------------------------------------------------------------------

class ArsenalAgent(BaseSlipBuilder):
    """Identifies strikeout and total-bases edges from pitch-arsenal matchups.

    Strategy:
        Cross-references a starting pitcher's pitch-type usage (from
        ``pitcher_arsenal_json``) against the opposing batter's whiff rates
        per pitch type (``batter_whiff_json``).

        An edge is flagged when:
          - The pitcher throws a specific pitch ≥ 35% of the time, AND
          - The batter's whiff rate against that pitch type is ≥ 28%.

        K-Over edges are generated for batters in the opposing lineup who
        can't handle the pitcher's primary pitch.  TB-Under edges are
        generated for high-chase hitters who expand their zone against the
        pitch mix.

    Target props:
        - ``strikeouts`` (Over — pitcher prop)
        - ``total_bases`` (Under — batter prop)

    Source:
        Any (ML engine output or market feed).  Arsenal data must be
        populated via the RabbitMQ message fields ``pitcher_arsenal_json``
        and ``batter_whiff_json``.

    Probability gate:
        ``true_prob >= 0.54``
    """

    # Pitch-type labels that constitute a "breaking / swing-and-miss" arsenal
    BREAKING_PITCHES: frozenset = frozenset(
        {"sweeper", "slider", "curveball", "curve", "slurve", "knuckle_curve"}
    )
    USAGE_THRESHOLD: float = 0.35   # ≥35% usage = primary pitch
    WHIFF_THRESHOLD: float = 0.28   # ≥28% whiff rate = exploitable

    @property
    def agent_name(self) -> str:
        return "ArsenalAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Keep props where the pitch-type arsenal creates a K or TB edge.

        Args:
            props: Full incoming prop pool.

        Returns:
            Filtered props with ``true_prob >= 0.54`` and a confirmed
            pitch-type matchup edge based on usage and whiff thresholds.
        """
        eligible: List[PropEdge] = []
        for prop in props:
            if prop.true_prob < 0.54:
                continue
            if prop.prop_type.lower() not in ("strikeouts", "ks", "total_bases"):
                continue
            # Parse arsenal and whiff dicts (gracefully skip if empty/invalid)
            try:
                arsenal: dict = json.loads(prop.pitcher_arsenal_json)
                whiff_rates: dict = json.loads(prop.batter_whiff_json)
            except (json.JSONDecodeError, AttributeError):
                continue
            if not arsenal or not whiff_rates:
                continue

            # Find primary pitches that both exceed usage threshold and match
            # a known weakness in the batter's whiff profile
            matchup_edge = False
            for pitch_type, pitch_data in arsenal.items():
                usage = pitch_data.get("usage_rate", 0.0)
                if usage < self.USAGE_THRESHOLD:
                    continue
                batter_whiff = whiff_rates.get(pitch_type, 0.0)
                if batter_whiff >= self.WHIFF_THRESHOLD:
                    matchup_edge = True
                    break

            if not matchup_edge:
                continue

            # K-Over: pitcher has dominant pitch vs. high-whiff lineup
            # TB-Under: batter expands zone → fewer balls in play
            if prop.prop_type.lower() in ("strikeouts", "ks") and prop.side == "Over":
                eligible.append(prop)
            elif prop.prop_type.lower() == "total_bases" and prop.side == "Under":
                eligible.append(prop)

        # Rank by highest whiff delta (best matchup first)
        eligible.sort(key=lambda p: p.edge_pct, reverse=True)
        return eligible


# ---------------------------------------------------------------------------
# Agent 12 — PlatoonAgent (Handedness Specialist)
# ---------------------------------------------------------------------------

class PlatoonAgent(BaseSlipBuilder):
    """Calculates Expected Matchup Value (EMV) from L/R handedness splits.

    Algorithm (per spec):
        EMV = (EMV_starter + EMV_bullpen) × (1 − γ)

        where:
          EMV_starter  = wRC+_{vR|vL} × (pa_starter / pa_total)
          EMV_bullpen  = (wRC+_vL × P_LHP + wRC+_vR × P_RHP)
                         × (pa_bullpen / pa_total)
          γ (gamma)    = pinch_hit_risk when wRC+_vL < 70

        Δ = EMV_Total − wRC+_overall
          Δ > +15 → OVER  (advantageous platoon spot books haven't priced)
          Δ < −15 → UNDER (exposed to weak side or high pinch-hit risk)

    Target props:
        - ``hits``, ``total_bases``, ``runs``, ``rbis`` (Over/Under)

    Probability gate:
        ``true_prob >= 0.52``
    """

    EMV_OVER_DELTA: float = 15.0    # Δ threshold for a platoon Over edge
    EMV_UNDER_DELTA: float = -15.0  # Δ threshold for a platoon Under edge
    SEVERE_PLATOON_WRC: float = 70.0  # wRC+ vs weak side below this = sub risk

    @property
    def agent_name(self) -> str:
        return "PlatoonAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Apply EMV platoon delta logic to hitting props.

        Args:
            props: Full incoming prop pool.

        Returns:
            Props where the EMV delta vs overall wRC+ exceeds ±15 points,
            confirming a meaningful handedness mismatch the books haven't
            fully priced into the line.
        """
        hitting_props = {
            "hits", "total_bases", "singles", "doubles",
            "runs", "runs_scored", "rbis", "rbi",
        }
        eligible: List[PropEdge] = []

        for prop in props:
            if prop.true_prob < 0.52:
                continue
            if prop.prop_type.lower() not in hitting_props:
                continue
            if prop.pa_total <= 0:
                continue

            # Step 1 — Starter exposure
            if prop.pitcher_handedness.upper() == "L":
                emv_starter = prop.wrc_plus_vl * (prop.pa_starter / prop.pa_total)
            else:
                # Default RHP when pitcher_handedness is empty
                emv_starter = prop.wrc_plus_vr * (prop.pa_starter / prop.pa_total)

            # Step 2 — Bullpen exposure (blended handedness)
            pa_bullpen = max(prop.pa_total - prop.pa_starter, 0.0)
            emv_bullpen = (
                prop.wrc_plus_vl * prop.p_lhp_bullpen
                + prop.wrc_plus_vr * prop.p_rhp_bullpen
            ) * (pa_bullpen / prop.pa_total)

            # Step 3 — Pinch-hit discount (γ)
            gamma = prop.pinch_hit_risk
            if prop.wrc_plus_vl < self.SEVERE_PLATOON_WRC:
                # Severe platoon weakness — elevate substitution risk floor
                gamma = max(gamma, 0.20)

            # Step 4 — Final EMV
            emv_total = (emv_starter + emv_bullpen) * (1.0 - gamma)
            delta = emv_total - prop.wrc_plus_overall

            # Step 5 — Direction gate
            if delta > self.EMV_OVER_DELTA and prop.side == "Over":
                eligible.append(prop)
            elif delta < self.EMV_UNDER_DELTA and prop.side == "Under":
                eligible.append(prop)

        eligible.sort(key=lambda p: p.edge_pct, reverse=True)
        return eligible


# ---------------------------------------------------------------------------
# Agent 13 — CatcherAgent (Framer & Cannon Specialist)
# ---------------------------------------------------------------------------

class CatcherAgent(BaseSlipBuilder):
    """Identifies K and stolen-base edges from catcher + pitcher battery metrics.

    Two distinct signals:

    1. **Framing upgrade** — An elite framing catcher (``catcher_framing_runs``
       > 2.0) paired with a wide umpire zone (``umpire_cs_pct`` > 0.345) creates
       a "double upgrade" on Pitcher Strikeout Overs.  The framer converts
       borderline pitches; the wide-zone umpire cooperates.

    2. **Stolen-base window** — A slow-to-plate pitcher (``pitcher_time_to_plate``
       > 1.4 s) combined with a weak-armed catcher (``catcher_pop_time`` > 2.0 s)
       maximises the stolen-base window for fast runners.  Flags Over Stolen
       Bases for any baserunner in the opposing lineup with a known SB threat.

    Target props:
        - ``strikeouts`` (Over — pitcher prop, framing signal)
        - ``stolen_bases`` (Over — batter prop, battery speed signal)

    Probability gate:
        ``true_prob >= 0.54``
    """

    ELITE_FRAMING_RUNS: float = 2.0   # net strike gains (Statcast)
    WIDE_ZONE_CS_PCT: float = 0.345   # umpire called-strike % threshold
    SLOW_PITCHER_THRESHOLD: float = 1.40   # seconds to plate (> = slow)
    WEAK_ARM_POP_TIME: float = 2.00   # seconds to 2B (> = weak arm)

    @property
    def agent_name(self) -> str:
        return "CatcherAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Apply battery-level framing and stolen-base signal logic.

        Args:
            props: Full incoming prop pool.

        Returns:
            K-Over and SB-Over props confirmed by catcher/pitcher battery
            metrics, with probability >= 0.54.
        """
        eligible: List[PropEdge] = []

        for prop in props:
            if prop.true_prob < 0.54:
                continue

            # ── Signal 1: Framing double-upgrade → Over Strikeouts ──────────
            if prop.prop_type.lower() in ("strikeouts", "ks") and prop.side == "Over":
                elite_framer = prop.catcher_framing_runs > self.ELITE_FRAMING_RUNS
                wide_zone = prop.umpire_cs_pct > self.WIDE_ZONE_CS_PCT
                if elite_framer:
                    # Single upgrade on framing alone; double upgrade with wide zone
                    prop_weight = 2.0 if wide_zone else 1.0
                    logger.debug(
                        "CatcherAgent: K-Over framing signal (weight=%.1f) for %s",
                        prop_weight, prop.player_name,
                    )
                    eligible.append(prop)
                    continue

            # ── Signal 2: Battery speed window → Over Stolen Bases ──────────
            if (
                prop.prop_type.lower() in ("stolen_bases", "sb", "stolen_base")
                and prop.side == "Over"
            ):
                slow_pitcher = prop.pitcher_time_to_plate > self.SLOW_PITCHER_THRESHOLD
                weak_arm = prop.catcher_pop_time > self.WEAK_ARM_POP_TIME
                if slow_pitcher and weak_arm:
                    logger.debug(
                        "CatcherAgent: SB-Over battery window for %s "
                        "(pitcher_ttp=%.2fs, catcher_pop=%.2fs)",
                        prop.player_name,
                        prop.pitcher_time_to_plate,
                        prop.catcher_pop_time,
                    )
                    eligible.append(prop)

        eligible.sort(key=lambda p: p.edge_pct, reverse=True)
        return eligible


# ---------------------------------------------------------------------------
# Agent 14 — LineupAgent (Volume & PA Projector)
# ---------------------------------------------------------------------------

class LineupAgent(BaseSlipBuilder):
    """Exploits plate-appearance volume mismatches from confirmed lineup data.

    Algorithm (per spec):
        PA_expected = 3.6 − (0.11 × lineup_position) + (0.15 × team_total_runs)
        PA_final    = PA_expected × (1 − pinch_hit_risk)

        Δ_vol = PA_final − pa_average

        Δ_vol >  +0.45 → OVER  (lineup bump / high team total = more PAs)
        Δ_vol < −0.45 → UNDER (lineup drop / low team total / sub risk)

    Example:
        Batter moved to 2-hole (position 2), team implied 5 runs:
        PA_expected = 3.6 − 0.22 + 0.75 = 4.13
        ≈ 13% chance of a 5th PA.  If pa_average was 3.5 → Δ_vol = +0.63 → OVER.

    Target props:
        - ``hits``, ``total_bases``, ``runs``, ``rbis`` (Over/Under)

    Probability gate:
        ``true_prob >= 0.52``
    """

    # Regression coefficients (league-calibrated)
    BETA_0: float = 3.6    # baseline PAs for leadoff on 0-run team
    BETA_1: float = 0.11   # PA penalty per lineup slot
    BETA_2: float = 0.15   # PA boost per implied team run
    VOL_DELTA_THRESHOLD: float = 0.45  # minimum meaningful PA delta

    @property
    def agent_name(self) -> str:
        return "LineupAgent"

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Keep props where confirmed lineup position creates a PA volume edge.

        Args:
            props: Full incoming prop pool.

        Returns:
            Over/Under hitting props confirmed by the PA projection formula,
            filtered against the 14-day pa_average baseline.
        """
        hitting_props = {
            "hits", "total_bases", "singles", "doubles",
            "runs", "runs_scored", "rbis", "rbi",
        }
        eligible: List[PropEdge] = []

        for prop in props:
            if prop.true_prob < 0.52:
                continue
            if prop.prop_type.lower() not in hitting_props:
                continue
            # Require a confirmed lineup slot (default 5 is neutral; skip)
            if prop.lineup_position == 0:
                continue

            # Step 1 — Base PA projection
            pa_expected = (
                self.BETA_0
                - (self.BETA_1 * prop.lineup_position)
                + (self.BETA_2 * prop.team_total_runs)
            )

            # Step 2 — Platoon/pinch-hit discount
            pa_final = pa_expected * (1.0 - prop.pinch_hit_risk)

            # Step 3 — Volume delta vs sportsbook anchor
            delta_vol = pa_final - prop.pa_average

            logger.debug(
                "LineupAgent: %s pos=%d PA_exp=%.2f PA_final=%.2f Δ=%.2f",
                prop.player_name, prop.lineup_position, pa_expected, pa_final, delta_vol,
            )

            if delta_vol > self.VOL_DELTA_THRESHOLD and prop.side == "Over":
                eligible.append(prop)
            elif delta_vol < -self.VOL_DELTA_THRESHOLD and prop.side == "Under":
                eligible.append(prop)

        eligible.sort(key=lambda p: p.edge_pct, reverse=True)
        return eligible


# ---------------------------------------------------------------------------
# Agent 15 — GetawayAgent (MLB Schedule Anomaly Specialist)
# ---------------------------------------------------------------------------

class GetawayAgent(BaseSlipBuilder):
    """Fades offenses in proven fatigue spots created by brutal MLB scheduling.

    Fatigue triggers (any one qualifies):
        - ``hours_rest < 14``            — sub-overnight turnaround
        - ``time_zone_change >= 2``
          AND ``hours_rest < 22``         — cross-country flight with short rest
        - ``previous_game_innings > 11`` — extra-innings exhaustion

    Severe fatigue (tighter filter — 2 conditions required):
        - ``hours_rest < 12`` (same-day turnaround), OR
        - ``time_zone_change >= 3`` AND ``hours_rest < 20``

    Target props (Under only — against the fatigued team):
        - ``hits``, ``runs``, ``rbis``, ``total_bases``

    Example:
        Team played a 14-inning Sunday Night game in New York and flies to
        LA for Monday night.  hours_rest ≈ 17, time_zone_change = 3.
        GetawayAgent flags Under Team Total / Under Hits for that lineup.

    Probability gate:
        ``true_prob >= 0.52``
    """

    # Fatigue thresholds
    SHORT_REST_HOURS: float = 14.0
    TIMEZONE_THRESHOLD: int = 2
    TIMEZONE_REST_HOURS: float = 22.0
    EXTRA_INNINGS_THRESHOLD: int = 11

    @property
    def agent_name(self) -> str:
        return "GetawayAgent"

    def _is_fatigue_spot(self, prop: PropEdge) -> bool:
        """Return True if at least one schedule-fatigue trigger fires.

        Args:
            prop: Prop edge with travel and rest metadata.

        Returns:
            True when the team is in a measurable fatigue spot.
        """
        short_rest = prop.hours_rest < self.SHORT_REST_HOURS
        cross_country = (
            prop.time_zone_change >= self.TIMEZONE_THRESHOLD
            and prop.hours_rest < self.TIMEZONE_REST_HOURS
        )
        extra_innings = prop.previous_game_innings > self.EXTRA_INNINGS_THRESHOLD
        return short_rest or cross_country or extra_innings

    def filter_props(self, props: List[PropEdge]) -> List[PropEdge]:
        """Keep Under hitting props for teams confirmed to be in fatigue spots.

        Args:
            props: Full incoming prop pool.

        Returns:
            Under props for fatigued-team batters, ranked by severity
            (shortest hours_rest first).
        """
        fatigue_props = {
            "hits", "total_bases", "singles",
            "runs", "runs_scored", "rbis", "rbi",
        }
        eligible: List[PropEdge] = []

        for prop in props:
            if prop.true_prob < 0.52:
                continue
            # Only Under props — we're fading the fatigued offense
            if prop.side != "Under":
                continue
            if prop.prop_type.lower() not in fatigue_props:
                continue
            if not self._is_fatigue_spot(prop):
                continue

            logger.debug(
                "GetawayAgent: fatigue spot — %s (rest=%.1fh tz_shift=%dh prev_inn=%d)",
                prop.player_name,
                prop.hours_rest,
                prop.time_zone_change,
                prop.previous_game_innings,
            )
            eligible.append(prop)

        # Rank by most severe fatigue (fewest rest hours first)
        eligible.sort(key=lambda p: p.hours_rest)
        return eligible


class ExecutionSquad:
    """Orchestrates all ten agents on a shared RabbitMQ consumer loop.

    Inbound bindings:
        - ``alerts.market_edges``  — LineValue/Steam/Fade scanner output
        - ``mlb.projections.*``    — ML Engine calibrated probability payloads

    Processing model:
        Messages are consumed and buffered into an in-memory prop pool.
        The pool is flushed to all four agents every ``FLUSH_EVERY_N``
        The pool is flushed to all ten agents every ``FLUSH_EVERY_N``
        messages or every ``FLUSH_EVERY_S`` seconds, whichever comes first.
        This batching model reduces redundant combination generation while
        keeping latency under control.

    Agent roster:
        1.  EVHunter       — Generalist top-EV across all sources
        2.  UnderMachine   — All-Under contrarian specialist
        3.  F5Agent        — First-5-innings props only
        4.  MLEdgeAgent    — Pure XGBoost calibrated ML signal
        5.  UmpireAgent    — Umpire K-rate and run-environment plays
        6.  FadeAgent      — Public fade contrarian plays
        7.  LineValueAgent — Sharp no-vig consensus gaps
        8.  BullpenAgent   — Bullpen fatigue and rest-pattern plays
        9.  WeatherAgent   — Wind, temperature, and park-factor plays
        10. SteamAgent     — Sharp line-movement velocity plays
        11. ArsenalAgent   — Pitch-type matchup vs batter whiff rates
        12. PlatoonAgent   — EMV wRC+ handedness split analysis
        13. CatcherAgent   — Framing runs + pop time battery analysis
        14. LineupAgent    — PA volume projection from lineup position
        15. GetawayAgent   — MLB schedule fatigue and travel anomalies

    Args:
        amqp_url: AMQP connection string (``None`` → mock mode).
    """

    EXCHANGE = "propiq_events"
    QUEUE_NAME = "execution_squad_queue"
    BINDING_KEYS: List[str] = ["alerts.market_edges", "mlb.projections.*"]
    FLUSH_EVERY_N: int = 50
    FLUSH_EVERY_S: float = 300.0  # 5 minutes

    def __init__(self, amqp_url: Optional[str] = None) -> None:
        self._amqp_url = amqp_url
        self._prop_pool: List[PropEdge] = []
        self._message_count: int = 0
        self._last_flush: float = time.time()
        self._agents: List[BaseSlipBuilder] = [
            EVHunter(amqp_url=amqp_url),
            UnderMachine(amqp_url=amqp_url),
            F5Agent(amqp_url=amqp_url),
            MLEdgeAgent(amqp_url=amqp_url),
            ArbitrageAgent(amqp_url=amqp_url),
            UnderMachine(amqp_url=amqp_url),
            F5Agent(amqp_url=amqp_url),
            MLEdgeAgent(amqp_url=amqp_url),
            UmpireAgent(amqp_url=amqp_url),
            FadeAgent(amqp_url=amqp_url),
            LineValueAgent(amqp_url=amqp_url),
            BullpenAgent(amqp_url=amqp_url),
            WeatherAgent(amqp_url=amqp_url),
            SteamAgent(amqp_url=amqp_url),
            ArsenalAgent(amqp_url=amqp_url),
            PlatoonAgent(amqp_url=amqp_url),
            CatcherAgent(amqp_url=amqp_url),
            LineupAgent(amqp_url=amqp_url),
            GetawayAgent(amqp_url=amqp_url),
        ]
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Any = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Connect to RabbitMQ and begin blocking consumption."""
        if not self._amqp_url:
            logger.warning("ExecutionSquad: no AMQP URL — mock mode.")
            return
        params = pika.URLParameters(self._amqp_url)
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self.EXCHANGE, exchange_type="topic", durable=True,
        )
        self._channel.queue_declare(queue=self.QUEUE_NAME, durable=True)
        for key in self.BINDING_KEYS:
            self._channel.queue_bind(
                exchange=self.EXCHANGE,
                queue=self.QUEUE_NAME,
                routing_key=key,
            )
        self._channel.basic_consume(
            queue=self.QUEUE_NAME,
            on_message_callback=self._on_message,
            auto_ack=False,
        )
        logger.info("ExecutionSquad: consuming from %s", self.BINDING_KEYS)
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        """Gracefully shut down all agent publishers and the consumer."""
        if self._channel:
            self._channel.stop_consuming()
        if self._connection and not self._connection.is_closed:
            self._connection.close()
        for agent in self._agents:
            agent.publisher.close()

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _on_message(
        self,
        ch: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> None:
        """Deserialise an inbound message, buffer it, and flush if needed.

        Args:
            ch:         Channel reference.
            method:     Delivery method.
            properties: AMQP message properties.
            body:       Raw JSON bytes.
        """
        try:
            data: Dict[str, Any] = json.loads(body)
            prop = self._parse_prop_edge(data)
            if prop:
                self._prop_pool.append(prop)
            self._message_count += 1
            if self._should_flush():
                self._flush_to_agents()
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            logger.error("ExecutionSquad: message parse error: %s", exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _should_flush(self) -> bool:
        """Return True if the pool should be flushed to agents now."""
        elapsed = time.time() - self._last_flush
        return (
            self._message_count >= self.FLUSH_EVERY_N
            or elapsed >= self.FLUSH_EVERY_S
        )

    def _flush_to_agents(self) -> None:
        """Distribute the current prop pool to all four agents, then clear."""
        """Distribute the current prop pool to all ten agents, then clear."""
        if not self._prop_pool:
            return
        logger.info(
            "ExecutionSquad: flushing %d props to %d agents.",
            len(self._prop_pool), len(self._agents),
        )
        for agent in self._agents:
            agent.build_and_publish_slips(self._prop_pool)
        self._prop_pool.clear()
        self._message_count = 0
        self._last_flush = time.time()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_prop_edge(data: Dict[str, Any]) -> Optional[PropEdge]:
    def _parse_prop_edge(self, data: Dict[str, Any]) -> Optional[PropEdge]:
        """Convert a raw RabbitMQ message dict into a :class:`PropEdge`.

        Required fields: ``player_id``, ``prop_type``, ``side``,
        ``true_prob``, ``edge_percentage``.

        Args:
            data: Decoded JSON from the broker.

        Returns:
            :class:`PropEdge` if the data is valid, else ``None``.
        """
        required = {"player_id", "prop_type", "side", "true_prob", "edge_percentage"}
        if not required.issubset(data.keys()):
            logger.warning(
                "ExecutionSquad: dropping message missing fields: %s",
                required - data.keys(),
            )
            return None

        raw_source: str = data.get("scanner_type", "ml_engine")

        # ── Redis enrichment: merge cache data over sparse broker fields ──
        enrichment = self._enricher.enrich(
            player_id=data.get("player_id", ""),
            player_name=data.get("player_name", ""),
            team_abbr=data.get("team_abbr", ""),
            game_id=data.get("game_id", ""),
            game_date=data.get("game_date", ""),
            pitcher_id=data.get("pitcher_id", ""),
            is_pitcher=data.get("prop_type", "").startswith("pitcher_"),
            catcher_id=data.get("catcher_id", ""),
        )
        # Broker fields take precedence: broker > enrichment > PropEdge default
        merged = {**enrichment, **{k: v for k, v in data.items() if v not in (None, "", {}, [])}}
        # Normalise scanner_type strings to short source labels
        source = (
            raw_source.lower()
            .replace("scanner", "")
            .replace("linevalue", "linevalue")
            .strip()
        ) or "ml_engine"

        return PropEdge(
            player_id=data["player_id"],
            player_name=data.get("player_name", data["player_id"]),
            prop_type=data["prop_type"],
            line=float(data.get("underdog_line", 0.0)),
            side=data["side"],
            true_prob=float(data["true_prob"]),
            edge_pct=float(data["edge_percentage"]),
            source=source,
            is_f5=bool(data.get("is_f5", False)),
            # --- Sportsbook odds for no-vig EV calculation ---
            odds_over=float(data.get("odds_over", -110.0)),
            odds_under=float(data.get("odds_under", -110.0)),
            # --- UmpireAgent ---
            umpire_cs_pct=float(data.get("umpire_cs_pct", 0.0)),
            # --- FadeAgent ---
            ticket_pct=float(data.get("ticket_pct", 0.0)),
            money_pct=float(data.get("money_pct", 0.0)),
            # --- BullpenAgent ---
            fatigue_index=float(data.get("fatigue_index", 0.0)),
            # --- WeatherAgent ---
            wind_speed=float(data.get("wind_speed", 0.0)),
            wind_direction=str(data.get("wind_direction", "")),
            # --- SteamAgent ---
            steam_velocity=float(data.get("steam_velocity", 0.0)),
            steam_book_count=int(data.get("steam_book_count", 0)),
            # --- ArsenalAgent ---
            pitcher_arsenal_json=str(data.get("pitcher_arsenal_json", "{}")),
            batter_whiff_json=str(data.get("batter_whiff_json", "{}")),
            # --- PlatoonAgent ---
            wrc_plus_vl=float(data.get("wrc_plus_vl", 100.0)),
            wrc_plus_vr=float(data.get("wrc_plus_vr", 100.0)),
            wrc_plus_overall=float(data.get("wrc_plus_overall", 100.0)),
            batter_handedness=str(data.get("batter_handedness", "")),
            pitcher_handedness=str(data.get("pitcher_handedness", "")),
            pa_starter=float(data.get("pa_starter", 2.5)),
            pa_total=float(data.get("pa_total", 4.0)),
            p_lhp_bullpen=float(data.get("p_lhp_bullpen", 0.30)),
            p_rhp_bullpen=float(data.get("p_rhp_bullpen", 0.70)),
            pinch_hit_risk=float(data.get("pinch_hit_risk", 0.0)),
            # --- CatcherAgent ---
            catcher_framing_runs=float(data.get("catcher_framing_runs", 0.0)),
            catcher_pop_time=float(data.get("catcher_pop_time", 1.90)),
            pitcher_time_to_plate=float(data.get("pitcher_time_to_plate", 1.30)),
            # --- LineupAgent ---
            lineup_position=int(data.get("lineup_position", 5)),
            team_total_runs=float(data.get("team_total_runs", 4.5)),
            pa_average=float(data.get("pa_average", 3.8)),
            # --- GetawayAgent ---
            hours_rest=float(data.get("hours_rest", 24.0)),
            time_zone_change=int(data.get("time_zone_change", 0)),
            previous_game_innings=int(data.get("previous_game_innings", 9)),
            player_id=merged["player_id"],
            player_name=merged.get("player_name", merged["player_id"]),
            prop_type=merged["prop_type"],
            line=float(merged.get("underdog_line", 0.0)),
            side=merged["side"],
            true_prob=float(merged["true_prob"]),
            edge_pct=float(merged["edge_percentage"]),
            source=source,
            is_f5=bool(merged.get("is_f5", False)),
            # --- Sportsbook odds for no-vig EV calculation ---
            odds_over=float(merged.get("odds_over", -110.0)),
            odds_under=float(merged.get("odds_under", -110.0)),
            # --- UmpireAgent ---
            umpire_cs_pct=float(merged.get("umpire_cs_pct", 0.0)),
            # --- FadeAgent ---
            ticket_pct=float(merged.get("ticket_pct", 0.0)),
            money_pct=float(merged.get("money_pct", 0.0)),
            # --- BullpenAgent ---
            fatigue_index=float(merged.get("fatigue_index", 0.0)),
            # --- WeatherAgent ---
            wind_speed=float(merged.get("wind_speed", 0.0)),
            wind_direction=str(merged.get("wind_direction", "")),
            # --- SteamAgent ---
            steam_velocity=float(merged.get("steam_velocity", 0.0)),
            steam_book_count=int(merged.get("steam_book_count", 0)),
            # --- ArsenalAgent ---
            pitcher_arsenal_json=str(merged.get("pitcher_arsenal_json", "{}")),
            batter_whiff_json=str(merged.get("batter_whiff_json", "{}")),
            # --- PlatoonAgent ---
            wrc_plus_vl=float(merged.get("wrc_plus_vl", 100.0)),
            wrc_plus_vr=float(merged.get("wrc_plus_vr", 100.0)),
            wrc_plus_overall=float(merged.get("wrc_plus_overall", 100.0)),
            batter_handedness=str(merged.get("batter_handedness", "")),
            pitcher_handedness=str(merged.get("pitcher_handedness", "")),
            pa_starter=float(merged.get("pa_starter", 2.5)),
            pa_total=float(merged.get("pa_total", 4.0)),
            p_lhp_bullpen=float(merged.get("p_lhp_bullpen", 0.30)),
            p_rhp_bullpen=float(merged.get("p_rhp_bullpen", 0.70)),
            pinch_hit_risk=float(merged.get("pinch_hit_risk", 0.0)),
            # --- CatcherAgent ---
            catcher_framing_runs=float(merged.get("catcher_framing_runs", 0.0)),
            catcher_pop_time=float(merged.get("catcher_pop_time", 1.90)),
            pitcher_time_to_plate=float(merged.get("pitcher_time_to_plate", 1.30)),
            # --- LineupAgent ---
            lineup_position=int(merged.get("lineup_position", 5)),
            team_total_runs=float(merged.get("team_total_runs", 4.5)),
            pa_average=float(merged.get("pa_average", 3.8)),
            # --- GetawayAgent ---
            hours_rest=float(merged.get("hours_rest", 24.0)),
            time_zone_change=int(merged.get("time_zone_change", 0)),
            previous_game_innings=int(merged.get("previous_game_innings", 9)),
        )
