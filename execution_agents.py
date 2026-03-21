"""execution_agents.py — PropIQ Analytics Execution Tier

Four independent execution agents that consume ML probability outputs and
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


# ---------------------------------------------------------------------------
# Slip Publisher
# ---------------------------------------------------------------------------

class SlipPublisher:
    """Publishes finalised slip payloads to ``alerts.discord.slips``."""

    EXCHANGE = "propiq_events"
    ROUTING_KEY = "alerts.discord.slips"

    def __init__(self, amqp_url: Optional[str] = None) -> None:
        self._amqp_url = amqp_url
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
        return True

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _format_payload(
        self,
        legs: List[PropEdge],
        slip_eval: SlipEvaluation,
    ) -> SlipPayload:
        """Serialise a validated slip into the standard Discord-bound JSON.

        Args:
            legs:      Validated list of :class:`PropEdge` objects.
            slip_eval: :class:`SlipEvaluation` from
                       :meth:`UnderdogMathEngine.evaluate_slip`.

        Returns:
            Slip payload conforming to the Discord dispatcher schema.
            The ``recommended_entry_type`` field ("FLEX" or "STANDARD") drives
            the badge rendered by the Discord dispatcher.
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

    Strategy:
        Pure expected-value maximisation — no filters on Over/Under bias,
        prop category, or signal source.  The EVHunter's job is to find
        the single most profitable combination available on any given day.
    """

    TOP_N: int = 10

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
        return eligible[: self.TOP_N]


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

    Inbound bindings:
        - ``alerts.market_edges``  — LineValue/Steam/Fade scanner output
        - ``mlb.projections.*``    — ML Engine calibrated probability payloads

    Processing model:
        Messages are consumed and buffered into an in-memory prop pool.
        The pool is flushed to all four agents every ``FLUSH_EVERY_N``
        messages or every ``FLUSH_EVERY_S`` seconds, whichever comes first.
        This batching model reduces redundant combination generation while
        keeping latency under control.

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
        )
