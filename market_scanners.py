"""market_scanners.py — PropIQ Analytics Market Tier

Three scanners that consume a real-time RabbitMQ firehose of sportsbook odds
and compare them against Underdog Fantasy's fixed lines to identify market
inefficiencies.  No predictive stats are used; logic is purely mathematical.

Scanners:
    LineValueScanner          – Sharp consensus no-vig gap detector
    SteamScanner              – Velocity / line movement alert engine
    FadeScanner               – Contrarian / public overreaction detector
    MarketScannerOrchestrator – Unified entry point for all three scanners

RabbitMQ routing key (inbound):  *.player_props.*
RabbitMQ routing key (outbound): alerts.market_edges
"""

from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, Iterator, List, Optional, Tuple

import pika
import pika.exceptions

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------
OddsPayload = Dict[str, Any]
EdgePayload = Dict[str, Any]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class OddsTick:
    """A single odds price observation at a specific point in time.

    Attributes:
        player_id:     Unique player identifier (e.g., "aaron_judge_nyy").
        prop_type:     Prop category (e.g., "total_bases", "strikeouts").
        book:          Sportsbook name (e.g., "pinnacle", "circa").
        american_odds: American-format odds for the Over side (e.g., -140).
        timestamp:     Unix epoch seconds when this tick was observed.
    """

    player_id: str
    prop_type: str
    book: str
    american_odds: int
    timestamp: float = field(default_factory=time.time)


@dataclass
class PropSnapshot:
    """Aggregated odds state for a single prop across multiple books.

    Attributes:
        player_id:     Unique player identifier.
        prop_type:     Prop category.
        underdog_line: Underdog Fantasy's numeric projection line.
        sharp_odds:    Dict mapping book name (and side) → American odds.
                       Convention: ``"pinnacle"`` = Over odds,
                       ``"pinnacle_under"`` = Under odds.
        public_pct:    Fraction of public money on the Over side (0.0–1.0).
        timestamp:     Unix epoch seconds for snapshot time.
    """

    player_id: str
    prop_type: str
    underdog_line: float
    sharp_odds: Dict[str, int]
    public_pct: float = 0.0
    timestamp: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Probability helpers
# ---------------------------------------------------------------------------

def american_to_implied(american_odds: int) -> float:
    """Convert American odds to raw implied probability (includes vig).

    For negative odds (favourite):
        implied = |odds| / (|odds| + 100)

    For positive odds (underdog):
        implied = 100 / (odds + 100)

    Args:
        american_odds: Integer American odds (e.g., -110, +130).

    Returns:
        Raw implied probability in [0, 1].  Note: this is NOT the fair-value
        probability — it still contains the bookmaker's margin (vig).
    """
    if american_odds < 0:
        return (-american_odds) / (-american_odds + 100)
    return 100 / (american_odds + 100)


def no_vig_prob(over_american: int, under_american: int) -> Tuple[float, float]:
    """Remove the bookmaker's vig (overround) from a two-sided market.

    Standard de-vig method: divide each side's raw implied probability by the
    sum of both sides (the overround), yielding fair-value probabilities that
    sum exactly to 1.0.

    Mathematical derivation:
        raw_over   = implied_prob(over_american)
        raw_under  = implied_prob(under_american)
        overround  = raw_over + raw_under        # > 1.0 ← bookmaker's margin
        fair_over  = raw_over  / overround
        fair_under = raw_under / overround

    Example:
        Pinnacle prices a prop at -135 / +115.
        raw_over  = 135/235 ≈ 0.5745
        raw_under = 100/215 ≈ 0.4651
        overround = 1.0396  (≈ 3.96% juice)
        fair_over  = 0.5745 / 1.0396 ≈ 0.5526  (55.3%)
        fair_under = 0.4651 / 1.0396 ≈ 0.4474  (44.7%)

    Args:
        over_american:  American odds for the Over side (e.g., -135).
        under_american: American odds for the Under side (e.g., +115).

    Returns:
        Tuple (fair_over_prob, fair_under_prob) guaranteed to sum to 1.0.
    """
    raw_over = american_to_implied(over_american)
    raw_under = american_to_implied(under_american)
    overround = raw_over + raw_under
    return raw_over / overround, raw_under / overround


def underdog_baseline_prob() -> float:
    """Return Underdog Fantasy's implied per-leg probability.

    Underdog's standard Higher/Lower payout structure approximates -115 juice
    per leg, equivalent to an implied probability of ~53.5%.  This is the
    baseline the scanners compare against to measure edge.

    Returns:
        Underdog's implied probability per leg (≈ 0.5349).
    """
    return american_to_implied(-115)


# ---------------------------------------------------------------------------
# Scanner 1 — LineValueScanner
# ---------------------------------------------------------------------------

class LineValueScanner:
    """Detects gaps between sharp-book consensus and Underdog Fantasy lines.

    Workflow:
        1. Receive a :class:`PropSnapshot` containing sharp Over/Under odds.
        2. Average the no-vig probabilities across available sharp books.
        3. Compare the fair-value probability against Underdog's baseline.
        4. If the gap (sharp_fair_prob − underdog_implied_prob) ≥ ``min_edge``,
           return a standardised edge payload.

    Sharp books modelled:
        Pinnacle, Circa, Bookmaker — these books accept sharp action and have
        consistently efficient lines, making them the best proxy for true
        event probability.

    Args:
        min_edge: Minimum probability gap required to flag an edge.
                  Default 0.04 (4 percentage points).
    """

    SHARP_BOOKS: List[str] = ["pinnacle", "circa", "bookmaker"]

    def __init__(self, min_edge: float = 0.04) -> None:
        self.min_edge = min_edge

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self, snapshot: PropSnapshot) -> Optional[EdgePayload]:
        """Evaluate a single prop snapshot for a line-value edge.

        Args:
            snapshot: Aggregated odds data for one player prop.

        Returns:
            Standardised edge payload dict if an edge is found, else ``None``.
        """
        fair_over, fair_under = self._consensus_no_vig(snapshot.sharp_odds)
        if fair_over is None:
            return None

        ud = underdog_baseline_prob()

        # --- Over edge ---
        edge_over = fair_over - ud
        if edge_over >= self.min_edge:
            return self._build_payload(
                scanner_type="LineValueScanner",
                snapshot=snapshot,
                side="Over",
                sharp_implied_prob=fair_over,
                edge_percentage=edge_over,
            )

        # --- Under edge ---
        edge_under = fair_under - (1.0 - ud)
        if edge_under >= self.min_edge:
            return self._build_payload(
                scanner_type="LineValueScanner",
                snapshot=snapshot,
                side="Under",
                sharp_implied_prob=fair_under,
                edge_percentage=edge_under,
            )

        return None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _consensus_no_vig(
        self,
        sharp_odds: Dict[str, int],
    ) -> Tuple[Optional[float], Optional[float]]:
        """Average de-vigged probabilities across all available sharp books.

        The function looks for both ``"{book}"`` (Over odds) and
        ``"{book}_under"`` (Under odds) keys.  Books missing either side are
        skipped to avoid skewed consensus.

        Args:
            sharp_odds: Dict mapping book/side → American odds.

        Returns:
            Tuple (avg_fair_over, avg_fair_under) or (None, None) if no
            sharp books are represented.
        """
        fair_overs: List[float] = []
        fair_unders: List[float] = []

        for book in self.SHARP_BOOKS:
            over_key = book
            under_key = f"{book}_under"
            if over_key not in sharp_odds:
                continue
            over_odds = sharp_odds[over_key]
            under_odds = sharp_odds.get(under_key, -110)  # fallback to -110
            fo, fu = no_vig_prob(over_odds, under_odds)
            fair_overs.append(fo)
            fair_unders.append(fu)

        if not fair_overs:
            return None, None

        return sum(fair_overs) / len(fair_overs), sum(fair_unders) / len(fair_unders)

    @staticmethod
    def _build_payload(
        *,
        scanner_type: str,
        snapshot: PropSnapshot,
        side: str,
        sharp_implied_prob: float,
        edge_percentage: float,
    ) -> EdgePayload:
        """Construct the standardised edge output dict."""
        return {
            "scanner_type": scanner_type,
            "player_id": snapshot.player_id,
            "prop_type": snapshot.prop_type,
            "side": side,
            "underdog_line": snapshot.underdog_line,
            "sharp_implied_prob": round(sharp_implied_prob, 4),
            "edge_percentage": round(edge_percentage, 4),
            "timestamp": datetime.utcnow().isoformat(),
        }


# ---------------------------------------------------------------------------
# Scanner 2 — SteamScanner
# ---------------------------------------------------------------------------

class SteamScanner:
    """Detects aggressive line movement (steam) at sharp sportsbooks.

    Steam moves indicate that informed sharp money has hit a side and the
    market is rapidly repricing.  By detecting movement early, the system
    can pick off Underdog's stale projection before they adjust their lines.

    Caching strategy:
        Each (player_id, prop_type) pair maintains a bounded
        :class:`collections.deque` of :class:`OddsTick` objects covering
        the rolling ``window_seconds`` lookback.  Old ticks outside the window
        are evicted on every new ingest.

    RabbitMQ / Redis integration:
        The in-memory deque is sufficient for a single-process deployment.
        A Redis integration hook (:meth:`_seed_from_redis`) is provided for
        multi-process or cold-restart scenarios.

    Args:
        window_seconds: Rolling time window for movement measurement (default
                        600 s = 10 minutes).
        min_move_cents: Minimum American-odds shift to trigger an alert
                        (e.g., 20 means -110 → -130 qualifies).
        min_ticks:      Minimum tick history required before evaluating steam.
    """

    def __init__(
        self,
        window_seconds: int = 600,
        min_move_cents: int = 20,
        min_ticks: int = 2,
    ) -> None:
        self.window_seconds = window_seconds
        self.min_move_cents = min_move_cents
        self.min_ticks = min_ticks
        # key: (player_id, prop_type) → deque of OddsTick
        self._cache: Dict[Tuple[str, str], Deque[OddsTick]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest_tick(self, tick: OddsTick) -> Optional[EdgePayload]:
        """Process a new odds tick and evaluate for a steam alert.

        Args:
            tick: Latest price observation from the odds feed.

        Returns:
            Steam edge payload dict if a significant move is detected,
            else ``None``.
        """
        key = (tick.player_id, tick.prop_type)
        if key not in self._cache:
            self._cache[key] = deque()

        self._evict_stale_ticks(key, tick.timestamp)
        self._cache[key].append(tick)

        if len(self._cache[key]) < self.min_ticks:
            return None

        return self._evaluate_steam(key, tick)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _evict_stale_ticks(self, key: Tuple[str, str], now: float) -> None:
        """Remove ticks older than the rolling window cutoff.

        Args:
            key: Cache key (player_id, prop_type).
            now: Current Unix timestamp.
        """
        cutoff = now - self.window_seconds
        dq = self._cache[key]
        while dq and dq[0].timestamp < cutoff:
            dq.popleft()

    def _evaluate_steam(
        self,
        key: Tuple[str, str],
        latest: OddsTick,
    ) -> Optional[EdgePayload]:
        """Compare the oldest and newest ticks in the window.

        American odds interpretation:
            More negative = more expensive = higher implied probability.
            A move from -110 to -145 (delta = +35) means the Over price has
            increased sharply → steam on the Over side.

        Args:
            key:    Cache key.
            latest: The most recent tick just ingested.

        Returns:
            EdgePayload if |delta| ≥ ``min_move_cents``, else ``None``.
        """
        dq = self._cache[key]
        oldest = dq[0]
        # Positive delta → price moved more negative → Over is steaming
        delta = oldest.american_odds - latest.american_odds

        if abs(delta) < self.min_move_cents:
            return None

        direction = "Over" if delta > 0 else "Under"
        sharp_prob = american_to_implied(latest.american_odds)
        edge = max(sharp_prob - underdog_baseline_prob(), 0.0)

        logger.info(
            "STEAM | %s %s | %s | %+d cents → %s",
            latest.player_id, latest.prop_type, latest.book, delta, direction,
        )

        return {
            "scanner_type": "SteamScanner",
            "player_id": latest.player_id,
            "prop_type": latest.prop_type,
            "side": direction,
            "underdog_line": 0.0,          # enriched downstream by orchestrator
            "sharp_implied_prob": round(sharp_prob, 4),
            "edge_percentage": round(edge, 4),
            "steam_delta_cents": abs(delta),
            "window_seconds": self.window_seconds,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _seed_from_redis(
        self,
        key: Tuple[str, str],
        redis_client: Any,
    ) -> None:
        """Integration point: seed in-memory cache from a Redis sorted set.

        Call this on service startup to restore steam history across restarts.
        The Redis key convention is: ``"steam:{player_id}:{prop_type}"``,
        stored as a sorted set with score = Unix timestamp.

        Args:
            key:          Cache key (player_id, prop_type).
            redis_client: An initialised ``redis.Redis`` client instance.
        """
        # TODO: deserialise OddsTick JSON members from Redis sorted set and
        #       populate self._cache[key].
        pass
        redis_key = f"steam:{key[0]}:{key[1]}"
        try:
            raw_members = redis_client.zrangebyscore(
                redis_key, "-inf", "+inf", withscores=True
            )
            ticks: List[OddsTick] = []
            for member, score in raw_members:
                try:
                    raw = member if isinstance(member, str) else member.decode("utf-8")
                    data = json.loads(raw)
                    ticks.append(
                        OddsTick(
                            player_id=data["player_id"],
                            prop_type=data["prop_type"],
                            book=data["book"],
                            american_odds=int(data["american_odds"]),
                            timestamp=float(score),
                        )
                    )
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    continue

            if ticks:
                self._cache[key] = deque(ticks)
                logger.debug(
                    "SteamScanner: restored %d ticks for key %s from Redis.",
                    len(ticks),
                    key,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "SteamScanner._seed_from_redis failed for key %s: %s", key, exc
            )


# ---------------------------------------------------------------------------
# Scanner 3 — FadeScanner
# ---------------------------------------------------------------------------

class FadeScanner:
    """Detects public-overreaction props ripe for a contrarian Under fade.

    The scanner fires when three conditions are simultaneously true:
        1. Heavy public betting percentage on the Over (> ``public_threshold``).
        2. Sharp books price the Under at a meaningful premium (expensive Under
           odds indicate sharp money is on the Under side).
        3. The edge on the Under vs Underdog's implied probability ≥ ``min_edge``.

    Typical scenario:
        A high-profile player (Shohei Ohtani, Aaron Judge) draws 82% of public
        bets to the Over strikeouts prop.  Pinnacle, driven by sharp money, has
        the Under at -130 (≈ 56.5% no-vig).  Underdog still prices it at the
        baseline ~53.5%.  Net edge ≈ +3.0% on the Under.

    Args:
        public_threshold: Minimum public Over betting fraction to trigger scan.
                          Default 0.65 (65%).
        min_edge:         Minimum edge on the Under side. Default 0.03 (3%).
    """

    def __init__(
        self,
        public_threshold: float = 0.65,
        min_edge: float = 0.03,
    ) -> None:
        self.public_threshold = public_threshold
        self.min_edge = min_edge

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self, snapshot: PropSnapshot) -> Optional[EdgePayload]:
        """Evaluate a single prop snapshot for a public-fade opportunity.

        Args:
            snapshot: Aggregated odds data including public betting fraction.

        Returns:
            Fade edge payload if conditions are met, else ``None``.
        """
        if snapshot.public_pct < self.public_threshold:
            return None

        sharp_under_odds = self._best_sharp_under(snapshot.sharp_odds)
        if sharp_under_odds is None:
            return None

        sharp_under_prob = american_to_implied(sharp_under_odds)
        ud_under_prob = 1.0 - underdog_baseline_prob()
        edge = sharp_under_prob - ud_under_prob

        if edge < self.min_edge:
            return None

        logger.info(
            "FADE | %s %s | public_pct=%.0f%% sharp_under=%d edge=%.2f%%",
            snapshot.player_id, snapshot.prop_type,
            snapshot.public_pct * 100, sharp_under_odds, edge * 100,
        )

        return {
            "scanner_type": "FadeScanner",
            "player_id": snapshot.player_id,
            "prop_type": snapshot.prop_type,
            "side": "Under",
            "underdog_line": snapshot.underdog_line,
            "sharp_implied_prob": round(sharp_under_prob, 4),
            "edge_percentage": round(edge, 4),
            "public_pct": snapshot.public_pct,
            "timestamp": datetime.utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _best_sharp_under(self, sharp_odds: Dict[str, int]) -> Optional[int]:
        """Find the most aggressively priced Under across sharp books.

        Returns the most negative (cheapest to the house = most confident)
        Under American odds, which indicates the strongest sharp signal.

        Args:
            sharp_odds: Dict containing keys like ``"pinnacle_under"``.

        Returns:
            The most negative Under price found, or ``None`` if unavailable.
        """
        under_prices = [
            v for k, v in sharp_odds.items()
            if "under" in k.lower() and v < 0
        ]
        return min(under_prices) if under_prices else None


# ---------------------------------------------------------------------------
# RabbitMQ Publisher
# ------------------------------------------------------------------
# ---------------------------------------------------------------------------

class MarketEdgePublisher:
    """Publishes scanner edge payloads to the ``alerts.market_edges`` routing key."""

    EXCHANGE = "propiq_events"
    ROUTING_KEY = "alerts.market_edges"

    def __init__(self, amqp_url: Optional[str] = None) -> None:
        self._amqp_url = amqp_url
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Any = None

    def connect(self) -> None:
        """Establish a blocking RabbitMQ connection and declare the exchange."""
        if not self._amqp_url:
            logger.warning("MarketEdgePublisher: no AMQP URL — mock mode active.")
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
            logger.info("MarketEdgePublisher connected to RabbitMQ.")
        except pika.exceptions.AMQPConnectionError as exc:
            logger.error("MarketEdgePublisher: connection failed: %s — mock mode.", exc)

    def publish(self, payload: EdgePayload) -> None:
        """Publish a single edge payload dict to RabbitMQ.

        Falls back to a logged mock publish when the channel is unavailable
        (e.g., local development without a running broker).

        Args:
            payload: Standardised edge dict from any scanner.
        """
        body = json.dumps(payload)
        if self._channel is None:
            logger.info("[MOCK PUBLISH] %s → %s", self.ROUTING_KEY, body)
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
            "Edge published | %s %s | edge=%.2f%%",
            payload["player_id"], payload["prop_type"],
            payload["edge_percentage"] * 100,
        )

    def close(self) -> None:
        """Gracefully close the RabbitMQ connection."""
        if self._connection and not self._connection.is_closed:
            self._connection.close()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class MarketScannerOrchestrator:
    """Unified entry point that fans incoming messages to all three scanners.

    Binds to the ``*.player_props.*`` routing key on the ``propiq_events``
    topic exchange.  Prop snapshots are fanned to :class:`LineValueScanner`
    and :class:`FadeScanner`; raw odds ticks are routed to
    :class:`SteamScanner`.  Any edge found is immediately published to
    ``alerts.market_edges``.

    Args:
        amqp_url:         AMQP connection string (defaults to mock mode).
        min_line_edge:    Edge threshold for :class:`LineValueScanner`.
        steam_window:     Lookback window in seconds for :class:`SteamScanner`.
        public_threshold: Public-pct threshold for :class:`FadeScanner`.
    """

    EXCHANGE = "propiq_events"
    BINDING_KEY = "*.player_props.*"
    QUEUE_NAME = "market_scanner_queue"

    def __init__(
        self,
        amqp_url: Optional[str] = None,
        min_line_edge: float = 0.04,
        steam_window: int = 600,
        public_threshold: float = 0.65,
    ) -> None:
        self.line_scanner = LineValueScanner(min_edge=min_line_edge)
        self.steam_scanner = SteamScanner(window_seconds=steam_window)
        self.fade_scanner = FadeScanner(public_threshold=public_threshold)
        self.publisher = MarketEdgePublisher(amqp_url=amqp_url)
        self._amqp_url = amqp_url
        self._consume_connection: Optional[pika.BlockingConnection] = None
        self._consume_channel: Any = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Connect publisher + consumer, then begin blocking message loop."""
        self.publisher.connect()
        self._setup_consumer()
        logger.info("MarketScannerOrchestrator: listening on %s", self.BINDING_KEY)
        try:
            if self._consume_channel:
                self._consume_channel.start_consuming()
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        """Gracefully close all connections."""
        if self._consume_channel:
            self._consume_channel.stop_consuming()
        if self._consume_connection and not self._consume_connection.is_closed:
            self._consume_connection.close()
        self.publisher.close()
        logger.info("MarketScannerOrchestrator stopped.")

    # ------------------------------------------------------------------
    # Direct-call API (unit-test friendly)
    # ------------------------------------------------------------------

    def process_snapshot(self, snapshot: PropSnapshot) -> List[EdgePayload]:
        """Run LineValueScanner and FadeScanner against a prop snapshot.

        Can be called directly without RabbitMQ (e.g., for unit tests).

        Args:
            snapshot: Aggregated prop data.

        Returns:
            List of edge payloads found (0–2 items).
        """
        edges: List[EdgePayload] = []
        for scanner_fn in (
            lambda: self.line_scanner.scan(snapshot),
            lambda: self.fade_scanner.scan(snapshot),
        ):
            result = scanner_fn()
            if result:
                edges.append(result)
                self.publisher.publish(result)
        return edges

    def process_tick(self, tick: OddsTick) -> Optional[EdgePayload]:
        """Feed a raw odds tick to the SteamScanner.

        Args:
            tick: Latest price update from the odds feed.

        Returns:
            Steam edge payload if triggered, else ``None``.
        """
        result = self.steam_scanner.ingest_tick(tick)
        if result:
            self.publisher.publish(result)
        return result

    # ------------------------------------------------------------------
    # Consumer setup
    # ------------------------------------------------------------------

    def _setup_consumer(self) -> None:
        """Declare exchange/queue bindings and register the callback."""
        if not self._amqp_url:
            logger.warning("MarketScannerOrchestrator: no AMQP URL — consumer skipped.")
            return
        params = pika.URLParameters(self._amqp_url)
        self._consume_connection = pika.BlockingConnection(params)
        self._consume_channel = self._consume_connection.channel()
        self._consume_channel.exchange_declare(
            exchange=self.EXCHANGE, exchange_type="topic", durable=True,
        )
        self._consume_channel.queue_declare(queue=self.QUEUE_NAME, durable=True)
        self._consume_channel.queue_bind(
            exchange=self.EXCHANGE,
            queue=self.QUEUE_NAME,
            routing_key=self.BINDING_KEY,
        )
        self._consume_channel.basic_consume(
            queue=self.QUEUE_NAME,
            on_message_callback=self._on_message,
            auto_ack=False,
        )

    def _on_message(
        self,
        ch: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> None:
        """Route an inbound RabbitMQ message to the appropriate scanner.

        A message with ``"message_type": "odds_tick"`` is routed to the
        :class:`SteamScanner`; all others are treated as
        :class:`PropSnapshot` data.

        Args:
            ch:         Channel reference.
            method:     Delivery method (contains routing key).
            properties: AMQP message properties.
            body:       Raw message bytes.
        """
        try:
            data: Dict[str, Any] = json.loads(body)
            if data.get("message_type") == "odds_tick":
                tick = OddsTick(
                    player_id=data["player_id"],
                    prop_type=data["prop_type"],
                    book=data["book"],
                    american_odds=int(data["american_odds"]),
                    timestamp=float(data.get("timestamp", time.time())),
                )
                self.process_tick(tick)
            else:
                snapshot = PropSnapshot(
                    player_id=data["player_id"],
                    prop_type=data["prop_type"],
                    underdog_line=float(data["underdog_line"]),
                    sharp_odds=data["sharp_odds"],
                    public_pct=float(data.get("public_pct", 0.0)),
                )
                self.process_snapshot(snapshot)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            logger.error("MarketScannerOrchestrator: message parse error: %s", exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
