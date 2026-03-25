"""discord_dispatcher.py — PropIQ Analytics Discord Delivery Worker

Standalone background worker that listens to RabbitMQ for finalised betting
slip payloads and dispatches them to a private Discord server via Webhook.

This script performs no mathematical operations.  It is purely responsible
for formatting and delivery.

Responsibilities:
    - Durable RabbitMQ queue bound to ``alerts.discord.slips``
    - Rich Discord Embed formatting with agent-specific colour coding
    - HTTP POST to Discord Webhook with 429 rate-limit and 400 error handling
    - Runs indefinitely as a resilient background worker

Run:
    DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... \\
    AMQP_URL=amqp://user:pass@host:5672/ \\
    python discord_dispatcher.py

Expected inbound payload schema (from execution_agents.py)::

    {
        "agent_name": "EVHunter",
        "slip_type": "3-leg standard",
        "recommended_entry_type": "FLEX",        # "FLEX" or "STANDARD"
        "legs": [
            {
                "player": "Aaron Judge",
                "prop": "Total Bases",
                "line": 1.5,
                "side": "Over",
                "true_prob": 0.58
            }
        ],
        "total_ev": 0.045,
        "recommended_unit_size": 0.5
    }

Entry type is set by underdog_math_engine.py based on which payout structure
produces the higher Expected Value for the specific combination of leg
probabilities in the slip.

  FLEX (Insured) : absorbs 1 incorrect pick (2 for 6-8 leg entries) at a
                   reduced multiplier.  Preferred when individual leg
                   probabilities are moderate (55–62%).
  STANDARD       : requires all picks to be correct.  Higher multiplier.
                   Preferred for very high-confidence slips (63%+ per leg).
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pika
import pika.exceptions
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("discord_dispatcher")

# ---------------------------------------------------------------------------
# RabbitMQ constants
# ---------------------------------------------------------------------------
EXCHANGE: str = "propiq_events"
BINDING_KEY: str = "alerts.discord.slips"
QUEUE_NAME: str = "discord_dispatcher_queue"

# ---------------------------------------------------------------------------
# Discord embed colour map  (decimal RGB values)
# ---------------------------------------------------------------------------
AGENT_COLOURS: Dict[str, int] = {
    "EVHunter": 0x2ECC71,       # Green       — top-EV generalist
    "UnderMachine": 0x3498DB,   # Blue        — all-Under specialist
    "F5Agent": 0xF39C12,        # Orange      — First-5-innings
    "MLEdgeAgent": 0x9B59B6,    # Purple      — pure ML quant
    "UmpireAgent": 0xE74C3C,    # Red         — umpire environment
    "FadeAgent": 0x1ABC9C,      # Teal        — contrarian fades
    "WeatherAgent": 0x95A5A6,   # Grey        — weather / park factor
    "SteamAgent": 0xE67E22,     # Amber       — steam moves
    "LineValueAgent": 0x2980B9, # Dark blue   — sharp line value
    "BullpenAgent": 0x8E44AD,   # Dark purple — bullpen fatigue
    "ArsenalAgent": 0xD35400,   # Burnt orange — pitch-type matchup
    "PlatoonAgent": 0x27AE60,   # Forest green — handedness splits
    "CatcherAgent": 0x16A085,   # Dark teal    — framing & battery
    "LineupAgent": 0x2C3E50,    # Midnight     — PA volume projection
    "GetawayAgent": 0x7F8C8D,   # Slate grey   — schedule fatigue
}
DEFAULT_COLOUR: int = 0x34495E   # Charcoal fallback for unlisted agents

# High-EV threshold — override agent colour with gold when slip EV exceeds this
EV_HIGH_THRESHOLD: float = 0.07
EV_HIGH_COLOUR: int = 0xF1C40F   # Gold

# California DFS compliance stamp (appended to every embed)
DFS_PLATFORM_STAMP: str = "🐶 OPEN APP: Underdog Fantasy"

# Entry type display config
ENTRY_TYPE_BADGE: dict = {
    "FLEX": {
        "icon": "🛡️",
        "label": "INSURED (FLEX)",
        "description": (
            "1 incorrect pick still pays out at reduced multiplier. "
            "6-8 leg entries can absorb 2 losses."
        ),
    },
    "STANDARD": {
        "icon": "⚡",
        "label": "STANDARD",
        "description": "All picks must be correct. Higher multiplier, no insurance.",
    },
}

# Webhook retry configuration
RATE_LIMIT_SLEEP: float = 5.0
MAX_RETRIES: int = 3


# ---------------------------------------------------------------------------
# Discord Embed Formatter
# ---------------------------------------------------------------------------

def format_discord_embed(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a slip payload into a Discord-ready Embed dictionary.

    Formatting rules:
        Title:
            ``[AgentName] 🎯 {Slip Type} Slip``

        Colour:
            Agent-specific from :data:`AGENT_COLOURS`.  If ``total_ev``
            exceeds :data:`EV_HIGH_THRESHOLD` (7%), the colour is overridden
            to gold (:data:`EV_HIGH_COLOUR`) to visually highlight
            high-confidence plays.

        Leg fields:
            One non-inline field per leg showing player, side emoji,
            prop, line, and calibrated probability.

        Entry type field:
            Prominent badge showing ``🛡️ INSURED (FLEX)`` or ``⚡ STANDARD``
            sourced from ``recommended_entry_type`` in the payload (set by
            ``underdog_math_engine.py``).  Displayed as the first embed field
            so it is immediately visible before the leg breakdown.

        Metrics field:
            Total EV %, recommended unit size, and platform stamp.

        Footer:
            ``PropIQ Analytics • {AgentName} • California DFS Legal``

        Timestamp:
            UTC ISO-8601 string (Discord renders in the viewer's local time).

    Args:
        payload: Deserialised slip JSON from RabbitMQ.

    Returns:
        A Discord Embed object dict ready for webhook POST.
    """
    agent_name: str = payload.get("agent_name", "PropIQ")
    slip_type: str = payload.get("slip_type", "N-leg standard")
    legs: List[Dict[str, Any]] = payload.get("legs", [])
    total_ev: float = float(payload.get("total_ev", 0.0))
    unit_size: float = float(payload.get("recommended_unit_size", 0.0))

    # Determine embed colour
    avg_leg_ev: float = float(payload.get("avg_leg_ev", 0.0))
    unit_size: float = float(payload.get("recommended_unit_size", 0.0))

    # --- Entry type badge (set by underdog_math_engine.py) ---
    raw_entry_type: str = str(
        payload.get("recommended_entry_type", "FLEX")
    ).upper()
    # Default to FLEX for 3+ leg slips if not explicitly set
    if raw_entry_type not in ENTRY_TYPE_BADGE:
        n_legs = len(legs)
        raw_entry_type = "FLEX" if n_legs >= 3 else "STANDARD"
    entry_badge = ENTRY_TYPE_BADGE[raw_entry_type]

    # --- Determine embed colour ---
    if total_ev >= EV_HIGH_THRESHOLD:
        colour = EV_HIGH_COLOUR
    else:
        colour = AGENT_COLOURS.get(agent_name, DEFAULT_COLOUR)

    # Build one embed field per slip leg
    fields: List[Dict[str, Any]] = []
    # --- Entry type field (first field — most prominent) ---
    fields: List[Dict[str, Any]] = [
        {
            "name": f"{entry_badge['icon']} Entry Type: {entry_badge['label']}",
            "value": entry_badge["description"],
            "inline": False,
        }
    ]

    # --- One embed field per slip leg ---
    for i, leg in enumerate(legs, start=1):
        side: str = leg.get("side", "?")
        side_emoji = "⬆️" if side.lower() == "over" else "⬇️"
        prob_pct = round(float(leg.get("true_prob", 0.0)) * 100, 1)
    # --- One embed field per slip leg (ALL legs guaranteed) ---
    # Discord hard limit: 25 fields per embed.
    # Reserved: 1 entry-type field + 1 metrics field = 23 max legs.
    # PropIQ max is 5 legs — well within the limit.
    DISCORD_MAX_FIELDS: int = 25
    reserved_fields: int = 2  # entry-type + metrics
    max_leg_fields: int = DISCORD_MAX_FIELDS - reserved_fields  # 23
    DISCORD_MAX_FIELD_VALUE: int = 1024  # Discord per-field char limit

    for i, leg in enumerate(legs[:max_leg_fields], start=1):
        side: str = leg.get("side", "?")
        side_emoji = "⬆️" if side.lower() == "over" else "⬇️"
        prob_pct = round(float(leg.get("true_prob", 0.0)) * 100, 1)
        leg_ev = float(leg.get("no_vig_ev", 0.0))
        leg_ev_sign = "+" if leg_ev >= 0 else ""
        leg_ev_str = f"{leg_ev_sign}{round(leg_ev * 100, 1)}%"
        # Colour-code the EV label: green for positive, red for negative
        ev_label = f"✅ {leg_ev_str} EV vs no-vig" if leg_ev >= 0 else f"⚠️ {leg_ev_str} EV vs no-vig"
        fields.append({
            "name": f"Leg {i} — {leg.get('player', 'Unknown')}",
            "value": (
                f"{side_emoji} **{side}** "
                f"{leg.get('prop', '?')} "
                f"({leg.get('line', '?')}) "
                f"| {prob_pct}% prob"
                f"| {prob_pct}% prob | **{ev_label}**"
        leg_value = (
            f"{side_emoji} **{side}** "
            f"{leg.get('prop', '?')} "
            f"({leg.get('line', '?')}) "
            f"| {prob_pct}% prob | **{ev_label}**"
        )
        # Truncate at Discord's 1024-char field-value limit (safety net)
        if len(leg_value) > DISCORD_MAX_FIELD_VALUE:
            leg_value = leg_value[: DISCORD_MAX_FIELD_VALUE - 3] + "..."
        fields.append({
            "name": f"Leg {i} — {leg.get('player', 'Unknown')}",
            "value": leg_value,
            "inline": False,
        })

    # Overflow notice if somehow a slip has > 23 legs (should never occur)
    if len(legs) > max_leg_fields:
        overflow_count = len(legs) - max_leg_fields
        fields.append({
            "name": f"⚠️ +{overflow_count} more legs",
            "value": (
                f"Discord field limit reached. Full {len(legs)}-leg slip "
                "details in RabbitMQ payload."
            ),
            "inline": False,
        })

    # Metrics summary field
    # --- Metrics summary field ---
    ev_pct = round(total_ev * 100, 2)
    ev_sign = "+" if total_ev >= 0 else ""
    fields.append({
        "name": "📊 Slip Metrics",
        "value": (
            f"**Total EV:** {ev_sign}{ev_pct}%\n"
            f"**Unit Size:** {unit_size} units\n"
    avg_ev_pct = round(avg_leg_ev * 100, 2)
    avg_ev_sign = "+" if avg_leg_ev >= 0 else ""
    # Build agent-specific context line (helps orient users on each agent's edge)
    _AGENT_CONTEXT: Dict[str, str] = {
        "ArsenalAgent":   "🔬 Pitch-type matchup | Usage × Whiff rate",
        "PlatoonAgent":   "🤲 EMV platoon delta | wRC+ LHP vs RHP split",
        "CatcherAgent":   "🧤 Battery analysis | Framing runs + pop time",
        "LineupAgent":    "📋 PA volume edge | Lineup position × Team total",
        "GetawayAgent":   "✈️ Schedule fatigue | Rest hours + timezone shift",
        "UmpireAgent":    "⚖️ Umpire zone | CS% delta vs league average",
        "FadeAgent":      "🔄 Sharp fade | Ticket% vs money% divergence",
        "BullpenAgent":   "💪 Fatigue index | L3-5 day bullpen workload",
        "WeatherAgent":   "🌬️ Wind factor | MPH × direction × park",
        "SteamAgent":     "💨 Line velocity | Pts/min × book consensus",
        "EVHunter":       "💰 All-source top-EV | ML + market combined",
        "UnderMachine":   "⬇️ Under specialist | Public Over bias fade",
        "F5Agent":        "5️⃣ First-5 innings | Ignores bullpen variance",
        "MLEdgeAgent":    "🤖 Pure ML signal | XGBoost ≥ 0.55 prob gate",
        "LineValueAgent": "📐 No-vig gap | Sharp vs retail consensus delta",
    }
    agent_context = _AGENT_CONTEXT.get(agent_name, "🎯 PropIQ Analytics")
    fields.append({
        "name": "📊 Slip Metrics",
        "value": (
            f"🎯 **Avg Edge vs No-Vig: {avg_ev_sign}{avg_ev_pct}%**\n"
            f"Slip EV (Underdog math): {ev_sign}{ev_pct}%\n"
            f"**Unit Size:** {unit_size} units\n"
            f"*{agent_context}*\n"
            f"{DFS_PLATFORM_STAMP}"
        ),
        "inline": False,
    })

    return {
        "title": f"[{agent_name}] 🎯 {slip_type.title()} Slip",
        "color": colour,
        "fields": fields,
        "footer": {
            "text": f"PropIQ Analytics • {agent_name} • California DFS Legal",
        "title": (
            f"[{agent_name}] {entry_badge['icon']} "
            f"{slip_type.title()} — {entry_badge['label']}"
    avg_ev_badge = f"{'+' if avg_leg_ev >= 0 else ''}{round(avg_leg_ev * 100, 1)}% EV"
    return {
        "title": (
            f"[{agent_name}] {entry_badge['icon']} "
            f"{slip_type.title()} — {entry_badge['label']} | {avg_ev_badge}"
        ),
        "color": colour,
        "fields": fields,
        "footer": {
            "text": (
                f"PropIQ Analytics • {agent_name} • "
                f"{entry_badge['label']} • California DFS Legal"
            ),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Webhook Dispatcher
# ---------------------------------------------------------------------------

def dispatch_to_discord(
    embed: Dict[str, Any],
    webhook_url: str,
    retries: int = MAX_RETRIES,
) -> bool:
    """POST a formatted embed to a Discord Webhook URL.

    Error handling:
        HTTP 429 (Rate Limited):
            Back off for the ``retry_after`` duration from the response JSON
            (falls back to :data:`RATE_LIMIT_SLEEP` seconds) and retry.

        HTTP 400 (Bad Request):
            Log the full response body and return ``False`` immediately.
            Discord 400s indicate a malformed payload — retrying won't help.

        Other 4xx / 5xx:
            Log the status code and retry with exponential back-off up to
            ``retries`` attempts.

        Network errors (``requests.exceptions.RequestException``):
            Log the exception and retry with exponential back-off.

    Args:
        embed:       Discord Embed dict from :func:`format_discord_embed`.
        webhook_url: Full Discord Webhook URL.
        retries:     Maximum retry attempts before giving up.

    Returns:
        ``True`` if Discord returned 200 or 204, ``False`` otherwise.
    """
    webhook_payload: Dict[str, Any] = {"embeds": [embed]}
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    attempt = 0

    while attempt <= retries:
        try:
            response = requests.post(
                webhook_url,
                json=webhook_payload,
                headers=headers,
                timeout=10,
            )

            if response.status_code in (200, 204):
                logger.info(
                    "Discord OK | status=%d | embed=%s",
                    response.status_code,
                    embed.get("title", "?"),
                )
                return True

            if response.status_code == 429:
                retry_after: float = float(
                    response.json().get("retry_after", RATE_LIMIT_SLEEP)
                )
                logger.warning(
                    "Discord rate-limited (429) — sleeping %.1f s.", retry_after
                )
                time.sleep(retry_after)
                attempt += 1
                continue

            if response.status_code == 400:
                logger.error(
                    "Discord rejected payload (400) — %s. Not retrying.",
                    response.text[:500],
                )
                return False

            logger.warning(
                "Discord returned %d on attempt %d/%d — retrying.",
                response.status_code, attempt + 1, retries,
            )

        except requests.exceptions.RequestException as exc:
            logger.error("Network error dispatching to Discord: %s", exc)

        attempt += 1
        time.sleep(min(2.0 ** attempt, 30.0))  # capped exponential back-off

    logger.error("Discord dispatch failed after %d attempts.", retries)
    return False


# ---------------------------------------------------------------------------
# RabbitMQ Consumer
# ---------------------------------------------------------------------------

class DiscordDispatcher:
    """Blocking RabbitMQ consumer that fans slip payloads to Discord.

    Binds a durable queue to ``alerts.discord.slips`` on the
    ``propiq_events`` topic exchange.  Each message is:
        1. Deserialised from JSON.
        2. Formatted into a Discord Embed via :func:`format_discord_embed`.
        3. Dispatched via HTTP POST to the configured Webhook URL.
        4. Acked on success; nacked (no requeue) on persistent failure.

    The consumer runs indefinitely.  Send a ``KeyboardInterrupt`` (Ctrl+C)
    or ``SIGTERM`` to trigger a graceful shutdown.

    Args:
        amqp_url:    AMQP connection string.
        webhook_url: Discord Webhook URL.
    """

    def __init__(self, amqp_url: str, webhook_url: str) -> None:
        self._amqp_url = amqp_url
        self._webhook_url = webhook_url
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Any = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Connect to RabbitMQ, declare infrastructure, and begin consuming."""
        self._connect()
        logger.info(
            "DiscordDispatcher: listening on '%s' (exchange: %s)",
            BINDING_KEY, EXCHANGE,
        )
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("DiscordDispatcher: shutdown signal received.")
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully close the consumer and connection."""
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except Exception:
            pass
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception:
            pass
        logger.info("DiscordDispatcher: connection closed.")

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """Open the RabbitMQ connection and declare the durable queue."""
        params = pika.URLParameters(self._amqp_url)
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

        self._channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="topic",
            durable=True,
        )
        self._channel.queue_declare(queue=QUEUE_NAME, durable=True)
        self._channel.queue_bind(
            exchange=EXCHANGE,
            queue=QUEUE_NAME,
            routing_key=BINDING_KEY,
        )
        # Prefetch 1 ensures we don't buffer unacked messages during slow
        # Discord delivery (rate limits can extend processing time).
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=self._on_message,
            auto_ack=False,
        )

    # ------------------------------------------------------------------
    # Message handler
    # ------------------------------------------------------------------

    def _on_message(
        self,
        ch: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> None:
        """Deserialise, format, dispatch, and ack/nack a slip message.

        Args:
            ch:         RabbitMQ channel.
            method:     Delivery method (routing key, delivery tag, etc.).
            properties: AMQP message properties.
            body:       Raw JSON bytes from the broker.
        """
        try:
            payload: Dict[str, Any] = json.loads(body)
        except json.JSONDecodeError as exc:
            logger.error("DiscordDispatcher: invalid JSON payload: %s", exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        agent_name = payload.get("agent_name", "Unknown")
        n_legs = len(payload.get("legs", []))
        ev_pct = round(payload.get("total_ev", 0.0) * 100, 2)

        logger.info(
            "Slip received | agent=%s | legs=%d | ev=%+.2f%%",
            agent_name, n_legs, ev_pct,
        )

        embed = format_discord_embed(payload)
        success = dispatch_to_discord(embed, self._webhook_url)

        if success:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # Nack without requeue — send to dead-letter queue for inspection
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            logger.error(
                "DiscordDispatcher: failed to deliver slip from %s — nacked.",
                agent_name,
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Bootstrap the dispatcher from environment variables and start consuming."""
    amqp_url: str = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    webhook_url: Optional[str] = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        logger.critical(
            "DISCORD_WEBHOOK_URL environment variable not set — aborting."
        )
        raise SystemExit(1)

    logger.info("PropIQ Discord Dispatcher starting up...")
    dispatcher = DiscordDispatcher(amqp_url=amqp_url, webhook_url=webhook_url)
    dispatcher.start()


if __name__ == "__main__":
    main()
