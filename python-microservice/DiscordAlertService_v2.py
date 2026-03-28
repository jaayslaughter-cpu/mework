"""
DiscordAlertService.py
======================
Outbound-only Discord webhook integration for PropIQ Analytics Engine.

Reads DISCORD_WEBHOOK_URL from environment.  All methods are safe to call
even when the env var is missing or the webhook is unreachable — failures
are logged as warnings and silently swallowed so they never crash the engine.

Public API
----------
  send_startup_ping()              → fires on application boot
  send_bet_alert(bet: dict)        → called by AgentTasklet for every queued bet
  send_daily_recap(results, profit, date_str)  → called by GradingTasklet

Usage
-----
  from DiscordAlertService import discord_alert
  discord_alert.send_startup_ping()
  discord_alert.send_bet_alert(bet_dict)
  discord_alert.send_daily_recap(results, total_profit, "2026-03-21")
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("propiq.discord")

# ── Colour palette ────────────────────────────────────────────────────────────
_COLOUR_GREEN  = 0x2ECC71   # win / online
_COLOUR_RED    = 0xE74C3C   # loss / warning
_COLOUR_BLUE   = 0x3498DB   # bet alert
_COLOUR_GOLD   = 0xF1C40F   # daily recap
_COLOUR_GREY   = 0x95A5A6   # push / neutral

# ── Platform emoji map ────────────────────────────────────────────────────────
_PLATFORM_EMOJI = {
    "prizepicks": "🏆",
    "underdog":   "🐶",
}


class DiscordAlertService:
    """Thin wrapper around a single Discord incoming webhook URL."""

    def __init__(self) -> None:
        self._url: str = os.getenv("DISCORD_WEBHOOK_URL", "")

    # ── Internal helper ───────────────────────────────────────────────────────

    def _post(self, payload: dict[str, Any]) -> bool:
        """POST payload to the webhook.  Returns True on success."""
        url = os.getenv("DISCORD_WEBHOOK_URL", self._url)
        if not url:
            logger.warning("[Discord] DISCORD_WEBHOOK_URL not set — skipping alert.")
            return False
        try:
            resp = requests.post(
                url,
                json=payload,
                timeout=10,
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code in (200, 204):
                return True
            logger.warning("[Discord] Webhook returned HTTP %d: %s",
                           resp.status_code, resp.text[:200])
            return False
        except Exception as exc:
            logger.warning("[Discord] Failed to reach webhook: %s", exc)
            return False

    # ── Public methods ────────────────────────────────────────────────────────

    def send_startup_ping(self) -> None:
        """
        Fire a test message the absolute second the application is ready.
        Called from orchestrator.py lifespan startup.
        """
        ok = self._post({
            "embeds": [{
                "title": "✅ PropIQ Engine Online: Webhook Connected!",
                "description": (
                    "All tasklets are scheduled and running.\n"
                    "Redis / Kafka degradation mode active until services are available."
                ),
                "color": _COLOUR_GREEN,
                "footer": {"text": "PropIQ Analytics Engine"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        if ok:
            logger.info("[Discord] Startup ping sent successfully.")

    def send_bet_alert(self, bet: dict) -> None:
        """
        Send a formatted embed for a single queued bet.
        Called by AgentTasklet immediately after each bet is pushed to the queue.
        """
        player    = bet.get("player", "Unknown")
        prop_type = bet.get("prop_type", "")
        line      = bet.get("line", "")
        side      = bet.get("side", "")
        ev_pct    = bet.get("ev_pct", 0.0)
        kelly     = bet.get("kelly_units", 0.0)
        conf      = bet.get("confidence", 5)
        agents    = bet.get("agents", [bet.get("agent", "Unknown")])
        agent_cnt = bet.get("agent_count", len(agents))
        platform  = bet.get("recommended_platform", "PrizePicks")
        odds_raw  = bet.get("odds_american", -110)
        model_prob = bet.get("model_prob", 50.0)

        # Odds display
        odds_str = f"+{odds_raw}" if isinstance(odds_raw, int) and odds_raw > 0 else str(odds_raw)

        # Platform display
        plat_lower  = str(platform).lower()
        plat_emoji  = _PLATFORM_EMOJI.get(plat_lower, "🎯")
        plat_label  = platform.capitalize()

        # Side colour
        colour = _COLOUR_BLUE

        # Confidence bar  ████░░░░░░  (10 blocks)
        filled   = round(conf)
        conf_bar = "█" * filled + "░" * (10 - filled)

        checklist = bet.get("checklist", {})
        checks = " ".join(
            ("✅" if v else "❌") + k.replace("_ok", "").upper()
            for k, v in checklist.items()
        ) if checklist else "N/A"

        self._post({
            "embeds": [{
                "title": f"{plat_emoji} OPEN APP: {plat_label}",
                "color": colour,
                "fields": [
                    {"name": "🧑 Player",        "value": player,                                "inline": True},
                    {"name": "📊 Prop",           "value": f"{prop_type} {side} {line}",         "inline": True},
                    {"name": "💰 Odds",           "value": odds_str,                             "inline": True},
                    {"name": "📈 Edge (EV)",      "value": f"+{ev_pct:.1f}%",                    "inline": True},
                    {"name": "🎲 Kelly Units",    "value": f"{kelly:.3f}u",                      "inline": True},
                    {"name": "🤖 Model Prob",     "value": f"{model_prob:.1f}%",                 "inline": True},
                    {"name": "🔥 Confidence",     "value": f"{conf_bar}  {conf}/10",             "inline": False},
                    {"name": f"🤝 Agent Consensus ({agent_cnt}/10)",
                                                  "value": ", ".join(agents),                   "inline": False},
                    {"name": "✔️ 7-Point Check",  "value": checks,                              "inline": False},
                ],
                "footer": {"text": "PropIQ Analytics Engine"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })

    def send_daily_recap(
        self,
        results: list[dict],
        total_profit: float,
        date_str: str,
    ) -> None:
        """
        Send end-of-day settlement recap after GradingTasklet finishes.
        One embed per day with a line-by-line breakdown.
        """
        wins   = sum(1 for r in results if r.get("status") == "WIN")
        losses = sum(1 for r in results if r.get("status") == "LOSS")
        pushes = sum(1 for r in results if r.get("status") == "PUSH")

        sign   = "+" if total_profit >= 0 else ""
        colour = _COLOUR_GREEN if total_profit >= 0 else _COLOUR_RED

        # Build line-by-line description (max ~3 000 chars Discord allows)
        lines: list[str] = []
        for r in results:
            emoji    = {"WIN": "✅", "LOSS": "❌", "PUSH": "➖"}.get(r.get("status", ""), "❓")
            pl       = r.get("profit_loss", 0.0)
            pl_sign  = "+" if pl >= 0 else ""
            odds_raw = r.get("odds_american", -110)
            odds_str = f"+{odds_raw}" if isinstance(odds_raw, int) and odds_raw > 0 else str(odds_raw)
            lines.append(
                f"{emoji} **{r.get('player', '?')}** — "
                f"{r.get('prop_type', '?')} {r.get('side', '?')} | "
                f"{odds_str} | {pl_sign}{pl:.2f}u"
            )

        description = "\n".join(lines) or "_No graded bets._"
        # Truncate if too long
        if len(description) > 3_000:
            description = description[:2_950] + "\n…(truncated)"

        self._post({
            "embeds": [{
                "title": f"📊 PropIQ Daily Recap — {date_str}",
                "description": description,
                "color": colour,
                "fields": [
                    {"name": "📈 Units",   "value": f"{sign}{total_profit:.2f}u",      "inline": True},
                    {"name": "🏆 Record",  "value": f"{wins}-{losses}-{pushes} W-L-P", "inline": True},
                ],
                "footer": {"text": "Powered by PropIQ Analytics 🤖"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        logger.info("[Discord] Daily recap sent — %s  %s%+.2fu  %d-%d-%d",
                    date_str, sign, total_profit, wins, losses, pushes)


    def send_parlay_alert(self, parlay: dict) -> None:
        """
        Send a formatted Discord embed for a DFS parlay slip.
        Called by AgentTasklet when The Correlated Parlay Agent builds a valid slip.

        Embed shows:
          - Agent name as title
          - Each leg: Player | Stat | Over/Under | Underdog Line | Leg EV%
          - Combined EV% across all legs
        """
        agent_name   = parlay.get("agent", "The Correlated Parlay Agent")
        legs         = parlay.get("legs", [])
        combined_ev  = parlay.get("combined_ev_pct", 0.0)
        leg_count    = parlay.get("leg_count", len(legs))

        if not legs:
            return

        fields: list[dict] = []
        for i, leg in enumerate(legs, 1):
            player    = leg.get("player", "?")
            prop_type = leg.get("prop_type", "?")
            side      = leg.get("side", "?")
            line      = leg.get("line", "?")
            ud_raw    = leg.get("underdog_line", leg.get("odds_american", -120))
            ud_str    = (f"+{ud_raw}" if isinstance(ud_raw, int) and ud_raw > 0
                         else str(ud_raw))
            leg_ev    = leg.get("ev_pct", 0.0)
            fields.append({
                "name": f"Leg {i} — {player}",
                "value": (
                    f"**{prop_type} {side} {line}**  |  "
                    f"Underdog: `{ud_str}`  |  "
                    f"Leg EV: `+{leg_ev:.1f}%`"
                ),
                "inline": False,
            })

        fields.append({
            "name": f"🎯 Combined EV — {leg_count}-Leg Slip",
            "value": f"**+{combined_ev:.1f}%** total edge vs sharp consensus",
            "inline": False,
        })

        self._post({
            "embeds": [{
                "title": f"🐶 {agent_name} — {leg_count}-Leg Underdog Slip",
                "description": (
                    "Sharp consensus confirms mispricing vs Underdog lines. "
                    "**Open Underdog Fantasy to enter this slip.**"
                ),
                "color": _COLOUR_BLUE,
                "fields": fields,
                "footer": {"text": "PropIQ Analytics Engine • Underdog Fantasy"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        logger.info("[Discord] Parlay alert sent — %d legs, combined EV +%.1f%%",
                    leg_count, combined_ev)


# Module-level singleton — import this everywhere
discord_alert = DiscordAlertService()
