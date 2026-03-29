"""
DiscordAlertService.py
======================
Outbound-only Discord webhook integration for PropIQ Analytics Engine.

Reads DISCORD_WEBHOOK_URL from environment (with hardcoded fallback).
All methods are safe to call even when the webhook is unreachable --
failures are logged as warnings and silently swallowed so they never
crash the engine.

Public API
----------
  send_startup_ping()
  send_bet_alert(bet: dict)
  send_daily_recap(results, profit, date_str)
  send_parlay_alert(parlay: dict)

Usage
-----
  from DiscordAlertService import discord_alert, MAX_STAKE_USD
  discord_alert.send_startup_ping()
  discord_alert.send_parlay_alert(parlay_dict)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("propiq.discord")

# ── Exported constant (imported by live_dispatcher.py) ────────────────────────
MAX_STAKE_USD: float = 20.0   # Tier 5 ceiling ($20/unit)

# ── Colour palette ────────────────────────────────────────────────────────────
_COLOUR_GREEN  = 0x2ECC71   # win / online
_COLOUR_RED    = 0xE74C3C   # loss / warning
_COLOUR_BLUE   = 0x3498DB   # bet alert
_COLOUR_GOLD   = 0xF1C40F   # daily recap
_COLOUR_GREY   = 0x95A5A6   # push / neutral

# ── Fallback webhook (used when DISCORD_WEBHOOK_URL env var is absent) ────────
_FALLBACK_WEBHOOK = (
    "https://discordapp.com/api/webhooks/1484795164961800374/"
    "jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM"
)

# ── Platform emoji map ────────────────────────────────────────────────────────
_PLATFORM_EMOJI = {
    "prizepicks": "🏆",
    "underdog":   "🐶",
    "sleeper":    "😴",
}

# ── Tier badge map ────────────────────────────────────────────────────────────
_TIER_BADGE = {1: "🌱", 2: "🌿", 3: "⭐", 4: "🔥", 5: "👑"}


class DiscordAlertService:
    """Thin wrapper around a single Discord incoming webhook URL."""

    def __init__(self) -> None:
        self._url: str = os.getenv("DISCORD_WEBHOOK_URL", _FALLBACK_WEBHOOK)

    # ── Internal helper ───────────────────────────────────────────────────────

    def _post(self, payload: dict[str, Any]) -> bool:
        """POST payload to the webhook.  Returns True on success."""
        url = os.getenv("DISCORD_WEBHOOK_URL", self._url) or _FALLBACK_WEBHOOK
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
        """Fire a test message the absolute second the application is ready."""
        ok = self._post({
            "embeds": [{
                "title": "✅ PropIQ Engine Online: Webhook Connected!",
                "description": (
                    "All tasklets are scheduled and running.\n"
                    "10-Agent Army armed | Underdog + PrizePicks | Every 30s."
                ),
                "color": _COLOUR_GREEN,
                "footer": {"text": "PropIQ Analytics Engine"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        if ok:
            logger.info("[Discord] Startup ping sent successfully.")

    def send_bet_alert(self, bet: dict) -> None:
        """Send a formatted embed for a single queued bet."""
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

        odds_str = f"+{odds_raw}" if isinstance(odds_raw, int) and odds_raw > 0 else str(odds_raw)
        plat_lower  = str(platform).lower()
        plat_emoji  = _PLATFORM_EMOJI.get(plat_lower, "🎯")
        plat_label  = platform.capitalize()

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
                "color": _COLOUR_BLUE,
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
        tier_updates: list[str] | None = None,
    ) -> None:
        """Send end-of-day settlement recap with tier ladder progress."""
        wins   = sum(1 for r in results if r.get("status") == "WIN")
        losses = sum(1 for r in results if r.get("status") == "LOSS")
        pushes = sum(1 for r in results if r.get("status") == "PUSH")

        sign   = "+" if total_profit >= 0 else ""
        colour = _COLOUR_GREEN if total_profit >= 0 else _COLOUR_RED

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
        if len(description) > 3_000:
            description = description[:2_950] + "\n…(truncated)"

        fields = [
            {"name": "📈 Units",   "value": f"{sign}{total_profit:.2f}u",      "inline": True},
            {"name": "🏆 Record",  "value": f"{wins}-{losses}-{pushes} W-L-P", "inline": True},
        ]
        # Always show tier ladder progress — promotions AND in-progress streaks
        if tier_updates:
            fields.append({
                "name": "🏅 Tier Ladder",
                "value": "\n".join(tier_updates),
                "inline": False,
            })

        self._post({
            "embeds": [{
                "title": f"📊 PropIQ Daily Recap — {date_str}",
                "description": description,
                "color": colour,
                "fields": fields,
                "footer": {"text": "Powered by PropIQ Analytics 🤖  |  3 W or 3 L in a row = tier move"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        logger.info("[Discord] Daily recap sent — %s  %s%+.2fu  %d-%d-%d",
                    date_str, sign, total_profit, wins, losses, pushes)

    def send_parlay_alert(self, parlay: dict) -> None:
        """
        Send a formatted Discord embed for a DFS parlay slip.
        Called by live_dispatcher.py for each agent parlay.

        Expected parlay dict keys (from live_dispatcher.py build_parlay()):
            agent_name      str   — e.g. "EVHunter"
            legs            list  — list of leg dicts
            confidence      float — 0–10 score
            ev_pct          float — average EV% across legs
            stake           float — dollar stake from agent tier ($5–$20)
            tier            int   — agent tier 1–5 (optional, derived from stake)
            platform        str   — "prizepicks" or "underdog"
            season_stats    dict  — {wins, losses, pushes, roi} (optional)

        Each leg dict keys:
            player_name     str
            prop_type       str
            side            str   — "Over" or "Under"
            line            float
            ev_pct          float
            model_prob      float (0–1)
            platform        str
        """
        # --- Parlay-level fields ---
        agent_name  = parlay.get("agent_name") or parlay.get("agent", "Unknown Agent")
        legs        = parlay.get("legs", [])
        confidence  = parlay.get("confidence", 0.0)
        ev_pct      = parlay.get("ev_pct") or parlay.get("combined_ev_pct", 0.0)
        stake       = parlay.get("stake", parlay.get("unit_dollars", 5.0))
        leg_count   = len(legs)

        if not legs:
            return

        # Derive tier badge from stake amount
        stake_to_tier = {5.0: 1, 8.0: 2, 12.0: 3, 16.0: 4, 20.0: 5}
        tier = parlay.get("tier", stake_to_tier.get(float(stake), 1))
        tier_badge = _TIER_BADGE.get(tier, "🌱")

        # Confidence bar  ████░░░░░░  (10 blocks)
        filled   = max(0, min(10, round(confidence)))
        conf_bar = "█" * filled + "░" * (10 - filled)

        # Platform
        platform   = parlay.get("platform", "underdog")
        plat_lower = str(platform).lower()
        plat_emoji = _PLATFORM_EMOJI.get(plat_lower, "🎯")
        plat_label = "PrizePicks" if "prize" in plat_lower else "Underdog Fantasy"

        # --- Season record footer ---
        season = parlay.get("season_stats", {})
        s_wins   = season.get("wins", 0)
        s_losses = season.get("losses", 0)
        s_pushes = season.get("pushes", 0)
        s_roi    = season.get("roi_pct", season.get("roi", 0.0))
        season_str = f"{agent_name} Season: {s_wins}W-{s_losses}L-{s_pushes}P | ROI {s_roi:+.1f}%"

        # --- Leg fields ---
        fields: list[dict] = []
        for i, leg in enumerate(legs, 1):
            player    = (leg.get("player_name") or leg.get("player", "?")).title()
            prop_type = leg.get("prop_type", "?").replace("_", " ").title()
            side      = leg.get("side", "?")
            line      = leg.get("line", "?")
            leg_ev    = leg.get("ev_pct", 0.0)
            model_p   = leg.get("model_prob", 0.0)
            # model_prob may be 0-1 or 0-100
            if isinstance(model_p, float) and model_p <= 1.0:
                model_p *= 100.0
            leg_plat  = leg.get("platform", platform)
            lp_lower  = str(leg_plat).lower()
            lp_emoji  = _PLATFORM_EMOJI.get(lp_lower, "🎯")

            fields.append({
                "name": f"Leg {i} — {player}",
                "value": (
                    f"**{prop_type} {side} {line}**  {lp_emoji}\n"
                    f"Model: `{model_p:.1f}%`  |  EV: `+{leg_ev:.1f}%`"
                ),
                "inline": False,
            })

        fields.append({
            "name": f"📊 Summary — {leg_count}-Leg Slip",
            "value": (
                f"Avg EV: **+{ev_pct:.1f}%**  |  "
                f"Confidence: {conf_bar} {confidence:.1f}/10  |  "
                f"Stake: **${stake:.0f}**"
            ),
            "inline": False,
        })

        # Colour by confidence
        if confidence >= 8.5:
            color = _COLOUR_GOLD
        elif confidence >= 7.0:
            color = _COLOUR_GREEN
        else:
            color = _COLOUR_BLUE

        self._post({
            "embeds": [{
                "title": f"{tier_badge} {agent_name} — {leg_count}-Leg {plat_label} Slip",
                "description": (
                    f"{plat_emoji} **Open {plat_label} to enter this slip**\n"
                    f"Stake: **${stake:.0f}** | EV: **+{ev_pct:.1f}%**"
                ),
                "color": color,
                "fields": fields,
                "footer": {"text": season_str},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        logger.info("[Discord] Parlay alert sent — %s | %d legs | +%.1f%% EV | $%.0f stake | %s T%d",
                    agent_name, leg_count, ev_pct, stake, tier_badge, tier)

    def send_parlay_alert_streak(self, parlay: dict) -> None:
        """
        Streak-specific Discord embed with Underdog Streaks branding.
        Same field contract as send_parlay_alert() above.
        """
        agent_name = parlay.get("agent_name", "StreakAgent")
        legs       = parlay.get("legs", [])
        confidence = parlay.get("confidence", 0.0)
        ev_pct     = parlay.get("ev_pct", 0.0)
        stake      = parlay.get("stake", 5.0)
        leg_count  = len(legs)

        if not legs:
            return

        fields: list[dict] = []
        for i, leg in enumerate(legs, 1):
            player    = (leg.get("player_name") or leg.get("player", "?")).title()
            prop_type = leg.get("prop_type", "?").replace("_", " ").title()
            side      = leg.get("side", "?")
            line      = leg.get("line", "?")
            streak_n  = leg.get("streak_length", leg.get("streak", "?"))
            fields.append({
                "name": f"Leg {i} — {player}",
                "value": (
                    f"**{prop_type} {side} {line}**\n"
                    f"Streak: `{streak_n} consecutive`"
                ),
                "inline": False,
            })

        fields.append({
            "name": f"🔥 Streak Summary — {leg_count}-Leg",
            "value": f"EV: **+{ev_pct:.1f}%** | Confidence: **{confidence:.1f}/10** | Stake: **${stake:.0f}**",
            "inline": False,
        })

        season = parlay.get("season_stats", {})
        s_wins   = season.get("wins", 0)
        s_losses = season.get("losses", 0)
        s_roi    = season.get("roi_pct", season.get("roi", 0.0))
        footer_str = f"StreakAgent Season: {s_wins}W-{s_losses}L | ROI {s_roi:+.1f}%"

        self._post({
            "embeds": [{
                "title": f"🔥 StreakAgent — {leg_count}-Leg Underdog Streak",
                "description": "🐶 **Open Underdog Fantasy → Streaks tab**",
                "color": _COLOUR_GOLD,
                "fields": fields,
                "footer": {"text": footer_str},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        logger.info("[Discord] Streak alert sent — %d legs, +%.1f%% EV, $%.0f stake",
                    leg_count, ev_pct, stake)


# Module-level singleton — import this everywhere
discord_alert = DiscordAlertService()
