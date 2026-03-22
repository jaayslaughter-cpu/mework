"""
DiscordAlertService.py
======================
Outbound-only Discord webhook integration for PropIQ Analytics Engine.

Changes in Phase 19:
  - Parlay format: one embed per parlay with ALL legs visible (no truncation)
  - $20 hard-cap stake per parlay
  - Platform badge per leg (PrizePicks vs Underdog) — picks better win-prob
  - Fantasy-points legs supported (Hitter Fantasy Score / Pitcher Fantasy Score)
  - Innings-pitched props removed
  - Hits + Runs + RBIs combo prop supported
  - Pitching strikeouts labelled as "Pitcher Ks" for clarity

Public API
----------
  discord_alert.send_startup_ping()
  discord_alert.send_parlay_alert(parlay: dict)    ← NEW — replaces send_bet_alert
  discord_alert.send_bet_alert(bet: dict)           ← kept for single-leg fallback
  discord_alert.send_daily_recap(results, profit, date_str)

Environment
-----------
  DISCORD_WEBHOOK_URL  — incoming webhook URL (required)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("propiq.discord")

# ── Colour palette ────────────────────────────────────────────────────────────
_COLOUR_GREEN  = 0x2ECC71
_COLOUR_RED    = 0xE74C3C
_COLOUR_BLUE   = 0x3498DB
_COLOUR_GOLD   = 0xF1C40F
_COLOUR_PURPLE = 0x9B59B6   # parlay alert
_COLOUR_GREY   = 0x95A5A6

# ── Platform badges ───────────────────────────────────────────────────────────
_PLATFORM_EMOJI: dict[str, str] = {
    "prizepicks": "🏆",
    "underdog":   "🐶",
}

# ── Prop display labels (human-friendly) ──────────────────────────────────────
_PROP_LABELS: dict[str, str] = {
    "strikeouts":      "Pitcher Ks",
    "hits":            "Hits",
    "home_runs":       "Home Runs",
    "rbis":            "RBIs",
    "runs":            "Runs",
    "total_bases":     "Total Bases",
    "stolen_bases":    "Stolen Bases",
    "walks":           "Walks",
    "hits_runs_rbis":  "Hits+Runs+RBIs",
    "fantasy_hitter":  "Hitter Fantasy Pts",
    "fantasy_pitcher": "Pitcher Fantasy Pts",
    "earned_runs":     "Earned Runs",
    "doubles":         "Doubles",
    "triples":         "Triples",
}

# ── Entry type badges ─────────────────────────────────────────────────────────
_ENTRY_BADGES: dict[str, str] = {
    "FLEX":     "🔀 FLEX",
    "STANDARD": "⭐ STANDARD",
}

# ── Max stake ─────────────────────────────────────────────────────────────────
MAX_STAKE_USD: float = 20.00

# ── Underdog payout multipliers (approximate, for display) ────────────────────
UD_PAYOUT: dict[int, dict[str, float]] = {
    2:  {"FLEX": 3.0,   "STANDARD": 6.0},
    3:  {"FLEX": 6.0,   "STANDARD": 10.0},
    4:  {"FLEX": 10.0,  "STANDARD": 20.0},
    5:  {"FLEX": 16.0,  "STANDARD": 40.0},
    6:  {"FLEX": 25.0,  "STANDARD": 70.0},
}

# PrizePicks payout multipliers (approximate)
PP_PAYOUT: dict[int, dict[str, float]] = {
    2:  {"FLEX": 3.0,   "POWER": 3.0},
    3:  {"FLEX": 5.0,   "POWER": 6.0},
    4:  {"FLEX": 10.0,  "POWER": 10.0},
    5:  {"FLEX": 16.0,  "POWER": 20.0},
    6:  {"FLEX": 25.0,  "POWER": 40.0},
}


class DiscordAlertService:
    """Thin wrapper around a single Discord incoming webhook URL."""

    def __init__(self) -> None:
        self._url: str = os.getenv(
            "DISCORD_WEBHOOK_URL",
            "https://discordapp.com/api/webhooks/1484795164961800374/"
            "jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM"
        )

    # ── Internal helper ───────────────────────────────────────────────────────

    def _post(self, payload: dict[str, Any]) -> bool:
        url = os.getenv("DISCORD_WEBHOOK_URL", self._url)
        if not url:
            logger.warning("[Discord] DISCORD_WEBHOOK_URL not set — skipping.")
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
            logger.warning("[Discord] Webhook HTTP %d: %s",
                           resp.status_code, resp.text[:200])
            return False
        except Exception as exc:
            logger.warning("[Discord] Webhook failed: %s", exc)
            return False

    # ── Startup ping ──────────────────────────────────────────────────────────

    def send_startup_ping(self) -> None:
        ok = self._post({
            "embeds": [{
                "title": "✅ PropIQ Engine Online",
                "description": (
                    "All 15 agents armed and ready 🤖\n"
                    "PrizePicks + Underdog Fantasy lines active.\n"
                    "Parlay alerts will fire daily pre-game."
                ),
                "color": _COLOUR_GREEN,
                "footer": {"text": "PropIQ Analytics Engine — Phase 19"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        if ok:
            logger.info("[Discord] Startup ping sent.")

    # ── Parlay alert (PRIMARY) ────────────────────────────────────────────────

    def send_parlay_alert(self, parlay: dict) -> None:
        """
        Send a fully formatted parlay embed with ALL legs visible.

        Expected parlay dict keys:
            agent_name     str    e.g. "EVHunter"
            agent_emoji    str    e.g. "🎯"
            legs           list[dict]  — see _format_leg()
            entry_type     str    "FLEX" | "STANDARD" | "POWER"
            ev_pct         float  overall parlay EV %
            confidence     int    1–10
            notes          str    optional agent commentary

        Each leg dict:
            player_name    str
            prop_type      str    key from _PROP_LABELS
            side           str    "Over" | "Under"
            line           float
            platform       str    "PrizePicks" | "Underdog"
            implied_prob   float  0.0–1.0
            entry_type     str    "FLEX" | "STANDARD" (Underdog) or "" (PP)
            fantasy_pts    float  expected fantasy pts (for FP legs, else 0)
        """
        legs          = parlay.get("legs", [])
        agent_name    = parlay.get("agent_name", "PropIQ Agent")
        agent_emoji   = parlay.get("agent_emoji", "🤖")
        entry_type    = parlay.get("entry_type", "FLEX")
        ev_pct        = parlay.get("ev_pct", 0.0)
        conf          = int(parlay.get("confidence", 7))
        notes         = parlay.get("notes", "")
        n_legs        = len(legs)

        if not legs:
            return

        # Determine dominant platform (majority of legs)
        plat_counts: dict[str, int] = {}
        for leg in legs:
            p = leg.get("platform", "PrizePicks")
            plat_counts[p] = plat_counts.get(p, 0) + 1
        dom_platform = max(plat_counts, key=lambda k: plat_counts[k])
        dom_emoji    = _PLATFORM_EMOJI.get(dom_platform.lower(), "🎯")

        # Payout estimate
        payout_mult = self._est_payout(n_legs, entry_type, dom_platform)
        max_payout  = round(MAX_STAKE_USD * payout_mult, 2)

        # Build leg lines
        leg_lines: list[str] = []
        for i, leg in enumerate(legs, 1):
            leg_lines.append(self._format_leg(i, leg))

        legs_text = "\n".join(leg_lines)

        # Confidence bar
        conf_bar = "█" * conf + "░" * (10 - conf)

        # Entry type badge
        etype_badge = _ENTRY_BADGES.get(entry_type, entry_type)

        title = (
            f"{dom_emoji} {dom_platform.upper()} — "
            f"{agent_emoji} {agent_name} | {n_legs}-Leg {entry_type} Parlay"
        )

        # Mixed platforms note
        plat_note = ""
        if len(plat_counts) > 1:
            breakdown = " + ".join(
                f"{v} {k}" for k, v in sorted(plat_counts.items(),
                                               key=lambda x: -x[1])
            )
            plat_note = f"\n> *Mixed: {breakdown} legs*"

        description = (
            f"**💵 Stake: ${MAX_STAKE_USD:.0f}  →  Max Payout: ${max_payout:.2f}**\n"
            f"**{etype_badge}**{plat_note}\n\n"
            f"{legs_text}"
        )

        if notes:
            description += f"\n\n> 📝 {notes}"

        fields = [
            {"name": "📈 Parlay EV",    "value": f"+{ev_pct:.1f}%",          "inline": True},
            {"name": "🔥 Confidence",   "value": f"{conf_bar} {conf}/10",     "inline": True},
            {"name": "🎰 Payout",       "value": f"{payout_mult:.1f}x = ${max_payout:.2f}", "inline": True},
        ]

        # ── Season record footer field ─────────────────────────────────────
        season_stats = parlay.get("season_stats", {})
        if season_stats and isinstance(season_stats, dict):
            rec     = season_stats.get("record", "0W-0L-0P")
            units   = season_stats.get("units_profit", 0.0)
            roi     = season_stats.get("roi_pct", 0.0)
            pending = season_stats.get("pending", 0)
            last    = season_stats.get("last_result", "—")

            sign    = "+" if units >= 0 else ""
            pend_str = f"  ·  {pending} pending" if pending > 0 else ""
            last_str = f"  ·  Last: {last}" if last != "—" else ""

            season_line = (
                f"{rec}  ·  {sign}{units:.1f}u  ·  ROI: {sign}{roi:.1f}%"
                f"{pend_str}{last_str}"
            )
            fields.append({
                "name":   "📊 2026 Season Record",
                "value":  season_line,
                "inline": False,
            })

        self._post({
            "embeds": [{
                "title": title,
                "description": description,
                "color": _COLOUR_PURPLE,
                "fields": fields,
                "footer": {
                    "text": (
                        f"PropIQ Analytics — {agent_name} | "
                        f"Open {dom_platform} and enter manually"
                    )
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })

    def _format_leg(self, idx: int, leg: dict) -> str:
        """Format a single parlay leg as a Discord-friendly string."""
        player    = leg.get("player_name", "Unknown")
        prop_raw  = leg.get("prop_type", "")
        prop_lbl  = _PROP_LABELS.get(prop_raw, prop_raw.replace("_", " ").title())
        side      = leg.get("side", "Over")
        line      = leg.get("line", 0.0)
        platform  = leg.get("platform", "PrizePicks")
        prob      = leg.get("implied_prob", 0.52)
        fp_pts    = leg.get("fantasy_pts", 0.0)
        etype     = leg.get("entry_type", "")

        plat_emoji = _PLATFORM_EMOJI.get(platform.lower(), "🎯")
        side_emoji = "🔼" if side == "Over" else "🔽"

        prob_pct = f"{prob * 100:.1f}%"

        # Platform label with entry type for Underdog
        plat_label = platform
        if platform.lower() == "underdog" and etype:
            plat_label = f"Underdog ({etype})"

        # Fantasy points note
        fp_note = ""
        if fp_pts > 0.1:
            fp_note = f" *(exp {fp_pts:.1f} FP)*"

        return (
            f"**{idx}.** {player} — "
            f"{side_emoji} **{side} {line:.1f}** {prop_lbl}{fp_note}\n"
            f"   {plat_emoji} {plat_label} | Win Prob: **{prob_pct}**"
        )

    @staticmethod
    def _est_payout(n_legs: int, entry_type: str, platform: str) -> float:
        """Return approximate payout multiplier."""
        n = min(max(n_legs, 2), 6)
        if platform.lower() == "underdog":
            return UD_PAYOUT.get(n, {}).get(entry_type, 10.0)
        # PrizePicks
        etype = "POWER" if entry_type in ("POWER", "STANDARD") else "FLEX"
        return PP_PAYOUT.get(n, {}).get(etype, 10.0)

    # ── Single bet fallback ───────────────────────────────────────────────────

    def send_bet_alert(self, bet: dict) -> None:
        """Single-leg bet alert (fallback for agents that don't build parlays)."""
        player    = bet.get("player", "Unknown")
        prop_type = bet.get("prop_type", "")
        prop_lbl  = _PROP_LABELS.get(prop_type, prop_type.replace("_", " ").title())
        line      = bet.get("line", "")
        side      = bet.get("side", "")
        ev_pct    = bet.get("ev_pct", 0.0)
        kelly     = bet.get("kelly_units", 0.0)
        conf      = int(bet.get("confidence", 5))
        platform  = bet.get("recommended_platform", "PrizePicks")
        odds_raw  = bet.get("odds_american", -110)
        model_prob = bet.get("model_prob", 50.0)

        odds_str   = f"+{odds_raw}" if isinstance(odds_raw, int) and odds_raw > 0 else str(odds_raw)
        plat_lower = str(platform).lower()
        plat_emoji = _PLATFORM_EMOJI.get(plat_lower, "🎯")
        conf_bar   = "█" * conf + "░" * (10 - conf)

        self._post({
            "embeds": [{
                "title": f"{plat_emoji} {platform.upper()} — Single Prop Alert",
                "color": _COLOUR_BLUE,
                "fields": [
                    {"name": "🧑 Player",     "value": player,                              "inline": True},
                    {"name": "📊 Prop",        "value": f"{prop_lbl} {side} {line}",         "inline": True},
                    {"name": "💰 Odds",        "value": odds_str,                            "inline": True},
                    {"name": "📈 Edge (EV)",   "value": f"+{ev_pct:.1f}%",                   "inline": True},
                    {"name": "🎲 Kelly",       "value": f"{kelly:.3f}u",                     "inline": True},
                    {"name": "🤖 Model Prob",  "value": f"{model_prob:.1f}%",                "inline": True},
                    {"name": "🔥 Confidence",  "value": f"{conf_bar}  {conf}/10",            "inline": False},
                    {"name": "💵 Stake",       "value": f"${MAX_STAKE_USD:.0f} max",         "inline": True},
                ],
                "footer": {"text": "PropIQ Analytics Engine"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })

    # ── Daily recap ───────────────────────────────────────────────────────────

    def send_daily_recap(
        self,
        results: list[dict],
        total_profit: float,
        date_str: str,
    ) -> None:
        """End-of-day recap after GradingTasklet settles all bets."""
        wins   = sum(1 for r in results if r.get("status") == "WIN")
        losses = sum(1 for r in results if r.get("status") == "LOSS")
        pushes = sum(1 for r in results if r.get("status") == "PUSH")
        sign   = "+" if total_profit >= 0 else ""
        colour = _COLOUR_GREEN if total_profit >= 0 else _COLOUR_RED

        lines_txt: list[str] = []
        for r in results:
            emoji    = {"WIN": "✅", "LOSS": "❌", "PUSH": "➖"}.get(r.get("status", ""), "❓")
            pl       = r.get("profit_loss", 0.0)
            pl_sign  = "+" if pl >= 0 else ""
            platform = r.get("platform", "PP")
            plat_em  = _PLATFORM_EMOJI.get(platform.lower(), "🎯")
            prop_lbl = _PROP_LABELS.get(r.get("prop_type", ""), r.get("prop_type", "?"))
            lines_txt.append(
                f"{emoji} {plat_em} **{r.get('player', '?')}** — "
                f"{prop_lbl} {r.get('side', '?')} {r.get('line', '?')} | "
                f"{pl_sign}{pl:.2f}u"
            )

        description = "\n".join(lines_txt) or "_No graded bets._"
        if len(description) > 3_000:
            description = description[:2_950] + "\n…(truncated)"

        self._post({
            "embeds": [{
                "title": f"📊 PropIQ Daily Recap — {date_str}",
                "description": description,
                "color": colour,
                "fields": [
                    {"name": "📈 P&L",    "value": f"{sign}{total_profit:.2f}u",      "inline": True},
                    {"name": "🏆 Record", "value": f"{wins}-{losses}-{pushes} W-L-P", "inline": True},
                    {"name": "💵 Stake",  "value": f"${MAX_STAKE_USD:.0f}/parlay",     "inline": True},
                ],
                "footer": {"text": "Powered by PropIQ Analytics 🤖"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        logger.info("[Discord] Daily recap — %s  %s%+.2fu  %d-%d-%d",
                    date_str, sign, total_profit, wins, losses, pushes)


# Module-level singleton
discord_alert = DiscordAlertService()
