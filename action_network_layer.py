"""
action_network_layer.py
=======================
PropIQ — Action Network data layer.

Fetches MLB prop projections, game sentiment, and sharp money signals
from Action Network's internal API (cookie-authenticated).

Required env var: ACTION_NETWORK_COOKIE — raw token (no "Bearer" prefix).

PR #585: pitcher_walks key corrected to walks_allowed;
         implied-prob clamp widened to max(0.30, min(0.70, ...)) to allow
         real sharp flow beyond the old ±10pp cap.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger("propiq.action_network")

_AN_COOKIE  = os.environ.get("ACTION_NETWORK_COOKIE", "")

# Market name → canonical prop_type
_AN_MARKET: dict[str, str] = {
    "pitcher strikeouts":       "strikeouts",
    "strikeouts":               "strikeouts",
    "hits":                     "hits",
    "total bases":              "total_bases",
    "total_bases":              "total_bases",
    "hits runs rbis":           "hits_runs_rbis",
    "hits + runs + rbis":       "hits_runs_rbis",
    "h+r+rbi":                  "hits_runs_rbis",
    "batter strikeouts":        "hitter_strikeouts",
    "hitter strikeouts":        "hitter_strikeouts",
    "pitching outs":            "pitching_outs",
    "outs":                     "pitching_outs",
    "hits allowed":             "hits_allowed",
    "earned runs":              "earned_runs",
    "walks allowed":            "walks_allowed",
    "pitcher walks":            "walks_allowed",  # PR #585: was pitcher_walks (wrong)
    "walks":                    "walks_allowed",
}

_HEADERS: dict = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://www.actionnetwork.com",
    "Referer": "https://www.actionnetwork.com/",
}


def _auth_headers() -> dict:
    h = dict(_HEADERS)
    if _AN_COOKIE:
        h["x-auth-token"] = _AN_COOKIE
    return h


def fetch_mlb_prop_projections() -> list[dict]:
    """
    Fetch MLB player prop projections with over/under ticket % and money %.
    Returns list of dicts with player, prop_type, line, over_ticket_pct, etc.
    Returns [] on any failure.
    """
    if not _AN_COOKIE:
        logger.debug("[AN] No ACTION_NETWORK_COOKIE — prop projections skipped.")
        return []
    try:
        return _fetch_prop_proj_rest()
    except Exception as exc:
        logger.warning("[AN] fetch_mlb_prop_projections failed: %s", exc)
        return []


def _fetch_prop_proj_rest() -> list[dict]:
    """REST fallback via Action Network internal API."""
    import requests  # noqa: PLC0415
    url = "https://api.actionnetwork.com/web/v2/games?league=mlb&market=player_props&bookIds=15,30"
    try:
        resp = requests.get(url, headers=_auth_headers(), timeout=12)
        if resp.status_code != 200:
            logger.debug("[AN] prop proj REST: HTTP %d", resp.status_code)
            return []
        data = resp.json()
    except Exception as exc:
        logger.debug("[AN] prop proj REST request failed: %s", exc)
        return []

    results = []
    for game in data.get("games", []) or []:
        for prop in game.get("player_props", []) or []:
            player = (prop.get("player_name") or prop.get("player") or "").strip()
            market = (prop.get("market_name") or prop.get("type") or "").lower().strip()
            prop_type = _AN_MARKET.get(market, market)
            line = prop.get("line") or prop.get("value")
            if not player or not prop_type:
                continue
            over_t  = prop.get("over_ticket_pct",  prop.get("over_bets_pct",  50))
            under_t = prop.get("under_ticket_pct", prop.get("under_bets_pct", 50))
            over_m  = prop.get("over_money_pct",   50)
            under_m = prop.get("under_money_pct",  50)
            # PR #585: widened clamp from ±10pp to ±20pp around 50%
            _raw = float(over_m or over_t or 50)
            implied = max(0.30, min(0.70, _raw / 100.0))
            results.append({
                "player":            player,
                "prop_type":         prop_type,
                "line":              float(line) if line is not None else None,
                "over_ticket_pct":   float(over_t or 50),
                "under_ticket_pct":  float(under_t or 50),
                "over_money_pct":    float(over_m or 50),
                "under_money_pct":   float(under_m or 50),
                "implied_prob":      round(implied, 4),
                "rlm_signal":        False,
                "rlm_direction":     None,
                "source":            "action_network",
            })
    logger.debug("[AN] prop proj REST: %d props", len(results))
    return results


def fetch_mlb_game_sentiment() -> dict:
    """
    Fetch per-team game sentiment (over/under ticket % and money %).
    Returns dict keyed by team_name → {over_ticket_pct, over_money_pct, ...}.
    Returns {} on any failure.
    """
    if not _AN_COOKIE:
        return {}
    try:
        import requests  # noqa: PLC0415
        resp = requests.get(
            "https://api.actionnetwork.com/web/v2/games?league=mlb",
            headers=_auth_headers(), timeout=10,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        result: dict = {}
        for game in data.get("games", []) or []:
            for side in ("away", "home"):
                team = game.get(f"{side}_team", {})
                name = team.get("abbr") or team.get("name") or ""
                if not name:
                    continue
                result[name] = {
                    "over_ticket_pct":  game.get("over_bets_pct",  50),
                    "over_money_pct":   game.get("over_money_pct", 50),
                    "under_ticket_pct": game.get("under_bets_pct", 50),
                    "under_money_pct":  game.get("under_money_pct",50),
                }
        return result
    except Exception as exc:
        logger.warning("[AN] fetch_mlb_game_sentiment failed: %s", exc)
        return {}


def build_sharp_report() -> dict:
    """
    Build a sharp-money report dict: player → {sharp_side, rlm, steam}.
    Returns {} on failure.
    """
    try:
        props = fetch_mlb_prop_projections()
        report: dict = {}
        for p in props:
            player = p.get("player", "")
            pt = p.get("prop_type", "")
            if not player or not pt:
                continue
            over_t = float(p.get("over_ticket_pct", 50))
            over_m = float(p.get("over_money_pct",  50))
            # Sharp on Over when money% > ticket% by 10pp+
            _sharp_over  = (over_m - over_t) >= 10
            _sharp_under = (over_t - over_m) >= 10
            report[f"{player.lower()}:{pt}"] = {
                "sharp_side": "OVER" if _sharp_over else ("UNDER" if _sharp_under else None),
                "rlm":        p.get("rlm_signal", False),
                "over_ticket_pct": over_t,
                "over_money_pct":  over_m,
            }
        return report
    except Exception as exc:
        logger.warning("[AN] build_sharp_report failed: %s", exc)
        return {}


def fetch_live_projections() -> list[dict]:
    """Alias for fetch_mlb_prop_projections — live projections from AN."""
    return fetch_mlb_prop_projections()


def fetch_all_projections() -> dict:
    """
    Return projections split into batters/pitchers DataFrames-like structure.
    Used by DraftEdge iteration path.
    """
    try:
        import pandas as _pd  # noqa: PLC0415
        props = fetch_mlb_prop_projections()
        batters  = [p for p in props if p.get("prop_type") in {"hits", "total_bases", "hits_runs_rbis", "hitter_strikeouts"}]
        pitchers = [p for p in props if p.get("prop_type") in {"strikeouts", "pitching_outs", "hits_allowed", "earned_runs", "walks_allowed"}]
        return {
            "batters":  _pd.DataFrame(batters)  if batters  else _pd.DataFrame(),
            "pitchers": _pd.DataFrame(pitchers) if pitchers else _pd.DataFrame(),
        }
    except ImportError:
        return {"batters": [], "pitchers": []}
    except Exception as exc:
        logger.warning("[AN] fetch_all_projections failed: %s", exc)
        return {"batters": [], "pitchers": []}
