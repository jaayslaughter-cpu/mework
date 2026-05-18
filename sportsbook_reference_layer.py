"""
sportsbook_reference_layer.py
=============================
PropIQ — Sportsbook sharp-line reference layer.

Fallback chain:
  Tier 0   — DraftKings Internal API (6 prop types, no auth)
  Tier 0.5 — FanDuel Internal API (5 pitcher types)
  Tier 1   — The Odds API (Pinnacle+DK+FD+MGM)
  Tier 1.5b — odds-api.net (bet365+betr)
  Tier 4   — DraftEdge projections
  Tier 5   — ActionNetwork money%
  Tier 6   — TheRundown (all 9 MLB prop markets)
  Tier 7   — VegasInsider scrape
  Tier 8   — RotoWire supplement

PR #585: ActionNetwork always-run supplement + TheRundown all 9 markets.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger("propiq.sportsbook_reference")


def build_sportsbook_reference() -> dict:
    """
    Build sharp-line reference dict keyed by (player_name_lower, prop_type).
    Returns dict with implied-prob values from sharp books.
    Returns empty dict on any failure so callers degrade gracefully.
    """
    try:
        return _build()
    except Exception as exc:
        logger.warning("[SBRef] build_sportsbook_reference failed: %s", exc)
        return {}


def _build() -> dict:
    """Internal — assemble reference data from all available tiers."""
    _mem_ref: dict = {}

    # ── Tier 0: DraftKings Internal API ─────────────────────────────────────
    try:
        from draftkings_layer import get_dk_sharp_prob as _dk_prob  # noqa: PLC0415
        _dk_result = _dk_prob()
        if isinstance(_dk_result, dict):
            for k, v in _dk_result.items():
                _mem_ref.setdefault(k, v)
        logger.debug("[SBRef] Tier 0 DK: %d entries", len(_dk_result) if isinstance(_dk_result, dict) else 0)
    except Exception as _e0:
        logger.debug("[SBRef] Tier 0 DK failed: %s", _e0)

    # ── Tier 0.5: FanDuel Internal API ──────────────────────────────────────
    try:
        from fanduel_layer import get_fd_sharp_prob as _fd_prob  # noqa: PLC0415
        _fd_result = _fd_prob()
        if isinstance(_fd_result, dict):
            for k, v in _fd_result.items():
                _mem_ref.setdefault(k, v)
        logger.debug("[SBRef] Tier 0.5 FD: %d entries", len(_fd_result) if isinstance(_fd_result, dict) else 0)
    except Exception as _e05:
        logger.debug("[SBRef] Tier 0.5 FD failed: %s", _e05)

    # ── Tier 1: The Odds API ─────────────────────────────────────────────────
    try:
        import redis as _redis_mod  # noqa: PLC0415
        import json as _json        # noqa: PLC0415
        from datetime import datetime  # noqa: PLC0415
        import pytz                 # noqa: PLC0415
        _r_url = os.environ.get("REDIS_URL") or os.environ.get("REDIS_PUBLIC_URL")
        if _r_url:
            _r = _redis_mod.from_url(_r_url, decode_responses=True)
            _date = datetime.now(pytz.timezone("America/Los_Angeles")).strftime("%Y%m%d")
            _cached = _r.get(f"sb_ref_{_date}")
            if _cached:
                _cached_dict = _json.loads(_cached)
                for k, v in _cached_dict.items():
                    _mem_ref.setdefault(k, v)
                logger.debug("[SBRef] Tier 1 Redis cache: %d entries", len(_cached_dict))
    except Exception as _e1:
        logger.debug("[SBRef] Tier 1 Redis read failed: %s", _e1)

    # ── Tier 5 (always-run supplement): ActionNetwork ────────────────────────
    try:
        from action_network_layer import (  # noqa: PLC0415
            fetch_mlb_prop_projections as _an_fetch,
        )
        _an_props = _an_fetch()
        _an_count = 0
        for p in (_an_props or []):
            if not isinstance(p, dict):
                continue
            _player = (p.get("player") or "").strip().lower()
            _pt = (p.get("prop_type") or "").strip().lower()
            if not _player or not _pt:
                continue
            _over_pct = p.get("over_ticket_pct") or p.get("over_pct")
            _money_pct = p.get("over_money_pct")
            if _over_pct is None and _money_pct is None:
                continue
            _ref_pct = float(_over_pct or _money_pct or 50)
            _implied = max(0.30, min(0.70, _ref_pct / 100.0))
            key = f"{_player}:{_pt}"
            _mem_ref.setdefault(key, {"implied_prob": _implied, "source": "action_network"})
            _an_count += 1
        if _an_count:
            logger.debug("[SBRef] Tier 5 AN supplement: %d entries", _an_count)
    except Exception as _e5:
        logger.debug("[SBRef] Tier 5 AN supplement failed: %s", _e5)

    # ── Tier 6 (always-run supplement): TheRundown ───────────────────────────
    _RUNDOWN_MARKETS: dict[str, int] = {
        "strikeouts":           19,
        "hits":                 53,
        "total_bases":          54,
        "hits_runs_rbis":       55,
        "hitter_strikeouts":    56,
        "pitching_outs":        57,
        "hits_allowed":         58,
        "earned_runs":          59,
        "walks_allowed":        60,
    }
    _rundown_key = os.environ.get("RUNDOWN_API_KEY", "")
    if _rundown_key:
        try:
            import requests as _req  # noqa: PLC0415
            _added = 0
            for _prop_type, _market_id in _RUNDOWN_MARKETS.items():
                try:
                    _resp = _req.get(
                        f"https://therundown-therundown-v1.p.rapidapi.com/sports/3/odds/market/{_market_id}",
                        headers={
                            "X-RapidAPI-Key":  _rundown_key,
                            "X-RapidAPI-Host": "therundown-therundown-v1.p.rapidapi.com",
                        },
                        timeout=8,
                    )
                    if _resp.status_code != 200:
                        continue
                    data = _resp.json()
                    for event in data.get("events", []) or []:
                        for line in event.get("lines", {}).values():
                            _pname = (line.get("player_name") or "").strip().lower()
                            if not _pname:
                                continue
                            _over_ml = line.get("over")
                            _under_ml = line.get("under")
                            if _over_ml is None:
                                continue
                            try:
                                _op = abs(_over_ml)
                                _over_dec = (100 / _op + 1) if _over_ml < 0 else (_op / 100 + 1)
                                _up = abs(_under_ml) if _under_ml else _op
                                _under_dec = (100 / _up + 1) if (_under_ml or 0) < 0 else (_up / 100 + 1)
                                _fair = (1 / _over_dec) / (1 / _over_dec + 1 / _under_dec)
                            except Exception:
                                _fair = 0.5
                            _key = f"{_pname}:{_prop_type}"
                            if _key not in _mem_ref:
                                _mem_ref[_key] = {"implied_prob": round(_fair, 4), "source": "therundown"}
                                _added += 1
                except Exception:
                    continue
            if _added:
                logger.debug("[SBRef] Tier 6 TheRundown supplement: %d entries", _added)
        except Exception as _e6:
            logger.debug("[SBRef] Tier 6 TheRundown failed: %s", _e6)

    return _mem_ref
