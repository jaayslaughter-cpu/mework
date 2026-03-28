"""
live_dispatcher.py
==================
Daily pre-game parlay dispatcher for PropIQ Analytics Engine.

Runs once per day (triggered by scheduler at ~11 AM ET before first pitch).
Fetches live props from PrizePicks and Underdog Fantasy, runs platform
comparison, applies agent-specific filters, builds optimal parlays per
agent, and fires Discord alerts for manual entry.

Flow
----
  1. Fetch today's MLB schedule via MLB Stats API (free, no key)
  2. Fetch live props from PrizePicks and Underdog Fantasy APIs
  3. Merge and deduplicate player lines across both platforms
  4. Fetch baseline stat projections from MLB Stats API (season averages)
  5. Run platform_selector for each prop -> pick PrizePicks or Underdog
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

# Season record tracker (writes to agent SQL DB)
try:
    from season_record import record_parlay, get_agent_season_stats
    _SEASON_RECORD_AVAILABLE = True
except ImportError:
    _SEASON_RECORD_AVAILABLE = False
    def record_parlay(*a, **kw): return False            # noqa: E704
    def get_agent_season_stats(agent): return {}         # noqa: E704

try:
    from platform_selector import PlatformSelector, SelectionResult
    platform_selector = PlatformSelector()
    _PLATFORM_SELECTOR_AVAILABLE = True
except ImportError:
    _PLATFORM_SELECTOR_AVAILABLE = False
    platform_selector = None  # type: ignore[assignment]

from DiscordAlertService import discord_alert, MAX_STAKE_USD

# ── Phase 48 gap-fix: per-agent unit sizing ───────────────────────────────────
try:
    from agent_unit_sizing import get_all_units as _get_all_units
    _UNIT_SIZING_AVAILABLE = True
except ImportError:
    _UNIT_SIZING_AVAILABLE = False
    def _get_all_units() -> dict: return {}  # noqa: E704

# ── Phase 27: Enhancement layer imports (all optional -- graceful fallback) ──
try:
    from draftedge_scraper import enrich_props_with_draftedge as _de_enrich
    _DE_AVAILABLE = True
except ImportError:
    _DE_AVAILABLE = False
    def _de_enrich(props: list) -> list: return props  # noqa: E704

try:
    from statcast_feature_layer import (
        enrich_props_with_statcast as _sc_enrich,
        StatcastFeatureLayer,
    )
    _SC_AVAILABLE = True
except ImportError:
    _SC_AVAILABLE = False
    def _sc_enrich(props: list, player_type: str, layer=None) -> list: return props  # noqa: E704
    class StatcastFeatureLayer:  # noqa: E302
        pass

try:
    from public_trends_scraper import PublicTrendsScraper, get_fade_signal as _get_fade_signal
    _SBD_AVAILABLE = True
except ImportError:
    _SBD_AVAILABLE = False
    def _get_fade_signal(*a, **kw):  # noqa: E302, E704
        return 0.0, "none"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s -- %(message)s",
)
logger = logging.getLogger("propiq.live")

# ---------------------------------------------------------------------------
# Hot/cold form layer (MLB Stats API game logs -- free, no key required)
# ---------------------------------------------------------------------------
try:
    from mlb_form_layer import form_layer as _form_layer
    _FORM_LAYER_AVAILABLE = True
    logger.info("[Form] Hot/cold form layer loaded.")
except ImportError:
    _FORM_LAYER_AVAILABLE = False

    class _DummyFormLayer:  # noqa: D101
        def prefetch_form_data(self, *a, **kw) -> None: pass       # noqa: E704
        def get_form_adjustment(self, *a, **kw) -> float: return 0.0  # noqa: E704

    _form_layer = _DummyFormLayer()  # type: ignore[assignment]
    logger.warning("[Form] mlb_form_layer not found -- form adjustments disabled.")

# ---------------------------------------------------------------------------
# FanGraphs season stats layer (pybaseball -- Phase 34)
# Provides CSW%, wRC+, xFIP, ISO, wOBA and 10 additional metrics.
# Cached daily; gracefully disabled if pybaseball is unavailable.
# ---------------------------------------------------------------------------
try:
    from fangraphs_layer import (
        fangraphs_adjustment as _fg_adjustment,
        get_batter as _fg_get_batter,
        get_pitcher as _fg_get_pitcher,
    )
    _FG_AVAILABLE = True
    logger.info("[FG] FanGraphs layer loaded.")
except ImportError:
    _FG_AVAILABLE = False

    def _fg_adjustment(*_a, **_kw) -> float: return 0.0  # noqa: E704
    def _fg_get_batter(*_a, **_kw) -> dict: return {}     # noqa: E704
    def _fg_get_pitcher(*_a, **_kw) -> dict: return {}    # noqa: E704

    logger.warning("[FG] fangraphs_layer not found -- FanGraphs adjustments disabled.")

# ── Layer 7: Sportsbook Reference (The Odds API -- vig-stripped sharp market) ──
try:
    from sportsbook_reference_layer import enrich_props_with_sportsbook as _sb_enrich
    _SB_REF_AVAILABLE = True
    logger.info("[SB_REF] Sportsbook reference layer loaded.")
except ImportError:
    _SB_REF_AVAILABLE = False

    def _sb_enrich(props: list, date: str | None = None) -> list: return props  # noqa: E704

    logger.warning("[SB_REF] sportsbook_reference_layer not found -- Layer 7 disabled.")

# ── Layer 8a: Marcel 3-year projections (baseball-sims algorithm, no BHQ) ────
# Derived from thomasosbot/baseball-sims src/features/marcel.py.
# Uses only public FanGraphs JSON API -- no subscription required.
try:
    from marcel_layer import MarcelLayer as _MarcelLayer
    from marcel_layer import marcel_adjustment as _marcel_adjustment
    _marcel_layer = _MarcelLayer()
    _MARCEL_AVAILABLE = True
    logger.info("[Marcel] Marcel projection layer loaded.")
except ImportError:
    _MARCEL_AVAILABLE = False
    _marcel_layer = None  # type: ignore[assignment]

    def _marcel_adjustment(*_a, **_kw) -> float: return 0.0  # noqa: E704

    logger.warning("[Marcel] marcel_layer not found -- Layer 8a disabled.")

# ── Layer 8b: Predict+ pitcher unpredictability (PredictPlus port) ────────────
# Ported from jaime12minaya/PredictPlus (R -> Python).
# Measures pitch-sequence unpredictability via multinomial LR surprise ratio.
# High Predict+ (>=110) -> K Over boost; Low Predict+ (<=90) -> K Under boost.
try:
    from predict_plus_layer import PredictPlusLayer as _PredictPlusLayer
    from predict_plus_layer import predict_plus_adjustment as _pp_adjustment
    _pp_layer = _PredictPlusLayer()
    _PP_AVAILABLE = True
    logger.info("[PP+] Predict+ layer loaded.")
except ImportError:
    _PP_AVAILABLE = False
    _pp_layer = None  # type: ignore[assignment]

    def _pp_adjustment(*_a, **_kw) -> float: return 0.0  # noqa: E704

    logger.warning("[PP+] predict_plus_layer not found -- Layer 8b disabled.")
# ── Phase 51: Dome Stadium Adjustment ─────────────────────────────────────
try:
    from dome_adjustment import apply_dome_adjustment as _apply_dome_adj
    _DOME_AVAILABLE = True
    logger.info("[Dome] Dome adjustment module loaded.")
except ImportError:
    _DOME_AVAILABLE = False

    def _apply_dome_adj(prob, prop_type, venue, roof_status="closed", is_home_team=True):  # noqa: E302
        return prob, 0.0

    logger.warning("[Dome] dome_adjustment not found -- dome context modifier disabled.")

# ── Phase 53: Altitude park factor adjustment ─────────────────────────────────
try:
    from altitude_adjustment import (
        apply_altitude_adjustments as _alt_adjust,
        get_humidor_status as _get_humidor,
    )
    _ALT_AVAILABLE = True
    logger.info("[Altitude] Altitude adjustment module loaded.")
except ImportError:
    _ALT_AVAILABLE = False

    def _alt_adjust(base_projection, prop_type, venue, humidor_active=False):  # noqa: E704
        return base_projection

    def _get_humidor(venue):  # noqa: E704
        return False

    logger.warning("[Altitude] altitude_adjustment not found -- altitude adjustments disabled.")

# ── Phase 47: Live temperature calibration ───────────────────────────────────
# Loads per-agent T scalars from DB at dispatcher startup (single bulk query).
# T is fitted nightly by nightly_recap.py after each settlement.
# Applies Platt scaling inside build_parlay._eff_prob() to compress
# overconfident raw probabilities before the agent claiming pass.
try:
    from temperature_calibration import (
        load_all_temperatures as _load_all_temperatures,
        apply_temperature as _apply_temperature,
        T_DEFAULT as _T_DEFAULT,
    )
    _TEMP_CAL_AVAILABLE = True
    logger.info("[Phase47] Temperature calibration module loaded.")
except ImportError:
    _TEMP_CAL_AVAILABLE = False
    def _load_all_temperatures(names): return {n: 1.0 for n in names}  # noqa: E302
    def _apply_temperature(p, T): return p                              # noqa: E302
    _T_DEFAULT = 1.0
    logger.warning("[Phase47] temperature_calibration not found -- using T=%.1f default.", _T_DEFAULT)


# ── Phase 35: Operations layer (agent config, risk manager, decision logger) ──
try:
    import yaml as _yaml
    _config_path = os.path.join(os.path.dirname(__file__), "agent_config.yaml")
    with open(_config_path) as _f:
        _agent_config: dict = _yaml.safe_load(_f) or {}
    _CONFIG_VERSION: str = _agent_config.get("version", "unknown")
    logger.info("[CONFIG] Loaded agent_config.yaml v%s", _CONFIG_VERSION)
except Exception as _cfg_exc:
    _agent_config = {}
    _CONFIG_VERSION = "unknown"
    logger.warning("[CONFIG] Could not load agent_config.yaml: %s", _cfg_exc)

try:
    from risk_manager import RiskManager as _RiskManager
    _risk_manager = _RiskManager()
    logger.info("[RISK] RiskManager loaded")
except Exception as _rm_exc:
    _risk_manager = None  # type: ignore[assignment]
    logger.warning("[RISK] RiskManager not available: %s", _rm_exc)

try:
    import decision_logger as _decision_logger
    _DL_AVAILABLE = True
    logger.info("[DL] Decision logger loaded")
except ImportError:
    _DL_AVAILABLE = False
    _decision_logger = None  # type: ignore[assignment]
    logger.warning("[DL] decision_logger not available -- leg decisions will not be logged")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_EV_PCT   = 3.0     # minimum EV gate
MIN_PROB     = 0.52    # minimum implied win probability per leg
MAX_LEGS     = 4       # hard cap -- no parlay may exceed 4 legs
MIN_LEGS     = 2       # min legs to send alert
HALF_KELLY   = 0.5     # Kelly fraction multiplier
MAX_KELLY    = 0.10    # bankroll cap

# ---------------------------------------------------------------------------
# OmegaStack ensemble constants
# ---------------------------------------------------------------------------
# Per-agent additive edge above raw implied_prob (each agent's unique signal strength)
_OMEGA_AGENT_EDGE: dict[str, float] = {
    "VultureStack": 0.040,   # strongest: dual-confirmation fatigue consensus
    "UmpireAgent":  0.020,   # moderate: ump tendencies predictive but subtle
    "FadeAgent":    0.015,   # marginal: contrarian adds a small systematic edge
}
OMEGA_STACK_WEIGHTS: dict[str, float] = {
    "VultureStack": 0.60,
    "UmpireAgent":  0.25,
    "FadeAgent":    0.15,
}
OMEGA_STACK_MIN_PROB = 0.65   # stacked prob gate -- rarest, highest conviction
OMEGA_STACK_MAX_LEGS = 3      # tight: surgical parlays only
BANKROLL_USD = 200.0   # reference bankroll for Kelly sizing
MAX_STAKE    = MAX_STAKE_USD   # $20 hard cap -- Kelly ceiling (unchanged)

# Phase 48: cache populated at dispatch start from agent_unit_sizing table
_AGENT_UNITS_CACHE: dict = {}

# ---------------------------------------------------------------------------
# Prop-type configuration
# ---------------------------------------------------------------------------

# prop_type -> {player_type, min_prob, sides}

# ── MLB historical base-rate probabilities ────────────────────────────────────
# (line, P_over) tuples from 2019-2024 MLB regular-season data.
# Used by _build_legs() base_prob() interpolation.
_BASE_RATES: dict[str, list[tuple[float, float]]] = {
    "hits": [
        (0.5, 0.72), (1.5, 0.38), (2.5, 0.13), (3.5, 0.03),
    ],
    "home_runs": [
        (0.5, 0.09), (1.5, 0.005),
    ],
    "rbis": [
        (0.5, 0.42), (1.5, 0.20), (2.5, 0.08), (3.5, 0.025),
    ],
    "runs": [
        (0.5, 0.45), (1.5, 0.18), (2.5, 0.06), (3.5, 0.015),
    ],
    "total_bases": [
        (0.5, 0.85), (1.5, 0.55), (2.5, 0.28), (3.5, 0.12), (4.5, 0.04),
    ],
    "stolen_bases": [
        (0.5, 0.06), (1.5, 0.008),
    ],
    "hits_runs_rbis": [
        (0.5, 0.95), (1.5, 0.72), (2.5, 0.48), (3.5, 0.28),
        (4.5, 0.14), (5.5, 0.06), (6.5, 0.02),
    ],
    "strikeouts": [
        (1.5, 0.72), (3.5, 0.55), (5.5, 0.38), (7.5, 0.22),
        (9.5, 0.10), (11.5, 0.03),
    ],
    "earned_runs": [
        (0.5, 0.55), (1.5, 0.38), (2.5, 0.22), (3.5, 0.12), (4.5, 0.05),
    ],
    "walks": [
        (0.5, 0.25), (1.5, 0.06), (2.5, 0.01),
    ],
    "fantasy_hitter": [
        (5.0, 0.88), (10.0, 0.68), (15.0, 0.46), (20.0, 0.28),
        (25.0, 0.15), (30.0, 0.07), (40.0, 0.02),
    ],
    "fantasy_pitcher": [
        (15.0, 0.80), (20.0, 0.60), (25.0, 0.42), (30.0, 0.27),
        (35.0, 0.15), (40.0, 0.08), (50.0, 0.02),
    ],
    "hits_allowed": [
        (1.5, 0.85), (3.5, 0.58), (5.5, 0.30), (7.5, 0.11),
    ],
    "walks_allowed": [
        (0.5, 0.55), (1.5, 0.28), (2.5, 0.09), (3.5, 0.02),
    ],
    "pitching_outs": [
        (8.5, 0.62), (11.5, 0.46), (14.5, 0.30), (17.5, 0.17), (20.5, 0.06),
    ],
}

PROP_CONFIG: dict[str, dict] = {
    # Hitter props
    "hits":           {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over"]},
    "home_runs":      {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over"]},
    "rbis":           {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over", "Under"]},
    "runs":           {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over", "Under"]},
    "total_bases":    {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over", "Under"]},
    "stolen_bases":   {"player_type": "hitter", "min_prob": 0.52, "sides": ["Over"]},
    "hits_runs_rbis": {"player_type": "hitter", "min_prob": 0.54, "sides": ["Over", "Under"]},
    "fantasy_hitter": {"player_type": "hitter", "min_prob": 0.54, "sides": ["Over", "Under"]},
    # Pitcher props
    "strikeouts":     {"player_type": "pitcher", "min_prob": 0.54, "sides": ["Over", "Under"]},
    "earned_runs":    {"player_type": "pitcher", "min_prob": 0.54, "sides": ["Under"]},
    "fantasy_pitcher":{"player_type": "pitcher", "min_prob": 0.54, "sides": ["Over", "Under"]},
}

# Agent roster with filter functions (applied to each SelectionResult)
# Filters return True = include leg, False = exclude
AGENT_CONFIGS: list[dict] = [
    {
        "name": "EVHunter",
        "emoji": "🎯",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.implied_prob >= 0.54,
        "note": "Top-EV generalist -- all prop types",
    },
    {
        "name": "UnderMachine",
        "emoji": "🔽",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.side == "Under" and r.implied_prob >= 0.55,
        "note": "Strictly Unders -- exploiting public Over bias",
    },
    {
        "name": "MLEdgeAgent",
        "emoji": "🧠",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.implied_prob >= 0.56,
        "note": "Pure model probability -- highest confidence only",
    },
    {
        # ── F5Agent -- First 5 Innings specialist ───────────────────────────
        # Focuses on pitcher-centric props where outcomes are driven by the
        # starter's quality.  Bullpen variance is eliminated because the prop
        # resolves before any reliever touches the game.
        # Targets high-probability pitcher props: K and hits/runs suppression.
        # Strict probability gate (>= 0.55) compensates for the small prop pool.
        "name": "F5Agent",
        "emoji": "5️⃣",
        "max_legs": 3,
        "entry_type": "STANDARD",
        "filter": lambda r: r.prop_type in (
            "strikeouts", "earned_runs", "hits_runs_rbis", "runs"
        ) and r.implied_prob >= 0.55,
        "note": "First-5-innings props -- ignores bullpen, SP quality only",
    },
    {
        "name": "UmpireAgent",
        "emoji": "⚖️",
        "max_legs": 3,
        "entry_type": "STANDARD",
        "filter": lambda r: r.prop_type in ("strikeouts", "runs", "earned_runs")
                            and r.implied_prob >= 0.54,
        "note": "K rate & run environment -- home-plate umpire tendencies",
    },
    {
        "name": "FadeAgent",
        "emoji": "👻",
        "max_legs": 4,
        "entry_type": "FLEX",
        # Phase 27: prefer real SBD fade signal; fall back to implied_prob gate
        "filter": lambda r: (
            getattr(r, "is_fade_signal", False) or r.implied_prob >= 0.53
        ) and r.side == "Under",
        "note": "Contrarian fades against public consensus (SBD ticket% >= 65% preferred)",
    },
    {
        "name": "LineValueAgent",
        "emoji": "📐",
        "max_legs": 4,
        "entry_type": "FLEX",
        # Phase 39: sportsbook reference upgrade.
        # Qualifies if our model is confident (>= 0.55) OR if the sharp sportsbook
        # market ALSO shows >= 0.55 implied while our model clears the base gate (>= 0.53).
        # sb_implied_prob comes from The Odds API (vig-stripped DK/FD/BetMGM consensus).
        "filter": lambda r: (
            r.implied_prob >= 0.55
            or (getattr(r, "sb_implied_prob", 0.0) >= 0.55 and r.implied_prob >= 0.53)
        ),
        "note": "Sharp line gaps -- sportsbook consensus vs DFS line (The Odds API Layer 7)",
    },
    {
        "name": "BullpenAgent",
        "emoji": "🔥",
        "max_legs": 3,
        "entry_type": "FLEX",
        # Enhanced: earned_runs weighted heavier (clearest bullpen fatigue signal),
        # runs second, hits only when prob is elevated (>=0.56) to filter noise.
        # Decay logic: props that represent late-inning exposure (ER, Runs) get a
        # 0.02 synthetic boost vs. hits which need a higher raw threshold.
        "filter": lambda r: (
            (r.prop_type == "earned_runs" and r.implied_prob >= 0.54) or
            (r.prop_type == "runs"         and r.implied_prob >= 0.55) or
            (r.prop_type == "hits"         and r.implied_prob >= 0.56)
        ),
        "note": "Bullpen fatigue & rest -- ER > Runs > Hits (weighted decay)",
    },
    {
        "name": "WeatherAgent",
        "emoji": "🌬️",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("home_runs", "total_bases",
                                             "hits", "runs", "hits_runs_rbis")
                            and r.implied_prob >= 0.54,
        "note": "Wind & park-factor adjustments via Open-Meteo",
    },

    {
        "name": "ArsenalAgent",
        "emoji": "⚾",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("strikeouts", "total_bases")
                            and r.implied_prob >= 0.54,
        "note": "Pitch-type matchup -- K & total bases",
    },
    {
        "name": "PlatoonAgent",
        "emoji": "🤜",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("hits", "home_runs", "rbis",
                                             "total_bases", "hits_runs_rbis")
                            and r.implied_prob >= 0.53,
        "note": "Handedness splits -- L vs R matchups",
    },
    {
        "name": "CatcherAgent",
        "emoji": "🧤",
        "max_legs": 3,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("strikeouts", "stolen_bases")
                            and r.implied_prob >= 0.54,
        "note": "Catcher framing & battery chemistry",
    },
    {
        "name": "LineupAgent",
        "emoji": "📋",
        "max_legs": 4,
        "entry_type": "FLEX",
        # Phase 37: batting order awareness
        #   - Confirmed top-6 hitters (1-6) get a lower prob gate (more PA -> more opportunities)
        #   - Confirmed bottom-3 (7-9) need higher prob to justify the reduced PA exposure
        #   - Unconfirmed lineup (batting_order_pos == 0) keeps the original 0.53 gate
        "filter": lambda r: r.prop_type in ("hits", "rbis", "runs",
                                             "hits_runs_rbis", "fantasy_hitter")
                            and (
                                (r.batting_order_pos in range(1, 7) and r.implied_prob >= 0.52)
                                or (r.batting_order_pos in range(7, 10) and r.implied_prob >= 0.55)
                                or (r.batting_order_pos == 0 and r.implied_prob >= 0.53)
                            ),
        "note": "Volume & PA specialist -- batting order aware (top-6 preferred)",
    },
    {
        "name": "GetawayAgent",
        "emoji": "✈️",
        "max_legs": 4,
        "entry_type": "FLEX",
        # Enhanced: Travel fatigue scoring -- time-zone crossing degrades
        # batter performance (most reliable on hits_runs_rbis composite).
        # hits_runs_rbis: lowest threshold (broadest fatigue signal).
        # rbis: intermediate -- RBI production suffers most in fatigue.
        # runs: highest threshold -- scoring requires full effort even tired.
        # All are Under-side only (fatigue -> under-performance).
        "filter": lambda r: r.side == "Under" and (
            (r.prop_type == "hits_runs_rbis" and r.implied_prob >= 0.52) or
            (r.prop_type == "rbis"           and r.implied_prob >= 0.53) or
            (r.prop_type == "hits"           and r.implied_prob >= 0.54) or
            (r.prop_type == "runs"           and r.implied_prob >= 0.55)
        ),
        "note": "Travel fatigue Unders -- H+R+RBI > RBI > H > R (decay order)",
    },
    {
        "name": "FantasyPtsAgent",
        "emoji": "💫",
        "max_legs": 4,
        "entry_type": "FLEX",
        "filter": lambda r: r.prop_type in ("fantasy_hitter", "fantasy_pitcher")
                            and r.implied_prob >= 0.54,
        "note": "Fantasy-score lines -- best scoring format per platform",
    },
    {
        # ── 17th agent: VultureStack ───────────────────────────────────────
        # Consensus mechanism: fires ONLY when BOTH BullpenAgent criteria
        # (runs/ER exposure) AND GetawayAgent criteria (travel fatigue Under)
        # overlap on the same prop.  Dual-confirmation -> higher confidence.
        #
        # Filter logic:
        #   • Under side only (both agents agree on direction)
        #   • Props where bullpen fatigue AND travel fatigue intersect:
        #       runs, earned_runs, hits_runs_rbis
        #   • Stricter probability gate (>= 0.57) -- consensus already earned it
        #   • Max 3 legs: tighter pool = premium picks only
        "name": "VultureStack",
        "emoji": "🦅",
        "max_legs": 3,
        "entry_type": "FLEX",
        "filter": lambda r: r.side == "Under" and r.prop_type in (
            "runs", "earned_runs", "hits_runs_rbis"
        ) and r.implied_prob >= 0.57,
        "note": "BullpenAgent ∩ GetawayAgent consensus -- Under fatigue picks only",
    },
]

# ---------------------------------------------------------------------------
# MLB Stats API helpers
# ---------------------------------------------------------------------------

_MLBAPI_BASE = "https://statsapi.mlb.com/api/v1"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# PrizePicks session -- warm up by visiting the app home page first so
# Cloudflare + DataDome issue valid cookies, then use those cookies for
# the API call.  The session is module-level so the warm-up only fires
# once per process (the daily 11 AM dispatch is a single process).
_pp_session: requests.Session | None = None


def _get_pp_session() -> requests.Session:
    """Return a warmed-up PrizePicks session, creating one if needed."""
    global _pp_session
    if _pp_session is not None:
        return _pp_session
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.0 Mobile/15E148 Safari/604.1"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    try:
        s.get("https://app.prizepicks.com/", timeout=12)
        logger.info("[PP] Session warmed up -- cookies: %s", list(s.cookies.keys()))
    except Exception as exc:
        logger.warning("[PP] Warm-up request failed: %s", exc)
    # Switch to JSON API headers for subsequent calls
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://app.prizepicks.com/",
        "Origin": "https://app.prizepicks.com",
    })
    _pp_session = s
    return s


def fetch_today_schedule(date_str: str | None = None) -> list[dict]:
    """Fetch today's MLB schedule. Returns list of game dicts."""
    date = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            f"{_MLBAPI_BASE}/schedule",
            params={"sportId": 1, "date": date, "hydrate": "team,linescore,venue"},
            headers=_HEADERS, timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("[Schedule] HTTP %d", resp.status_code)
            return []
        games = []
        for date_block in resp.json().get("dates", []):
            for g in date_block.get("games", []):
                games.append({
                    "game_id":   g.get("gamePk"),
                    "home_team": g.get("teams", {}).get("home", {}).get("team", {}).get("name"),
                    "away_team": g.get("teams", {}).get("away", {}).get("team", {}).get("name"),
                    "venue":     (g.get("venue") or {}).get("name", ""),
                    "time_utc":  g.get("gameDate"),
                    "status":    g.get("status", {}).get("detailedState", ""),
                })
        logger.info("[Schedule] Found %d games for %s", len(games), date)
        return games
    except Exception as exc:
        logger.warning("[Schedule] Failed: %s", exc)
        return []


def fetch_today_lineups(date_str: str | None = None) -> dict[int, int]:
    """
    Fetch confirmed batting order positions for today's games via MLB Stats API.

    Returns {mlbam_player_id: batting_order_position (1-9)}.
    Returns empty dict if lineups not yet posted or on any error.
    Players not in a confirmed lineup get batting_order_pos = 0 (unconfirmed).
    """
    date = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            f"{_MLBAPI_BASE}/schedule",
            params={"sportId": 1, "date": date, "hydrate": "lineups"},
            headers=_HEADERS, timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("[Lineups] HTTP %d -- batting order unavailable", resp.status_code)
            return {}
        order: dict[int, int] = {}
        for date_block in resp.json().get("dates", []):
            for game in date_block.get("games", []):
                lineups = game.get("lineups", {})
                for side in ("homePlayers", "awayPlayers"):
                    for pos, player in enumerate(lineups.get(side, []), start=1):
                        pid = player.get("id")
                        if pid:
                            order[int(pid)] = pos
        logger.info("[Lineups] Confirmed batting order for %d players", len(order))
        return order
    except Exception as exc:
        logger.warning("[Lineups] Fetch failed: %s -- batting order unavailable", exc)
        return {}


def fetch_player_season_stats(player_id: int) -> dict[str, float]:
    """
    Fetch current-season averages per game for a player.
    Returns a flat dict: {hits, home_runs, rbis, runs, stolen_bases,
                          doubles, triples, walks, strikeouts, innings_pitched,
                          earned_runs, ...}
    """
    try:
        resp = requests.get(
            f"{_MLBAPI_BASE}/people/{player_id}/stats",
            params={"stats": "season", "group": "hitting,pitching", "season": str(datetime.now(timezone.utc).year)},
            headers=_HEADERS, timeout=10,
        )
        if resp.status_code != 200:
            return {}
        stats_out: dict[str, float] = {}
        for stat_group in resp.json().get("stats", []):
            splits = stat_group.get("splits", [])
            if not splits:
                continue
            s = splits[0].get("stat", {})
            g = max(int(s.get("gamesPlayed", 1)), 1)
            # Hitting per-game
            if stat_group.get("group", {}).get("displayName") == "hitting":
                stats_out["hits"]          = int(s.get("hits", 0)) / g
                stats_out["home_runs"]     = int(s.get("homeRuns", 0)) / g
                stats_out["rbis"]          = int(s.get("rbi", 0)) / g
                stats_out["runs"]          = int(s.get("runs", 0)) / g
                stats_out["stolen_bases"]  = int(s.get("stolenBases", 0)) / g
                stats_out["doubles"]       = int(s.get("doubles", 0)) / g
                stats_out["triples"]       = int(s.get("triples", 0)) / g
                stats_out["walks"]         = int(s.get("baseOnBalls", 0)) / g
                stats_out["total_bases"]   = int(s.get("totalBases", 0)) / g
                # H+R+RBI combo
                stats_out["hits_runs_rbis"] = (
                    stats_out["hits"] + stats_out["runs"] + stats_out["rbis"]
                )
            # Pitching per-game
            elif stat_group.get("group", {}).get("displayName") == "pitching":
                gs = max(int(s.get("gamesStarted", 1)), 1)
                stats_out["strikeouts"]      = int(s.get("strikeOuts", 0)) / gs
                stats_out["innings_pitched"] = float(s.get("inningsPitched", 0)) / gs
                stats_out["earned_runs"]     = int(s.get("earnedRuns", 0)) / gs
                stats_out["hits_allowed"]    = int(s.get("hits", 0)) / gs
                stats_out["walks_allowed"]   = int(s.get("baseOnBalls", 0)) / gs
                stats_out["wins"]            = int(s.get("wins", 0)) / gs
                stats_out["quality_starts"]  = int(s.get("qualityStarts", 0)) / gs
        return stats_out
    except Exception as exc:
        logger.warning("[Stats] player %d failed: %s", player_id, exc)
        return {}


# ---------------------------------------------------------------------------
# Live prop fetchers (both platforms)
# ---------------------------------------------------------------------------

# Baseball-specific stat types used to identify MLB props on PrizePicks.
# Updated to match actual API responses (live-probed Mar 2026):
#   "pitcher strikeouts" replaced bare "strikeouts"
#   "earned runs allowed" replaced "earned runs"
#   "hits allowed" and "walks allowed" added for pitcher props
#   "pitching outs" added (outs recorded prop)
_PP_MLB_STAT_TYPES = {
    "hits", "home runs", "rbis", "rbi", "runs",
    "total bases", "stolen bases",
    "hits+runs+rbis", "hits + runs + rbis",
    "hitter fantasy score", "pitcher fantasy score",
    "doubles", "triples",
    # Pitcher props (actual PP label as of 2026)
    "pitcher strikeouts", "strikeouts",   # keep bare form as fallback
    "earned runs allowed", "earned runs",  # keep old form as fallback
    "hits allowed", "walks allowed", "pitching outs",
    "walks",
}

# Underdog stat -> our internal prop_type
_UD_STAT_MAP: dict[str, str] = {
    "strikeouts":    "strikeouts",
    "pitch_outs":    "strikeouts",   # alternate UD label for pitcher Ks
    "hits":          "hits",
    "total_bases":   "total_bases",
    "rbis":          "rbis",
    "runs":          "runs",
    "stolen_bases":  "stolen_bases",
    "home_runs":     "home_runs",
    "hits_runs_rbis":"hits_runs_rbis",
    "earned_runs":   "earned_runs",
    "runs_allowed":  "earned_runs",
    "walks":         "walks",
    "walks_allowed": "walks",
    "fantasy_points_hitter":  "fantasy_hitter",
    "fantasy_points_pitcher": "fantasy_pitcher",
}

_UD_PITCHER_POSITIONS = {"SP", "RP", "P", "CP"}


def _fetch_prizepicks_via_apify() -> list[dict]:
    """
    Fallback: run the Apify PrizePicks MLB scraper actor when the direct
    API call is blocked (Railway datacenter IP gets 403 from DataDome).

    Actor ID : 4AmgQeem8dEgMEiRF
    Input    : {"leagues": ["MLB"]}
    Returns  : same format as fetch_prizepicks_props()
    """
    apify_key = os.environ.get("APIFY_API_KEY", "")
    if not apify_key:
        logger.warning("[PP-Apify] APIFY_API_KEY not set — skipping fallback")
        return []

    # ── Apify stat label → our internal stat_type ─────────────────────────
    _APIFY_STAT_MAP: dict[str, str] = {
        "hitter fantasy score":   "hitter fantasy score",
        "pitcher fantasy score":  "pitcher fantasy score",
        "hits":                   "hits",
        "home runs":              "home runs",
        "total bases":            "total bases",
        "rbis":                   "rbis",
        "rbi":                    "rbi",
        "runs":                   "runs",
        "stolen bases":           "stolen bases",
        "hits+runs+rbis":         "hits+runs+rbis",
        "hits + runs + rbis":     "hits + runs + rbis",
        "pitcher strikeouts":     "pitcher strikeouts",
        "strikeouts":             "strikeouts",
        "earned runs allowed":    "earned runs allowed",
        "earned runs":            "earned runs",
        "hits allowed":           "hits allowed",
        "walks allowed":          "walks allowed",
        "pitching outs":          "pitching outs",
        "walks":                  "walks",
        "doubles":                "doubles",
        "triples":                "triples",
    }

    try:
        run_url = (
            f"https://api.apify.com/v2/acts/4AmgQeem8dEgMEiRF/runs"
            f"?token={apify_key}&waitForFinish=120"
        )
        r = requests.post(
            run_url,
            json={"leagues": ["MLB"]},
            timeout=130,
        )
        if r.status_code not in (200, 201):
            logger.warning("[PP-Apify] Run start HTTP %d", r.status_code)
            return []

        run_data  = r.json().get("data", {})
        dataset_id = run_data.get("defaultDatasetId", "")
        run_status = run_data.get("status", "")
        run_id     = run_data.get("id", "")

        # Poll until SUCCEEDED (waitForFinish handles most of this, but be safe)
        if run_status not in ("SUCCEEDED", "READY"):
            for _ in range(20):
                time.sleep(6)
                poll = requests.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}?token={apify_key}",
                    timeout=15,
                )
                run_status = poll.json().get("data", {}).get("status", "")
                dataset_id = poll.json().get("data", {}).get("defaultDatasetId", dataset_id)
                if run_status == "SUCCEEDED":
                    break
            else:
                logger.warning("[PP-Apify] Run %s never finished (status=%s)", run_id, run_status)
                return []

        # Fetch dataset items
        items_url = (
            f"https://api.apify.com/v2/datasets/{dataset_id}/items"
            f"?token={apify_key}&limit=8000&clean=true"
        )
        resp = requests.get(items_url, timeout=30)
        if resp.status_code != 200:
            logger.warning("[PP-Apify] Dataset fetch HTTP %d", resp.status_code)
            return []

        items = resp.json()
        props: list[dict] = []
        for item in items:
            stat_raw = str(item.get("stat", "") or "").strip()
            stat_key = stat_raw.lower()
            if stat_key not in _PP_MLB_STAT_TYPES:
                continue
            if "inning" in stat_key:
                continue
            # Skip alt lines and promo (goblin / demon) lines — main board only
            board = str(item.get("board_type", "") or "").lower().strip()
            if board and board != "standard":
                continue
            if item.get("is_promo", False):
                continue

            line_val = item.get("line")
            if line_val is None:
                continue
            pname = str(item.get("player_name", "") or "").strip()
            if not pname:
                continue
            # Normalise stat label to match our internal naming
            stat_out = _APIFY_STAT_MAP.get(stat_key, stat_raw)
            props.append({
                "source":      "prizepicks",
                "player_name": pname,
                "stat_type":   stat_out,
                "line":        float(line_val),
            })

        logger.info("[PP-Apify] Fetched %d MLB props via Apify", len(props))
        return props

    except Exception as exc:
        logger.warning("[PP-Apify] Fallback failed: %s", exc)
        return []


def fetch_prizepicks_props() -> list[dict]:
    """
    Fetch PrizePicks MLB projections.

    Primary  : Direct API with session-cookie warm-up.
    Fallback : Apify actor 4AmgQeem8dEgMEiRF when Railway IP is 403-blocked.

    Returns raw list of dicts (same format either way).
    """
    global _pp_session
    try:
        data = None
        for attempt in range(3):
            if attempt:
                time.sleep(2 ** attempt)   # 2s, 4s back-off
                _pp_session = None
            sess = _get_pp_session()
            resp = sess.get(
                "https://api.prizepicks.com/projections",
                params={"per_page": 250, "single_stat": True, "league_id": 2},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                break
            logger.warning("[PP] HTTP %d (attempt %d/3)", resp.status_code, attempt + 1)
            if resp.status_code == 403:
                _pp_session = None
        if data is None:
            logger.info("[PP] Direct API blocked — falling back to Apify actor")
            return _fetch_prizepicks_via_apify()

        # Build player id -> name map from included resources
        player_map: dict[str, str] = {}
        for item in data.get("included", []):
            if item.get("type") == "new_player":
                pid  = item["id"]
                name = item.get("attributes", {}).get("display_name", "")
                if name:
                    player_map[pid] = name

        props = []
        for proj in data.get("data", []):
            attrs    = proj.get("attributes", {})
            stat_raw = str(attrs.get("stat_type", "") or "")

            if stat_raw.lower() not in _PP_MLB_STAT_TYPES:
                continue
            if "inning" in stat_raw.lower():
                continue
            # Skip alt lines and promo (goblin / demon) lines — main board only
            board = str(attrs.get("board_type", "") or "").lower().strip()
            if board and board != "standard":
                continue
            if attrs.get("is_promo", False):
                continue

            line_val = attrs.get("line_score")
            if line_val is None:
                continue

            pid   = (proj.get("relationships", {})
                        .get("new_player", {})
                        .get("data", {})
                        .get("id", ""))
            pname = player_map.get(pid, "")
            if not pname:
                continue

            props.append({
                "source":      "prizepicks",
                "player_name": pname,
                "stat_type":   stat_raw,
                "line":        float(line_val),
            })

        logger.info("[PP] Fetched %d MLB props", len(props))
        return props
    except Exception as exc:
        logger.warning("[PP] Fetch failed: %s — trying Apify fallback", exc)
        return _fetch_prizepicks_via_apify()


def fetch_underdog_props() -> list[dict]:
    """
    Fetch Underdog Fantasy MLB over/under lines.

    Correct join chain (confirmed from Phase 18 UnderdogLinesFetcher):
      over_under_lines[n]["over_under"]["appearance_stat"]["stat"]
                                       ["appearance_stat"]["appearance_id"]
        -> appearances_map[appearance_id]["player_id"]
        -> players_map[player_id]["sport_id"] == "MLB"

    Filters out innings (removed Phase 19) and inactive lines.
    Returns raw list of dicts.
    """
    try:
        _ud_headers = {
            "User-Agent": "Underdog Fantasy/3.0 (iPhone; iOS 17.0) CFNetwork/1474 Darwin/23.0.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "x-api-key": "KeepItSecret",
        }
        resp = requests.get(
            "https://api.underdogfantasy.com/beta/v5/over_under_lines",
            headers=_ud_headers, timeout=20,
        )
        if resp.status_code != 200:
            logger.warning("[UD] HTTP %d", resp.status_code)
            return []
        data = resp.json()

        # Build lookup maps
        players_map: dict[str, dict] = {
            p["id"]: p for p in data.get("players", [])
        }
        appearances_map: dict[str, dict] = {
            a["id"]: a for a in data.get("appearances", [])
        }

        props: list[dict] = []
        seen: set[str] = set()

        for line in data.get("over_under_lines", []):
            # Active only
            if line.get("status") != "active":
                continue

            # Deduplicate on stable_id
            stable_id = line.get("stable_id", line.get("id", ""))
            if stable_id in seen:
                continue

            # Navigate embedded over_under -> appearance_stat
            ou        = line.get("over_under") or {}
            app_stat  = ou.get("appearance_stat") or {}
            stat_ud   = app_stat.get("stat", "")
            app_id    = app_stat.get("appearance_id", "")

            if not stat_ud or not app_id:
                continue

            # Skip innings
            if "inning" in stat_ud.lower():
                continue

            # Resolve player
            appearance = appearances_map.get(app_id, {})
            player_id  = appearance.get("player_id", "")
            player     = players_map.get(player_id, {})

            # Must be MLB
            if player.get("sport_id") != "MLB":
                continue

            pname = f"{player.get('first_name','')} {player.get('last_name','')}".strip()
            if not pname:
                continue

            line_val   = float(line.get("stat_value") or 0)
            position   = player.get("position_name", "")
            entry_type = "FLEX"   # Underdog default; STANDARD for limited markets

            # Check options for entry type indicator
            opts = line.get("options", [])
            higher_opt = next((o for o in opts if o.get("choice") == "higher"), {})
            if not higher_opt.get("payout_multiplier"):
                entry_type = "STANDARD"

            seen.add(stable_id)
            props.append({
                "source":      "underdog",
                "player_name": pname,
                "stat_type":   stat_ud,
                "line":        line_val,
                "entry_type":  entry_type,
                "position":    position,
            })

        logger.info("[UD] Fetched %d MLB lines", len(props))
        return props
    except Exception as exc:
        logger.warning("[UD] Fetch failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Prop normalisation
# ---------------------------------------------------------------------------

# raw stat_type string -> PROP_CONFIG key
_STAT_TYPE_MAP: dict[str, str] = {
    # Strikeouts / pitcher ks
    "strikeouts":           "strikeouts",
    "pitcher strikeouts":   "strikeouts",
    "ks":                   "strikeouts",
    # Hits
    "hits":                 "hits",
    # Home runs
    "home runs":            "home_runs",
    "home_runs":            "home_runs",
    # RBIs
    "rbis":                 "rbis",
    "rbi":                  "rbis",
    # Runs
    "runs":                 "runs",
    # Total bases
    "total bases":          "total_bases",
    "total_bases":          "total_bases",
    # Stolen bases
    "stolen bases":         "stolen_bases",
    "stolen_bases":         "stolen_bases",
    # Combo
    "hits+runs+rbis":       "hits_runs_rbis",
    "hits + runs + rbis":   "hits_runs_rbis",
    # Fantasy
    "hitter fantasy score": "fantasy_hitter",
    "pitcher fantasy score":"fantasy_pitcher",
    "fantasy_points_hitter":"fantasy_hitter",
    "fantasy_points_pitcher":"fantasy_pitcher",
    # Earned runs (PrizePicks uses "earned runs allowed" as of 2026)
    "earned runs":          "earned_runs",
    "earned_runs":          "earned_runs",
    "earned runs allowed":  "earned_runs",
    # Pitcher hits / walks allowed (PP labels as of 2026)
    "hits allowed":         "hits_allowed",
    "walks allowed":        "walks_allowed",
    "pitching outs":        "pitching_outs",
    # Walks (batter)
    "walks":                "walks",
    # Doubles / triples
    "doubles":              "doubles",
    "triples":              "triples",
}


def normalise_stat(raw: str) -> str | None:
    """Return PROP_CONFIG key for a raw stat_type string, or None if unknown."""
    key = raw.strip().lower().replace("-", " ")
    return _STAT_TYPE_MAP.get(key)


# ---------------------------------------------------------------------------
# EV & Kelly math
# ---------------------------------------------------------------------------

def american_to_decimal(odds: float) -> float:
    if odds >= 0:
        return 1 + odds / 100
    return 1 - 100 / odds


def implied_prob_from_odds(odds: float) -> float:
    dec = american_to_decimal(odds)
    return 1.0 / dec


def calc_ev(true_prob: float, odds: float = -110.0) -> float:
    """Return EV percentage given true probability and American odds."""
    dec     = american_to_decimal(odds)
    implied = implied_prob_from_odds(odds)
    no_vig  = true_prob
    ev_pct  = (no_vig * (dec - 1) - (1 - no_vig)) * 100
    return ev_pct


def kelly_fraction(prob: float, odds: float = -110.0) -> float:
    """Half-Kelly capped at MAX_KELLY."""
    dec = american_to_decimal(odds)
    b   = dec - 1.0
    q   = 1.0 - prob
    if b <= 0:
        return 0.0
    k = (b * prob - q) / b
    return max(0.0, min(k * HALF_KELLY, MAX_KELLY))


# ---------------------------------------------------------------------------
# Parlay builder
# ---------------------------------------------------------------------------

@dataclass
class PropLeg:
    """A fully evaluated parlay leg ready for Discord."""
    player_name:    str
    prop_type:      str
    side:           str
    line:           float
    platform:       str
    implied_prob:   float
    entry_type:     str   = ""
    fantasy_pts:    float = 0.0
    ev_pct:         float = 0.0
    # Phase 27: Enhancement layer signals
    de_boost:       float = 0.0   # DraftEdge probability blend delta
    sbd_ticket_pct: float = 0.0   # SBD public ticket% (FadeAgent signal)
    is_fade_signal: bool  = False  # True when ticket_pct >= 65%
    # Phase 36: pre-nudge snapshot (before hot/cold form + FanGraphs layers)
    # Used by BullpenAgent, SteamAgent, and StreakAgent to bypass form/FG nudges
    prob_pre_form:  float = 0.0   # prob after Statcast -- before form & FG layers
    # Phase 37: confirmed batting order position (1-9, 0 = unconfirmed)
    batting_order_pos: int = 0
    # Phase 39 (Layer 7): Sportsbook reference signals (The Odds API)
    sb_implied_prob: float = 0.0   # vig-stripped sportsbook consensus prob (Over)
    sb_line_gap:     float = 0.0   # DFS line - SB line (negative = DFS favorable for Over)
    # Phase 44 (Layer 9): CV consistency gate
    mlbam_id:        int   = 0     # MLBAM player ID (for CV game log fetch)
    cv_nudge:        float = 0.0   # probability nudge from CV consistency layer


def build_parlay(
    legs: list[PropLeg],
    agent: dict,
    excluded_keys: set[tuple] | None = None,
    agent_T: float = 1.0,
) -> dict | None:
    """
    From a list of candidate legs, apply agent filter and build a parlay dict
    ready for DiscordAlertService.send_parlay_alert().

    excluded_keys: set of (player_name, prop_type, side) tuples already claimed
                   by a higher-priority agent -- these legs are off-limits.
    """
    if excluded_keys is None:
        excluded_keys = set()

    # Phase 36: agents that must not be nudged by hot/cold form or FanGraphs.
    #   BullpenAgent -- form (player recency) conflicts with bullpen fatigue signal
    #   StreakAgent  -- precision over volume; form/FG can inflate marginal picks
    #                  past the 0.80 gate, hurting win-rate with lower-quality legs
    _SKIP_NUDGE_AGENTS = {"BullpenAgent", "StreakAgent"}
    _use_pre_form = agent["name"] in _SKIP_NUDGE_AGENTS

    def _eff_prob(leg: PropLeg) -> float:
        """Return the probability appropriate for this agent's signal model.
        Phase 47: applies Platt temperature scaling to compress overconfident
        raw probabilities.  T=1.5 by default (conservative prior); refitted
        nightly after >= 30 graded picks per agent.
        """
        raw = leg.prob_pre_form if _use_pre_form else leg.implied_prob
        if _TEMP_CAL_AVAILABLE and agent_T != 1.0:
            return _apply_temperature(raw, agent_T)
        return raw

    filtered = [
        l for l in legs
        if agent["filter"](
            type("SR", (), {
                "side":               l.side,
                "prop_type":          l.prop_type,
                "implied_prob":       _eff_prob(l),
                "fantasy_pts_edge":   l.fantasy_pts,
                # Phase 27: enhancement signals exposed to agent filters
                "is_fade_signal":     l.is_fade_signal,
                "sbd_ticket_pct":     l.sbd_ticket_pct,
                "de_boost":           l.de_boost,
                # Phase 37: batting order position for LineupAgent
                "batting_order_pos":  l.batting_order_pos,
                # Phase 39 (Layer 7): sportsbook reference for LineValueAgent
                "sb_implied_prob":    l.sb_implied_prob,
                "sb_line_gap":        l.sb_line_gap,
            })()
        )
        # Hard exclusion: leg already used by a previous agent
        and (l.player_name, l.prop_type, l.side) not in excluded_keys
    ]

    # Sort by effective prob desc, cap at min(agent max_legs, global MAX_LEGS)
    filtered.sort(key=lambda x: -_eff_prob(x))
    cap = min(agent["max_legs"], MAX_LEGS)
    selected = filtered[:cap]

    if len(selected) < MIN_LEGS:
        return None

    # Overall parlay EV (average of per-leg EVs)
    ev_pct    = sum(l.ev_pct for l in selected) / len(selected)

    # --- Confidence score (1-10) ---
    # Three components:
    #   1. prob_score  : avg win prob scaled 0->7 over the range 50%->80%
    #   2. ev_bonus    : EV% scaled 0->2 (caps at 15% EV)
    #   3. legs_penalty: -0.3 per leg above 3 (more legs = more variance)
    avg_prob     = sum(_eff_prob(l) for l in selected) / len(selected)
    prob_score   = (avg_prob - 0.50) / 0.30 * 7.0
    ev_bonus     = min(ev_pct / 15.0 * 2.0, 2.0)
    legs_penalty = max(0, len(selected) - 3) * 0.3
    conf         = round(min(10.0, max(1.0, prob_score + ev_bonus - legs_penalty)), 1)

    # Dominant entry type
    etypes     = [l.entry_type for l in selected if l.entry_type]
    entry_type = max(set(etypes), key=etypes.count) if etypes else agent.get("entry_type", "FLEX")

    return {
        "agent_name":  agent["name"],
        "agent_emoji": agent["emoji"],
        "entry_type":  entry_type,
        "ev_pct":      round(ev_pct, 2),
        "confidence":  conf,
        "notes":       agent.get("note", ""),
        "legs": [
            {
                "player_name":  l.player_name,
                "prop_type":    l.prop_type,
                "side":         l.side,
                "line":         l.line,
                "platform":     l.platform,
                "implied_prob": round(l.implied_prob, 4),
                "entry_type":   l.entry_type,
                "fantasy_pts":  round(l.fantasy_pts, 2),
            }
            for l in selected
        ],
    }


# ---------------------------------------------------------------------------
# OmegaStack ensemble meta-model (18th agent)
# ---------------------------------------------------------------------------

def build_omega_parlay(
    legs: list[PropLeg],
    excluded_keys: set[tuple] | None = None,
    agent_T: float = 1.0,
) -> dict | None:
    """
    OmegaStack -- 18th agent: true ensemble meta-model.

    Triple confirmation required: a leg must pass ALL of
    VultureStack, UmpireAgent, and FadeAgent filters.  Each contributing
    agent then adds its own additive edge to the raw implied_prob, and the
    weighted stacked probability is computed:

        stacked_prob = 0.60 × (prob + 0.040)   # VultureStack
                     + 0.25 × (prob + 0.020)   # UmpireAgent
                     + 0.15 × (prob + 0.015)   # FadeAgent

    Only legs with stacked_prob >= OMEGA_STACK_MIN_PROB (0.65) are included.
    Raw implied_prob must be ~0.62+ to clear the gate -- the tightest bar in
    the entire system.  Result: fewest parlays, highest conviction.
    """
    if excluded_keys is None:
        excluded_keys = set()

    # Resolve contributing-agent filter lambdas by name once
    agent_filters: dict[str, any] = {
        a["name"]: a["filter"] for a in AGENT_CONFIGS
    }
    vulture_f = agent_filters.get("VultureStack", lambda _r: False)
    umpire_f  = agent_filters.get("UmpireAgent",  lambda _r: False)
    fade_f    = agent_filters.get("FadeAgent",    lambda _r: False)

    qualified: list[tuple[PropLeg, float]] = []
    for leg in legs:
        if (leg.player_name, leg.prop_type, leg.side) in excluded_keys:
            continue

        # Synthetic SelectionResult for filter evaluation
        sr = type("SR", (), {
            "side":              leg.side,
            "prop_type":         leg.prop_type,
            "implied_prob":      leg.implied_prob,
            "fantasy_pts_edge":  leg.fantasy_pts,
        })()

        # Triple confirmation -- all three must agree
        if not (vulture_f(sr) and umpire_f(sr) and fade_f(sr)):
            continue

        # Weighted stacked probability with per-agent edge boosts
        vulture_eff  = leg.implied_prob + _OMEGA_AGENT_EDGE["VultureStack"]
        umpire_eff   = leg.implied_prob + _OMEGA_AGENT_EDGE["UmpireAgent"]
        fade_eff     = leg.implied_prob + _OMEGA_AGENT_EDGE["FadeAgent"]
        stacked_prob = (
            OMEGA_STACK_WEIGHTS["VultureStack"] * vulture_eff
            + OMEGA_STACK_WEIGHTS["UmpireAgent"]  * umpire_eff
            + OMEGA_STACK_WEIGHTS["FadeAgent"]    * fade_eff
        )

        # Phase 47: apply T to stacked probability as final calibration step
        if _TEMP_CAL_AVAILABLE and agent_T != 1.0:
            stacked_prob = _apply_temperature(stacked_prob, agent_T)

        if stacked_prob < OMEGA_STACK_MIN_PROB:
            continue

        qualified.append((leg, stacked_prob))

    if len(qualified) < MIN_LEGS:
        return None

    # Sort by stacked_prob desc, cap at OMEGA_STACK_MAX_LEGS
    qualified.sort(key=lambda t: -t[1])
    selected_pairs = qualified[: min(OMEGA_STACK_MAX_LEGS, MAX_LEGS)]

    selected_legs  = [p[0] for p in selected_pairs]
    selected_probs = [p[1] for p in selected_pairs]

    ev_pct       = sum(calc_ev(sp) for sp in selected_probs) / len(selected_probs)
    avg_prob     = sum(selected_probs) / len(selected_probs)
    prob_score   = (avg_prob - 0.50) / 0.30 * 7.0
    ev_bonus     = min(ev_pct / 15.0 * 2.0, 2.0)
    legs_penalty = max(0, len(selected_legs) - 3) * 0.3
    conf         = round(min(10.0, max(1.0, prob_score + ev_bonus - legs_penalty)), 1)

    return {
        "agent_name":  "OmegaStack",
        "agent_emoji": "🔱",
        "entry_type":  "STANDARD",
        "ev_pct":      round(ev_pct, 2),
        "confidence":  conf,
        "notes":       "VultureStack×0.60 + UmpireAgent×0.25 + FadeAgent×0.15 -- triple confirm >= 0.65 stacked",
        "legs": [
            {
                "player_name":  l.player_name,
                "prop_type":    l.prop_type,
                "side":         l.side,
                "line":         l.line,
                "platform":     l.platform,
                "implied_prob": round(sp, 4),
                "entry_type":   l.entry_type,
                "fantasy_pts":  round(l.fantasy_pts, 2),
            }
            for l, sp in zip(selected_legs, selected_probs)
        ],
    }


# ---------------------------------------------------------------------------
# Phase 27: MLB player name -> mlbam_id lookup
# ---------------------------------------------------------------------------

def _build_mlbam_lookup() -> dict:
    """
    Fetch all active MLB players from the Stats API and build a
    lowercase-full-name -> mlbam_id mapping.  Used to attach Statcast
    features to raw props (which carry player names, not MLBAM IDs).

    Falls back to an empty dict on any network/parse error -- Statcast
    enrichment degrades gracefully without it.
    """
    try:
        import datetime as _dt
        season = _dt.datetime.now(_dt.timezone.utc).year
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/sports/1/players",
            params={"season": season, "gameType": "R"},
            headers={"User-Agent": "PropIQ/1.0"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("[MLBAM] Lookup HTTP %d -- Statcast enrichment skipped",
                           resp.status_code)
            return {}
        lookup: dict = {}
        for person in resp.json().get("people", []):
            full_name = person.get("fullName", "")
            pid = person.get("id")
            if full_name and pid:
                lookup[full_name.lower()] = int(pid)
                parts = full_name.split()
                if len(parts) >= 2:
                    lookup[parts[-1].lower()] = int(pid)  # last-name fallback
        logger.info("[MLBAM] Lookup built: %d players", len(lookup))
        return lookup
    except Exception as exc:
        logger.warning("[MLBAM] Lookup failed: %s -- Statcast enrichment skipped", exc)
        return {}


def _build_player_venue_map() -> dict:
    """
    Build player_name_lower -> venue_name mapping for altitude adjustments.

    Uses the same MLB Stats API /sports/1/players endpoint as _build_mlbam_lookup()
    but maps currentTeam.name -> TEAM_TO_VENUE to resolve the player's home park.
    Falls back gracefully to an empty dict on any error.
    """
    try:
        from altitude_adjustment import TEAM_TO_VENUE as _TEAM_TO_VENUE
        import datetime as _dt
        season = _dt.datetime.now(_dt.timezone.utc).year
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/sports/1/players",
            params={"season": season, "gameType": "R"},
            headers={"User-Agent": "PropIQ/1.0"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(
                "[Altitude] Player venue map HTTP %d -- altitude skipped",
                resp.status_code,
            )
            return {}
        venue_map: dict = {}
        for person in resp.json().get("people", []):
            full_name = person.get("fullName", "")
            team_name = person.get("currentTeam", {}).get("name", "")
            venue = _TEAM_TO_VENUE.get(team_name, "")
            if full_name and venue:
                venue_map[full_name.lower()] = venue
        logger.info(
            "[Altitude] Player venue map built: %d players mapped to venues",
            len(venue_map),
        )
        return venue_map
    except Exception as exc:
        logger.warning(
            "[Altitude] Player venue map failed: %s -- altitude adjustments skipped",
            exc,
        )
        return {}


# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------

def _build_player_team_map() -> dict:
    """
    Fetch active MLB roster from Stats API.
    Returns {player_name_lower: current_team_name} for dome venue lookup.
    Falls back to empty dict on any error -- dome adjustment degrades gracefully.
    """
    try:
        import datetime as _dt
        season = _dt.datetime.now(_dt.timezone.utc).year
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/sports/1/players",
            params={"season": season, "gameType": "R"},
            headers={"User-Agent": "PropIQ/1.0"},
            timeout=15,
        )
        if resp.status_code != 200:
            return {}
        mapping: dict = {}
        for person in resp.json().get("people", []):
            full_name = person.get("fullName", "")
            team_name = (person.get("currentTeam") or {}).get("name", "")
            if full_name and team_name:
                mapping[full_name.lower()] = team_name
        logger.info("[Dome] Player-team map built: %d players", len(mapping))
        return mapping
    except Exception as exc:
        logger.warning("[Dome] Player-team map failed: %s -- dome adjustment degraded", exc)
        return {}


class LiveDispatcher:
    """Orchestrates daily prop fetching, analysis, and Discord dispatch."""

    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run   = dry_run
        self.selector  = platform_selector
        self._agent_units_cache = {}

    def run(self, date_str: str | None = None) -> None:
        date = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # Phase 47: load per-agent temperature scalars (single bulk query at startup)
        _all_agent_names = [a["name"] for a in AGENT_CONFIGS] + [
            "OmegaStack", "StreakAgent"
        ]
        self._agent_temperatures = _load_all_temperatures(_all_agent_names)
        logger.info(
            "[Phase47] Temperatures loaded (%d agents, avg T=%.2f)",
            len(self._agent_temperatures),
            sum(self._agent_temperatures.values()) / max(len(self._agent_temperatures), 1),
        )

        # ── Phase 48: per-agent unit sizes (single bulk query at startup) ──
        if _UNIT_SIZING_AVAILABLE:
            try:
                self._agent_units_cache = _get_all_units()
                logger.info(
                    "[Phase48] Unit sizes loaded (%d agents)",
                    len(self._agent_units_cache),
                )
            except Exception as _unit_err:
                logger.warning("[Phase48] unit sizing load failed: %s -- floor $5 applied", _unit_err)
                self._agent_units_cache = {}
        else:
            self._agent_units_cache = {}

        logger.info("=" * 60)
        logger.info("PropIQ LiveDispatcher -- %s", date)
        logger.info("=" * 60)

        # 1. Schedule check
        games = fetch_today_schedule(date)
        if not games:
            logger.warning("No MLB games found for %s -- no alerts sent.", date)
            return
        logger.info("%d games scheduled", len(games))

        # ── Phase 51: Build team/venue maps for dome adjustment ───────────
        self._team_venue_map: dict = {}
        self._home_teams: set = set()
        for _g in games:
            _venue = _g.get("venue", "")
            _ht = _g.get("home_team", "")
            _at = _g.get("away_team", "")
            if _venue:
                if _ht:
                    self._team_venue_map[_ht] = _venue
                    self._home_teams.add(_ht)
                if _at:
                    self._team_venue_map[_at] = _venue
        self._player_team_map: dict = (
            _build_player_team_map() if _DOME_AVAILABLE else {}
        )
        logger.info(
            "[Dome] Venue map: %d teams | Player map: %d players",
            len(self._team_venue_map), len(self._player_team_map),
        )

        # 2. Fetch live props from both platforms
        pp_props = fetch_prizepicks_props()
        ud_props = fetch_underdog_props()
        # Strip Underdog FLEX lines from main pool — only STANDARD Underdog
        # lines reach agents. FLEX/alt are StreakAgent territory only.
        ud_props = [p for p in ud_props
                    if p.get("entry_type", "FLEX") == "STANDARD"]
        all_raw  = pp_props + ud_props

        if not all_raw:
            logger.warning("No props fetched from either platform -- aborting.")
            return

        # ── Phase 27: Enrich raw props with enhancement layers ────────────
        # Step 1: DraftEdge projections (name-based, adds de_* fields)
        if _DE_AVAILABLE:
            try:
                all_raw = _de_enrich(all_raw)
                logger.info("[Phase27] DraftEdge enrichment applied to %d props",
                            len(all_raw))
            except Exception as exc:
                logger.warning("[Phase27] DraftEdge enrichment failed: %s", exc)

        # Step 2: Statcast XGBoost features (mlbam_id resolved from Stats API)
        if _SC_AVAILABLE:
            try:
                mlbam_lookup = _build_mlbam_lookup()
                for p in all_raw:
                    name_key = p.get("player_name", "").lower()
                    p["mlbam_id"] = mlbam_lookup.get(name_key, 0)
                _sc_layer = StatcastFeatureLayer()
                _ud_pitcher_pos = {"SP", "RP", "P", "CP"}
                pitcher_raw = [p for p in all_raw
                               if p.get("position", "") in _ud_pitcher_pos]
                batter_raw  = [p for p in all_raw
                               if p.get("position", "") not in _ud_pitcher_pos]
                pitcher_raw = _sc_enrich(pitcher_raw, "pitcher", layer=_sc_layer)
                batter_raw  = _sc_enrich(batter_raw,  "batter",  layer=_sc_layer)
                all_raw = pitcher_raw + batter_raw
                logger.info("[Phase27] Statcast enrichment applied")
            except Exception as exc:
                logger.warning("[Phase27] Statcast enrichment failed: %s", exc)

        # Step 2b: Confirmed batting order from MLB Stats API
        try:
            batting_order = fetch_today_lineups(date)
            for p in all_raw:
                mid = int(p.get("mlbam_id") or 0)
                p["batting_order_pos"] = batting_order.get(mid, 0) if mid else 0
            confirmed = sum(1 for p in all_raw if p.get("batting_order_pos", 0) > 0)
            logger.info("[Lineups] Batting order attached: %d/%d props confirmed",
                        confirmed, len(all_raw))
        except Exception as exc:
            logger.warning("[Lineups] Batting order attachment failed: %s", exc)

        # Step 2c: Sportsbook reference -- sharp market comparison (Layer 7)
        # Fetches DK/FD/BetMGM player prop lines via The Odds API and strips vig.
        # Adds sb_implied_prob, sb_line, sb_line_gap to each prop for LineValueAgent.
        # Consumes ~16 requests/day (1 event list + 1 per game). Cached daily.
        if _SB_REF_AVAILABLE:
            try:
                all_raw = _sb_enrich(all_raw, date)
                _sb_matched = sum(1 for p in all_raw if p.get("sb_implied_prob", 0) > 0)
                logger.info(
                    "[SB_REF] Enrichment complete -- %d/%d props matched to sportsbook lines",
                    _sb_matched, len(all_raw),
                )
            except Exception as _sb_exc:
                logger.warning("[SB_REF] Enrichment failed: %s -- Layer 7 skipped", _sb_exc)

        # Step 3: SportsBettingDime public trends (FadeAgent precision upgrade)
        sbd_game_df = None
        sbd_prop_df = None
        if _SBD_AVAILABLE:
            try:
                _sbd = PublicTrendsScraper()
                sbd_game_df, sbd_prop_df = _sbd.fetch()
                logger.info("[Phase27] SBD trends: %d games, %d player props",
                            len(sbd_game_df), len(sbd_prop_df))
            except Exception as exc:
                logger.warning("[Phase27] SBD fetch failed: %s", exc)
        # ── End Phase 27 enrichment ────────────────────────────────────────

        # ── Phase 42: Layer 8a -- Marcel projections prefetch ──────────────────
        # Loads 3-year weighted FanGraphs projections (weekly cache).
        # No MLBAM ID needed -- name-based lookup matches existing prop data.
        if _MARCEL_AVAILABLE and _marcel_layer is not None:
            try:
                _marcel_layer.prefetch()
                logger.info("[Marcel] Marcel projections ready.")
            except Exception as _marcel_err:
                logger.warning(
                    "[Marcel] Prefetch failed: %s -- Layer 8a degraded.", _marcel_err
                )

        # ── Phase 42: Layer 8b -- Predict+ prefetch & score attachment ─────────
        # Computes pitcher unpredictability scores from Savant pitch sequences.
        # Weekly cache means Savant CSV is only fetched once per week per pitcher.
        # Scores are attached to pitcher props as `predict_plus_score` field.
        if _PP_AVAILABLE and _pp_layer is not None:
            try:
                _ud_pitcher_pos = {"SP", "RP", "P", "CP"}
                _seen_pp_ids: set[int] = set()
                _unique_pitchers: list[tuple[int, str]] = []
                for _p in all_raw:
                    if _p.get("position", "") in _ud_pitcher_pos:
                        _mid = int(_p.get("mlbam_id") or 0)
                        if _mid > 0 and _mid not in _seen_pp_ids:
                            _seen_pp_ids.add(_mid)
                            _unique_pitchers.append(
                                (_mid, _p.get("player_name", ""))
                            )
                if _unique_pitchers:
                    _pp_layer.prefetch(_unique_pitchers)
                    logger.info(
                        "[PP+] Predict+ prefetch complete (%d pitchers).",
                        len(_unique_pitchers),
                    )
                # Attach scores to every raw prop (0.0 for non-pitchers or cache miss)
                for _p in all_raw:
                    _mid = int(_p.get("mlbam_id") or 0)
                    _p["predict_plus_score"] = (
                        _pp_layer.get_score(_mid, _p.get("player_name", ""))
                        if _mid > 0 else 0.0
                    )
            except Exception as _pp_err:
                logger.warning(
                    "[PP+] Prefetch/attach failed: %s -- Layer 8b degraded.", _pp_err
                )
        # ── End Phase 42 prefetch ──────────────────────────────────────────────

        # ── Phase 53: Build player -> venue map for altitude adjustments ─────────
        _player_venue_map: dict = {}
        if _ALT_AVAILABLE:
            try:
                _player_venue_map = _build_player_venue_map()
            except Exception as _av_err:
                logger.warning("[Altitude] Venue map build failed: %s", _av_err)

        # 3. Build evaluated leg pool (enrichment data flows through)
        leg_pool: list[PropLeg] = self._evaluate_props(
            all_raw, sbd_game_df, sbd_prop_df, _player_venue_map
        )
        logger.info("Leg pool: %d evaluated legs (min prob %.0f%%)",
                    len(leg_pool), MIN_PROB * 100)

        # ── Layer 9: CV Consistency Gate ──────────────────────────────────────
        # CV = std/mean over last 10 games for the relevant stat per player.
        # Very consistent (CV < 0.50) -> +0.01  |  Normal (0.50-0.80) -> +/-0
        # Volatile (0.81-1.10) -> -0.02          |  Very volatile (>1.10) -> -0.04
        # Zero-mean L10 treated as maximally volatile (sentinel CV = 2.0).
        # Uses MLB Stats API game logs (free, no key). Cached daily per player.
        try:
            from cv_consistency_layer import apply_cv_consistency_layer
            import datetime as _cv_dt
            _cv_season = _cv_dt.datetime.now(_cv_dt.timezone.utc).year
            _cv_props = [
                {
                    "player_id": l.mlbam_id,
                    "prop_type": l.prop_type,
                    "implied_prob": l.implied_prob,
                    "description": f"{l.player_name} {l.prop_type} {l.side} {l.line}",
                }
                for l in leg_pool
            ]
            _cv_enriched = apply_cv_consistency_layer(_cv_props, season=_cv_season)
            for leg, cv_data in zip(leg_pool, _cv_enriched):
                leg.implied_prob = cv_data["implied_prob"]
                leg.cv_nudge = cv_data.get("cv_nudge", 0.0)
            _cv_nudged = sum(1 for l in leg_pool if getattr(l, "cv_nudge", 0.0) != 0.0)
            logger.info(
                "Layer 9 (CV consistency) -- %d/%d legs nudged.",
                _cv_nudged, len(leg_pool),
            )
        except Exception as _cv_err:
            logger.warning("Layer 9 CV skipped (fallback): %s", _cv_err)
        # ── End Layer 9 ───────────────────────────────────────────────────────

        if not leg_pool:
            logger.warning("No legs passed EV/prob gates -- no alerts today.")
            return

        # 4. Build all candidate parlays -- exclusive pick claiming pass
        # Rule: each (player_name, prop_type, side) may appear in at most ONE
        # parlay across the entire dispatch. Highest-confidence parlay wins any
        # contested pick. Parlays that lose enough legs to fall below MIN_LEGS
        # are dropped entirely. Parlays with 1-4 legs are all valid.
        # Phase 35: active agent gate -- respects config toggles + auto cool-down
        _active_agents = (
            set(_risk_manager.get_active_agents())
            if _risk_manager else None
        )

        candidate_parlays: list[dict] = []

        # ── 17 specialist agents -- collect without sending ─────────────────
        for agent in AGENT_CONFIGS:
            if _active_agents is not None and agent["name"] not in _active_agents:
                logger.info("[RISK] %s -- skipped (disabled or in cool-down)", agent["name"])
                continue
            _agent_T = self._agent_temperatures.get(agent["name"], _T_DEFAULT)
            parlay = build_parlay(leg_pool, agent, agent_T=_agent_T)
            if not parlay:
                logger.info("[%s] No qualifying parlay today.", agent["name"])
                continue
            ev   = parlay.get("ev_pct", 0)
            conf = parlay.get("confidence", 0)
            if ev < MIN_EV_PCT:
                logger.info("[%s] EV %.1f%% below gate -- skipped.", agent["name"], ev)
                continue
            if conf < 5.5:
                logger.info(
                    "[%s] Confidence %.1f/10 below 5.5 gate -- skipped.",
                    agent["name"], conf,
                )
                continue
            parlay["_agent_meta"] = agent  # stash for risk manager
            candidate_parlays.append(parlay)

        # ── OmegaStack -- triple-confirmation ensemble ──────────────────────
        _omega_T = self._agent_temperatures.get("OmegaStack", _T_DEFAULT)
        omega = build_omega_parlay(leg_pool, agent_T=_omega_T)
        if omega:
            ev_o   = omega.get("ev_pct", 0)
            conf_o = omega.get("confidence", 0)
            if ev_o >= MIN_EV_PCT and conf_o >= 5.5:
                omega["_agent_meta"] = None
                candidate_parlays.append(omega)
            else:
                logger.info(
                    "[OmegaStack] EV=%.1f%% conf=%.1f/10 -- below 5.5 gate, skipped.",
                    ev_o, conf_o,
                )
        else:
            logger.info("[OmegaStack] No triple-confirmation legs today.")

        # ── Dedup pass: drop 100% identical parlays only ─────────────────
        # Agents are independent bets — they MAY share individual legs.
        # Only suppress a parlay if its full leg fingerprint is an exact
        # duplicate of one already queued (same player+prop+side set).
        candidate_parlays.sort(key=lambda p: -p.get("confidence", 0))
        seen_fingerprints: set[frozenset] = set()
        final_parlays: list[dict] = []

        for parlay in candidate_parlays:
            fp = frozenset(
                (leg["player_name"].lower(), leg["prop_type"], leg["side"])
                for leg in parlay["legs"]
            )
            if fp in seen_fingerprints:
                logger.info(
                    "[%s] Dropped — exact duplicate of a higher-confidence parlay.",
                    parlay["agent_name"],
                )
                continue
            seen_fingerprints.add(fp)
            final_parlays.append(parlay)

        logger.info(
            "Dedup pass: %d/%d parlays queued to send",
            len(final_parlays), len(candidate_parlays),
        )

        # ── Send all surviving parlays ─────────────────────────────────────
        sent = 0
        for parlay in final_parlays:
            agent_name = parlay["agent_name"]
            ev         = parlay.get("ev_pct", 0)
            conf       = parlay.get("confidence", 0)
            n          = len(parlay["legs"])
            logger.info(
                "[%s] %d-leg parlay EV=%.1f%% conf=%.1f/10 -> SEND",
                agent_name, n, ev, conf,
            )
            if not self.dry_run:
                parlay["season_stats"] = get_agent_season_stats(agent_name)
                discord_alert.send_parlay_alert(parlay)
                time.sleep(1.5)
                record_parlay(
                    date=date,
                    agent=agent_name,
                    num_legs=n,
                    confidence=conf,
                    ev_pct=ev,
                    legs=[
                        {
                            "player_name": l.get("player_name", l.get("player", "")),
                            "prop_type":   l["prop_type"],
                            "side":        l["side"],
                            "line":        l["line"],
                        }
                        for l in parlay["legs"]
                    ],
                )
            else:
                logger.info(
                    "[DRY-RUN] Would send: %s",
                    json.dumps(
                        {k: v for k, v in parlay.items() if not k.startswith("_")},
                        indent=2,
                    )[:400],
                )
            if _risk_manager and not self.dry_run:
                _agent_unit = self._agent_units_cache.get(agent_name, 5.0)
                _risk_manager.record_stake(agent_name, _agent_unit)
            sent += 1

        logger.info("Dispatch complete -- %d parlays sent for %s", sent, date)

        # ── StreakAgent (19th agent) -- runs after the main 18-agent dispatch ──
        # Single best pick per day for Underdog Streaks format (11 consecutive
        # correct picks -> $1K/$5K/$10K prize). Confidence gate >= 8/10.
        try:
            from streak_agent import run_streak_pick
            streak_entry = int(os.getenv("STREAK_ENTRY_AMOUNT", "1"))
            streak_result = run_streak_pick(
                date_str     = date,
                entry_amount = streak_entry,
                dry_run      = self.dry_run,
            )
            if streak_result:
                logger.info(
                    "[StreakAgent] Pick #%d/%d sent -- %s %s %.1f %s "
                    "| conf=%.1f | prob=%.1f%%",
                    streak_result["pick_number"], 11,
                    streak_result["player_name"],
                    streak_result["prop_type"],
                    streak_result["line"],
                    streak_result["direction"],
                    streak_result["confidence"],
                    streak_result["probability"] * 100,
                )
            else:
                logger.info("[StreakAgent] No qualifying pick today (conf >= 8.0 gate not met).")
        except ImportError:
            logger.debug("[StreakAgent] streak_agent.py not found -- skipping.")
        except Exception as _streak_err:
            logger.warning("[StreakAgent] Error during streak pick: %s", _streak_err)

        # Phase 35: flush decision log buffer (batch write all leg decisions)
        if _DL_AVAILABLE:
            try:
                n_logged = _decision_logger.flush_buffer()
                logger.info("[DL] Flushed %d leg decisions to decision_log", n_logged)
            except Exception as _dl_err:
                logger.warning("[DL] Flush failed: %s", _dl_err)

        logger.info("Dispatch complete -- %d parlays posted", sent)

    # ── private ───────────────────────────────────────────────────────────────

    def _evaluate_props(
        self,
        raw_props: list[dict],
        sbd_game_df=None,
        sbd_prop_df=None,
        player_venue_map: dict | None = None,
    ) -> list[PropLeg]:
        """
        Normalise raw props, compare platforms, apply EV gate.

        Algorithm:
        1. Group props by (player_name_lower, prop_type) across both platforms.
        2. For each group: pick the platform with the more favourable line:
               Over -> lower line is better  (easier to clear)
               Under -> higher line is better (more room to stay under)
        3. Estimate implied win probability using MLB historical base rates
           for each stat+line combination (logistic function around base rate).
        4. Apply min_prob and EV gates.

        This avoids re-fetching APIs (platform_selector runs its own cache
        independently -- we work directly from already-fetched raw_props).
        """
        # ── MLB historical base-rate probabilities ─────────────────────────
        # Uses module-level _BASE_RATES (single source of truth shared with
        # See top of file for full table + documentation.

        # ── Per-game line range validation ────────────────────────────────
        # Lines outside these ranges are season-long or special markets.
        # We ONLY bet per-game props. These are realistic MLB per-game ranges.
        _GAME_LINE_RANGES: dict[str, tuple[float, float]] = {
            "hits":           (0.5, 4.5),
            "home_runs":      (0.5, 2.5),
            "rbis":           (0.5, 4.5),
            "runs":           (0.5, 3.5),
            "total_bases":    (0.5, 5.5),
            "stolen_bases":   (0.5, 2.5),
            "hits_runs_rbis": (0.5, 8.5),
            "strikeouts":     (1.5, 12.5),
            "earned_runs":    (0.5, 6.5),
            "walks":          (0.5, 5.5),
            "fantasy_hitter": (5.0, 60.0),
            "fantasy_pitcher":(15.0, 70.0),
        }

        def is_game_prop(prop_type: str, line: float) -> bool:
            """Return True only if line is within realistic per-game range."""
            rng = _GAME_LINE_RANGES.get(prop_type)
            if rng is None:
                return True
            return rng[0] <= line <= rng[1]

        def base_prob(prop_type: str, line: float, side: str) -> float:
            """Interpolate MLB base-rate probability for a given prop + line."""
            rates = _BASE_RATES.get(prop_type, [])
            if not rates:
                return 0.50
            xs = [r[0] for r in rates]
            ys = [r[1] for r in rates]
            # Clamp
            if line <= xs[0]:
                p_over = ys[0]
            elif line >= xs[-1]:
                p_over = ys[-1]
            else:
                # Linear interpolation
                for i in range(len(xs) - 1):
                    if xs[i] <= line <= xs[i + 1]:
                        t = (line - xs[i]) / (xs[i + 1] - xs[i])
                        p_over = ys[i] + t * (ys[i + 1] - ys[i])
                        break
                else:
                    p_over = 0.50
            return p_over if side == "Over" else (1.0 - p_over)

        from collections import defaultdict
        groups: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)

        for raw in raw_props:
            pname    = raw.get("player_name", "")
            raw_stat = raw.get("stat_type", "")
            line_val = float(raw.get("line") or 0)
            source   = raw.get("source", "")
            etype    = raw.get("entry_type", "FLEX")
            position = raw.get("position", "")

            prop_type = normalise_stat(raw_stat)
            if not prop_type or prop_type not in PROP_CONFIG:
                continue
            if line_val <= 0:
                continue

            key     = (pname.lower().strip(), prop_type)
            platform = "prizepicks" if source == "prizepicks" else "underdog"

            existing = groups[key].get(platform)
            if not existing:
                groups[key][platform] = {
                    "line": line_val, "entry_type": etype,
                    "player_name": pname, "position": position,
                    "de_hit_pct":   float(raw.get("de_hit_pct",  0.0) or 0.0),
                    "de_hr_pct":    float(raw.get("de_hr_pct",   0.0) or 0.0),
                    "de_k_pct":     float(raw.get("de_k_pct",    0.0) or 0.0),
                    "de_sb_pct":    float(raw.get("de_sb_pct",   0.0) or 0.0),
                    "de_run_pct":   float(raw.get("de_run_pct",  0.0) or 0.0),
                    "de_rbi_pct":   float(raw.get("de_rbi_pct",  0.0) or 0.0),
                    "sc_whiff_rate":      float(raw.get("sc_whiff_rate",      0.0) or 0.0),
                    "sc_hard_hit_rate":   float(raw.get("sc_hard_hit_rate",   0.0) or 0.0),
                    "sc_season_avg":      float(raw.get("sc_season_avg",      0.0) or 0.0),
                    # Phase 37: Baseball Savant expected stats + batting order
                    "sc_xwoba":           float(raw.get("sc_xwoba",           0.0) or 0.0),
                    "sc_xba":             float(raw.get("sc_xba",             0.0) or 0.0),
                    "sc_xslg":            float(raw.get("sc_xslg",            0.0) or 0.0),
                    "sc_barrel_rate":     float(raw.get("sc_barrel_rate",     0.0) or 0.0),
                    "sc_avg_launch_speed": float(raw.get("sc_avg_launch_speed", 0.0) or 0.0),
                    "sc_xiso":            float(raw.get("sc_xiso",            0.0) or 0.0),
                    "batting_order_pos":  int(raw.get("batting_order_pos",    0)   or 0),
                    # Phase 39 (Layer 7): sportsbook reference fields
                    "sb_implied_prob":    float(raw.get("sb_implied_prob",    0.0) or 0.0),
                    "sb_line":            float(raw.get("sb_line",            0.0) or 0.0),
                    "sb_line_gap":        float(raw.get("sb_line_gap",        0.0) or 0.0),
                }

        # ── Pre-fetch hot/cold form data for all unique players ─────────────
        # Collect every distinct player name from the merged groups dict so
        # we issue MLB Stats API game-log requests once per player (cached).
        _form_unique: set[str] = {
            info["player_name"]
            for plat_map in groups.values()
            for info in plat_map.values()
            if info.get("player_name")
        }
        try:
            _form_layer.prefetch_form_data(_form_unique)
        except Exception as _form_exc:
            logger.warning("[Form] Pre-fetch skipped: %s", _form_exc)

        # ── Evaluate each group ────────────────────────────────────────────
        legs: list[PropLeg] = []
        seen: set[tuple[str, str, str]] = set()

        for (player_lower, prop_type), platforms in groups.items():
            cfg = PROP_CONFIG[prop_type]

            for side in cfg["sides"]:
                leg_key = (player_lower, prop_type, side)
                if leg_key in seen:
                    continue

                # Pick best platform per side
                pp_entry = platforms.get("prizepicks")
                ud_entry = platforms.get("underdog")

                # For Over: prefer lower line; for Under: prefer higher line
                def line_score(entry: dict | None, s: str) -> float:
                    if entry is None:
                        return float("inf") if s == "Over" else float("-inf")
                    return entry["line"] if s == "Over" else -entry["line"]

                if pp_entry and ud_entry:
                    # Compare -- pick platform with better (more favorable) line
                    if line_score(pp_entry, side) <= line_score(ud_entry, side):
                        chosen_entry = pp_entry
                        chosen_platform = "PrizePicks"
                    else:
                        chosen_entry = ud_entry
                        chosen_platform = "Underdog"
                elif pp_entry:
                    chosen_entry = pp_entry
                    chosen_platform = "PrizePicks"
                elif ud_entry:
                    chosen_entry = ud_entry
                    chosen_platform = "Underdog"
                else:
                    continue

                line_val   = chosen_entry["line"]
                entry_type = chosen_entry.get("entry_type", "FLEX")
                pname      = chosen_entry.get("player_name",
                             player_lower.title())

                # Block season-long props (e.g., RBI Under 76.5 is a season total)
                if not is_game_prop(prop_type, line_val):
                    continue

                # Calculate implied probability from MLB base rates
                prob = base_prob(prop_type, line_val, side)
                _prob_base = prob        # Phase 35: track layer contributions
                _prob_after_de = prob    # updated after DraftEdge
                _prob_after_sc = prob    # updated after Statcast
                _prob_after_form = prob  # updated after form layer

                # Platform edge bonus: if both platforms available and we chose
                # the better line, add a small bonus (0.5-2.5%) for line advantage
                if pp_entry and ud_entry:
                    pp_line = pp_entry["line"]
                    ud_line = ud_entry["line"]
                    line_diff = abs(pp_line - ud_line)
                    # Each 0.5 unit of line difference ≈ +0.5% edge
                    platform_bonus = min(0.03, line_diff * 0.01)
                    prob = min(0.80, prob + platform_bonus)

                # ── Phase 27: Enhancement layer boosts ────────────────────
                de_boost_val = 0.0

                # 1. DraftEdge blend (65% base + 35% DE signal when available)
                def _de_signal(pt: str, sd: str, ent: dict) -> float:
                    """Map prop_type + side to the relevant DraftEdge probability."""
                    _over_map = {
                        "hits":         "de_hit_pct",
                        "home_runs":    "de_hr_pct",
                        "stolen_bases": "de_sb_pct",
                        "runs":         "de_run_pct",
                        "rbis":         "de_rbi_pct",
                        "strikeouts":   "de_k_pct",
                    }
                    if sd == "Over":
                        key = _over_map.get(pt)
                        return float(ent.get(key, 0.0) or 0.0) if key else 0.0
                    else:  # Under -- invert the over probability
                        key = _over_map.get(pt)
                        if not key:
                            return 0.0
                        p_over = float(ent.get(key, 0.0) or 0.0)
                        return (1.0 - p_over) if p_over > 0 else 0.0

                de_sig = _de_signal(prop_type, side, chosen_entry)
                if de_sig > 0:
                    blended = 0.65 * prob + 0.35 * de_sig
                    de_boost_val = round(blended - prob, 4)
                    prob = min(0.80, blended)
                _prob_after_de = prob  # Phase 35: snapshot after DraftEdge

                # 2. Statcast boosts (small additive signal on K and HR/TB props)
                if prop_type == "strikeouts" and side == "Over":
                    sc_whiff = float(chosen_entry.get("sc_whiff_rate", 0.0) or 0.0)
                    if sc_whiff > 0:
                        # whiff_rate 0.20-0.35 typical -> adds 3-5% to K prob
                        prob = min(0.80, prob + sc_whiff * 0.15)
                elif prop_type in ("home_runs", "total_bases") and side == "Over":
                    sc_hh = float(chosen_entry.get("sc_hard_hit_rate", 0.0) or 0.0)
                    if sc_hh > 0:
                        prob = min(0.80, prob + sc_hh * 0.10)
                elif prop_type == "hits" and side == "Over":
                    sc_avg = float(chosen_entry.get("sc_season_avg", 0.0) or 0.0)
                    if sc_avg > 0:
                        # Positive contact rate (avg > .270) adds slight hit prob bump
                        prob = min(0.80, prob + max(0.0, sc_avg - 0.250) * 0.10)

                # Phase 37: xwOBA boost for hits/contact props
                # xwOBA > 0.320 (above avg) = confirmed quality contact -> +hit prob
                if prop_type in ("hits", "hits_runs_rbis") and side == "Over":
                    sc_xwoba = float(chosen_entry.get("sc_xwoba", 0.0) or 0.0)
                    if sc_xwoba > 0.320:
                        prob = min(0.80, prob + (sc_xwoba - 0.320) * 0.12)

                # Phase 37: barrel rate boost for HR/TB props
                # Barrel% > 8% (above avg) = elite hard contact -> +power prop prob
                if prop_type in ("home_runs", "total_bases") and side == "Over":
                    sc_barrel = float(chosen_entry.get("sc_barrel_rate", 0.0) or 0.0)
                    if sc_barrel > 0.08:
                        prob = min(0.80, prob + (sc_barrel - 0.08) * 0.10)

                # Phase 37: xSLG boost for total_bases Over
                if prop_type == "total_bases" and side == "Over":
                    sc_xslg = float(chosen_entry.get("sc_xslg", 0.0) or 0.0)
                    if sc_xslg > 0.420:  # above avg slugger
                        prob = min(0.80, prob + (sc_xslg - 0.420) * 0.08)

                _prob_after_sc = prob  # Phase 35: snapshot after Statcast

                # 3. SBD FadeAgent signal
                sbd_ticket_pct = 0.0
                is_fade_signal = False
                if sbd_game_df is not None and sbd_prop_df is not None and _SBD_AVAILABLE:
                    try:
                        raw_pct, _ = _get_fade_signal(
                            pname, "", prop_type, sbd_game_df, sbd_prop_df,
                        )
                        sbd_ticket_pct = float(raw_pct or 0.0)
                        is_fade_signal = sbd_ticket_pct >= 65.0
                    except Exception:
                        pass  # non-fatal: SBD data unavailable for this prop
                # ── End Phase 27 ──────────────────────────────────────────

                # Phase 36: snapshot prob before form + FG nudges.
                # BullpenAgent, SteamAgent, StreakAgent use this in build_parlay.
                _prob_pre_form = prob  # noqa: SIM117

                # ── Hot/cold form adjustment (MLB Stats API rolling avg) ────────
                # Compares player's last-7-game rolling stat avg vs prior-season
                # per-game avg.  Returns +/-0.035 max -- never blocks a prop on its
                # own, just nudges probability in the right direction.
                try:
                    _form_adj = _form_layer.get_form_adjustment(pname, prop_type)
                    if _form_adj != 0.0:
                        logger.debug(
                            "[Form] %-22s  %-16s  adj=%+.3f  %.3f->%.3f",
                            pname, prop_type, _form_adj, prob, prob + _form_adj,
                        )
                    prob = min(0.80, max(0.40, prob + _form_adj))
                except Exception:
                    pass  # graceful degradation -- never let form data kill a leg
                _prob_after_form = prob  # Phase 35: snapshot after form layer

                # ── Layer 5: FanGraphs season stats (Phase 34) ───────────────
                # Per-agent signal routing:
                #   UmpireAgent / ArsenalAgent  -> CSW%, SwStr%, K-BB%
                #   MLEdgeAgent / F5Agent       -> xFIP, SIERA
                #   BullpenAgent / VultureStack -> FIP
                #   UnderMachine / OmegaStack   -> xFIP (Under pitcher props)
                #   LineupAgent / PlatoonAgent  -> wRC+, wOBA
                #   WeatherAgent                -> ISO, HR/FB%
                #   GetawayAgent                -> BABIP regression flag
                #   FadeAgent                   -> LOB%, BABIP
                #   EVHunter / StreakAgent      -> full signal set
                if _FG_AVAILABLE:
                    try:
                        # Phase 150 fix: cfg["player_type"] was "hitter"|"pitcher"
                        # player_type was previously undefined (NameError silently caught)
                        _fg_ptype = cfg["player_type"]
                        if _fg_ptype == "pitcher":
                            _fg_data = _fg_get_pitcher(pname)
                        else:
                            _fg_data = _fg_get_batter(pname)
                        _fg_adj_ptype = "pitcher" if _fg_ptype == "pitcher" else "batter"
                        _fg_adj = _fg_adjustment(prop_type, side, _fg_adj_ptype, _fg_data)
                        if _fg_adj != 0.0:
                            logger.debug(
                                "[FG] %-22s  %-16s  adj=%+.3f  %.3f->%.3f",
                                pname, prop_type, _fg_adj, prob, prob + _fg_adj,
                            )
                        prob = min(0.80, max(0.40, prob + _fg_adj))
                    except Exception:
                        pass  # FanGraphs is additive -- never let it crash the leg
                # ── End Layer 5 ──────────────────────────────────────────────

                # ── Layer 8a: Marcel projection adjustment (Phase 42) ─────────
                # 3-year weighted FanGraphs projection (baseball-sims algorithm).
                # Compares player's Marcel-projected rate to league average and
                # applies a subtle nudge (max +/-0.018) based on multi-season history.
                # Fires after all 7 real-time layers so it never overrides context.
                _prob_after_fg = prob   # snapshot before 8a/8b for decision logging
                if _MARCEL_AVAILABLE and _marcel_layer is not None:
                    try:
                        _fg_ptype = cfg["player_type"]
                        if _fg_ptype == "pitcher":
                            _marcel_data = _marcel_layer.get_pitcher(pname)
                        else:
                            _marcel_data = _marcel_layer.get_batter(pname)
                        _m_ptype = "pitcher" if _fg_ptype == "pitcher" else "batter"
                        _marcel_adj = _marcel_adjustment(
                            prop_type, side, _m_ptype, _marcel_data
                        )
                        if _marcel_adj != 0.0:
                            logger.debug(
                                "[Marcel] %-22s  %-16s  adj=%+.3f  %.3f->%.3f",
                                pname, prop_type, _marcel_adj, prob, prob + _marcel_adj,
                            )
                        prob = min(0.80, max(0.40, prob + _marcel_adj))
                    except Exception:
                        pass   # Marcel is additive -- never crash a leg

                # ── Layer 8b: Predict+ adjustment (Phase 42, K props only) ─────
                # Pitcher unpredictability score from pitch-sequence multinomial LR.
                # Ported from jaime12minaya/PredictPlus (R -> Python).
                # Only fires for strikeout props where pitcher MLBAM ID is known.
                if _PP_AVAILABLE and prop_type == "strikeouts":
                    try:
                        _pp_score = float(chosen_entry.get("predict_plus_score", 0.0) or 0.0)
                        _pp_adj = _pp_adjustment(prop_type, side, _pp_score)
                        if _pp_adj != 0.0:
                            logger.debug(
                                "[PP+] %-22s  K %-5s  score=%.1f  adj=%+.3f  %.3f->%.3f",
                                pname, side, _pp_score, _pp_adj, prob, prob + _pp_adj,
                            )
                        prob = min(0.80, max(0.40, prob + _pp_adj))
                    except Exception:
                        pass   # Predict+ is additive -- never crash a leg
                # ── End Layers 8a / 8b ───────────────────────────────────────

                # ── Phase 53: Altitude park factor adjustment ──────────────────
                # Fires last (after Form, FG, Marcel, Predict+) so the gate sees
                # the fully calibrated probability.
                # Chase Field: gets both dome (if roof closed) and altitude.
                # Coors Field: altitude factor dampened ~35% by humidor.
                if _ALT_AVAILABLE and player_venue_map:
                    _alt_venue = player_venue_map.get(pname.lower(), "")
                    if _alt_venue:
                        _prob_pre_alt = prob
                        prob = _alt_adjust(
                            base_projection=prob,
                            prop_type=prop_type,
                            venue=_alt_venue,
                            humidor_active=_get_humidor(_alt_venue),
                        )
                        prob = min(0.80, max(0.40, prob))
                        if abs(prob - _prob_pre_alt) > 0.0001:
                            logger.debug(
                                "[Altitude] %-22s  %-16s  venue=%s  adj=%+.4f",
                                pname, prop_type, _alt_venue, prob - _prob_pre_alt,
                            )
                # ── End Phase 53 ───────────────────────────────────────────────
                # ── Phase 51: Dome Stadium Adjustment ────────────────────────────
                # Applied after all real-time layers. Zeroes weather boosts for
                # dome/closed-roof games; applies turf + environment modifiers.
                if _DOME_AVAILABLE:
                    try:
                        _player_team = getattr(self, "_player_team_map", {}).get(
                            pname.lower(), ""
                        )
                        _player_venue = getattr(self, "_team_venue_map", {}).get(
                            _player_team, ""
                        )
                        if _player_venue:
                            _is_home = _player_team in getattr(
                                self, "_home_teams", set()
                            )
                            prob, _dome_nudge = _apply_dome_adj(
                                prob, prop_type, _player_venue,
                                roof_status="closed",
                                is_home_team=_is_home,
                            )
                            if _dome_nudge != 0.0:
                                logger.debug(
                                    "[Dome] %-22s  %-16s  venue=%s  nudge=%+.3f  final=%.3f",
                                    pname, prop_type, _player_venue, _dome_nudge, prob,
                                )
                    except Exception:
                        pass  # dome adjustment is additive -- never crash a leg
                # ── End Phase 51 ────────────────────────────────────────────

                # Gate checks -- Phase 35: log all decisions with full feature trail
                if prob < cfg["min_prob"]:
                    if _DL_AVAILABLE:
                        _decision_logger.log_leg(
                            agent_name="dispatcher", player_name=pname,
                            prop_type=prop_type, direction=side, line=line_val,
                            platform=chosen_platform,
                            prob_base=_prob_base,
                            prob_draftedge=round(_prob_after_de - _prob_base, 4),
                            prob_statcast=round(_prob_after_sc - _prob_after_de, 4),
                            prob_sbd=sbd_ticket_pct / 100.0 if sbd_ticket_pct else 0.0,
                            prob_form=round(_prob_after_form - _prob_after_sc, 4),
                            prob_fangraphs=round(prob - _prob_after_form, 4),
                            prob_final=prob, edge_pct=0.0,
                            decision="REJECTED",
                            reject_reason=f"prob {prob:.3f} < min {cfg['min_prob']:.3f}",
                        )
                    continue

                ev = calc_ev(prob)
                if ev < MIN_EV_PCT:
                    if _DL_AVAILABLE:
                        _decision_logger.log_leg(
                            agent_name="dispatcher", player_name=pname,
                            prop_type=prop_type, direction=side, line=line_val,
                            platform=chosen_platform,
                            prob_base=_prob_base,
                            prob_draftedge=round(_prob_after_de - _prob_base, 4),
                            prob_statcast=round(_prob_after_sc - _prob_after_de, 4),
                            prob_sbd=sbd_ticket_pct / 100.0 if sbd_ticket_pct else 0.0,
                            prob_form=round(_prob_after_form - _prob_after_sc, 4),
                            prob_fangraphs=round(prob - _prob_after_form, 4),
                            prob_final=prob, edge_pct=ev,
                            decision="REJECTED",
                            reject_reason=f"EV {ev:.4f} < min {MIN_EV_PCT:.4f}",
                        )
                    continue

                seen.add(leg_key)
                legs.append(PropLeg(
                    player_name=pname,
                    prop_type=prop_type,
                    side=side,
                    line=line_val,
                    platform=chosen_platform,
                    implied_prob=round(prob, 4),
                    entry_type=entry_type,
                    fantasy_pts=0.0,
                    ev_pct=round(ev, 2),
                    de_boost=de_boost_val,
                    sbd_ticket_pct=round(sbd_ticket_pct, 1),
                    is_fade_signal=is_fade_signal,
                    # Phase 36: store pre-nudge prob for agent-specific overrides
                    prob_pre_form=round(_prob_pre_form, 4),
                    # Phase 37: confirmed batting order position
                    batting_order_pos=int(chosen_entry.get("batting_order_pos", 0) or 0),
                    # Phase 39 (Layer 7): sportsbook reference signals
                    sb_implied_prob=float(chosen_entry.get("sb_implied_prob", 0.0) or 0.0),
                    sb_line_gap=float(chosen_entry.get("sb_line_gap", 0.0) or 0.0),
                    mlbam_id=int(chosen_entry.get("mlbam_id", 0) or 0),
                ))
                # Phase 35: log INCLUDED leg with full feature trail
                if _DL_AVAILABLE:
                    _decision_logger.log_leg(
                        agent_name="dispatcher", player_name=pname,
                        prop_type=prop_type, direction=side, line=line_val,
                        platform=chosen_platform,
                        prob_base=_prob_base,
                        prob_draftedge=round(_prob_after_de - _prob_base, 4),
                        prob_statcast=round(_prob_after_sc - _prob_after_de, 4),
                        prob_sbd=sbd_ticket_pct / 100.0 if sbd_ticket_pct else 0.0,
                        prob_form=round(_prob_after_form - _prob_after_sc, 4),
                        prob_fangraphs=round(prob - _prob_after_form, 4),
                        prob_final=prob, edge_pct=ev,
                        decision="INCLUDED",
                        features={
                            "sbd_ticket_pct": sbd_ticket_pct,
                            "is_fade_signal": is_fade_signal,
                            "de_boost": de_boost_val,
                        },
                    )

        # Sort by implied prob desc
        legs.sort(key=lambda l: -l.implied_prob)
        logger.info("Leg pool built: %d legs across %d platform comparisons",
                    len(legs), len(groups))
        return legs


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIQ Live Dispatcher")
    parser.add_argument("--date",    type=str, help="Date override YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log parlays without sending Discord alerts")
    args = parser.parse_args()

    discord_alert.send_startup_ping()
    dispatcher = LiveDispatcher(dry_run=args.dry_run)
    dispatcher.run(date_str=args.date)
