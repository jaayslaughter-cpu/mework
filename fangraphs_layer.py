"""
fangraphs_layer.py
------------------
Phase 34 — FanGraphs season statistics via pybaseball.

Provides per-agent signal enhancement for all 19 agents:

  Pitchers
  --------
  csw_pct    : Called Strikes + Whiffs % (best single K predictor)
  swstr_pct  : Swinging Strike %
  k_bb_pct   : K% minus BB% (true command metric)
  xfip       : Expected FIP — strips HR variance (true skill ERA)
  siera      : Skill-Interactive ERA (sequence-adjusted skill metric)
  fip        : Fielding Independent Pitching
  hr_fb_pct  : Home run per fly ball rate
  lob_pct    : Left-on-base strand rate (regression flag)
  babip      : Pitcher BABIP (luck normaliser)

  Batters
  -------
  wrc_plus   : wRC+ — park/league adjusted run creation (100 = average)
  woba       : Weighted On-Base Average
  iso        : Isolated Power (SLG - AVG)
  babip      : Batter BABIP (hot/cold luck flag)
  o_swing    : O-Swing% — chase rate outside zone
  z_contact  : Z-Contact% — contact inside zone
  hr_fb_pct  : Batter HR/FB rate (power + wind interaction)
  k_pct      : Strikeout rate (batter Ks)
  bb_pct     : Walk rate

Data is fetched once daily and cached to /tmp/ so all 19 agents share
the same dataset without hammering FanGraphs.

Fallback: if pybaseball is unavailable or FanGraphs is down, every
get_*() call returns an empty dict — all adjustments in the dispatcher
will be 0.0 and the pipeline continues normally.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

# ─── Module-level caches ──────────────────────────────────────────────────────
_BATTER_CACHE: dict[str, dict[str, float]] = {}
_PITCHER_CACHE: dict[str, dict[str, float]] = {}
_loaded: bool = False

# ─── League-average baselines (2024 season) ──────────────────────────────────
LEAGUE_DEFAULTS: dict[str, dict[str, float]] = {
    "pitcher": {
        "csw_pct":   0.280,
        "swstr_pct": 0.108,
        "k_bb_pct":  0.130,
        "xfip":      4.20,
        "siera":     4.20,
        "fip":       4.20,
        "hr_fb_pct": 0.120,
        "lob_pct":   0.720,
        "babip":     0.300,
    },
    "batter": {
        "wrc_plus":  100.0,
        "woba":      0.320,
        "iso":       0.150,
        "babip":     0.300,
        "o_swing":   0.310,
        "z_contact": 0.850,
        "hr_fb_pct": 0.120,
        "k_pct":     0.230,
        "bb_pct":    0.085,
    },
}
def _load() -> None:
    """Fetch or load from daily cache.  Sets _loaded = True on completion."""

    today = date.today().isoformat()

    # ── Try cache first ──────────────────────────────────────────────────────
    with tempfile.TemporaryFile(mode='w+') as tmp:
        try:
            tmp.seek(0)
            data = json.load(tmp)
            _BATTER_CACHE  = data.get("batters", {})
            _PITCHER_CACHE = data.get("pitchers", {})
            logger.info(
                "[FG] Loaded from cache — %d batters  %d pitchers",
                len(_BATTER_CACHE), len(_PITCHER_CACHE),
            )
            _loaded = True
            return
        except Exception as exc:
            logger.warning("[FG] Cache read failed (%s) — fetching live", exc)

    # ── Live fetch via pybaseball ────────────────────────────────────────────
    try:
        import pybaseball as pb  # noqa: PLC0415

        # Silence pybaseball's progress bar in production
        pb.cache.enable()

        season = date.today().year

        # ── Batting stats ────────────────────────────────────────────────────
        bd = LEAGUE_DEFAULTS["batter"]
        bat_df = pb.batting_stats(season, qual=20)
        for _, row in bat_df.iterrows():
            key = _normalise_name(str(row.get("Name", "")))
            if not key:
                continue
            _BATTER_CACHE[key] = {
                "wrc_plus":  _safe_float(row.get("wRC+"),        bd["wrc_plus"]),
                "woba":      _safe_float(row.get("wOBA"),        bd["woba"]),
                "iso":       _safe_float(row.get("ISO"),         bd["iso"]),
                "babip":     _safe_float(row.get("BABIP"),       bd["babip"]),
                "o_swing":   _safe_float(row.get("O-Swing%"),    bd["o_swing"]),
                "z_contact": _safe_float(row.get("Z-Contact%"),  bd["z_contact"]),
                "hr_fb_pct": _safe_float(row.get("HR/FB"),       bd["hr_fb_pct"]),
                "k_pct":     _safe_float(row.get("K%"),          bd["k_pct"]),
                "bb_pct":    _safe_float(row.get("BB%"),         bd["bb_pct"]),
            }

        # ── Pitching stats ───────────────────────────────────────────────────
        pd_ = LEAGUE_DEFAULTS["pitcher"]
        pit_df = pb.pitching_stats(season, qual=10)
        for _, row in pit_df.iterrows():
            key = _normalise_name(str(row.get("Name", "")))
            if not key:
                continue
            _PITCHER_CACHE[key] = {
                "csw_pct":   _safe_float(row.get("CSW%"),    pd_["csw_pct"]),
                "swstr_pct": _safe_float(row.get("SwStr%"),  pd_["swstr_pct"]),
                "k_bb_pct":  _safe_float(row.get("K-BB%"),   pd_["k_bb_pct"]),
                "xfip":      _safe_float(row.get("xFIP"),    pd_["xfip"]),
                "siera":     _safe_float(row.get("SIERA"),   pd_["siera"]),
                "fip":       _safe_float(row.get("FIP"),     pd_["fip"]),
                "hr_fb_pct": _safe_float(row.get("HR/FB"),   pd_["hr_fb_pct"]),
                "lob_pct":   _safe_float(row.get("LOB%"),    pd_["lob_pct"]),
                "babip":     _safe_float(row.get("BABIP"),   pd_["babip"]),
            }

        # ── Persist cache ────────────────────────────────────────────────────
        with open(cache_path, "w") as fh:
            json.dump({"batters": _BATTER_CACHE, "pitchers": _PITCHER_CACHE}, fh)

        logger.info(
            "[FG] Fetched live — %d batters  %d pitchers  cached→%s",
            len(_BATTER_CACHE), len(_PITCHER_CACHE), cache_path,
        )

    except ImportError:
        logger.warning("[FG] pybaseball not installed — FanGraphs layer disabled")
    except Exception as exc:
        logger.warning("[FG] FanGraphs fetch failed: %s — continuing without", exc)

    _loaded = True  # Always set True so we don't retry on every prop


# ─── Public getters ───────────────────────────────────────────────────────────

def get_batter(name: str) -> dict[str, float]:
    """Return FanGraphs batting stats for *name*.  Empty dict if not found."""
    if not _loaded:
        _load()
    return _BATTER_CACHE.get(_normalise_name(name), {})


def get_pitcher(name: str) -> dict[str, float]:
    """Return FanGraphs pitching stats for *name*.  Empty dict if not found."""
    if not _loaded:
        _load()
    return _PITCHER_CACHE.get(_normalise_name(name), {})


# ─── Probability adjustment engine ───────────────────────────────────────────

# Hard cap: no single FanGraphs nudge exceeds ±0.030
_FG_CAP = 0.030

# Per-agent signal routing
# Maps (player_type, prop_type_group) → adjustment logic key
_PROP_GROUPS: dict[str, list[str]] = {
    "k_props":     ["strikeouts", "pitcher_strikeouts"],
    "er_props":    ["earned_runs", "earned_runs_allowed"],
    "hits_allow":  ["hits_allowed", "pitcher_hits", "walks_allowed"],
    "hit_props":   ["hits", "singles", "doubles"],
    "power_props": ["home_runs", "total_bases"],
    "rbi_run":     ["rbis", "runs", "rbi"],
    "batter_k":    ["batter_strikeouts"],
    "sb_props":    ["stolen_bases"],
}


def _in_group(prop_type: str, group: str) -> bool:
    return prop_type in _PROP_GROUPS.get(group, [])


def fangraphs_adjustment(
    prop_type: str,
    direction: str,           # "Over" or "Under"
    player_type: str,         # "pitcher" or "batter"
    fg: dict[str, float],
) -> float:
    """
    Compute a probability nudge from FanGraphs season stats.

    Signal routing per agent specialty:
      UmpireAgent / ArsenalAgent  → CSW%, SwStr%, K-BB%   (K props)
      MLEdgeAgent / F5Agent       → xFIP, SIERA            (pitcher quality)
      BullpenAgent / VultureStack → FIP                    (bullpen arms)
      UnderMachine / OmegaStack   → xFIP                   (Under pitcher props)
      LineupAgent / PlatoonAgent  → wRC+, wOBA             (hitting volume)
      WeatherAgent                → ISO, HR/FB%            (power + wind)
      GetawayAgent                → BABIP                  (regression flag)
      FadeAgent                   → LOB%, BABIP            (public overvaluation)
      EVHunter / StreakAgent      → full signal set

    Returns float in [-0.030, +0.030].  Returns 0.0 if fg is empty.
    """
    if not fg:
        return 0.0

    adj = 0.0
    ld_p = LEAGUE_DEFAULTS["pitcher"]
    ld_b = LEAGUE_DEFAULTS["batter"]

    is_over  = direction.lower() == "over"
    flip     = -1.0 if not is_over else 1.0

    if player_type == "pitcher":
        # ── K props: CSW% (primary) + SwStr% (secondary) + K-BB% (tertiary) ──
        if _in_group(prop_type, "k_props"):
            csw     = fg.get("csw_pct",   ld_p["csw_pct"])
            swstr   = fg.get("swstr_pct", ld_p["swstr_pct"])
            k_bb    = fg.get("k_bb_pct",  ld_p["k_bb_pct"])
            # CSW% 0.28 = avg; elite ≥0.32; terrible ≤0.24
            csw_adj   = (csw   - 0.280) / 0.040 * 0.014
            swstr_adj = (swstr - 0.108) / 0.030 * 0.008
            k_bb_adj  = (k_bb  - 0.130) / 0.050 * 0.006
            adj += flip * (csw_adj + swstr_adj + k_bb_adj)

        # ── ER allowed: xFIP primary + SIERA secondary ───────────────────────
        elif _in_group(prop_type, "er_props"):
            xfip = fg.get("xfip",  ld_p["xfip"])
            siera = fg.get("siera", ld_p["siera"])
            # Low xFIP → elite pitcher → Under ER is more likely
            xfip_adj  = (4.20 - xfip)  / 0.70 * 0.015
            siera_adj = (4.20 - siera) / 0.70 * 0.008
            # For Under ER: positive adj when pitcher is good
            adj += flip * (xfip_adj + siera_adj)

        # ── Hits/walks allowed: SwStr% + BABIP luck flag ─────────────────────
        elif _in_group(prop_type, "hits_allow"):
            swstr = fg.get("swstr_pct", ld_p["swstr_pct"])
            babip = fg.get("babip",     ld_p["babip"])
            # High SwStr → more misses → fewer hits allowed (Under signal)
            swstr_adj = (swstr - 0.108) / 0.030 * 0.012
            # Low BABIP pitcher is due to regress → Over hits allowed signal
            babip_adj = (0.300 - babip) / 0.030 * 0.008
            adj += flip * swstr_adj
            # BABIP regression is direction-agnostic — always nudge toward reversion
            adj -= babip_adj  # positive BABIP gap always nudges toward more hits

    else:  # batter
        # ── Hitting props (hits/singles/doubles): wRC+ + wOBA ────────────────
        if _in_group(prop_type, "hit_props"):
            wrc  = fg.get("wrc_plus", ld_b["wrc_plus"])
            woba = fg.get("woba",     ld_b["woba"])
            wrc_adj  = (wrc  - 100.0) / 30.0 * 0.015
            woba_adj = (woba - 0.320) / 0.060 * 0.010
            adj += flip * (wrc_adj + woba_adj)

        # ── Power props (HR/TB): ISO + HR/FB% + wRC+ ─────────────────────────
        elif _in_group(prop_type, "power_props"):
            iso    = fg.get("iso",     ld_b["iso"])
            hr_fb  = fg.get("hr_fb_pct", ld_b["hr_fb_pct"])
            wrc    = fg.get("wrc_plus", ld_b["wrc_plus"])
            iso_adj   = (iso   - 0.150) / 0.070 * 0.014
            hr_fb_adj = (hr_fb - 0.120) / 0.050 * 0.010
            wrc_adj   = (wrc   - 100.0) / 30.0  * 0.006
            adj += flip * (iso_adj + hr_fb_adj + wrc_adj)

        # ── RBI / runs: wOBA + wRC+ ───────────────────────────────────────────
        elif _in_group(prop_type, "rbi_run"):
            wrc  = fg.get("wrc_plus", ld_b["wrc_plus"])
            woba = fg.get("woba",     ld_b["woba"])
            adj += flip * ((wrc - 100.0) / 30.0 * 0.012 + (woba - 0.320) / 0.060 * 0.010)

        # ── Batter strikeouts: O-Swing% + K% ─────────────────────────────────
        elif _in_group(prop_type, "batter_k"):
            o_swing = fg.get("o_swing", ld_b["o_swing"])
            k_pct   = fg.get("k_pct",   ld_b["k_pct"])
            # High chase rate → more Ks
            o_adj = (o_swing - 0.310) / 0.100 * 0.015
            k_adj = (k_pct   - 0.230) / 0.050 * 0.012
            adj += flip * (o_adj + k_adj)

        # ── Stolen bases: BB% (on-base dependency) + speed proxy ─────────────
        elif _in_group(prop_type, "sb_props"):
            bb_pct = fg.get("bb_pct", ld_b["bb_pct"])
            adj += flip * (bb_pct - 0.085) / 0.030 * 0.010

    # ── Hard cap ─────────────────────────────────────────────────────────────
    return max(-_FG_CAP, min(_FG_CAP, adj))
