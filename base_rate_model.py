"""
base_rate_model.py — PropIQ calibrated base-rate probability model.

Replaces flat 50% fallback in _model_prob() when XGBoost isn't trained yet.
Uses historical MLB frequencies (2022-2024) interpolated from the same
_BASE_RATES table live_dispatcher.py uses, then layers on FanGraphs stats
and context signals already attached by prop_enrichment_layer.

Usage (already wired in tasklets.py):
    from base_rate_model import get_model_prob as _base_rate_prob
    prob_pct = _base_rate_prob(prop, "OVER")   # returns 0-100
"""
from __future__ import annotations
import logging
logger = logging.getLogger("propiq.base_rate_model")

_BASE_RATES: dict[str, list[tuple[float, float]]] = {
    # home_runs, stolen_bases, walks, walks_allowed removed — not approved prop types
    "hits":           [(0.5,0.72),(1.5,0.38),(2.5,0.13),(3.5,0.03)],
    "rbis":           [(0.5,0.42),(1.5,0.20),(2.5,0.08),(3.5,0.025)],
    "rbi":            [(0.5,0.42),(1.5,0.20),(2.5,0.08),(3.5,0.025)],
    "runs":           [(0.5,0.47),(1.5,0.18),(2.5,0.06),(3.5,0.015)],
    "total_bases":    [(0.5,0.85),(1.5,0.55),(2.5,0.28),(3.5,0.12),(4.5,0.04)],
    "hits_runs_rbis": [(0.5,0.95),(1.5,0.72),(2.5,0.48),(3.5,0.28),(4.5,0.14),(5.5,0.06),(6.5,0.02)],
    "strikeouts":     [(1.5,0.72),(3.5,0.55),(5.5,0.38),(6.5,0.30),(7.5,0.22),(8.5,0.15),(9.5,0.10),(10.5,0.06),(11.5,0.03)],
    "earned_runs":    [(0.5,0.55),(1.5,0.38),(2.5,0.22),(3.5,0.12),(4.5,0.05)],
    "hits_allowed":   [(1.5,0.85),(3.5,0.58),(5.5,0.30),(7.5,0.11)],
    "pitching_outs":  [(8.5,0.62),(11.5,0.46),(14.5,0.30),(17.5,0.17),(20.5,0.06)],
    "fantasy_hitter": [(5.0,0.88),(10.0,0.68),(15.0,0.46),(20.0,0.28),(25.0,0.15),(30.0,0.07),(40.0,0.02)],
    "fantasy_pitcher":[(15.0,0.80),(20.0,0.60),(25.0,0.42),(30.0,0.27),(35.0,0.15),(40.0,0.08),(50.0,0.02)],
}

_LG = {"csw":0.275,"swstr":0.110,"k_bb":0.139,"xfip":4.20,"siera":4.20,
       "wrc":100.0,"woba":0.310,"iso":0.155,"hr_fb":0.105,"o_sw":0.310,"k_pct":0.224}
_FG_CAP = 0.030

def _interp(rates, line):
    xs, ys = [r[0] for r in rates], [r[1] for r in rates]
    if line <= xs[0]:  return ys[0]
    if line >= xs[-1]: return ys[-1]
    for i in range(len(xs)-1):
        if xs[i] <= line <= xs[i+1]:
            t = (line-xs[i])/(xs[i+1]-xs[i])
            return ys[i] + t*(ys[i+1]-ys[i])
    return 0.50

def _fg_adj(prop_type, prop, flip):
    adj = 0.0
    pt = prop_type
    if pt in {"strikeouts","pitching_outs"}:
        adj += flip*((float(prop.get("csw_pct",_LG["csw"])or _LG["csw"])-_LG["csw"])/0.040*0.014
                    +(float(prop.get("swstr_pct",_LG["swstr"])or _LG["swstr"])-_LG["swstr"])/0.030*0.008
                    +(float(prop.get("k_bb_pct",_LG["k_bb"])or _LG["k_bb"])-_LG["k_bb"])/0.050*0.006)
    elif pt in {"earned_runs","fantasy_pitcher"}:
        adj += flip*((4.20-float(prop.get("xfip",_LG["xfip"])or _LG["xfip"]))/0.70*0.015
                    +(4.20-float(prop.get("siera",_LG["siera"])or _LG["siera"]))/0.70*0.008)
    elif pt in {"hits","hits_runs_rbis"}:
        adj += flip*((float(prop.get("wrc_plus",_LG["wrc"])or _LG["wrc"])-_LG["wrc"])/30.0*0.015
                    +(float(prop.get("woba",_LG["woba"])or _LG["woba"])-_LG["woba"])/0.060*0.010)
    elif pt in {"home_runs","total_bases","fantasy_hitter"}:
        adj += flip*((float(prop.get("iso",_LG["iso"])or _LG["iso"])-_LG["iso"])/0.070*0.014
                    +(float(prop.get("hr_fb_pct",_LG["hr_fb"])or _LG["hr_fb"])-_LG["hr_fb"])/0.050*0.010
                    +(float(prop.get("wrc_plus",_LG["wrc"])or _LG["wrc"])-_LG["wrc"])/30.0*0.006)
    elif pt in {"rbis","rbi","runs"}:
        adj += flip*((float(prop.get("wrc_plus",_LG["wrc"])or _LG["wrc"])-_LG["wrc"])/30.0*0.012
                    +(float(prop.get("woba",_LG["woba"])or _LG["woba"])-_LG["woba"])/0.060*0.010)
    return max(-_FG_CAP, min(_FG_CAP, adj))

def get_model_prob(prop: dict, side: str = "OVER") -> float:
    """Return calibrated probability 0-100 for prop/side. Replaces flat 50%."""
    prop_type = str(prop.get("prop_type","")).lower()
    line      = float(prop.get("line", 1.5) or 1.5)
    is_over   = side.upper() in ("OVER","O")
    rates     = _BASE_RATES.get(prop_type)
    p = _interp(rates, line) if rates else 0.50
    if not is_over: p = 1.0 - p
    flip = 1.0 if is_over else -1.0
    p += _fg_adj(prop_type, prop, flip)
    # Context signals from prop_enrichment_layer
    wind = float(prop.get("_wind_speed",0) or 0)
    if not prop.get("is_dome") and wind >= 10:
        if prop_type in {"home_runs","total_bases","hits_runs_rbis","fantasy_hitter"}:
            if "out" in str(prop.get("_wind_direction","")).lower() and is_over:
                p += min(0.08, (wind-10)*0.004+0.04)
    temp = float(prop.get("_temp_f",72) or 72)
    if not prop.get("is_dome") and temp >= 85 and is_over:
        if prop_type in {"home_runs","total_bases","hits","rbis","runs","hits_runs_rbis"}:
            p += min(0.04, (temp-85)*0.004+0.02)
    if int(prop.get("altitude_ft",0) or 0) >= 4000 and not prop.get("humidor") and is_over:
        if prop_type in {"home_runs","total_bases","hits","rbis","runs"}:
            p += 0.05
    p += float(prop.get("_bayesian_nudge",0) or 0)
    p += float(prop.get("_cv_nudge",0) or 0)
    p += float(prop.get("_form_adj",0) or 0)
    p = max(0.05, min(0.95, p))
    return round(p * 100, 2)
