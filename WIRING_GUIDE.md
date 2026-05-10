# PropIQ Model Wiring — Implementation Guide
# ============================================
# Provide this file plus the three .py files to Claude Code or another
# AI coding system. This document tells it exactly what to do and where.

---

## WHAT THIS BUNDLE FIXES

These are not new features — they are connections between code that already
exists in the repo but has never been wired together.

| File | What it wires |
|------|--------------|
| `wire_xgb_models.py` | XGBoost K and hit models → prop evaluation |
| `wire_model_layers.py` | PA model, Bernoulli drama, BVI, injury block → prop evaluation |
| `wire_park_and_temperature.py` | Merged park factor table + temperature audit |

---

## PRIORITY ORDER

Do these in order. Each is independent — if one causes a conflict, skip and continue.

### 1. XGBoost K/Hit Blend (wire_xgb_models.py) — HIGHEST PRIORITY

This is the largest single improvement available. Every K prop runs on pure
Poisson/Bayesian formula with zero XGBoost contribution. The per-line models
(xgb_k_3_5.pkl through xgb_k_6_5.pkl) exist in models/ and are trained.

**Step 1:** Copy `wire_xgb_models.py` to the repo root.

**Step 2:** Verify models exist:
```bash
python wire_xgb_models.py --check
```
If `xgb_k_ready()` returns False, run:
```bash
python scripts/xgb_k_training.py
```
This trains the models from your existing Statcast data.

**Step 3:** Add ONE line to tasklets.py (or prop_enrichment_layer.py).
Find the section where `model_prob` is finalized before EV gating.
It will look like a block that ends with something like:
```python
model_prob = some_final_value
ev = model_prob - market_implied
```

Add immediately before the `ev =` line:
```python
from wire_xgb_models import apply_xgb_blend
model_prob = apply_xgb_blend(model_prob, prop)
```

That is the entire change. The function:
- K props: blends 80% formula + 20% XGBoost (conservative, safe to deploy now)
- Hit props: blends 70% formula + 30% XGBoost
- Everything else: passes through unchanged
- Models not loaded: passes through unchanged (safe fallback)

**Verification:** After deployment, check logs for:
```
[XGBWire] K-prop line=5.5 | formula=58.5% | xgb=61.2% | blend=59.1%
```
If you never see this line, the wiring didn't take effect.

---

### 2. Injury Block (wire_model_layers.py) — HIGH PRIORITY

Props for IL players are currently being evaluated and potentially fired.
An IL-15 pitcher who somehow appears in a prop feed will generate a bet.

**Step 1:** Copy `wire_model_layers.py` to the repo root.

**Step 2:** Verify it works:
```bash
python wire_model_layers.py --test
```
Expected: "✅ All layer tests passed."

**Step 3:** In prop_enrichment_layer.py, find the top of the per-prop loop:
```python
for prop in raw_props:
    player = prop.get("player", "")
    # ... enrichment code ...
```

Add at the very top of the loop (before any enrichment):
```python
from wire_model_layers import check_injury_block
block = check_injury_block(player, hub)
if block["should_kill"]:
    logger.info("[Enrichment] Skipping %s — %s", player, block["status"])
    continue
# Reduce confidence for DTD/QUESTIONABLE
prop["_injury_confidence_penalty"] = block["confidence_penalty"]
```

Then in the confidence calculation (wherever `confidence` is computed):
```python
confidence *= (1.0 - prop.get("_injury_confidence_penalty", 0.0))
```

**Note:** For this to work, `hub["injuries"]` must be populated by
injury_layer.py in the DataHub refresh cycle. Verify by checking whether
`DataHubTasklet` calls `injury_layer`. If not, add to the DataHub refresh:
```python
try:
    from injury_layer import fetch_injury_data
    hub["injuries"] = fetch_injury_data()
except Exception:
    hub["injuries"] = []
    logger.warning("[DataHub] Injury data fetch failed — using empty list")
```

---

### 3. PA Model Matchup Rate (wire_model_layers.py) — MEDIUM PRIORITY

The lineup K-rate used in the Poisson lambda calculation currently defaults
to league average (0.228) for most props. pa_model.py has the correct
odds-ratio math but is never called.

**Integration point:** In prop_enrichment_layer.py, find where
`opp_lineup_k_pct` or `opp_k_rate` is set. It probably looks like:
```python
opp_k_rate = prop.get("opp_k_pct", 0.228)  # flat league avg fallback
```

Replace with:
```python
from wire_model_layers import compute_matchup_k_rate
batter_profiles = hub.get("context", {}).get("lineups", [])
pitcher_profile = {"k_rate": prop.get("k_rate") or prop.get("season_k9", 0) / 27}
opp_k_rate = compute_matchup_k_rate(batter_profiles, pitcher_profile)
prop["opp_lineup_k_pct"] = opp_k_rate
```

This uses the Bill James multiplicative odds-ratio instead of a flat average.
For a strong K pitcher (32% K-rate) vs a weak lineup (26% K-rate), this
produces ~0.287 instead of 0.228 — a meaningful difference at the margins.

---

### 4. Bernoulli Drama Penalty (wire_model_layers.py) — MEDIUM PRIORITY

High-Drama pitchers have volatile per-start outcomes even when their season
K-rate looks good. The penalty is -5pp for Drama > 65%, -3pp for > 50%, etc.

**Integration point:** In the strikeout prop evaluation section:
```python
from wire_model_layers import get_bernoulli_drama_penalty

if prop.get("prop_type") == "strikeouts":
    pitcher_stats = {
        "season_ip":   prop.get("avg_ip", 0) * prop.get("recent_start_count", 0),
        "season_runs": prop.get("season_er", 0),
    }
    drama_pp = get_bernoulli_drama_penalty(pitcher_name, pitcher_stats)
    if drama_pp < 0:
        model_prob = model_prob + (drama_pp / 100)
        model_prob = max(0.03, model_prob)
        prop["_bernoulli_drama_adj"] = drama_pp
```

Note: The Drama penalty is applied in addition to the correlated adjustment
dampener (fix1). The dampener handles stacking of multiple signals; the Drama
penalty is a standalone signal about pitcher variance mode.

---

### 5. BVI Adjustment (wire_model_layers.py) — LOWER PRIORITY

BVI data must be in hub["physics"]["bvi"] for this to work. Verify first:
```python
# In a debug session or log:
print(hub.get("physics", {}).get("bvi", "NOT_POPULATED"))
```

If NOT_POPULATED, BVI output is not being saved to hub. Add to DataHubTasklet:
```python
try:
    from bvi_layer import compute_all_team_bvi
    hub["physics"]["bvi"] = compute_all_team_bvi()
    logger.info("[DataHub] BVI computed for %d teams", len(hub["physics"]["bvi"]))
except Exception as exc:
    hub["physics"]["bvi"] = {}
    logger.warning("[DataHub] BVI computation failed: %s", exc)
```

Once populated, add to pitcher prop evaluation:
```python
from wire_model_layers import apply_bvi_adjustment
model_prob = apply_bvi_adjustment(model_prob, prop, hub)
```

---

### 6. Unified Park Factors (wire_park_and_temperature.py) — LOWER PRIORITY

Replace both `park_factors.get_park_factor()` and `park_k_factors.get_park_k_mult()`
calls with the unified function:

```python
# BEFORE (two separate imports, conflicting data):
from park_factors import get_park_factor
from park_k_factors import get_park_k_mult

pf = get_park_factor(venue, prop_type)
k_mult, _ = get_park_k_mult(team)

# AFTER (single authoritative source):
from wire_park_and_temperature import get_park_mult, is_dome, get_altitude

pf = get_park_mult(venue, prop_type)
# Also available:
dome_flag = is_dome(venue)
altitude = get_altitude(venue)
```

Verify the data is correct first:
```bash
python wire_park_and_temperature.py --show-parks
python wire_park_and_temperature.py --test
```

---

### 7. Temperature Column Audit (wire_park_and_temperature.py)

Run this with your DATABASE_URL set to check whether temperature calibration
is actually working:
```bash
DATABASE_URL=postgresql://... python wire_park_and_temperature.py --audit-temp
```

Expected output if working: temperatures differ from 1.0 per agent
Expected output if broken: "BROKEN — no calibration data, temperatures stuck at 1.0"

If broken, the fix is adding temperature_calibration.run() to the nightly
grading cycle (the audit output shows the exact code to add).

---

## TESTING CHECKLIST

Run these in order after each integration:

```bash
# 1. XGBoost wiring
python wire_xgb_models.py --test
python wire_xgb_models.py --check

# 2. Model layers
python wire_model_layers.py --test

# 3. Park factors
python wire_park_and_temperature.py --test
python wire_park_and_temperature.py --show-parks

# 4. Temperature audit (needs DATABASE_URL)
python wire_park_and_temperature.py --audit-temp
```

All should exit without errors before deploying.

---

## POST-DEPLOYMENT VERIFICATION

After each fix is live, look for these log patterns to confirm it's working:

| Fix | Expected log line |
|-----|------------------|
| XGBoost K | `[XGBWire] K-prop line=5.5 \| formula=58.5% \| xgb=61.2% \| blend=59.1%` |
| XGBoost Hit | `[XGBWire] Hit-prop \| formula=55.0% \| xgb=58.3% \| blend=55.9%` |
| Injury block | `[InjuryWire] KILL PlayerName — status=IL-15 (elbow)` |
| BVI | `[BVIWire] NYY BVI=0.72 → -2.0pp on pitching_outs prop` |
| Bernoulli Drama | `[BernoulliWire] PitcherName Drama=55.0% → penalty -3.0pp` |
| Park factor | No log (silent lookup) — check that Coors K props are being adjusted |

If you see NONE of these after 5+ prop evaluations, the wiring didn't apply
and the function is being called on a code path that doesn't reach the hook.

---

## WHAT DOESN'T NEED CODE (Just Configuration)

Two issues can be fixed without code changes:

**AgentTasklet cutoff window:** Change `agent_config.yaml`:
```yaml
agents:
  cutoff_minutes_before_pitch: 30   # current — too tight
  # Change to:
  cutoff_minutes_before_pitch: 45   # gives 15 min buffer for late data
```

**simulate_prop() silent fallback:** Add this logging to tasklets.py
to know when simulation falls back:
```python
if not _SIM_ENGINE_AVAILABLE:
    logger.warning("[TaskletStartup] simulation_engine not available — using Poisson-only model")
```
This makes the fallback visible instead of silent.
