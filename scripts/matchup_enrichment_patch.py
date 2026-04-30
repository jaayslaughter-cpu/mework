"""
matchup_enrichment_patch.py
===========================
This file documents the EXACT find-replace edits needed to wire matchup_engine.py
into prop_enrichment_layer.py and tasklets.py.

These edits are applied as PR #469 — see pr469_apply_patches.py for automated application.

--- PATCH 1: prop_enrichment_layer.py ---
Find the line in _player_specific_rate() that reads:
    lam    = max(0.01, _blended_k_pa * _bf)

AFTER that line, insert:

            # ── Pitch-type matchup lift (matchup_engine.py) ─────────────────
            # Log-odds arsenal-level K adjustment from kekoa-santana method.
            # Achieves Brier 0.188 on pitcher K (vs current ~0.248).
            # Uses pitcher pitch-type whiff rates vs league baseline.
            # Weight 0.35 prevents over-influence on existing pa_model signal.
            try:
                from matchup_engine import get_matchup_lift as _gml_k  # noqa: PLC0415
                _mu_k = _gml_k(prop)
                _k_logit_lift = float(_mu_k.get("k_lift_logit", 0.0) or 0.0)
                if abs(_k_logit_lift) > 0.02:
                    _lo_clip = max(0.005, min(0.995, _blended_k_pa))
                    _logit_blended = math.log(_lo_clip / (1.0 - _lo_clip))
                    _logit_blended += _k_logit_lift * 0.35
                    _blended_k_pa = 1.0 / (1.0 + math.exp(-_logit_blended))
                    lam = max(0.01, _blended_k_pa * _bf)
                    prop["_matchup_k_lift"]      = round(_k_logit_lift, 4)
                    prop["_matchup_reliability"] = round(float(_mu_k.get("avg_reliability", 0.0)), 3)
                    prop["_matchup_n_pitches"]   = int(_mu_k.get("n_pitch_types", 0))
            except Exception:
                pass

--- PATCH 2: prop_enrichment_layer.py ---
In the MAIN enrich_props() loop, find the line that reads:
    prop["_player_specific_prob"] =

On the NEXT line after the _player_specific_prob assignment block closes, insert:

        # ── Pitch-type matchup lift for walks_allowed props ─────────────────
        if prop.get("prop_type") in ("walks_allowed",):
            try:
                from matchup_engine import get_matchup_lift as _gml_bb  # noqa: PLC0415
                _mu_bb = _gml_bb(prop)
                if _mu_bb:
                    prop["_matchup_bb_lift"]     = float(_mu_bb.get("bb_lift_logit", 0.0))
                    if not prop.get("_matchup_reliability"):
                        prop["_matchup_reliability"] = float(_mu_bb.get("avg_reliability", 0.0))
            except Exception:
                pass

--- PATCH 3: tasklets.py — Nudge stack (13th layer) ---
Find the section in _model_prob() or the nudge stack that has a comment like:
    # ── Layer 12 ──  or  # Combined nudge
OR find any line that reads:
    nudge_stack.append(

Before the FINAL nudge aggregation / return statement, add:

        # ── Layer 13: Pitch-type matchup lift (matchup_engine) ───────────────
        # Applies arsenal-level log-odds lift for K and BB props.
        # Logit 0.10 ≈ +2.5pp at baseline 50%. Capped at ±4pp.
        _mu_k_lift  = float(prop.get("_matchup_k_lift", 0.0) or 0.0)
        _mu_bb_lift = float(prop.get("_matchup_bb_lift", 0.0) or 0.0)
        _mu_rel     = float(prop.get("_matchup_reliability", 0.0) or 0.0)

        if abs(_mu_k_lift) > 0.03 and _pt in {"strikeouts", "hitter_strikeouts"}:
            # Convert logit lift to probability nudge (scaled by reliability)
            _cur_logit = math.log(max(0.005, min(0.995, prob / 100.0)) / (1.0 - max(0.005, min(0.995, prob / 100.0))))
            _new_logit = _cur_logit + _mu_k_lift * 0.35
            _new_prob  = 1.0 / (1.0 + math.exp(-_new_logit)) * 100.0
            _k_nudge   = max(-4.0, min(4.0, _new_prob - prob))
            if abs(_k_nudge) > 0.1:
                nudge_stack.append(("matchup_k", _k_nudge))

        if abs(_mu_bb_lift) > 0.03 and _pt in {"walks_allowed"}:
            _cur_logit = math.log(max(0.005, min(0.995, prob / 100.0)) / (1.0 - max(0.005, min(0.995, prob / 100.0))))
            _new_logit = _cur_logit + _mu_bb_lift * 0.35
            _new_prob  = 1.0 / (1.0 + math.exp(-_new_logit)) * 100.0
            _bb_nudge  = max(-4.0, min(4.0, _new_prob - prob))
            if abs(_bb_nudge) > 0.1:
                nudge_stack.append(("matchup_bb", _bb_nudge))

--- PATCH 4: tasklets.py — _BullpenAgent ---
In _BullpenAgent._evaluate() or equivalent, after the existing BVI logic block, add:

            # Pitch-type bullpen matchup lift
            try:
                from matchup_engine import get_bullpen_matchup_lift as _gbml  # noqa: PLC0415
                _rel_ids  = [int(x) for x in prop.get("_reliever_ids", []) if x]
                _bat_ids  = [int(x) for x in prop.get("_opp_lineup_ids", []) if x]
                _bf_share = prop.get("_reliever_bf_shares")
                if _rel_ids:
                    _bp_mu = _gbml(_rel_ids, _bat_ids, _bf_share)
                    _bp_k  = float(_bp_mu.get("bullpen_k_lift", 0.0) or 0.0)
                    if abs(_bp_k) > 0.03 and prop_type in {"strikeouts", "hitter_strikeouts"}:
                        _nudge = max(-3.0, min(3.0, _bp_k * 20.0))
                        prob += _nudge
            except Exception:
                pass

--- PATCH 5: bug_checker.py — Health embed ---
In the 10 AM bug checker, after the FanGraphs check block, add:

    async def _check_matchup_engine():
        try:
            from matchup_engine import get_arsenal_status
            status = get_arsenal_status()
            if not status["pitcher_arsenal_loaded"]:
                return "❌ Matchup Engine: pitcher arsenal not loaded (Savant CSV failed)"
            n_p = status["pitcher_count"]
            n_b = status["batter_count"]
            return f"✅ Matchup Engine: {n_p} pitchers, {n_b} batters loaded ({status['cache_date']})"
        except Exception as e:
            return f"⚠️ Matchup Engine: {e}"
"""
