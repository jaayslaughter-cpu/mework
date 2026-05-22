#!/usr/bin/env python3
"""
PR #591 patch script — apply to tasklets.py
Run: python apply_patch_pr591.py

Fixes:
  1. _EVHunter.evaluate() — pick'em EV uses even-money profit (1.0) not vig (-115)
  2. _UnderMachine.evaluate() — same fix
  3. Outer agent loop — bypass sportsbook sharp gate for UD/PP props
"""
import pathlib

PATH = pathlib.Path("tasklets.py")
assert PATH.exists(), "Run from repo root!"
code = PATH.read_text()

# ── Fix 1: EVHunter pick'em EV ────────────────────────────────────────────
old1 = '''            if _ODDS_MATH_AVAILABLE:
                from odds_math import american_to_decimal as _a2d  # noqa: PLC0415
                profit    = _a2d(odds) - 1.0
                ev_pct    = _true_odds_ev(stake=1.0, profit=profit, prob=model_p / 100)
            else:
                ev_pct = (model_p / 100 - implied) / implied'''

new1 = '''            _is_pickem_ev = prop.get("platform", "").lower() in ("underdog", "prizepicks")
            if _ODDS_MATH_AVAILABLE:
                from odds_math import american_to_decimal as _a2d  # noqa: PLC0415
                profit    = 1.0 if _is_pickem_ev else (_a2d(odds) - 1.0)
                ev_pct    = _true_odds_ev(stake=1.0, profit=profit, prob=model_p / 100)
            else:
                ev_pct = (model_p / 100 - 0.5) if _is_pickem_ev else (model_p / 100 - implied) / implied'''

assert old1 in code, "Fix 1 pattern not found!"
code = code.replace(old1, new1, 1)
print("Fix 1 applied: EVHunter pick'em EV")

# ── Fix 2: UnderMachine pick'em EV ───────────────────────────────────────
old2 = '''        if _ODDS_MATH_AVAILABLE:
            from odds_math import american_to_decimal as _a2d  # noqa: PLC0415
            profit = _a2d(under_odds) - 1.0
            ev_pct = _true_odds_ev(stake=1.0, profit=profit, prob=model_prob / 100)
        else:
            ev_pct = (model_prob / 100 - implied) / implied

        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):'''

new2 = '''        _is_pickem_um = prop.get("platform", "").lower() in ("underdog", "prizepicks")
        if _ODDS_MATH_AVAILABLE:
            from odds_math import american_to_decimal as _a2d  # noqa: PLC0415
            profit = 1.0 if _is_pickem_um else (_a2d(under_odds) - 1.0)
            ev_pct = _true_odds_ev(stake=1.0, profit=profit, prob=model_prob / 100)
        else:
            ev_pct = (model_prob / 100 - 0.5) if _is_pickem_um else (model_prob / 100 - implied) / implied

        if ev_pct >= _get_ev_threshold(prop.get("_sim_edge_reasons", [])):'''

assert old2 in code, "Fix 2 pattern not found!"
code = code.replace(old2, new2, 1)
print("Fix 2 applied: UnderMachine pick'em EV")

# ── Fix 3: Outer loop — bypass sharp gate for pick'em ────────────────────
old3 = '''                sharp_prob = _get_sharp_consensus(hub, player, prop_type)
                if sharp_prob is None:
                    # No sharp book data for this specific player/prop.
                    # The reference chain (OddsAPI \u2192 PropOdds \u2192 Pinnacle \u2192 Covers \u2192
                    # DraftEdge \u2192 ActionNetwork \u2192 TheRundown) already ran \u2014 if it's
                    # still None this prop genuinely has no coverage today.
                    logger.debug(
                        "[AgentTasklet] %s %s %s \u2014 no sharp consensus data, skipping",
                        agent.name, player, prop_type,
                    )
                    _rj_no_sharp += 1
                    continue

                side    = bet["side"]
                ud_odds = (prop.get("over_american", -120)
                           if side == "OVER"
                           else prop.get("under_american", -120))
                edge = _underdog_edge(ud_odds, sharp_prob)
                if edge < MIN_EV_THRESH * 100:
                    _rj_ev_low += 1
                    continue

                # WagerBrain: also compute dollar EV for logging
                if _ODDS_MATH_AVAILABLE:
                    dollar_ev = _prop_ev_dollar(
                        model_prob=sharp_prob / 100,
                        odds_american=ud_odds,
                    )
                    bet["dollar_ev"] = round(dollar_ev, 4)

                bet["ev_pct"]          = round(edge, 2)
                bet["model_prob"]      = round(sharp_prob, 1)
                bet["sharp_consensus"] = True
                bet["underdog_line"]   = prop.get("underdog_line",
                                                    prop.get("over_american", -120))
                agent_hits.append(bet)'''

new3 = '''                _prop_platform   = prop.get("platform", "").lower()
                _is_pickem_outer = _prop_platform in ("underdog", "prizepicks")

                if _is_pickem_outer:
                    # Pick'em (UD/PP): bypass sportsbook sharp gate entirely.
                    # EV already computed at even-money pricing in evaluate().
                    # sharp_prob gate is meaningless for fixed-payout platforms.
                    _pickem_ev = float(bet.get("ev_pct", 0))
                    if _pickem_ev < MIN_EV_THRESH * 100:
                        _rj_ev_low += 1
                        continue
                    bet["sharp_consensus"] = False
                    agent_hits.append(bet)
                else:
                    sharp_prob = _get_sharp_consensus(hub, player, prop_type)
                    if sharp_prob is None:
                        # No sharp book data for this specific player/prop.
                        # The reference chain (OddsAPI \u2192 PropOdds \u2192 Pinnacle \u2192 Covers \u2192
                        # DraftEdge \u2192 ActionNetwork \u2192 TheRundown) already ran \u2014 if it's
                        # still None this prop genuinely has no coverage today.
                        logger.debug(
                            "[AgentTasklet] %s %s %s \u2014 no sharp consensus data, skipping",
                            agent.name, player, prop_type,
                        )
                        _rj_no_sharp += 1
                        continue

                    side    = bet["side"]
                    ud_odds = (prop.get("over_american", -120)
                               if side == "OVER"
                               else prop.get("under_american", -120))
                    edge = _underdog_edge(ud_odds, sharp_prob)
                    if edge < MIN_EV_THRESH * 100:
                        _rj_ev_low += 1
                        continue

                    # WagerBrain: also compute dollar EV for logging
                    if _ODDS_MATH_AVAILABLE:
                        dollar_ev = _prop_ev_dollar(
                            model_prob=sharp_prob / 100,
                            odds_american=ud_odds,
                        )
                        bet["dollar_ev"] = round(dollar_ev, 4)

                    bet["ev_pct"]          = round(edge, 2)
                    bet["model_prob"]      = round(sharp_prob, 1)
                    bet["sharp_consensus"] = True
                    bet["underdog_line"]   = prop.get("underdog_line",
                                                        prop.get("over_american", -120))
                    agent_hits.append(bet)'''

assert old3 in code, "Fix 3 pattern not found!"
code = code.replace(old3, new3, 1)
print("Fix 3 applied: Outer loop pick'em bypass")

PATH.write_text(code)
print("\n✅ All 3 fixes applied to tasklets.py")
print("Lines added:", code.count('\n') - PATH.read_text().count('\n') + code.count('\n'))
