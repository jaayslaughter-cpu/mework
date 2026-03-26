"""
Phase 43 — Two targeted additions to live_dispatcher.py.

======================================================================
ADDITION 1: Near the top of the file, after the season_record imports
======================================================================

# Phase 43: Per-agent dynamic unit sizing
try:
    from agent_unit_sizing import get_all_units as _get_all_units
    _UNIT_SIZING_AVAILABLE = True
except ImportError:
    _UNIT_SIZING_AVAILABLE = False
    def _get_all_units() -> dict:  # noqa: E302
        return {}

======================================================================
ADDITION 2: Inside LiveDispatcher.run(), near the top of the method,
after the games/props fetch — before the candidate parlays loop.
Add this block so unit sizes are available for the sending loop:
======================================================================

        # Phase 43: Load per-agent unit sizes (tier ladder $5→$8→$12→$16→$20)
        _agent_unit_map: dict[str, float] = {}
        try:
            _agent_unit_map = _get_all_units()
            logger.info(
                "[Phase43] Unit sizes loaded for %d agents", len(_agent_unit_map)
            )
        except Exception as _unit_load_err:
            logger.warning("[Phase43] Unit size load failed: %s — defaulting to $5", _unit_load_err)

======================================================================
ADDITION 3: In the sending loop (around line 840 in live_dispatcher.py),
replace the hard-coded `20.0` stake with the per-agent unit size.

FIND:
                if _risk_manager and not self.dry_run:
                    _risk_manager.record_stake(agent_name, 20.0)

REPLACE WITH:
                # Phase 43: use dynamic unit size from tier ladder
                _unit_stake = _agent_unit_map.get(agent_name, 5.0)
                if _risk_manager and not self.dry_run:
                    _risk_manager.record_stake(agent_name, _unit_stake)

======================================================================
ADDITION 4: In the same sending loop, pass stake to record_parlay().

FIND:
                record_parlay(
                    date=date,
                    agent=agent_name,
                    num_legs=n,
                    confidence=conf,
                    ev_pct=ev,
                    legs=[...]
                )

REPLACE WITH (add stake= kwarg):
                record_parlay(
                    date=date,
                    agent=agent_name,
                    num_legs=n,
                    confidence=conf,
                    ev_pct=ev,
                    stake=_agent_unit_map.get(agent_name, 5.0),   # Phase 43
                    legs=[...]
                )

======================================================================
ADDITION 5: In Discord alert call, pass unit size so it shows in the embed.

FIND:
                discord_alert.send_parlay_alert(parlay)

REPLACE WITH:
                parlay["unit_dollars"] = _agent_unit_map.get(agent_name, 5.0)  # Phase 43
                discord_alert.send_parlay_alert(parlay)
======================================================================
"""
