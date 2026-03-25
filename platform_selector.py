"""
platform_selector.py
====================
Platform selection utilities for PropIQ live dispatcher.

SelectionResult is a lightweight dataclass representing a single
evaluated prop leg.  PlatformSelector is a no-op coordinator —
the actual platform selection logic runs inline in
LiveDispatcher._evaluate_props() to avoid redundant API calls.

This module exists to satisfy the import in live_dispatcher.py and
to define the SelectionResult dataclass used by agent filter lambdas.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SelectionResult:
    """Represents a single evaluated prop leg after platform selection."""

    player_name:      str   = ""
    prop_type:        str   = ""
    side:             str   = ""        # "Over" | "Under"
    line:             float = 0.0
    platform:         str   = ""        # "PrizePicks" | "Underdog"
    implied_prob:     float = 0.0       # estimated win probability 0-1
    entry_type:       str   = "FLEX"    # "FLEX" | "STANDARD"
    fantasy_pts_edge: float = 0.0       # fantasy-score EV edge


class PlatformSelector:
    """
    No-op platform selector coordinator.

    The platform selection logic is implemented inline in
    LiveDispatcher._evaluate_props() for efficiency (avoiding a
    second pass over already-fetched raw props).  This class
    satisfies the import without duplicating work.
    """

    @staticmethod
    def select(*args, **kwargs) -> SelectionResult | None:  # noqa: D102
        return None
