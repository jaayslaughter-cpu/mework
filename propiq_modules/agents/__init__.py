"""
PropIQ Agent Army — 10 competing agents.

Original 7 (fixed — no deviations):
  1. EVHunterAgent   — +EV props (1-3 legs, EV > 5%)
  2. UnderMachineAgent — ERA < 3.50 pitcher duels (1-3 legs)
  3. ThreeLegAgent   — Correlated player props (exactly 3 legs)
  4. ParlayAgent     — Game outcomes (2-4 legs)
  5. LiveAgent       — In-play line movement > 5% (1 leg)
  6. ArbAgent        — Cross-book arbitrage > 1% (2 legs)
  7. GradingAgent    — Boxscore settlement (0 legs)

New specialists (3 added):
  8. UmpireAgent     — K props when ump K% > 22% + FIP < 3.80
  9. F5Agent         — First 5 innings unders: FIP < 3.50 + SwStr > 12%
  10. FadeAgent      — Public >70% → opposite side (RLM/sharp money)
"""

from .base_agent import BaseAgent, BetRecommendation
from .ev_hunter import EVHunterAgent
from .under_machine import UnderMachineAgent
from .three_leg import ThreeLegAgent
from .parlay_agent import ParlayAgent
from .live_agent import LiveAgent
from .arb_agent import ArbAgent
from .grading_agent import GradingAgent
from .umpire_agent import UmpireAgent
from .f5_agent import F5Agent
from .fade_agent import FadeAgent

ALL_AGENTS = [
    EVHunterAgent,
    UnderMachineAgent,
    ThreeLegAgent,
    ParlayAgent,
    LiveAgent,
    ArbAgent,
    GradingAgent,
    UmpireAgent,
    F5Agent,
    FadeAgent,
]

AGENT_NAMES = [a.name for a in ALL_AGENTS]

__all__ = [
    "BaseAgent", "BetRecommendation",
    "EVHunterAgent", "UnderMachineAgent", "ThreeLegAgent",
    "ParlayAgent", "LiveAgent", "ArbAgent", "GradingAgent",
    "UmpireAgent", "F5Agent", "FadeAgent",
    "ALL_AGENTS", "AGENT_NAMES",
]
