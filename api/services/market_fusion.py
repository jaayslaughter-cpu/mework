"""
api/services/market_fusion.py
Bridges the multi-provider OddsFetcher output into the PropIQ agent pipeline.
Converts MergedOdds → PropEdge enriched with CLV metadata.
Feeds EVHunter, ArbitrageAgent, and LineValueAgent directly.

PEP 8 compliant.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict
from typing import Any

from api.services.odds_fetcher import MergedOdds, OddsFetcher

logger = logging.getLogger(__name__)

# Minimum CLV threshold for promotion into the agent pipeline
CLV_GATE_PCT  = float(0.02)    # 2%
MIN_PROVIDERS = 2              # Must be seen on ≥2 providers to be valid


def _merged_to_prop_edge(m: MergedOdds, model_prob: float | None = None) -> dict[str, Any]:
    """
    Convert a MergedOdds object to a PropEdge-compatible dict.
    model_prob can be injected later by the ML pipeline; defaults to consensus.
    """
    return {
        "player_name":         m.player_name,
        "prop_type":           m.prop_type,
        "line":                m.line,
        "model_probability":   model_prob if model_prob is not None else m.consensus_prob_over,
        "edge_pct":            m.clv_edge_pct,
        "source":              "market_fusion",
        "timestamp":           time.time(),
        "odds_over":           m.best_odds_over,
        "odds_under":          m.best_odds_under,
        "game_id":             m.game_id,
        "commence_time":       m.commence_time,
        # CLV-specific fields
        "consensus_prob_over":  m.consensus_prob_over,
        "consensus_prob_under": m.consensus_prob_under,
        "clv_edge_pct":         m.clv_edge_pct,
        "best_over_provider":   m.best_over_provider,
        "best_under_provider":  m.best_under_provider,
        "providers_sampled":    m.providers_sampled,
        # Defaults for required PropEdge fields (enriched downstream)
        "player_id":           "",
        "umpire_cs_pct":       0.0,
        "ticket_pct":          0.0,
        "money_pct":           0.0,
        "fatigue_index":       0.0,
        "wind_speed":          0.0,
        "wind_direction":      "N",
        "steam_velocity":      0.0,
        "steam_book_count":    len(m.providers_sampled),
    }


class MarketFusionEngine:
    """
    Pulls multi-provider odds, applies CLV gate, and emits PropEdge dicts
    ready for the 15-agent execution squad.

    Usage:
        engine = MarketFusionEngine()
        prop_edges = engine.run()
        # Pass prop_edges to ExecutionSquad or EVHunter / LineValueAgent
    """

    def __init__(
        self,
        clv_gate: float = CLV_GATE_PCT,
        min_providers: int = MIN_PROVIDERS,
    ) -> None:
        self._fetcher      = OddsFetcher()
        self._clv_gate     = clv_gate
        self._min_providers = min_providers

    def run(self, n_top: int = 50) -> list[dict[str, Any]]:
        """
        Full pipeline:
        1. Fetch all provider lines
        2. Merge + consensus
        3. Filter by CLV gate + min-provider count
        4. Convert to PropEdge dicts

        Returns list of PropEdge-compatible dicts sorted by CLV edge descending.
        """
        logger.info("[MarketFusion] Starting multi-provider odds pull...")
        raw    = self._fetcher.fetch_all()
        merged = self._fetcher.merge_odds(raw)

        # Quality filter
        qualified = [
            m for m in merged
            if m.clv_edge_pct >= self._clv_gate
            and len(m.providers_sampled) >= self._min_providers
        ]
        logger.info(
            "[MarketFusion] %d/%d props passed CLV gate (≥%.1f%%) + provider count (≥%d)",
            len(qualified), len(merged),
            self._clv_gate * 100, self._min_providers,
        )

        prop_edges = [_merged_to_prop_edge(m) for m in qualified[:n_top]]
        return prop_edges

    def arbitrage_scan(self) -> list[dict[str, Any]]:
        """
        Find true arbitrage: over on provider A + under on provider B both +EV.
        Returns props where (best_over_implied + best_under_implied) < 1.0.
        """
        raw    = self._fetcher.fetch_all()
        merged = self._fetcher.merge_odds(raw)

        arb_props: list[dict[str, Any]] = []
        for m in merged:
            from api.services.odds_fetcher import _american_to_implied
            over_implied  = _american_to_implied(m.best_odds_over)
            under_implied = _american_to_implied(m.best_odds_under)
            total_implied = over_implied + under_implied
            if total_implied < 1.0:
                pe = _merged_to_prop_edge(m)
                pe["arb_margin"]      = round(1.0 - total_implied, 4)
                pe["source"]          = "arbitrage"
                arb_props.append(pe)

        arb_props.sort(key=lambda x: x["arb_margin"], reverse=True)
        logger.info("[MarketFusion] Found %d arbitrage opportunities", len(arb_props))
        return arb_props
