"""
api/services/market_fusion.py

Bridges the multi-provider OddsFetcher output into the PropIQ agent pipeline.

Uses fetch_aggregated_odds() to get three pre-segmented opportunity buckets:
    top_clv       → EVHunter, LineValueAgent
    arbitrage     → ArbitrageAgent
    dislocations  → EVHunter (CLV enrichment), SteamAgent

Detects Pinnacle/Circa/CRIS vs. soft-book price dislocations and scores each
PropEdge with a dislocation_score for downstream agent weighting.

PEP 8 compliant.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from api.services.odds_fetcher import MergedOdds, OddsFetcher, _american_to_implied

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CLV_GATE_PCT        = 0.02    # 2 % minimum CLV edge
MIN_PROVIDERS       = 2       # Must appear on ≥2 providers
DISLOCATION_GATE    = 0.03    # 3 % sharp/soft probability gap

# Books treated as the "sharp" reference for dislocation scoring
_SHARP_BOOKS = frozenset({
    "Pinnacle", "Circa", "CRIS", "Bookmaker",
    "Heritage", "BetPhoenix", "5Dimes",
})


# ---------------------------------------------------------------------------
# PropEdge builder
# ---------------------------------------------------------------------------
def _merged_to_prop_edge(
    m: MergedOdds,
    model_prob: float | None = None,
    source_override: str | None = None,
    dislocation_score: float = 0.0,
) -> dict[str, Any]:
    """
    Convert a MergedOdds object to a PropEdge-compatible dict.

    Args:
        m:                  Source MergedOdds object.
        model_prob:         Optional ML-calibrated probability (overrides consensus).
        source_override:    Force ``source`` field (e.g. ``"arbitrage"``).
        dislocation_score:  Pinnacle/soft-book probability gap (0.0–1.0).

    Returns:
        PropEdge dict consumed by the 15-agent execution squad.
    """
    return {
        # Core prop fields
        "player_name":          m.player_name,
        "prop_type":            m.prop_type,
        "line":                 m.line,
        "model_probability":    model_prob if model_prob is not None else m.consensus_prob_over,
        "edge_pct":             m.clv_edge_pct,
        "source":               source_override or "market_fusion",
        "timestamp":            time.time(),
        "odds_over":            m.best_odds_over,
        "odds_under":           m.best_odds_under,
        "game_id":              m.game_id,
        "commence_time":        m.commence_time,
        # CLV-specific fields
        "consensus_prob_over":  m.consensus_prob_over,
        "consensus_prob_under": m.consensus_prob_under,
        "clv_edge_pct":         m.clv_edge_pct,
        "best_over_provider":   m.best_over_provider,
        "best_under_provider":  m.best_under_provider,
        "providers_sampled":    m.providers_sampled,
        # Dislocation metadata (Pinnacle vs. soft-book gap)
        "dislocation_score":    round(dislocation_score, 4),
        "is_sharp_dislocation": dislocation_score >= DISLOCATION_GATE,
        # Required PropEdge defaults (enriched by context modifiers downstream)
        "player_id":            "",
        "umpire_cs_pct":        0.0,
        "ticket_pct":           0.0,
        "money_pct":            0.0,
        "fatigue_index":        0.0,
        "wind_speed":           0.0,
        "wind_direction":       "N",
        "steam_velocity":       0.0,
        "steam_book_count":     len(m.providers_sampled),
    }


def _compute_dislocation_score(m: MergedOdds) -> float:
    """
    Compute the Pinnacle/soft-book no-vig probability gap for a MergedOdds.

    Walks raw_lines to find the sharpest book line and the best soft-book line.
    Returns the absolute probability difference.  0.0 if insufficient data.
    """
    sharp_lines = [
        ol for ol in m.raw_lines
        if any(tag in ol.provider for tag in _SHARP_BOOKS)
    ]
    soft_lines = [
        ol for ol in m.raw_lines
        if not any(tag in ol.provider for tag in _SHARP_BOOKS)
    ]
    if not sharp_lines or not soft_lines:
        return 0.0

    sharp_p_over, _ = _american_to_implied(sharp_lines[0].odds_over), None
    # Use the function directly instead of _strip_vig to keep it simple
    def _nv(ov: int, un: int) -> float:
        p_o = _american_to_implied(ov)
        p_u = _american_to_implied(un)
        total = p_o + p_u
        return p_o / total if total > 0 else 0.5

    sharp_p = _nv(sharp_lines[0].odds_over, sharp_lines[0].odds_under)
    soft_ps = [_nv(ol.odds_over, ol.odds_under) for ol in soft_lines]
    best_soft = max(soft_ps) if soft_ps else 0.0
    return abs(best_soft - sharp_p)


# ---------------------------------------------------------------------------
# MarketFusionEngine
# ---------------------------------------------------------------------------
class MarketFusionEngine:
    """
    Pulls multi-provider odds via fetch_aggregated_odds(), applies quality
    gates, and emits PropEdge dicts for each agent segment.

    Three output channels:
        run()              → CLV PropEdges for EVHunter / LineValueAgent
        arbitrage_scan()   → ArbitrageAgent PropEdges (total implied < 1.0)
        dislocation_scan() → Sharp/soft gap PropEdges for EVHunter enrichment

    Usage::

        engine = MarketFusionEngine()
        clv_edges = engine.run()                    # EVHunter feed
        arb_edges = engine.arbitrage_scan()         # ArbitrageAgent feed
        dis_edges = engine.dislocation_scan()       # CLV enrichment
    """

    def __init__(
        self,
        clv_gate: float = CLV_GATE_PCT,
        min_providers: int = MIN_PROVIDERS,
        dislocation_gate: float = DISLOCATION_GATE,
    ) -> None:
        self._fetcher          = OddsFetcher()
        self._clv_gate         = clv_gate
        self._min_providers    = min_providers
        self._dislocation_gate = dislocation_gate
        # Cache the last aggregated pull to avoid double-fetching in same cycle
        self._cache: dict[str, list] | None = None
        self._cache_ts: float = 0.0
        self._cache_ttl: float = 300.0   # 5-minute TTL

    # ------------------------------------------------------------------
    # Internal: cached aggregated pull
    # ------------------------------------------------------------------
    def _get_aggregated(self, n: int = 100) -> dict[str, list]:
        now = time.time()
        if self._cache and (now - self._cache_ts) < self._cache_ttl:
            return self._cache
        self._cache    = self._fetcher.fetch_aggregated_odds(
            n=n,
            min_clv_pct=self._clv_gate,
            min_dislocation_pct=self._dislocation_gate,
        )
        self._cache_ts = now
        return self._cache

    # ------------------------------------------------------------------
    # Internal: quality filter
    # ------------------------------------------------------------------
    def _qualify(self, merged_list: list[MergedOdds]) -> list[MergedOdds]:
        """Apply CLV gate + min-provider count filter."""
        return [
            m for m in merged_list
            if m.clv_edge_pct >= self._clv_gate
            and len(m.providers_sampled) >= self._min_providers
        ]

    # ------------------------------------------------------------------
    # run() — CLV PropEdges (EVHunter / LineValueAgent)
    # ------------------------------------------------------------------
    def run(self, n_top: int = 50) -> list[dict[str, Any]]:
        """
        Full pipeline: fetch_aggregated_odds → CLV gate → PropEdge dicts.

        Enriches each edge with a dislocation_score so EVHunter can weight
        props with confirmed Pinnacle/soft-book gaps more heavily.

        Args:
            n_top: Maximum number of PropEdge dicts to return.

        Returns:
            List of PropEdge dicts sorted by CLV edge descending.
        """
        logger.info("[MarketFusion] Starting multi-provider odds pull (CLV segment)...")
        agg       = self._get_aggregated(n=n_top * 2)
        qualified = self._qualify(agg["top_clv"])

        logger.info(
            "[MarketFusion] %d/%d CLV props passed gate (≥%.1f%%) + ≥%d providers",
            len(qualified), len(agg["top_clv"]),
            self._clv_gate * 100, self._min_providers,
        )

        prop_edges: list[dict[str, Any]] = []
        for m in qualified[:n_top]:
            dis_score = _compute_dislocation_score(m)
            prop_edges.append(_merged_to_prop_edge(m, dislocation_score=dis_score))

        return prop_edges

    # ------------------------------------------------------------------
    # arbitrage_scan() — ArbitrageAgent feed
    # ------------------------------------------------------------------
    def arbitrage_scan(self) -> list[dict[str, Any]]:
        """
        Surface true arbitrage opportunities (total implied < 1.0).

        Over on provider A + Under on provider B both priced in the backer's
        favour.  Returns PropEdges with ``source="arbitrage"`` and an
        ``arb_margin`` field showing the guaranteed profit percentage.

        Returns:
            List of arbitrage PropEdge dicts sorted by arb_margin descending.
        """
        agg      = self._get_aggregated()
        arb_list = agg.get("arbitrage", [])

        arb_edges: list[dict[str, Any]] = []
        for m in arb_list:
            over_impl  = _american_to_implied(m.best_odds_over)
            under_impl = _american_to_implied(m.best_odds_under)
            arb_margin = round(1.0 - (over_impl + under_impl), 4)
            pe = _merged_to_prop_edge(
                m,
                source_override="arbitrage",
                dislocation_score=_compute_dislocation_score(m),
            )
            pe["arb_margin"] = arb_margin
            arb_edges.append(pe)

        arb_edges.sort(key=lambda x: x["arb_margin"], reverse=True)
        logger.info("[MarketFusion] %d arbitrage opportunities found", len(arb_edges))
        return arb_edges

    # ------------------------------------------------------------------
    # dislocation_scan() — Pinnacle/soft-book gap edges
    # ------------------------------------------------------------------
    def dislocation_scan(
        self,
        min_gap: float = DISLOCATION_GATE,
    ) -> list[dict[str, Any]]:
        """
        Identify significant pricing dislocations between sharp books
        (Pinnacle, Circa, CRIS) and soft/recreational books.

        A large gap signals the soft book hasn't adjusted to the sharp
        consensus — these are the highest-confidence CLV edges.

        Args:
            min_gap: Minimum probability gap to surface (default 3 %).

        Returns:
            List of PropEdge dicts with ``source="dislocation"`` sorted by
            dislocation_score descending.
        """
        agg  = self._get_aggregated()
        diss = agg.get("dislocations", [])

        dis_edges: list[dict[str, Any]] = []
        for m in diss:
            score = _compute_dislocation_score(m)
            if score < min_gap:
                continue
            pe = _merged_to_prop_edge(
                m,
                source_override="dislocation",
                dislocation_score=score,
            )
            dis_edges.append(pe)

        dis_edges.sort(key=lambda x: x["dislocation_score"], reverse=True)
        logger.info(
            "[MarketFusion] %d dislocation edges (gap ≥ %.1f%%)",
            len(dis_edges), min_gap * 100,
        )
        return dis_edges
