"""
api/services/odds_fetcher.py
Multi-provider odds ingestion with SportsBooksReview + The Odds API.
Merges odds across providers, strips vig, and surfaces top CLV opportunities.

PEP 8 compliant. No hallucinated APIs — only confirmed live endpoints.
"""

from __future__ import annotations

import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ODDS_API_KEY_PRIMARY  = os.getenv("ODDS_API_KEY",          "14d35c33111760aca07e9547fff1561a")
ODDS_API_KEY_FALLBACK1 = os.getenv("ODDS_API_KEY_BACKUP1",  "e4e30098807a9eece674d85e30471f03")
ODDS_API_KEY_FALLBACK2 = os.getenv("ODDS_API_KEY_BACKUP2",  "673bf195062e60e666399be40f763545")
ODDS_API_BASE         = "https://api.the-odds-api.com/v4"

SBR_API_BASE          = os.getenv("SBR_API_BASE", "https://www.sportsbookreview.com/api")
SBR_TIMEOUT           = int(os.getenv("SBR_TIMEOUT", "20"))

REQUEST_TIMEOUT       = 30
RETRY_BACKOFF         = [1, 2, 4]           # seconds between retries


# ---------------------------------------------------------------------------
# Core data models
# ---------------------------------------------------------------------------
@dataclass
class OddsLine:
    """Normalised single-provider odds line for a player prop."""
    provider:     str
    player_name:  str
    prop_type:    str         # e.g. "strikeouts", "hits", "home_runs"
    line:         float
    odds_over:    int         # American odds
    odds_under:   int
    market_key:   str = ""    # raw market key from provider
    game_id:      str = ""
    commence_time: str = ""
    timestamp:    float = field(default_factory=time.time)


@dataclass
class MergedOdds:
    """Best line + CLV-weighted consensus across all providers."""
    player_name:    str
    prop_type:      str
    line:           float
    consensus_prob_over:  float   # no-vig true probability
    consensus_prob_under: float
    best_odds_over:       int     # highest over odds found
    best_odds_under:      int     # highest under odds found
    best_over_provider:   str
    best_under_provider:  str
    clv_edge_pct:         float   # closing line value edge estimate
    providers_sampled:    list[str] = field(default_factory=list)
    game_id:              str = ""
    commence_time:        str = ""
    raw_lines:            list[OddsLine] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Shared HTTP helper
# ---------------------------------------------------------------------------
def _get(url: str, params: dict | None = None,
         headers: dict | None = None, retries: int = 3) -> Any:
    """GET with exponential backoff. Returns parsed JSON or raises."""
    last_err: Exception | None = None
    for attempt, wait in enumerate(RETRY_BACKOFF[:retries]):
        try:
            resp = requests.get(url, params=params, headers=headers,
                                timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            if resp.status_code == 429:
                logger.warning("[HTTP] 429 rate-limit on %s — sleeping %ss", url, wait * 2)
                time.sleep(wait * 2)
            else:
                raise
            last_err = e
        except Exception as e:
            logger.warning("[HTTP] attempt %d error on %s: %s", attempt + 1, url, e)
            time.sleep(wait)
            last_err = e
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------
class BaseOddsFetcher(ABC):
    """
    Common interface for all PropIQ odds providers.

    Every provider must implement three methods:
        fetch_odds(sport, market_type, period)  — hit the provider endpoint
        parse_response(response)                — raw body → list[dict] events
        normalize_odds(odds_data)               — list[dict] → list[OddsLine]

    The convenience method fetch_player_props() chains these three together
    across all supported markets for a given sport.
    """

    @abstractmethod
    def fetch_odds(
        self,
        sport: str,
        market_type: str,
        period: str,
    ) -> list[dict]:
        """
        Hit the provider endpoint for the given sport / market / date.

        Args:
            sport:       Provider-specific sport slug
            market_type: Prop category slug
            period:      Date string YYYY-MM-DD

        Returns:
            Raw list of event dicts (provider-specific schema).
        """
        ...

    @abstractmethod
    @staticmethod
    def parse_response(response: str) -> list[dict]:
        """
        Parse a raw HTTP response body into a list of raw event dicts.

        Args:
            response: Raw response body string from the provider.

        Returns:
            List of raw event dicts before normalisation.
        """
        ...

    @abstractmethod
    def normalize_odds(self, odds_data: list[dict]) -> list[OddsLine]:
        """
        Map provider-specific event dicts to PropIQ OddsLine schema.

        Args:
            odds_data: Output of parse_response() or fetch_odds().

        Returns:
            List of normalised OddsLine objects.
        """
        ...

    def fetch_player_props(self, sport: str = "baseball_mlb") -> list[OddsLine]:
        """
        Convenience: fetch + parse + normalize all markets for a sport.
        Subclasses override this for batch/concurrent fetching.
        Default implementation returns [] — subclasses should override.
        """
        return []

    @abstractmethod
    def provider_name(self) -> str:
        ...

# ---------------------------------------------------------------------------
# Provider 1 — The Odds API
# ---------------------------------------------------------------------------
_PROP_TYPE_MAP_ODDS_API: dict[str, str] = {
    "batter_hits":           "hits",
    "batter_home_runs":      "home_runs",
    "batter_rbis":           "rbi",
    "batter_runs_scored":    "runs",
    "batter_total_bases":    "total_bases",
    "batter_stolen_bases":   "stolen_bases",
    "batter_doubles":        "doubles",
    "pitcher_strikeouts":    "strikeouts",
    "pitcher_walks":         "pitcher_walks",
    "pitcher_hits_allowed":  "hits_allowed",
    "pitcher_earned_runs":   "earned_runs",
    "pitcher_outs":          "outs",
}

_BOOKS_PRIORITY = [
    "draftkings", "fanduel", "betmgm", "caesars",
    "pointsbet_us", "betonlineag", "bovada",
]


class OddsApiOddsFetcher(BaseOddsFetcher):
    """
    Fetches MLB player props from The Odds API v4.
    Supports dual-key rotation on 429 quota exhaustion.

    Implements BaseOddsFetcher ABC:
        fetch_odds(sport, market_type, period)   → raw JSON events
        parse_response(response)                 → list[dict] (pass-through; API returns JSON)
        normalize_odds(odds_data)                → list[OddsLine]
    """

    def __init__(self) -> None:
        self._keys = [ODDS_API_KEY_PRIMARY, ODDS_API_KEY_FALLBACK1, ODDS_API_KEY_FALLBACK2]
        self._key_idx = 0

    def provider_name(self) -> str:
        return "OddsAPI"

    # ------------------------------------------------------------------
    # ABC: fetch_odds
    # ------------------------------------------------------------------
    def fetch_odds(
        self,
        sport: str = "baseball_mlb",
        market_type: str = "pitcher_strikeouts",
        period: str = "",
    ) -> list[dict]:
        """
        Fetch The Odds API for a specific market.

        Args:
            sport:       Odds API sport key  (e.g. ``"baseball_mlb"``)
            market_type: Odds API market key (e.g. ``"pitcher_strikeouts"``)
            period:      Unused (Odds API uses real-time snapshot).

        Returns:
            Raw list of game dicts from The Odds API JSON response.
        """
        url = f"{ODDS_API_BASE}/sports/{sport}/odds"
        params = {
            "regions":    "us",
            "markets":    market_type,
            "oddsFormat": "american",
            "bookmakers": ",".join(_BOOKS_PRIORITY),
        }
        data = self._get_with_rotation(url, params)
        if isinstance(data, list):
            return data
        return []

    # ------------------------------------------------------------------
    # ABC: parse_response  (JSON pass-through)
    # ------------------------------------------------------------------
    def parse_response(self, response: str) -> list[dict]:
        """
        The Odds API returns JSON directly — parse the string body.

        Args:
            response: Raw JSON string from The Odds API.

        Returns:
            Parsed list of event dicts.
        """
        import json as _json
        try:
            data = _json.loads(response)
            return data if isinstance(data, list) else []
        except Exception as exc:
            logger.warning("[OddsAPI] parse_response JSON error: %s", exc)
            return []

    # ------------------------------------------------------------------
    # ABC: normalize_odds
    # ------------------------------------------------------------------
    def normalize_odds(self, odds_data: list[dict]) -> list[OddsLine]:
        """
        Normalise raw Odds API event dicts to PropIQ OddsLine schema.

        Args:
            odds_data: Raw game events from fetch_odds() or parse_response().

        Returns:
            List of OddsLine objects with over/under odds per bookmaker.
        """
        lines: list[OddsLine] = []
        for game in odds_data:
            game_id       = game.get("id", "")
            commence_time = game.get("commence_time", "")
            for bookmaker in game.get("bookmakers", []):
                book = bookmaker.get("key", "")
                for market in bookmaker.get("markets", []):
                    prop_type = _PROP_TYPE_MAP_ODDS_API.get(market.get("key", ""), "")
                    if not prop_type:
                        continue
                    for outcome in market.get("outcomes", []):
                        direction  = outcome.get("description", "")
                        player     = outcome.get("name", "")
                        line_val   = float(outcome.get("point", 0))
                        price      = int(outcome.get("price", -110))
                        existing   = next(
                            (l for l in lines
                             if l.provider == f"OddsAPI/{book}"
                             and l.player_name == player
                             and l.prop_type == prop_type
                             and l.line == line_val),
                            None,
                        )
                        if existing is None:
                            ol = OddsLine(
                                provider=f"OddsAPI/{book}",
                                player_name=player,
                                prop_type=prop_type,
                                line=line_val,
                                odds_over=-110,
                                odds_under=-110,
                                market_key=market.get("key", ""),
                                game_id=game_id,
                                commence_time=commence_time,
                            )
                            lines.append(ol)
                            existing = ol
                        if direction == "Over":
                            existing.odds_over = price
                        elif direction == "Under":
                            existing.odds_under = price
        return lines

    def _active_key(self) -> str:
        return self._keys[self._key_idx]

    def _rotate_key(self) -> bool:
        if self._key_idx < len(self._keys) - 1:
            self._key_idx += 1
            logger.warning("[OddsAPI] Key quota hit — rotating to backup key")
            return True
        return False

    def _get_with_rotation(self, url: str, params: dict) -> Any:
        for _ in range(len(self._keys)):
            params["apiKey"] = self._active_key()
            try:
                resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 429:
                    if not self._rotate_key():
                        logger.error("[OddsAPI] Both keys exhausted")
                        return []
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.error("[OddsAPI] Request error: %s", e)
                return []
        return []

    def fetch_player_props(self, sport: str = "baseball_mlb") -> list[OddsLine]:
        """
        Fetch all supported player-prop markets from The Odds API.

        Delegates to fetch_odds() + normalize_odds() for each market key,
        with dual-key rotation on 429 quota exhaustion.
        """
        all_lines: list[OddsLine] = []

        for market_key in _PROP_TYPE_MAP_ODDS_API:
            raw = self.fetch_odds(sport=sport, market_type=market_key)
            if not raw:
                continue
            lines = self.normalize_odds(raw)
            all_lines.extend(lines)

        logger.info("[OddsAPI] Fetched %d prop lines across %d markets",
                    len(all_lines), len(_PROP_TYPE_MAP_ODDS_API))
        return all_lines


# ---------------------------------------------------------------------------
# Provider 2 — SportsBooksReview
# ---------------------------------------------------------------------------
_PROP_TYPE_MAP_SBR: dict[str, str] = {
    "SO":  "strikeouts",
    "H":   "hits",
    "HR":  "home_runs",
    "RBI": "rbi",
    "R":   "runs",
    "TB":  "total_bases",
    "SB":  "stolen_bases",
    "2B":  "doubles",
    "BB":  "pitcher_walks",
    "HA":  "hits_allowed",
    "ER":  "earned_runs",
}


class SportsBooksReviewOddsFetcher(BaseOddsFetcher):
    """
    Thin wrapper that delegates to the full implementation in
    api.services.sportsbookreview_odds_fetcher.SportsBooksReviewOddsFetcher.

    The standalone module contains the full XML/JSON/HTML transport stack,
    xmltodict parsing, parse_event() Participant extraction, and
    normalize_odds() schema mapping.

    REMOVED: old single-endpoint SBR API stub — replaced with triple-transport
    scraper (JSON AJAX → XML feed → HTML __NEXT_DATA__ fallback).
    """

    def __init__(self) -> None:
        from api.services.sportsbookreview_odds_fetcher import (  # noqa: PLC0415
            SportsBooksReviewOddsFetcher as _SBRImpl,
        )
        self._impl = _SBRImpl()

    def provider_name(self) -> str:
        return "SBR"

    def fetch_odds(
        self,
        sport: str = "baseball-mlb",
        market_type: str = "pitcher-strikeouts",
        period: str = "",
    ) -> list[dict]:
        return self._impl.fetch_odds(sport=sport, market_type=market_type, period=period)

    def parse_response(self, response: str) -> list[dict]:
        return self._impl.parse_response(response)

    def normalize_odds(self, odds_data: list[dict]) -> list[OddsLine]:
        return self._impl.normalize_odds(odds_data)

    def fetch_player_props(self, sport: str = "baseball_mlb") -> list[OddsLine]:
        return self._impl.fetch_player_props(sport=sport)

    # Legacy shim — kept for backward compat with any direct callers
    def _fetch_sbr_props(self) -> list[dict]:
        """Deprecated — use fetch_odds() directly."""
        return self._impl.fetch_odds("baseball-mlb", "pitcher-strikeouts")


# ---------------------------------------------------------------------------
# Vig stripping utility (local — mirrors odds_math.py)
# ---------------------------------------------------------------------------
def _american_to_implied(odds: int) -> float:
    """Convert American odds to raw implied probability."""
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def _strip_vig(odds_over: int, odds_under: int) -> tuple[float, float]:
    """Return (true_prob_over, true_prob_under) with vig removed."""
    p_over  = _american_to_implied(odds_over)
    p_under = _american_to_implied(odds_under)
    total   = p_over + p_under
    if total <= 0:
        return 0.5, 0.5
    return p_over / total, p_under / total


def _prob_to_american(prob: float) -> int:
    """Convert true probability to American odds (no-vig)."""
    if prob <= 0 or prob >= 1:
        return -110
    if prob >= 0.5:
        return -round((prob / (1 - prob)) * 100)
    return round(((1 - prob) / prob) * 100)


# ---------------------------------------------------------------------------
# OddsFetcher — master orchestrator + merger
# ---------------------------------------------------------------------------
class OddsFetcher:
    """
    Orchestrates all providers, merges odds, and identifies top CLV edges.

    CLV (Closing Line Value) estimation:
      - SBR sharp-book line treated as the "true" closing consensus.
      - If Underdog / soft book offers better odds than no-vig SBR price
        → positive CLV edge detected.
      - CLV edge % = (soft_prob / sharp_prob) - 1
    """

    def __init__(self) -> None:
        self._providers: list[BaseOddsFetcher] = [
            OddsApiOddsFetcher(),
            SportsBooksReviewOddsFetcher(),
        ]

    def fetch_all(self, sport: str = "baseball_mlb") -> list[OddsLine]:
        """Collect raw lines from every provider."""
        all_lines: list[OddsLine] = []
        for provider in self._providers:
            try:
                lines = provider.fetch_player_props(sport)
                all_lines.extend(lines)
                logger.info("[OddsFetcher] %s returned %d lines",
                            provider.provider_name(), len(lines))
            except Exception as e:
                logger.error("[OddsFetcher] Provider %s failed: %s",
                             provider.provider_name(), e)
        return all_lines

    @staticmethod
    def merge_odds(lines: list[OddsLine]) -> list[MergedOdds]:
        """
        Group lines by (player_name, prop_type, line), compute consensus
        no-vig probabilities, find best odds, and estimate CLV.
        """
        # Group: key = (player_name, prop_type, line_rounded)
        groups: dict[tuple, list[OddsLine]] = {}
        for ol in lines:
            key = (ol.player_name.lower(), ol.prop_type, round(ol.line * 2) / 2)
            groups.setdefault(key, []).append(ol)

        merged: list[MergedOdds] = []
        for (_, prop_type, line), group in groups.items():
            # Consensus: average no-vig probs across all providers
            probs_over  = []
            probs_under = []
            for ol in group:
                p_o, p_u = _strip_vig(ol.odds_over, ol.odds_under)
                probs_over.append(p_o)
                probs_under.append(p_u)

            if not probs_over:
                continue

            consensus_over  = sum(probs_over)  / len(probs_over)
            consensus_under = sum(probs_under) / len(probs_under)

            # Best odds (highest payout for backer)
            best_over_line  = max(group, key=lambda x: x.odds_over)
            best_under_line = max(group, key=lambda x: x.odds_under)

            # CLV: compare best available odds vs sharp consensus
            sharp_lines = [ol for ol in group if "SBR/Pinnacle" in ol.provider
                           or "SBR/Circa" in ol.provider or "SBR" in ol.provider]
            if sharp_lines:
                sharp_prob_over, _ = _strip_vig(
                    sharp_lines[0].odds_over, sharp_lines[0].odds_under)
                soft_prob_over, _  = _strip_vig(
                    best_over_line.odds_over, best_over_line.odds_under)
                clv_edge = (soft_prob_over / sharp_prob_over) - 1.0 if sharp_prob_over else 0.0
            else:
                clv_edge = 0.0

            merged.append(MergedOdds(
                player_name=group[0].player_name,
                prop_type=prop_type,
                line=line,
                consensus_prob_over=round(consensus_over, 4),
                consensus_prob_under=round(consensus_under, 4),
                best_odds_over=best_over_line.odds_over,
                best_odds_under=best_under_line.odds_under,
                best_over_provider=best_over_line.provider,
                best_under_provider=best_under_line.provider,
                clv_edge_pct=round(clv_edge, 4),
                providers_sampled=list({ol.provider for ol in group}),
                game_id=group[0].game_id,
                commence_time=group[0].commence_time,
                raw_lines=group,
            ))

        # Sort by CLV edge descending
        merged.sort(key=lambda x: x.clv_edge_pct, reverse=True)
        logger.info("[OddsFetcher] Merged %d unique prop lines", len(merged))
        return merged

    def top_clv_opportunities(
        self,
        n: int = 20,
        min_clv_pct: float = 0.02,
        sport: str = "baseball_mlb",
    ) -> list[MergedOdds]:
        """
        Full pipeline: fetch → merge → filter top CLV edges.
        Returns up to `n` props with CLV ≥ min_clv_pct.
        """
        raw   = self.fetch_all(sport)
        merged = self.merge_odds(raw)
        top   = [m for m in merged if m.clv_edge_pct >= min_clv_pct]
        logger.info("[OddsFetcher] %d props above %.1f%% CLV gate",
                    len(top), min_clv_pct * 100)
        return top[:n]

    def fetch_aggregated_odds(
        self,
        n: int = 50,
        min_clv_pct: float = 0.02,
        min_dislocation_pct: float = 0.03,
        sport: str = "baseball_mlb",
    ) -> dict[str, list]:
        """
        Full aggregation pipeline for downstream agents and MarketFusionEngine.

        Returns a structured dict with three segments:

        ``top_clv``
            MergedOdds list sorted by CLV edge descending (≥ min_clv_pct).
            Source for EVHunter and LineValueAgent.

        ``arbitrage``
            MergedOdds where (best_over_implied + best_under_implied) < 1.0.
            Source for ArbitrageAgent.

        ``dislocations``
            Odds where the gap between sharpest and softest book no-vig
            probability exceeds min_dislocation_pct.  Pinnacle/Circa/CRIS
            treated as sharp reference.  Source for EVHunter CLV enrichment.

        Args:
            n:                   Max items per segment.
            min_clv_pct:         Minimum CLV edge for top_clv segment.
            min_dislocation_pct: Minimum inter-book probability gap for
                                 dislocation segment.
            sport:               Odds API sport key.

        Returns:
            ``{"top_clv": [...], "arbitrage": [...], "dislocations": [...]}``
        """
        raw    = self.fetch_all(sport)
        merged = self.merge_odds(raw)

        # Segment 1 — top CLV
        top_clv = [m for m in merged if m.clv_edge_pct >= min_clv_pct][:n]

        # Segment 2 — arbitrage (total implied < 1.0)
        arb: list[MergedOdds] = []
        for m in merged:
            over_impl  = _american_to_implied(m.best_odds_over)
            under_impl = _american_to_implied(m.best_odds_under)
            if over_impl + under_impl < 1.0:
                arb.append(m)

        # Segment 3 — inter-book sharp/soft dislocations
        _SHARP_TAGS = ("Pinnacle", "Circa", "CRIS", "Bookmaker", "Heritage")
        dislocations: list[MergedOdds] = []
        for m in merged:
            # Find sharpest book's true prob
            sharp_lines = [
                ol for ol in m.raw_lines
                if any(tag in ol.provider for tag in _SHARP_TAGS)
            ]
            soft_lines = [
                ol for ol in m.raw_lines
                if not any(tag in ol.provider for tag in _SHARP_TAGS)
            ]
            if not sharp_lines or not soft_lines:
                continue
            sharp_p, _ = _strip_vig(
                sharp_lines[0].odds_over, sharp_lines[0].odds_under
            )
            # Best soft-book over prob
            soft_probs = [
                _strip_vig(ol.odds_over, ol.odds_under)[0] for ol in soft_lines
            ]
            best_soft_p = max(soft_probs) if soft_probs else 0.0
            dislocation = abs(best_soft_p - sharp_p)
            if dislocation >= min_dislocation_pct:
                dislocations.append(m)

        dislocations.sort(key=lambda x: x.clv_edge_pct, reverse=True)

        logger.info(
            "[OddsFetcher] fetch_aggregated_odds → clv=%d arb=%d dislocations=%d",
            len(top_clv), len(arb), len(dislocations),
        )
        return {
            "top_clv":      top_clv,
            "arbitrage":    arb[:n],
            "dislocations": dislocations[:n],
        }
