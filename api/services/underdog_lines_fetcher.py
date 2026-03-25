"""
underdog_lines_fetcher.py
=========================
Fetches live MLB player prop lines from Underdog Fantasy's unauthenticated
public API endpoint.

Endpoint: https://api.underdogfantasy.com/v1/over_under_lines
  - No API key required
  - Returns all sports; we filter sport_id == "MLB"
  - 18–19MB payload; must use streaming-aware parse or accept full load
  - Response shape:
      {
        "over_under_lines": [...],   # 7000+ cross-sport lines
        "appearances": [...],        # player-game linkage
        "players": [...],            # player metadata
        "games": [...],              # game metadata
        "solo_games": [...]
      }

Stat-type mapping (Underdog → PropIQ prop_type):
  strikeouts        → pitcher_strikeouts
  pitch_outs        → pitcher_outs
  hits_allowed      → pitcher_hits_allowed
  walks_allowed     → pitcher_walks_allowed
  runs_allowed      → pitcher_earned_runs
  hits              → batter_hits
  total_bases       → batter_total_bases
  rbis              → batter_rbis
  runs              → batter_runs
  stolen_bases      → batter_stolen_bases
  home_runs         → batter_home_runs
  hits_runs_rbis    → batter_hits_runs_rbis  (combo)

Usage:
    fetcher = UnderdogLinesFetcher()
    lines = fetcher.fetch_mlb_lines()
    propiq_lines = fetcher.to_propiq_format(lines)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UNDERDOG_API_URL = "https://api.underdogfantasy.com/v1/over_under_lines"

# Mobile app user-agent keeps the endpoint happy (no auth required)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
        "Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

# Underdog stat name → PropIQ prop_type
STAT_MAP: dict[str, str] = {
    "strikeouts": "pitcher_strikeouts",
    "pitch_outs": "pitcher_outs",
    "hits_allowed": "pitcher_hits_allowed",
    "walks_allowed": "pitcher_walks_allowed",
    "runs_allowed": "pitcher_earned_runs",
    "hits": "batter_hits",
    "total_bases": "batter_total_bases",
    "rbis": "batter_rbis",
    "runs": "batter_runs",
    "stolen_bases": "batter_stolen_bases",
    "home_runs": "batter_home_runs",
    "hits_runs_rbis": "batter_hits_runs_rbis",
}

# Underdog line_type values
LINE_TYPES = {"balanced", "boosted", "discounted"}

# PropIQ player type classification (by position_name)
PITCHER_POSITIONS = {"SP", "RP", "P", "CP"}

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class UnderdogMLBLine:
    """Represents a single MLB over/under line from Underdog Fantasy."""

    line_id: str
    stable_id: str
    player_id: str
    player_name: str
    position: str
    team_id: str
    game_id: int
    game_title: str               # "NYY @ SF"
    scheduled_at: str             # ISO-8601 UTC
    stat_ud: str                  # raw Underdog stat key, e.g. "strikeouts"
    stat_display: str             # human label, e.g. "Strikeouts"
    prop_type: str                # PropIQ mapped key, e.g. "pitcher_strikeouts"
    player_type: str              # "pitcher" | "batter"
    line: float                   # numeric line value
    line_type: str                # "balanced" | "boosted" | "discounted"
    higher_price: int             # American odds for Higher (over)
    lower_price: int              # American odds for Lower (under)
    higher_payout: float          # DFS payout multiplier for Higher
    lower_payout: float           # DFS payout multiplier for Lower
    status: str                   # "active" | "suspended" | etc.
    fetched_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# Main fetcher class
# ---------------------------------------------------------------------------


class UnderdogLinesFetcher:
    """
    Fetches and parses live MLB prop lines from Underdog Fantasy.

    No API key is required. The endpoint is publicly accessible.
    Filters to sport_id == "MLB" and maps Underdog stat names to
    PropIQ prop_type values.

    Example::

        fetcher = UnderdogLinesFetcher()
        lines = fetcher.fetch_mlb_lines()
        propiq = fetcher.to_propiq_format(lines)
    """

    def __init__(
        self,
        headers: dict[str, str] | None = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self._headers = headers or DEFAULT_HEADERS
        self._timeout = timeout
        self._max_retries = max_retries
        self._session = requests.Session()
        self._session.headers.update(self._headers)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_mlb_lines(
        self,
        active_only: bool = True,
        line_types: set[str] | None = None,
    ) -> list[UnderdogMLBLine]:
        """
        Fetch all current MLB prop lines from Underdog Fantasy.

        Parameters
        ----------
        active_only:
            If True (default), skip lines whose status != "active".
        line_types:
            Restrict to specific line types (e.g. {"balanced"}).
            Pass None to include all types.

        Returns
        -------
        List of :class:`UnderdogMLBLine` instances, one per stat per player.
        """
        raw = self._fetch_with_retry()
        return self._parse_mlb_lines(raw, active_only=active_only, line_types=line_types)

    def to_propiq_format(
        self, lines: list[UnderdogMLBLine]
    ) -> list[dict[str, Any]]:
        """
        Convert parsed lines to the PropIQ MarketFusionEngine schema.

        Each dict contains:
          game_date, player_name, player_type, prop_type,
          sportsbook ("underdog"), over_line, under_line,
          over_juice, under_juice, line_type, fetched_at
        """
        results = []
        for ln in lines:
            scheduled = ln.scheduled_at
            game_date = scheduled[:10] if scheduled else ""
            results.append(
                {
                    "game_date": game_date,
                    "player_name": ln.player_name,
                    "player_type": ln.player_type,
                    "prop_type": ln.prop_type,
                    "stat_ud": ln.stat_ud,
                    "sportsbook": "underdog",
                    "over_line": ln.line,
                    "under_line": ln.line,
                    "over_juice": ln.higher_price,
                    "under_juice": ln.lower_price,
                    "over_payout": ln.higher_payout,
                    "under_payout": ln.lower_payout,
                    "line_type": ln.line_type,
                    "game_title": ln.game_title,
                    "scheduled_at": ln.scheduled_at,
                    "status": ln.status,
                    "fetched_at": ln.fetched_at,
                    "stable_id": ln.stable_id,
                    "position": ln.position,
                }
            )
        return results

    def get_pitcher_lines(self, lines: list[UnderdogMLBLine]) -> list[UnderdogMLBLine]:
        """Filter to pitcher prop lines only."""
        return [ln for ln in lines if ln.player_type == "pitcher"]

    def get_batter_lines(self, lines: list[UnderdogMLBLine]) -> list[UnderdogMLBLine]:
        """Filter to batter prop lines only."""
        return [ln for ln in lines if ln.player_type == "batter"]

    def get_lines_for_player(
        self, lines: list[UnderdogMLBLine], player_name: str
    ) -> list[UnderdogMLBLine]:
        """Return all lines for a specific player (case-insensitive)."""
        name_lower = player_name.lower()
        return [ln for ln in lines if ln.player_name.lower() == name_lower]

    def get_lines_by_prop_type(
        self, lines: list[UnderdogMLBLine], prop_type: str
    ) -> list[UnderdogMLBLine]:
        """Return all lines matching a specific PropIQ prop_type."""
        return [ln for ln in lines if ln.prop_type == prop_type]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_with_retry(self) -> dict[str, Any]:
        """
        GET the Underdog endpoint with exponential back-off.
        Raises :class:`requests.HTTPError` after max retries.
        """
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(
                    "UnderdogLinesFetcher: attempt %d/%d", attempt, self._max_retries
                )
                resp = self._session.get(UNDERDOG_API_URL, timeout=self._timeout)
                resp.raise_for_status()
                data = resp.json()
                logger.info(
                    "UnderdogLinesFetcher: fetched %d total lines, %d games, %d players",
                    len(data.get("over_under_lines", [])),
                    len(data.get("games", [])),
                    len(data.get("players", [])),
                )
                return data
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "UnderdogLinesFetcher: attempt %d failed: %s", attempt, exc
                )
                if attempt < self._max_retries:
                    sleep_secs = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    logger.info("Retrying in %.1f seconds…", sleep_secs)
                    time.sleep(sleep_secs)

        raise last_exc  # type: ignore[misc]

    def _parse_mlb_lines(
        self,
        raw: dict[str, Any],
        *,
        active_only: bool,
        line_types: set[str] | None,
    ) -> list[UnderdogMLBLine]:
        """
        Parse raw API payload into :class:`UnderdogMLBLine` instances.

        Join chain:
          over_under_lines → appearances (via appearance_id)
                           → players    (via player_id)
                           → games      (via match_id)
        """
        # Build lookup maps (O(n) once)
        players_map: dict[str, dict] = {
            p["id"]: p for p in raw.get("players", [])
        }
        games_map: dict[str, dict] = {
            str(g["id"]): g for g in raw.get("games", [])
        }
        appearances_map: dict[str, dict] = {
            a["id"]: a for a in raw.get("appearances", [])
        }

        results: list[UnderdogMLBLine] = []
        seen: set[str] = set()  # deduplicate on stable_id

        for line in raw.get("over_under_lines", []):
            try:
                # Skip duplicates
                stable_id = line.get("stable_id", "")
                if stable_id in seen:
                    continue

                # Status filter
                if active_only and line.get("status") != "active":
                    continue

                # Line type filter
                lt = line.get("line_type", "balanced")
                if line_types is not None and lt not in line_types:
                    continue

                # Stat info
                ou = line.get("over_under", {})
                app_stat = ou.get("appearance_stat", {})
                stat_ud = app_stat.get("stat", "")

                # Only process stats we have a PropIQ mapping for
                prop_type = STAT_MAP.get(stat_ud)
                if prop_type is None:
                    continue

                # Resolve appearance → player + game
                app_id = app_stat.get("appearance_id", "")
                appearance = appearances_map.get(app_id, {})
                player_id = appearance.get("player_id", "")
                player = players_map.get(player_id, {})

                # Must be MLB
                if player.get("sport_id") != "MLB":
                    continue

                game_id = appearance.get("match_id")
                game = games_map.get(str(game_id), {})

                # Options → higher / lower
                higher_opt = next(
                    (o for o in line.get("options", []) if o.get("choice") == "higher"),
                    {},
                )
                lower_opt = next(
                    (o for o in line.get("options", []) if o.get("choice") == "lower"),
                    {},
                )

                higher_price = self._parse_american(higher_opt.get("american_price"))
                lower_price = self._parse_american(lower_opt.get("american_price"))
                higher_payout = float(higher_opt.get("payout_multiplier", 1.0))
                lower_payout = float(lower_opt.get("payout_multiplier", 1.0))

                position = player.get("position_name", "")
                player_type = (
                    "pitcher" if position.upper() in PITCHER_POSITIONS else "batter"
                )

                parsed = UnderdogMLBLine(
                    line_id=line.get("id", ""),
                    stable_id=stable_id,
                    player_id=player_id,
                    player_name=(
                        f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
                    ),
                    position=position,
                    team_id=player.get("team_id", ""),
                    game_id=game_id or 0,
                    game_title=game.get("title", game.get("abbreviated_title", "")),
                    scheduled_at=game.get("scheduled_at", ""),
                    stat_ud=stat_ud,
                    stat_display=app_stat.get("display_stat", stat_ud),
                    prop_type=prop_type,
                    player_type=player_type,
                    line=float(line.get("stat_value", 0)),
                    line_type=lt,
                    higher_price=higher_price,
                    lower_price=lower_price,
                    higher_payout=higher_payout,
                    lower_payout=lower_payout,
                    status=line.get("status", ""),
                )
                results.append(parsed)
                seen.add(stable_id)

            except (KeyError, TypeError, ValueError) as exc:
                logger.debug("Skipping malformed line: %s", exc)
                continue

        logger.info(
            "UnderdogLinesFetcher: parsed %d MLB lines "
            "(%d pitchers, %d batters)",
            len(results),
            sum(1 for r in results if r.player_type == "pitcher"),
            sum(1 for r in results if r.player_type == "batter"),
        )
        return results

    @staticmethod
    def _parse_american(price_str: str | None) -> int:
        """Convert American odds string to int, defaulting to -110."""
        if not price_str:
            return -110
        try:
            return int(float(price_str))
        except (ValueError, TypeError):
            return -110
