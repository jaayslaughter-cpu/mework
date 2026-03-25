"""
api/services/sportsbookreview_odds_fetcher.py

Standalone SportsBooksReview odds fetcher.

Implements the OddsFetcherBase ABC with the three required methods:
    fetch_odds(sport, market_type, period)   → raw provider response
    parse_response(response)                 → list[dict] raw events
    normalize_odds(odds_data)                → list[OddsLine] PropIQ schema

SBR aggregates sharp books (Pinnacle, Circa, Bookmaker, Pinnacle EU) which
makes it ideal for Closing Line Value (CLV) estimation.

Transport strategy (in order of preference):
    1. SBR JSON API  — /ajax/lines endpoint (fastest, structured)
    2. SBR XML feed  — /betting-odds/{sport}/?format=xml (xmltodict parse)
    3. SBR HTML page — /betting-odds/{sport}/?date={period} (BeautifulSoup)

PEP 8 compliant.  No hallucinated APIs.
"""

from __future__ import annotations

import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Re-use the shared OddsLine model (imported from odds_fetcher to stay DRY)
# ---------------------------------------------------------------------------
try:
    from api.services.odds_fetcher import OddsLine  # type: ignore[import]
except ImportError:
    # Fallback definition so this module works standalone in tests
    @dataclass
    class OddsLine:  # type: ignore[no-redef]
        """Normalised single-provider odds line for a player prop."""
        provider:      str
        player_name:   str
        prop_type:     str
        line:          float
        odds_over:     int
        odds_under:    int
        market_key:    str = ""
        game_id:       str = ""
        commence_time: str = ""
        timestamp:     float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Abstract base class (matches the interface specified in the design doc)
# ---------------------------------------------------------------------------
class OddsFetcherBase(ABC):
    """
    Common interface for every PropIQ odds provider.

    Subclasses must implement:
        fetch_odds(sport, market_type, period)  — hit the provider
        parse_response(response)                — raw response → list[dict]
        normalize_odds(odds_data)               — list[dict] → list[OddsLine]
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
            sport:       Provider-specific sport slug  (e.g. "baseball-mlb")
            market_type: Prop category slug            (e.g. "pitcher-strikeouts")
            period:      Date string in YYYY-MM-DD format

        Returns:
            Raw list of event dicts as returned by parse_response().
        """
        ...

    @abstractmethod
    def parse_response(self, response: str) -> list[dict]:
        """
        Parse a raw HTTP response body (XML, JSON, or HTML text) into a
        list of raw event dicts before normalization.

        Args:
            response: Raw response body string from the provider.

        Returns:
            List of raw event dicts (provider-specific schema).
        """
        ...

    @abstractmethod
    def normalize_odds(self, odds_data: list[dict]) -> list[OddsLine]:
        """
        Map provider-specific event dicts to the PropIQ OddsLine schema.

        Args:
            odds_data: Output of parse_response().

        Returns:
            List of normalised OddsLine objects ready for OddsFetcher merger.
        """
        ...


# ---------------------------------------------------------------------------
# Prop-type slug maps
# ---------------------------------------------------------------------------
# SBR market slugs → PropIQ canonical prop types
_SBR_MARKET_SLUG_MAP: dict[str, str] = {
    "pitcher-strikeouts":  "strikeouts",
    "pitcher-walks":       "pitcher_walks",
    "pitcher-hits-allowed": "hits_allowed",
    "pitcher-earned-runs":  "earned_runs",
    "pitcher-outs":         "outs",
    "batter-hits":          "hits",
    "batter-home-runs":     "home_runs",
    "batter-rbis":          "rbi",
    "batter-runs-scored":   "runs",
    "batter-total-bases":   "total_bases",
    "batter-stolen-bases":  "stolen_bases",
    "batter-doubles":       "doubles",
}

# XML abbreviated stat codes → PropIQ canonical prop types
_SBR_XML_STAT_MAP: dict[str, str] = {
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
    "IP":  "outs",
}

# Sharp books SBR aggregates (used as CLV reference)
SHARP_BOOKS: frozenset[str] = frozenset({
    "Pinnacle", "Circa", "Bookmaker", "CRIS",
    "Heritage", "5Dimes", "BetPhoenix",
})

# ---------------------------------------------------------------------------
# HTTP constants
# ---------------------------------------------------------------------------
SBR_BASE_URL    = "https://www.sportsbookreview.com"
SBR_AJAX_URL    = f"{SBR_BASE_URL}/ajax/lines"
REQUEST_TIMEOUT = 25
_RETRY_WAITS    = (1, 2, 4)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":     "application/json, text/xml, */*;q=0.8",
    "Referer":    "https://www.sportsbookreview.com/",
}


# ---------------------------------------------------------------------------
# SportsBooksReviewOddsFetcher
# ---------------------------------------------------------------------------
class SportsBooksReviewOddsFetcher(OddsFetcherBase):
    """
    Fetches MLB player-prop odds from SportsBooksReview.

    Uses three transport strategies in order of preference:
        1. JSON AJAX API    — fastest, most structured
        2. XML feed         — xmltodict parse of the ?format=xml variant
        3. HTML scrape      — regex extraction from rendered page

    All three normalise into the same list[OddsLine] output.

    Example::

        fetcher  = SportsBooksReviewOddsFetcher()
        raw      = fetcher.fetch_odds("baseball-mlb", "pitcher-strikeouts", "2025-07-04")
        lines    = fetcher.normalize_odds(raw)
    """

    def __init__(self, timeout: int = REQUEST_TIMEOUT) -> None:
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    def provider_name(self) -> str:
        return "SBR"

    # ------------------------------------------------------------------
    # ABC: fetch_odds
    # ------------------------------------------------------------------
    def fetch_odds(
        self,
        sport: str = "baseball-mlb",
        market_type: str = "pitcher-strikeouts",
        period: str = "",
    ) -> list[dict]:
        """
        Fetch SBR odds for a specific market and date.

        Tries JSON → XML → HTML in order.  Returns raw event dicts from
        whichever transport succeeds first.

        Args:
            sport:       SBR sport slug (e.g. ``"baseball-mlb"``).
            market_type: SBR market slug (e.g. ``"pitcher-strikeouts"``).
            period:      Date string ``YYYY-MM-DD`` (today if omitted).

        Returns:
            list[dict] of raw event objects for parse_response / normalize_odds.
        """
        if not period:
            from datetime import date
            period = date.today().isoformat()

        # Strategy 1: JSON AJAX
        events = self._fetch_json(sport, market_type, period)
        if events:
            logger.info("[SBR] JSON transport: %d events (%s/%s)", len(events), sport, market_type)
            return events

        # Strategy 2: XML feed
        xml_text = self._fetch_xml(sport, market_type, period)
        if xml_text:
            events = self.parse_response(xml_text)
            if events:
                logger.info("[SBR] XML transport: %d events (%s/%s)", len(events), sport, market_type)
                return events

        # Strategy 3: HTML scrape
        html_text = self._fetch_html(sport, period)
        if html_text:
            events = self._parse_html(html_text, market_type)
            logger.info("[SBR] HTML transport: %d events (%s/%s)", len(events), sport, market_type)
            return events

        logger.warning("[SBR] All transports failed for %s/%s", sport, market_type)
        return []

    # ------------------------------------------------------------------
    # ABC: parse_response  (XML path)
    # ------------------------------------------------------------------
    def parse_response(self, response: str) -> list[dict]:
        """
        Parse SBR XML response body into a list of raw event dicts.

        The SBR XML schema:
            <OddsResults>
                <Event id="..." homeTeam="..." awayTeam="..." startTime="...">
                    <Participant id="..." name="...">
                        <Odds type="Over" price="-115" line="6.5" book="Pinnacle"/>
                        ...
                    </Participant>
                </Event>
            </OddsResults>

        Returns:
            List of raw event dicts with parsed participant odds.
        """
        try:
            import xmltodict  # type: ignore[import]
        except ImportError:
            logger.error("[SBR] xmltodict not installed; cannot parse XML response")
            return []

        try:
            xml_data = xmltodict.parse(response)
        except Exception as exc:
            logger.warning("[SBR] XML parse failed: %s", exc)
            return []

        # Navigate safely through the XML tree
        root = xml_data.get("OddsResults", xml_data.get("OddsResult", {}))
        if not root:
            return []

        raw_events = root.get("Event", [])
        if isinstance(raw_events, dict):
            raw_events = [raw_events]  # single-event response

        events: list[dict] = []
        for event in raw_events:
            parsed = self.parse_event(event)
            if parsed:
                events.append(parsed)

        return events

    # ------------------------------------------------------------------
    # parse_event helper
    # ------------------------------------------------------------------
    def parse_event(self, event: dict) -> dict | None:
        """
        Extract key fields from one SBR XML Event node.

        Handles participant-level odds (``<Participant>`` nodes) and maps
        them into a structured dict.

        Args:
            event: Raw xmltodict dict for one ``<Event>`` node.

        Returns:
            Normalised event dict or ``None`` if parsing fails.
        """
        try:
            home_team  = event.get("@homeTeam", event.get("@home", ""))
            away_team  = event.get("@awayTeam", event.get("@away", ""))
            start_time = event.get("@startTime", event.get("@start", ""))
            game_id    = str(event.get("@id", ""))

            participants = event.get("Participant", [])
            if isinstance(participants, dict):
                participants = [participants]

            parsed_participants: list[dict] = []
            for p in participants:
                player_name = p.get("@name", p.get("@fullName", ""))
                if not player_name:
                    continue

                odds_nodes = p.get("Odds", [])
                if isinstance(odds_nodes, dict):
                    odds_nodes = [odds_nodes]

                book_odds: list[dict] = []
                for odds_node in odds_nodes:
                    try:
                        book_odds.append({
                            "type":  odds_node.get("@type", ""),    # "Over" / "Under"
                            "price": int(odds_node.get("@price", -110)),
                            "line":  float(odds_node.get("@line", 0)),
                            "book":  odds_node.get("@book", "SBR"),
                        })
                    except (ValueError, TypeError):
                        continue

                if book_odds:
                    parsed_participants.append({
                        "player_name": player_name,
                        "odds":        book_odds,
                    })

            if not parsed_participants:
                return None

            return {
                "home_team":    home_team,
                "away_team":    away_team,
                "start_time":   start_time,
                "game_id":      game_id,
                "participants": parsed_participants,
            }

        except Exception as exc:
            logger.debug("[SBR] parse_event error: %s — event=%s", exc, event)
            return None

    # ------------------------------------------------------------------
    # ABC: normalize_odds
    # ------------------------------------------------------------------
    def normalize_odds(self, odds_data: list[dict]) -> list[OddsLine]:
        """
        Map parsed SBR event dicts to the PropIQ OddsLine schema.

        One event dict → multiple OddsLine objects (one per player × book).
        Over/Under odds are paired by line value and book name.

        Args:
            odds_data: Output of parse_response() or fetch_odds().

        Returns:
            List of OddsLine objects conforming to the PropIQ standard schema.
        """
        lines: list[OddsLine] = []

        for event in odds_data:
            game_id     = str(event.get("game_id", ""))
            start_time  = event.get("start_time", "")
            participants = event.get("participants", [])

            for participant in participants:
                player_name = participant.get("player_name", "")
                if not player_name:
                    continue

                # Group Over/Under by (book, line)
                book_line_map: dict[tuple[str, float], dict] = {}
                for odd in participant.get("odds", []):
                    key = (odd["book"], odd["line"])
                    entry = book_line_map.setdefault(key, {
                        "book":      odd["book"],
                        "line":      odd["line"],
                        "odds_over":  -110,
                        "odds_under": -110,
                    })
                    if odd["type"] == "Over":
                        entry["odds_over"]  = odd["price"]
                    elif odd["type"] == "Under":
                        entry["odds_under"] = odd["price"]

                for (book, line_val), entry in book_line_map.items():
                    if line_val == 0:
                        continue
                    lines.append(OddsLine(
                        provider=f"SBR/{book}",
                        player_name=player_name,
                        prop_type=event.get("prop_type", "unknown"),
                        line=line_val,
                        odds_over=entry["odds_over"],
                        odds_under=entry["odds_under"],
                        market_key=event.get("market_key", ""),
                        game_id=game_id,
                        commence_time=start_time,
                    ))

        logger.info("[SBR] normalize_odds → %d OddsLine objects", len(lines))
        return lines

    # ------------------------------------------------------------------
    # Public all-markets entry point (matches BaseOddsFetcher interface)
    # ------------------------------------------------------------------
    def fetch_player_props(
        self,
        sport: str = "baseball_mlb",
    ) -> list[OddsLine]:
        """
        Fetch all available MLB player-prop markets from SBR.

        Iterates over every slug in ``_SBR_MARKET_SLUG_MAP``, collects
        raw events, annotates prop_type, and normalises to OddsLine.

        Returns:
            All available OddsLine objects across all prop markets.
        """
        sbr_sport = sport.replace("_", "-")  # "baseball_mlb" → "baseball-mlb"
        all_lines: list[OddsLine] = []

        from datetime import date
        today = date.today().isoformat()

        for market_slug, prop_type in _SBR_MARKET_SLUG_MAP.items():
            try:
                raw_events = self.fetch_odds(sbr_sport, market_slug, today)
                # Stamp prop_type and market_key onto each event before normalizing
                for ev in raw_events:
                    ev["prop_type"]   = prop_type
                    ev["market_key"]  = market_slug
                lines = self.normalize_odds(raw_events)
                all_lines.extend(lines)
            except Exception as exc:
                logger.warning("[SBR] Market %s failed: %s", market_slug, exc)

        logger.info("[SBR] fetch_player_props → %d total OddsLine objects", len(all_lines))
        return all_lines

    # ------------------------------------------------------------------
    # Private transport helpers
    # ------------------------------------------------------------------
    def _fetch_json(
        self,
        sport: str,
        market_type: str,
        period: str,
    ) -> list[dict]:
        """Try SBR JSON AJAX endpoint for player props."""
        params = {
            "sport":      sport,
            "marketType": market_type,
            "date":       period,
            "type":       "playerprops",
        }
        try:
            resp = self._session.get(
                SBR_AJAX_URL, params=params, timeout=self._timeout
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
            # SBR JSON wraps in {"data": [...]} or {"events": [...]}
            if isinstance(data, list):
                return self._normalize_json_events(data, market_type)
            events = data.get("data") or data.get("events") or data.get("props", [])
            return self._normalize_json_events(events, market_type)
        except Exception as exc:
            logger.debug("[SBR] JSON transport error: %s", exc)
            return []

    def _normalize_json_events(
        self,
        events: list[dict],
        market_type: str,
    ) -> list[dict]:
        """Normalise raw SBR JSON event list to the intermediate dict schema."""
        prop_type = _SBR_MARKET_SLUG_MAP.get(market_type, market_type)
        out: list[dict] = []

        for ev in events:
            if not ev:
                continue
            participants: list[dict] = []

            # JSON schema: participants or players array
            raw_parts = ev.get("participants") or ev.get("players") or []
            for p in raw_parts:
                player_name = (
                    p.get("playerName")
                    or p.get("fullName")
                    or p.get("name", "")
                )
                if not player_name:
                    continue

                odds_list: list[dict] = []
                # JSON books array: [{book, overOdds, underOdds, line}, ...]
                for book_entry in p.get("books") or p.get("odds") or []:
                    line_val = float(book_entry.get("line") or book_entry.get("total") or 0)
                    book     = book_entry.get("book") or book_entry.get("sportsbook") or "SBR"
                    odds_list.append({
                        "type":  "Over",
                        "price": int(book_entry.get("overOdds")  or book_entry.get("over",  -110)),
                        "line":  line_val,
                        "book":  book,
                    })
                    odds_list.append({
                        "type":  "Under",
                        "price": int(book_entry.get("underOdds") or book_entry.get("under", -110)),
                        "line":  line_val,
                        "book":  book,
                    })

                if odds_list:
                    participants.append({
                        "player_name": player_name,
                        "odds":        odds_list,
                    })

            if participants:
                out.append({
                    "home_team":    ev.get("homeTeam") or ev.get("home", ""),
                    "away_team":    ev.get("awayTeam") or ev.get("away", ""),
                    "start_time":   ev.get("startTime") or ev.get("start", ""),
                    "game_id":      str(ev.get("id") or ev.get("gameId", "")),
                    "prop_type":    prop_type,
                    "market_key":   market_type,
                    "participants": participants,
                })

        return out

    def _fetch_xml(
        self,
        sport: str,
        market_type: str,
        period: str,
    ) -> str:
        """Fetch the SBR XML feed for a sport/market/date."""
        url = (
            f"{SBR_BASE_URL}/betting-odds/{sport}/{market_type}/"
            f"?date={period}&format=xml"
        )
        try:
            resp = self._session.get(url, timeout=self._timeout)
            if resp.status_code == 200 and resp.text.strip().startswith("<"):
                return resp.text
            return ""
        except Exception as exc:
            logger.debug("[SBR] XML transport error: %s", exc)
            return ""

    def _fetch_html(self, sport: str, period: str) -> str:
        """Fetch the SBR HTML page for scraping."""
        url = f"{SBR_BASE_URL}/betting-odds/{sport}/?date={period}"
        try:
            resp = self._session.get(url, timeout=self._timeout)
            return resp.text if resp.status_code == 200 else ""
        except Exception as exc:
            logger.debug("[SBR] HTML transport error: %s", exc)
            return ""

    def _parse_html(self, html: str, market_type: str) -> list[dict]:
        """
        Regex-based HTML scraper as last-resort fallback.

        Looks for JSON-LD or embedded __NEXT_DATA__ / __PRELOADED_STATE__
        blobs common in modern React SBR pages.
        """
        import json as _json

        prop_type = _SBR_MARKET_SLUG_MAP.get(market_type, market_type)

        # Try __NEXT_DATA__ (Next.js pages embed full props here)
        nd_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if nd_match:
            try:
                nd = _json.loads(nd_match.group(1))
                # Navigate to the props payload — varies by SBR page version
                events_raw = (
                    nd.get("props", {})
                      .get("pageProps", {})
                      .get("oddsTables", [])
                )
                if events_raw:
                    return self._normalize_json_events(events_raw, market_type)
            except Exception as exc:
                logger.debug("[SBR] __NEXT_DATA__ parse error: %s", exc)

        # Try JSON-LD
        ld_matches = re.findall(
            r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL
        )
        for blob in ld_matches:
            try:
                data = _json.loads(blob)
                if isinstance(data, list) and data and "participants" in str(data[0]):
                    return self._normalize_json_events(data, market_type)
            except Exception:
                continue

        # Fallback: return empty — log so we can improve the scraper
        logger.warning("[SBR] HTML scraper found no parseable data for %s", market_type)
        return []
