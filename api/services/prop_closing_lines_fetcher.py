"""
prop_closing_lines_fetcher.py
─────────────────────────────
Fetches live and closing MLB player prop lines from The Odds API and
persists them into the mlb_pitcher_props / mlb_player_props tables.

Architecture
────────────
• Uses The Odds API v4 event-level endpoint, which supports granular
  player prop markets that the bulk `/odds` endpoint does NOT expose.
• Dual-key rotation: KEY_1 (primary) → KEY_2 (on 429 / quota exhausted)
• Rate limiter: max 1 request per 0.5 s to respect free-tier limits
• Stores data in agent SQL DB via sqlite3 using the DB path injected at
  construction (defaults to in-memory for testing)
• Also exposes raw dicts for integration with the existing PropIQ
  OddsFetcher / MarketFusionEngine pipeline

Supported Markets
─────────────────
  PITCHER:
    pitcher_strikeouts        – K totals (primary use case)
    pitcher_hits_allowed      – H allowed
    pitcher_walks             – BB allowed
    pitcher_earned_runs       – ER
    pitcher_outs              – outs recorded (½ innings × 3)

  BATTER:
    batter_hits               – H
    batter_total_bases        – TB
    batter_home_runs          – HR
    batter_rbis               – RBI
    batter_strikeouts         – K (batter)
    batter_stolen_bases       – SB

Usage
─────
  fetcher = PropClosingLinesFetcher(db_path="/path/to/agent.db")
  records = fetcher.fetch_and_store_all()

  Or target a specific event:
  records = fetcher.fetch_event_props(event_id, game_date)
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import requests

# ── Constants ─────────────────────────────────────────────────────────────────

_BASE_URL = "https://api.the-odds-api.com/v4"
_SPORT = "baseball_mlb"

# Approved API keys (rotate on 429 / quota exhausted)
_API_KEYS: List[str] = [
    "e4e30098807a9eece674d85e30471f03",   # KEY_1 primary
    "673bf195062e60e666399be40f763545",   # KEY_2 backup
]

# Markets split by player type
PITCHER_MARKETS: List[str] = [
    "pitcher_strikeouts",
    "pitcher_hits_allowed",
    "pitcher_walks",
    "pitcher_earned_runs",
    "pitcher_outs",
]

BATTER_MARKETS: List[str] = [
    "batter_hits",
    "batter_total_bases",
    "batter_home_runs",
    "batter_rbis",
    "batter_strikeouts",
    "batter_stolen_bases",
]

ALL_MARKETS: List[str] = PITCHER_MARKETS + BATTER_MARKETS

# Default books to include (free tier supports us region)
DEFAULT_REGIONS: str = "us"
DEFAULT_BOOKS: Optional[List[str]] = None  # None = all available books

# Throttle: max 2 req/s on free tier
_MIN_REQUEST_INTERVAL: float = 0.5

_log = logging.getLogger(__name__)


# ── Data Classes ──────────────────────────────────────────────────────────────

@dataclass
class PropLine:
    """A single over/under prop line from one sportsbook."""
    game_id: str
    game_date: date
    player_name: str
    player_type: str              # "pitcher" | "batter"
    prop_type: str                # e.g. "pitcher_strikeouts"
    sportsbook: str
    over_line: Optional[float]    # e.g. 5.5
    under_line: Optional[float]   # e.g. 5.5 (usually same)
    over_juice: Optional[int]     # American odds, e.g. -115
    under_juice: Optional[int]    # American odds, e.g. -105

    def to_pitcher_tuple(self) -> Tuple:
        """Returns tuple matching mlb_pitcher_props INSERT order."""
        return (
            self.game_id,
            str(self.game_date),
            self.player_name,
            self.prop_type,
            self.sportsbook,
            self.over_line,
            self.under_line,
            self.over_juice,
            self.under_juice,
        )

    def to_player_tuple(self) -> Tuple:
        """Returns tuple matching mlb_player_props INSERT order."""
        return (
            self.game_id,
            str(self.game_date),
            self.player_name,
            self.player_type,
            self.prop_type,
            self.sportsbook,
            self.over_line,
            self.under_line,
            self.over_juice,
            self.under_juice,
        )


@dataclass
class FetchSummary:
    """Summary of a fetch-and-store run."""
    events_fetched: int = 0
    api_calls_made: int = 0
    pitcher_props_stored: int = 0
    batter_props_stored: int = 0
    errors: List[str] = field(default_factory=list)
    key_rotations: int = 0
    quota_remaining: Optional[int] = None


# ── Core Service ──────────────────────────────────────────────────────────────

class PropClosingLinesFetcher:
    """
    Fetches MLB player prop closing lines from The Odds API and stores
    them into the agent SQL database.

    Parameters
    ──────────
    db_path : str
        Path to the SQLite database file.  Pass ":memory:" for unit tests.
    api_keys : list[str]
        Ordered list of API keys; rotates automatically on 429 / quota.
    markets : list[str]
        Prop markets to fetch.  Defaults to ALL_MARKETS.
    regions : str
        The Odds API regions param (default "us").
    books : list[str] | None
        Specific sportsbooks to filter; None fetches all available.
    request_delay : float
        Minimum seconds between API requests (rate limiting).
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        api_keys: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        regions: str = DEFAULT_REGIONS,
        books: Optional[List[str]] = DEFAULT_BOOKS,
        request_delay: float = _MIN_REQUEST_INTERVAL,
    ) -> None:
        self._db_path = db_path
        self._keys: List[str] = list(api_keys or _API_KEYS)
        self._key_idx: int = 0
        self._markets: List[str] = list(markets or ALL_MARKETS)
        self._regions = regions
        self._books = books
        self._delay = request_delay
        self._last_request_ts: float = 0.0
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

        # Cache a single connection for :memory: databases so tables persist
        # across all calls within the same instance.
        self._mem_conn: Optional[sqlite3.Connection] = None
        if db_path == ":memory:":
            self._mem_conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._mem_conn.row_factory = sqlite3.Row

        self._ensure_tables()

    # ── DB Helpers ─────────────────────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        if self._mem_conn is not None:
            return self._mem_conn
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_tables(self) -> None:
        """Create tables if they do not exist (idempotent)."""
        ddl_pitcher = """
        CREATE TABLE IF NOT EXISTS mlb_pitcher_props (
            game_id     TEXT,
            game_date   DATE,
            pitcher     TEXT,
            prop_type   TEXT,
            sportsbook  TEXT,
            over_line   REAL,
            under_line  REAL,
            over_juice  INTEGER,
            under_juice INTEGER,
            PRIMARY KEY (game_id, pitcher, prop_type, sportsbook)
        )
        """
        ddl_player = """
        CREATE TABLE IF NOT EXISTS mlb_player_props (
            game_id     TEXT,
            game_date   DATE,
            player_name TEXT,
            player_type TEXT,
            prop_type   TEXT,
            sportsbook  TEXT,
            over_line   REAL,
            under_line  REAL,
            over_juice  INTEGER,
            under_juice INTEGER,
            PRIMARY KEY (game_id, player_name, player_type, prop_type, sportsbook)
        )
        """
        with self._get_conn() as conn:
            conn.execute(ddl_pitcher)
            conn.execute(ddl_player)
            conn.commit()

    # ── API Helpers ────────────────────────────────────────────────────────

    @property
    def _current_key(self) -> str:
        return self._keys[self._key_idx]

    def _rotate_key(self) -> bool:
        """Advance to next available key.  Returns False if no more keys."""
        if self._key_idx < len(self._keys) - 1:
            self._key_idx += 1
            _log.warning("PropClosingLinesFetcher: rotated to key index %d", self._key_idx)
            return True
        return False

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_request_ts
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)
        self._last_request_ts = time.time()

    def _get(self, url: str, params: Dict, summary: FetchSummary) -> Optional[Dict]:
        """
        Perform a rate-limited GET, rotating keys on 429.
        Returns parsed JSON or None on unrecoverable error.
        """
        params = dict(params)  # copy
        for attempt in range(len(self._keys)):
            self._throttle()
            params["apiKey"] = self._current_key
            try:
                resp = self._session.get(url, params=params, timeout=15)
                summary.api_calls_made += 1

                # Update remaining quota header
                remaining = resp.headers.get("x-requests-remaining")
                if remaining is not None:
                    summary.quota_remaining = int(remaining)

                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code in (429, 401):
                    # Quota exceeded or key invalid → try next key
                    if not self._rotate_key():
                        summary.errors.append(f"All API keys exhausted (status {resp.status_code})")
                        return None
                    summary.key_rotations += 1
                elif resp.status_code == 422:
                    # Unsupported market or bad params
                    body = resp.json()
                    summary.errors.append(f"422 unprocessable: {body.get('message', 'unknown')}")
                    return None
                else:
                    summary.errors.append(f"HTTP {resp.status_code}: {url}")
                    return None
            except requests.RequestException as exc:
                summary.errors.append(f"Request error: {exc}")
                return None
        return None

    # ── Event Listing ──────────────────────────────────────────────────────

    def list_upcoming_events(self, summary: FetchSummary) -> List[Dict]:
        """Returns list of upcoming MLB event dicts from The Odds API."""
        url = f"{_BASE_URL}/sports/{_SPORT}/events"
        params = {"dateFormat": "iso"}
        data = self._get(url, params, summary)
        if data is None:
            return []
        events = data if isinstance(data, list) else []
        summary.events_fetched = len(events)
        return events

    # ── Market Fetching ────────────────────────────────────────────────────

    def fetch_event_props(
        self,
        event_id: str,
        game_date: date,
        markets: Optional[List[str]] = None,
        summary: Optional[FetchSummary] = None,
    ) -> List[PropLine]:
        """
        Fetch all prop lines for a single event.  Markets are batched into
        groups of 4 to stay within URL-length limits on the free tier.
        """
        if summary is None:
            summary = FetchSummary()
        target_markets = markets or self._markets
        url = f"{_BASE_URL}/sports/{_SPORT}/events/{event_id}/odds"
        base_params: Dict = {
            "regions": self._regions,
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
        if self._books:
            base_params["bookmakers"] = ",".join(self._books)

        all_lines: List[PropLine] = []

        # Batch markets into groups of 4 (avoids 414 Request-URI Too Long)
        batch_size = 4
        for i in range(0, len(target_markets), batch_size):
            batch = target_markets[i: i + batch_size]
            params = {**base_params, "markets": ",".join(batch)}
            data = self._get(url, params, summary)
            if data is None:
                continue
            lines = self._parse_event_odds(data, game_date)
            all_lines.extend(lines)

        return all_lines

    # ── Parsing ────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_event_odds(data: Dict, game_date: date) -> List[PropLine]:
        """
        Parse The Odds API event odds response into PropLine objects.

        Response shape:
          {
            "id": "<event_id>",
            "bookmakers": [
              { "key": "fanduel", "markets": [
                  { "key": "pitcher_strikeouts",
                    "outcomes": [
                      {"name": "Over",  "description": "Max Fried",
                       "price": -115,   "point": 5.5},
                      {"name": "Under", "description": "Max Fried",
                       "price": -105,   "point": 5.5},
                      ...
                    ]
                  }
                ]
              }
            ]
          }
        """
        game_id: str = data.get("id", "")
        lines: List[PropLine] = []

        for bookmaker in data.get("bookmakers", []):
            book_key: str = bookmaker.get("key", "")
            for market in bookmaker.get("markets", []):
                market_key: str = market.get("key", "")
                player_type = "pitcher" if market_key.startswith("pitcher") else "batter"

                # Group outcomes by player name
                player_outcomes: Dict[str, Dict] = {}
                for outcome in market.get("outcomes", []):
                    pname: str = outcome.get("description", "Unknown")
                    side: str = outcome.get("name", "").lower()  # "over" / "under"
                    price: Optional[float] = outcome.get("price")
                    point: Optional[float] = outcome.get("point")

                    if pname not in player_outcomes:
                        player_outcomes[pname] = {
                            "over_line": None,
                            "under_line": None,
                            "over_juice": None,
                            "under_juice": None,
                        }

                    if side == "over":
                        player_outcomes[pname]["over_line"] = point
                        player_outcomes[pname]["over_juice"] = (
                            int(price) if price is not None else None
                        )
                    elif side == "under":
                        player_outcomes[pname]["under_line"] = point
                        player_outcomes[pname]["under_juice"] = (
                            int(price) if price is not None else None
                        )

                for pname, oc in player_outcomes.items():
                    lines.append(
                        PropLine(
                            game_id=game_id,
                            game_date=game_date,
                            player_name=pname,
                            player_type=player_type,
                            prop_type=market_key,
                            sportsbook=book_key,
                            over_line=oc["over_line"],
                            under_line=oc["under_line"],
                            over_juice=oc["over_juice"],
                            under_juice=oc["under_juice"],
                        )
                    )

        return lines

    # ── DB Writes ──────────────────────────────────────────────────────────

    def store_lines(self, lines: List[PropLine], summary: FetchSummary) -> None:
        """
        Upsert PropLine records into the appropriate DB tables.
        Pitcher markets → mlb_pitcher_props
        Batter markets  → mlb_player_props
        """
        pitcher_rows: List[Tuple] = []
        batter_rows: List[Tuple] = []

        for pl in lines:
            if pl.player_type == "pitcher":
                pitcher_rows.append(pl.to_pitcher_tuple())
            else:
                batter_rows.append(pl.to_player_tuple())

        sql_pitcher = """
        INSERT OR REPLACE INTO mlb_pitcher_props
            (game_id, game_date, pitcher, prop_type, sportsbook,
             over_line, under_line, over_juice, under_juice)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        sql_player = """
        INSERT OR REPLACE INTO mlb_player_props
            (game_id, game_date, player_name, player_type, prop_type, sportsbook,
             over_line, under_line, over_juice, under_juice)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        with self._get_conn() as conn:
            if pitcher_rows:
                conn.executemany(sql_pitcher, pitcher_rows)
                summary.pitcher_props_stored += len(pitcher_rows)
            if batter_rows:
                conn.executemany(sql_player, batter_rows)
                summary.batter_props_stored += len(batter_rows)
            conn.commit()

    # ── Public API ─────────────────────────────────────────────────────────

    def fetch_and_store_all(
        self,
        target_date: Optional[date] = None,
        markets: Optional[List[str]] = None,
    ) -> FetchSummary:
        """
        Full pipeline: list events → fetch props per event → store.

        Parameters
        ──────────
        target_date : date | None
            If provided, only processes events on that calendar date
            (matched against commence_time UTC).
        markets : list[str] | None
            Override which markets to fetch; defaults to self._markets.

        Returns
        ───────
        FetchSummary with counts, errors, and remaining API quota.
        """
        summary = FetchSummary()
        events = self.list_upcoming_events(summary)

        for event in events:
            event_id: str = event.get("id", "")
            commence_raw: str = event.get("commence_time", "")
            try:
                game_dt = datetime.fromisoformat(
                    commence_raw.replace("Z", "+00:00")
                ).date()
            except (ValueError, AttributeError):
                game_dt = date.today()

            if target_date is not None and game_dt != target_date:
                continue

            lines = self.fetch_event_props(
                event_id=event_id,
                game_date=game_dt,
                markets=markets,
                summary=summary,
            )
            if lines:
                self.store_lines(lines, summary)
                _log.info(
                    "Stored %d prop lines for event %s (%s)",
                    len(lines), event_id, game_dt,
                )

        return summary

    # ── Query Helpers ──────────────────────────────────────────────────────

    def query_pitcher_props(
        self,
        pitcher: Optional[str] = None,
        prop_type: str = "pitcher_strikeouts",
        game_date: Optional[date] = None,
        sportsbook: Optional[str] = None,
    ) -> List[Dict]:
        """
        Query stored pitcher props with optional filters.
        Returns list of dicts with all columns.
        """
        clauses: List[str] = ["prop_type = ?"]
        params: List = [prop_type]

        if pitcher:
            clauses.append("pitcher = ?")
            params.append(pitcher)
        if game_date:
            clauses.append("game_date = ?")
            params.append(str(game_date))
        if sportsbook:
            clauses.append("sportsbook = ?")
            params.append(sportsbook)

        sql = f"""
        SELECT * FROM mlb_pitcher_props
        WHERE {" AND ".join(clauses)}
        ORDER BY game_date DESC, pitcher ASC
        """
        with self._get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def query_player_props(
        self,
        player_name: Optional[str] = None,
        prop_type: Optional[str] = None,
        player_type: Optional[str] = None,
        game_date: Optional[date] = None,
        sportsbook: Optional[str] = None,
    ) -> List[Dict]:
        """
        Query stored player props with optional filters.
        Returns list of dicts with all columns.
        """
        clauses: List[str] = []
        params: List = []

        if player_name:
            clauses.append("player_name = ?")
            params.append(player_name)
        if prop_type:
            clauses.append("prop_type = ?")
            params.append(prop_type)
        if player_type:
            clauses.append("player_type = ?")
            params.append(player_type)
        if game_date:
            clauses.append("game_date = ?")
            params.append(str(game_date))
        if sportsbook:
            clauses.append("sportsbook = ?")
            params.append(sportsbook)

        where_clause = (
            f"WHERE {' AND '.join(clauses)}" if clauses else ""
        )
        sql = f"""
        SELECT * FROM mlb_player_props
        {where_clause}
        ORDER BY game_date DESC, player_name ASC
        """
        with self._get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def consensus_line(
        self,
        game_id: str,
        pitcher: str,
        prop_type: str = "pitcher_strikeouts",
    ) -> Optional[Dict]:
        """
        Returns median over/under line and no-vig fair probability
        across all books for a given pitcher + game + prop.

        No-vig formula:
          p_over  = implied_over  / (implied_over + implied_under)
          p_under = 1 - p_over
          where implied = 1 / decimal_odds
        """
        rows = self.query_pitcher_props(
            pitcher=pitcher, prop_type=prop_type
        )
        rows = [r for r in rows if r["game_id"] == game_id]
        if not rows:
            return None

        over_juices = [r["over_juice"] for r in rows if r["over_juice"] is not None]
        under_juices = [r["under_juice"] for r in rows if r["under_juice"] is not None]
        over_lines = [r["over_line"] for r in rows if r["over_line"] is not None]

        if not over_lines:
            return None

        # Median line
        sorted_lines = sorted(over_lines)
        n = len(sorted_lines)
        median_line = (
            sorted_lines[n // 2]
            if n % 2 == 1
            else (sorted_lines[n // 2 - 1] + sorted_lines[n // 2]) / 2
        )

        # No-vig probability using average book juice
        def american_to_decimal(american: int) -> float:
            if american >= 0:
                return (american / 100) + 1.0
            return (100 / abs(american)) + 1.0

        def no_vig_prob(over_j: List[int], under_j: List[int]) -> Optional[float]:
            if not over_j or not under_j:
                return None
            avg_over = sum(over_j) / len(over_j)
            avg_under = sum(under_j) / len(under_j)
            d_over = american_to_decimal(int(avg_over))
            d_under = american_to_decimal(int(avg_under))
            imp_over = 1 / d_over
            imp_under = 1 / d_under
            total = imp_over + imp_under
            if total == 0:
                return None
            return round(imp_over / total, 4)

        prob_over = no_vig_prob(over_juices, under_juices)

        return {
            "game_id": game_id,
            "pitcher": pitcher,
            "prop_type": prop_type,
            "books": len(rows),
            "median_line": median_line,
            "prob_over": prob_over,
            "prob_under": round(1 - prob_over, 4) if prob_over is not None else None,
        }

    def closing_line_value(
        self,
        game_id: str,
        pitcher: str,
        your_line: float,
        your_juice: int,
        prop_type: str = "pitcher_strikeouts",
    ) -> Optional[Dict]:
        """
        Calculate Closing Line Value (CLV) for a bet already placed.

        CLV = your_implied_prob - consensus_prob_over
        Positive CLV means you beat the closing line (edge confirmation).

        Parameters
        ──────────
        your_line  : the over/under total you bet
        your_juice : the American odds price you received
        """
        consensus = self.consensus_line(game_id, pitcher, prop_type)
        if consensus is None or consensus["prob_over"] is None:
            return None
        if consensus["median_line"] != your_line:
            return {
                "clv": None,
                "note": f"Line mismatch: consensus {consensus['median_line']} vs your {your_line}",
            }

        def american_to_decimal(american: int) -> float:
            if american >= 0:
                return (american / 100) + 1.0
            return (100 / abs(american)) + 1.0

        your_decimal = american_to_decimal(your_juice)
        your_prob = round(1 / your_decimal, 4)
        clv = round(your_prob - consensus["prob_over"], 4)

        return {
            "game_id": game_id,
            "pitcher": pitcher,
            "prop_type": prop_type,
            "your_line": your_line,
            "your_juice": your_juice,
            "your_prob": your_prob,
            "consensus_prob_over": consensus["prob_over"],
            "clv": clv,
            "clv_signal": "positive" if clv > 0 else "negative",
        }

    # ── PropIQ Integration ─────────────────────────────────────────────────

    def to_propiq_format(
        self,
        lines: List[PropLine],
    ) -> List[Dict]:
        """
        Convert PropLine list to PropIQ-compatible dicts that plug into
        the existing MarketFusionEngine / EVHunter pipeline.

        Output shape matches the OddsFetcher `fetch_aggregated_odds()` schema.
        """
        result: List[Dict] = []
        for pl in lines:
            if pl.over_line is None:
                continue
            # Convert American juice to implied probability
            def amer_to_prob(j: Optional[int]) -> Optional[float]:
                if j is None:
                    return None
                if j >= 0:
                    return round(100 / (j + 100), 4)
                return round(abs(j) / (abs(j) + 100), 4)

            result.append({
                "game_id": pl.game_id,
                "game_date": str(pl.game_date),
                "player_name": pl.player_name,
                "player_type": pl.player_type,
                "prop_type": pl.prop_type,
                "sportsbook": pl.sportsbook,
                "line": pl.over_line,
                "over_odds": pl.over_juice,
                "under_odds": pl.under_juice,
                "prob_over": amer_to_prob(pl.over_juice),
                "prob_under": amer_to_prob(pl.under_juice),
                "source": "prop_closing_lines",
            })
        return result
