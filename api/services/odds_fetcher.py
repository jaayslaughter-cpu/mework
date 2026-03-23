"""
PropIQ Analytics — OddsFetcher
===============================
Rate-limited, cached, multi-key fetcher for The Odds API v4.
- Exponential backoff with jitter
- Per-minute + per-day quota tracking
- In-memory cache with TTL
- Both API keys with automatic rotation
- Player prop batching (one event at a time per API rules)

Drop this into: api/services/odds_fetcher.py
"""

import os
import time
import json
import random
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
API_KEYS = [
    os.getenv("ODDS_API_KEY_1", "e4e30098807a9eece674d85e30471f03"),
    os.getenv("ODDS_API_KEY_2", "673bf195062e60e666399be40f763545"),
]
BASE_URL = "https://api.the-odds-api.com/v4"
CACHE_TTL_SECONDS = int(os.getenv("ODDS_CACHE_TTL", "120"))   # 2 min default
MAX_RETRIES = 4
BASE_BACKOFF = 2.0   # seconds


# ─────────────────────────────────────────────
# In-memory cache
# ─────────────────────────────────────────────
class SimpleCache:
    def __init__(self, ttl: int = CACHE_TTL_SECONDS):
        self._store: Dict[str, Tuple[float, Any]] = {}
        self.ttl = ttl

    @staticmethod
    def _cache_key(url: str, params: Dict) -> str:
        raw = url + json.dumps(params, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, url: str, params: Dict) -> Optional[Any]:
        key = self._cache_key(url, params)
        if key in self._store:
            ts, val = self._store[key]
            if time.time() - ts < self.ttl:
                return val
            del self._store[key]
        return None

    def set(self, url: str, params: Dict, value: Any):
        key = self._cache_key(url, params)
        self._store[key] = (time.time(), value)

    def invalidate_all(self):
        self._store.clear()


# ─────────────────────────────────────────────
# Rate limiter
# ─────────────────────────────────────────────
class RateLimiter:
    """
    Tracks requests per minute and per day for each API key.
    Rotates to backup key when primary is exhausted.
    """

    def __init__(self, per_minute: int = 25, per_day: int = 450):
        self.per_minute = per_minute
        self.per_day = per_day
        self._minute_counts: Dict[int, Dict[str, int]] = {}  # key -> {minute_bucket: count}
        self._day_counts: Dict[str, int] = {k: 0 for k in API_KEYS}
        self._remaining: Dict[str, int] = {k: per_day for k in API_KEYS}
        self._day_reset: Dict[str, datetime] = {k: datetime.utcnow() + timedelta(days=1) for k in API_KEYS}

    @staticmethod
    def _minute_bucket() -> int:
        return int(time.time() // 60)

    def can_request(self, api_key: str) -> bool:
        # Reset daily if needed
        if datetime.utcnow() > self._day_reset.get(api_key, datetime.utcnow()):
            self._day_counts[api_key] = 0
            self._day_reset[api_key] = datetime.utcnow() + timedelta(days=1)

        bucket = self._minute_bucket()
        minute_count = self._minute_counts.get(api_key, {}).get(bucket, 0)
        day_count = self._day_counts.get(api_key, 0)

        return minute_count < self.per_minute and day_count < self.per_day

    def record_request(self, api_key: str):
        bucket = self._minute_bucket()
        if api_key not in self._minute_counts:
            self._minute_counts[api_key] = {}
        self._minute_counts[api_key][bucket] = self._minute_counts[api_key].get(bucket, 0) + 1
        self._day_counts[api_key] = self._day_counts.get(api_key, 0) + 1

    def update_from_headers(self, api_key: str, headers: Dict):
        """Parse X-RateLimit-Remaining headers."""
        remaining = headers.get("x-requests-remaining")
        if remaining:
            try:
                self._remaining[api_key] = int(remaining)
            except ValueError:
                pass

    def best_available_key(self) -> Optional[str]:
        """Return the key with the most remaining quota."""
        for key in API_KEYS:
            if self.can_request(key):
                return key
        return None

    def status(self) -> Dict:
        return {
            key: {
                "remaining_day": self._remaining.get(key, "?"),
                "used_today": self._day_counts.get(key, 0),
            }
            for key in API_KEYS
        }


# ─────────────────────────────────────────────
# OddsFetcher
# ─────────────────────────────────────────────
class OddsFetcher:
    """
    Main class for fetching live odds and player props.
    Usage:
        fetcher = OddsFetcher()
        games = fetcher.get_mlb_games()
        props = fetcher.get_player_props(event_id, ["batter_hits", "batter_strikeouts"])
    """

    PROP_MARKETS = [
        "batter_hits",
        "batter_total_bases",
        "batter_rbis",
        "batter_runs_scored",
        "batter_hits_runs_rbis",
        "batter_strikeouts",
        "batter_home_runs",
        "batter_singles",
        "batter_doubles",
        "batter_triples",
        "batter_walks",
        "pitcher_strikeouts",
        "pitcher_hits_allowed",
        "pitcher_walks",
        "pitcher_earned_runs",
        "pitcher_outs",
    ]

    GAME_MARKETS = ["h2h", "spreads", "totals"]

    def __init__(self):
        self.cache = SimpleCache()
        self.limiter = RateLimiter()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "PropIQ/2.0"})

    def _request(self, endpoint: str, params: Dict, use_cache: bool = True) -> Optional[Dict]:
        """
        Make a rate-limited, cached, retrying GET request.
        """
        url = f"{BASE_URL}{endpoint}"

        # Check cache first
        if use_cache:
            cached = self.cache.get(url, params)
            if cached is not None:
                logger.debug("Cache hit: %s", endpoint)
                return cached

        # Get best available API key
        api_key = self.limiter.best_available_key()
        if not api_key:
            logger.error("All API keys exhausted for today.")
            return None

        params["apiKey"] = api_key

        for attempt in range(MAX_RETRIES):
            try:
                self.limiter.record_request(api_key)
                resp = self.session.get(url, params=params, timeout=15)
                self.limiter.update_from_headers(api_key, dict(resp.headers))

                if resp.status_code == 200:
                    data = resp.json()
                    if use_cache:
                        self.cache.set(url, params, data)
                    return data

                elif resp.status_code == 422:
                    logger.error("Odds API 422 Unprocessable: %s", resp.text)
                    return None

                elif resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    logger.warning("Rate limited. Waiting %ss then rotating key.", retry_after)
                    time.sleep(retry_after)
                    # Try next key
                    new_key = self.limiter.best_available_key()
                    if new_key and new_key != api_key:
                        api_key = new_key
                        params["apiKey"] = api_key
                    continue

                else:
                    logger.warning("HTTP %s on attempt %s: %s", resp.status_code, attempt + 1, resp.text[:200])

            except requests.exceptions.Timeout:
                logger.warning("Timeout on attempt %s", attempt + 1)
            except requests.exceptions.ConnectionError as e:
                logger.warning("Connection error on attempt %s: %s", attempt + 1, e)
            except Exception as e:
                logger.error("Unexpected error: %s", e)

            # Exponential backoff with jitter
            wait = BASE_BACKOFF ** (attempt + 1) + random.uniform(0, 1)
            logger.info("Backing off %0.1fs before retry %s", wait, attempt + 2)
            time.sleep(wait)

        logger.error("All %s attempts failed for %s", MAX_RETRIES, endpoint)
        return None

    # ── Public methods ──────────────────────
    def get_mlb_games(self, regions: str = "us") -> List[Dict]:
        """Get all upcoming MLB games with game-level odds."""
        data = self._request(
            "/sports/baseball_mlb/odds",
            {"regions": regions, "markets": ",".join(self.GAME_MARKETS), "oddsFormat": "american"},
        )
        if not data:
            return []

        games = []
        for event in data:
            game = {
                "event_id": event.get("id"),
                "home_team": event.get("home_team"),
                "away_team": event.get("away_team"),
                "commence_time": event.get("commence_time"),
                "bookmakers": {},
            }
            for book in event.get("bookmakers", []):
                book_data = {}
                for market in book.get("markets", []):
                    mkey = market["key"]
                    outcomes = {o["name"]: o["price"] for o in market.get("outcomes", [])}
                    book_data[mkey] = outcomes
                game["bookmakers"][book["title"]] = book_data
            games.append(game)

        logger.info("Fetched %s MLB games", len(games))
        return games

    def get_mlb_events(self) -> List[Dict]:
        """Get list of events (for getting event IDs)."""
        data = self._request(
            "/sports/baseball_mlb/events",
            {"regions": "us"},
        )
        return data or []

    def get_player_props(
        self,
        event_id: str,
        markets: Optional[List[str]] = None,
        regions: str = "us",
    ) -> List[Dict]:
        """
        Get player props for a specific game.
        NOTE: Per Odds API rules, props MUST be fetched per-event.
        """
        if markets is None:
            markets = self.PROP_MARKETS[:6]   # Limit to avoid quota burns

        # Chunk markets to avoid URL length limits (max ~5 at a time)
        all_props = []
        chunks = [markets[i:i+5] for i in range(0, len(markets), 5)]

        for chunk in chunks:
            data = self._request(
                f"/sports/baseball_mlb/events/{event_id}/odds",
                {
                    "regions": regions,
                    "markets": ",".join(chunk),
                    "oddsFormat": "american",
                },
            )
            if not data:
                continue

            for book in data.get("bookmakers", []):
                for market in book.get("markets", []):
                    for outcome in market.get("outcomes", []):
                        all_props.append({
                            "event_id": event_id,
                            "book": book["title"],
                            "market": market["key"],
                            "player": outcome.get("description", outcome.get("name", "")),
                            "side": outcome["name"],   # "Over" or "Under"
                            "line": outcome.get("point", 0),
                            "odds": outcome["price"],
                            "implied_prob": self._implied_prob(outcome["price"]),
                            "last_update": market.get("last_update"),
                        })

            # Polite delay between chunks
            time.sleep(0.5)

        logger.info("Fetched %s prop lines for event %s", len(all_props), event_id)
        return all_props

    def get_all_mlb_props(
        self,
        markets: Optional[List[str]] = None,
        max_events: int = 15,
    ) -> Dict[str, List[Dict]]:
        """
        Fetch props for all active MLB events.
        Returns: {event_id: [props]}
        """
        events = self.get_mlb_events()
        result = {}

        for event in events[:max_events]:
            event_id = event["id"]
            props = self.get_player_props(event_id, markets)
            if props:
                result[event_id] = props
            time.sleep(1.0)   # 1s between events to stay under rate limits

        return result

    def get_consensus_line(
        self,
        player: str,
        prop_type: str,
        event_id: str,
    ) -> Optional[Dict]:
        """
        Get consensus (average) book line for a specific player prop.
        # Returns: {line, avg_over_prob, avg_under_prob, books}
        market_key = self._prop_type_to_market(prop_type)
        props = self.get_player_props(event_id, markets=[market_key])

        player_props = [
            p for p in props
            if player.lower() in p["player"].lower() and p["side"] == "Over"
        ]

        if not player_props:
            return None

        avg_line = sum(p["line"] for p in player_props) / len(player_props)
        avg_prob = sum(p["implied_prob"] for p in player_props) / len(player_props)
        books = list({p["book"] for p in player_props})

        return {
            "player": player,
            "prop_type": prop_type,
            "consensus_line": round(avg_line, 2),
            "avg_over_prob": round(avg_prob, 4),
            "books": books,
        }

    def get_api_status(self) -> Dict:
        """Return rate limit status for all keys."""
        return self.limiter.status()

    # ── Helpers ─────────────────────────────
    @staticmethod
    def _implied_prob(american_odds: int) -> float:
        if american_odds > 0:
            return round(100 / (american_odds + 100), 4)
        return round(abs(american_odds) / (abs(american_odds) + 100), 4)

    @staticmethod
    def _prop_type_to_market(prop_type: str) -> str:
        mapping = {
            "hits": "batter_hits",
            "total bases": "batter_total_bases",
            "home runs": "batter_home_runs",
            "hr": "batter_home_runs",
            "rbi": "batter_rbis",
            "rbis": "batter_rbis",
            "strikeouts": "batter_strikeouts",
            "walks": "batter_walks",
            "pitcher ks": "pitcher_strikeouts",
            "pitcher strikeouts": "pitcher_strikeouts",
            "earned runs": "pitcher_earned_runs",
        }
        return mapping.get(prop_type.lower(), "batter_hits")


# ─────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────
_fetcher_instance: Optional[OddsFetcher] = None


def get_fetcher() -> OddsFetcher:
    global _fetcher_instance
    if _fetcher_instance is None:
        _fetcher_instance = OddsFetcher()
    return _fetcher_instance
