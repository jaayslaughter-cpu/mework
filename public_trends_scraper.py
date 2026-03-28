"""
public_trends_scraper.py -- PropIQ Analytics: SportsBettingDime Public Betting Trends
======================================================================================
Fetches real BET% and MONEY% (ticket + stake percentages) from SportsBettingDime's
internal WordPress REST API (/wp-json/adpt/v1/) -- no Apify, no auth required.

Provides two data layers:
  1. Game-level splits:   moneyline/spread/total Over-Under bets% + money%
  2. Player-level splits: per-prop bets% + money% (populated day-of by books)

FadeAgent integration:
  from public_trends_scraper import enrich_props_with_public_trends
  props = enrich_props_with_public_trends(props)
  # Each prop now has: sbd_game_over_bets_pct, sbd_game_over_money_pct,
  #                    sbd_prop_over_bets_pct, sbd_prop_over_money_pct,
  #                    sbd_home_ml_bets_pct,   sbd_home_ml_money_pct
"""
from __future__ import annotations

import http.cookiejar
import json
import logging
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("public_trends_scraper")

# ── Constants ─────────────────────────────────────────────────────────────────
CACHE_DIR = Path(os.getenv("PROPIQ_CACHE_DIR", "cache/sbd"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.sportsbettingdime.com"
WARM_UP_URL = f"{BASE_URL}/mlb/public-betting-trends/"
SCHEDULE_URL = f"{BASE_URL}/wp-json/adpt/v1/mlb/schedule"
SPORT_EVENT_URL = f"{BASE_URL}/wp-json/adpt/v1/sport-event/mlb"
PLAYER_PROPS_URL = f"{BASE_URL}/wp-json/adpt/v1/player-props/mlb"

# Books to query -- covers >90% of public handle
BOOKS = "fanduel,draftkings,betmgm,caesars,pointsbet,betonlineag,bovada"

MAX_DAILY_FETCHES = 3          # Circuit breaker -- hard ceiling per day
MIN_DELAY = 1.5                # Seconds -- minimum jitter delay
MAX_DELAY = 4.0                # Seconds -- maximum jitter delay
BACKOFF_SEQUENCE = [30, 90, 270]  # Seconds -- on 429/503

_USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 OPR/107.0.0.0",
]

# ── Fetch counter (per-process daily cap) ─────────────────────────────────────
_fetch_count: dict[str, int] = {}   # {"2026-03-22": 2}


def _today() -> str:
    return date.today().isoformat()


def _check_cap() -> bool:
    """Return True if we are still under the daily fetch cap."""
    today = _today()
    return _fetch_count.get(today, 0) < MAX_DAILY_FETCHES


def _increment_cap() -> None:
    today = _today()
    _fetch_count[today] = _fetch_count.get(today, 0) + 1


# ── HTTP session ───────────────────────────────────────────────────────────────
class _Session:
    """Persistent HTTP session with CookieJar + rotating User-Agent."""

    def __init__(self) -> None:
        self._cj = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._cj)
        )
        self._warmed = False

    def _headers(self) -> list[tuple[str, str]]:
        return [
            ("User-Agent", random.choice(_USER_AGENTS)),
            ("Accept", "application/json, text/plain, */*"),
            ("Accept-Language", "en-US,en;q=0.9"),
            ("Accept-Encoding", "gzip, deflate, br"),
            ("Connection", "keep-alive"),
            ("Referer", WARM_UP_URL),
            ("Origin", BASE_URL),
            ("Sec-Fetch-Dest", "empty"),
            ("Sec-Fetch-Mode", "cors"),
            ("Sec-Fetch-Site", "same-origin"),
            ("DNT", "1"),
        ]

    def _warm_up(self) -> None:
        """Hit the HTML page first to seed session cookies."""
        if self._warmed:
            return
        try:
            logger.info("[SBD] Warming up session cookies…")
            html_headers = [
                ("User-Agent", random.choice(_USER_AGENTS)),
                ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
                ("Accept-Language", "en-US,en;q=0.9"),
                ("Referer", "https://www.google.com/"),
                ("Sec-Fetch-Site", "none"),
                ("Sec-Fetch-Mode", "navigate"),
                ("Upgrade-Insecure-Requests", "1"),
            ]
            self._opener.addheaders = html_headers
            self._opener.open(WARM_UP_URL, timeout=12)
            self._warmed = True
            _sleep()
        except Exception as exc:
            logger.warning("[SBD] Warm-up failed (non-fatal): %s", exc)

    def get_json(self, url: str, retries: int = 3) -> Any | None:
        """GET a JSON endpoint with retry + backoff on 429/503."""
        self._warm_up()
        self._opener.addheaders = self._headers()

        for attempt in range(retries):
            try:
                resp = self._opener.open(url, timeout=12)
                raw = resp.read()
                _sleep()
                return json.loads(raw.decode("utf-8", errors="replace"))
            except urllib.error.HTTPError as exc:
                if exc.code == 403:
                    logger.warning("[SBD] HTTP 403 on %s -- aborting", url)
                    return None
                if exc.code in (429, 503) and attempt < retries - 1:
                    wait = BACKOFF_SEQUENCE[min(attempt, len(BACKOFF_SEQUENCE) - 1)]
                    logger.warning(
                        "[SBD] HTTP %d -- backing off %ds (attempt %d/%d)",
                        exc.code, wait, attempt + 1, retries,
                    )
                    time.sleep(wait)
                    continue
                logger.error("[SBD] HTTP %d on %s", exc.code, url)
                return None
            except Exception as exc:
                logger.warning("[SBD] Request error on %s: %s", url, exc)
                if attempt < retries - 1:
                    time.sleep(BACKOFF_SEQUENCE[0])
                return None
        return None


def _sleep() -> None:
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


# ── Core fetchers ─────────────────────────────────────────────────────────────

def _fetch_schedule(session: _Session) -> list[dict]:
    """Return today's MLB schedule with event IDs and team matchups."""
    data = session.get_json(SCHEDULE_URL)
    if not data:
        return []
    games = data.get("data", [])
    today_iso = _today()
    # Filter to today's games only
    today_games = []
    for g in games:
        scheduled = g.get("scheduled", "")
        if today_iso in scheduled:
            today_games.append(g)
    logger.info("[SBD] Schedule: %d games today (%s total)", len(today_games), len(games))
    return today_games if today_games else games[:15]  # fallback: take first 15


def _fetch_game_splits(
    session: _Session,
    event_id: str,
) -> dict[str, Any]:
    """Return public betting splits for a single game.

    Returns dict with keys:
        moneyline_home_bets, moneyline_home_money,
        moneyline_away_bets, moneyline_away_money,
        total_over_bets,  total_over_money,
        total_under_bets, total_under_money,
        spread_home_bets, spread_home_money,
        spread_away_bets, spread_away_money,
    All values are floats 0-100 (percent) or None if unavailable.
    """
    url = f"{SPORT_EVENT_URL}/{urllib.parse.quote(event_id, safe=':')}?books={BOOKS}"
    data = session.get_json(url)
    if not data:
        return {}

    markets = data.get("data", {}).get("markets", {})

    def _pct(market: str, side: str, field: str) -> float | None:
        books = markets.get(market, {}).get("books", [])
        if not books:
            return None
        # Aggregate across books by averaging betsPercentage/stakePercentage
        vals = []
        for book in books:
            sides_data = book.get(side, {})
            if sides_data and field in sides_data:
                try:
                    vals.append(float(sides_data[field]))
                except (TypeError, ValueError):
                    pass
        return round(sum(vals) / len(vals), 1) if vals else None

    # Also try the direct nested structure (non-aggregated endpoint format)
    def _pct_direct(market_key: str, side_key: str, field: str) -> float | None:
        m = markets.get(market_key, {})
        s = m.get(side_key, {})
        if isinstance(s, dict):
            try:
                return float(s.get(field, 0) or 0) or None
            except (TypeError, ValueError):
                pass
        return _pct(market_key, side_key, field)

    return {
        "moneyline_home_bets":  _pct_direct("moneyline", "home", "betsPercentage"),
        "moneyline_home_money": _pct_direct("moneyline", "home", "stakePercentage"),
        "moneyline_away_bets":  _pct_direct("moneyline", "away", "betsPercentage"),
        "moneyline_away_money": _pct_direct("moneyline", "away", "stakePercentage"),
        "total_over_bets":      _pct_direct("total", "over",  "betsPercentage"),
        "total_over_money":     _pct_direct("total", "over",  "stakePercentage"),
        "total_under_bets":     _pct_direct("total", "under", "betsPercentage"),
        "total_under_money":    _pct_direct("total", "under", "stakePercentage"),
        "spread_home_bets":     _pct_direct("spread", "home", "betsPercentage"),
        "spread_home_money":    _pct_direct("spread", "home", "stakePercentage"),
        "spread_away_bets":     _pct_direct("spread", "away", "betsPercentage"),
        "spread_away_money":    _pct_direct("spread", "away", "stakePercentage"),
    }


def _fetch_player_props_splits(session: _Session) -> list[dict]:
    """Return player-level prop public betting splits.

    Returns list of dicts:
        {player_name, team, prop_type, line,
         prop_over_bets_pct, prop_over_money_pct,
         prop_under_bets_pct, prop_under_money_pct}
    """
    url = f"{PLAYER_PROPS_URL}?books={BOOKS}"
    data = session.get_json(url)
    if not data:
        return []

    records = []
    for player_entry in data.get("data", []):
        player_info = player_entry.get("player", {})
        player_name = (
            f"{player_info.get('first_name', '')} {player_info.get('last_name', '')}".strip()
            or player_info.get("name", "")
        )
        team_abbr = player_info.get("team_abbreviation", "")

        for market in player_entry.get("markets", []):
            market_type = market.get("market_type", "")
            line = market.get("line")

            over_bets = over_money = under_bets = under_money = None
            for outcome in market.get("outcomes", []):
                side = outcome.get("name", "").lower()
                bets = outcome.get("betsPercentage")
                money = outcome.get("stakePercentage")
                if "over" in side:
                    over_bets = float(bets) if bets is not None else None
                    over_money = float(money) if money is not None else None
                elif "under" in side:
                    under_bets = float(bets) if bets is not None else None
                    under_money = float(money) if money is not None else None

            if any(v is not None for v in [over_bets, over_money, under_bets, under_money]):
                records.append({
                    "player_name": player_name,
                    "team": team_abbr,
                    "prop_type": market_type,
                    "line": float(line) if line is not None else None,
                    "prop_over_bets_pct": over_bets,
                    "prop_over_money_pct": over_money,
                    "prop_under_bets_pct": under_bets,
                    "prop_under_money_pct": under_money,
                })

    logger.info("[SBD] Player prop splits: %d records", len(records))
    return records


# ── Main public API ────────────────────────────────────────────────────────────

class PublicTrendsScraper:
    """Daily public betting trends fetcher with full IP-ban protection.

    Usage:
        scraper = PublicTrendsScraper()
        game_df, prop_df = scraper.fetch()
    """

    def __init__(self) -> None:
        self._session = _Session()

    def _cache_path(self, name: str) -> Path:
        return CACHE_DIR / f"sbd_{name}_{_today()}.parquet"

    def _load_cache(self, name: str) -> pd.DataFrame | None:
        path = self._cache_path(name)
        if path.exists():
            logger.info("[SBD] Loading %s from cache: %s", name, path.name)
            return pd.read_parquet(path)
        return None

    def _save_cache(self, df: pd.DataFrame, name: str) -> None:
        path = self._cache_path(name)
        df.to_parquet(path, index=False)
        logger.info("[SBD] Saved %s cache: %s (%d rows)", name, path.name, len(df))

    def fetch(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Fetch public betting trends. Returns (game_df, prop_df).

        game_df columns:
            event_id, home_team, away_team, scheduled,
            moneyline_home_bets, moneyline_home_money,
            moneyline_away_bets, moneyline_away_money,
            total_over_bets, total_over_money,
            total_under_bets, total_under_money,
            spread_home_bets, spread_home_money,
            spread_away_bets, spread_away_money

        prop_df columns:
            player_name, team, prop_type, line,
            prop_over_bets_pct, prop_over_money_pct,
            prop_under_bets_pct, prop_under_money_pct
        """
        # Check caches first
        game_cached = self._load_cache("games")
        prop_cached = self._load_cache("props")
        if game_cached is not None and prop_cached is not None:
            return game_cached, prop_cached

        # Daily fetch cap check
        if not _check_cap():
            logger.warning("[SBD] Daily fetch cap reached -- returning empty DataFrames")
            return pd.DataFrame(), pd.DataFrame()

        _increment_cap()
        logger.info("[SBD] Fetching live public betting trends (fetch %d/%d)…",
                    _fetch_count.get(_today(), 1), MAX_DAILY_FETCHES)

        # 1. Game-level splits
        games = _fetch_schedule(self._session)
        game_rows = []
        for g in games:
            event_id = g.get("id", "")
            competitors = g.get("competitors", {})
            home = competitors.get("home", {})
            away = competitors.get("away", {})
            splits = _fetch_game_splits(self._session, event_id)

            game_rows.append({
                "event_id": event_id,
                "home_team": home.get("abbreviation", home.get("name", "")),
                "away_team": away.get("abbreviation", away.get("name", "")),
                "home_team_full": home.get("name", ""),
                "away_team_full": away.get("name", ""),
                "scheduled": g.get("scheduled", ""),
                **splits,
            })

        game_df = pd.DataFrame(game_rows)
        logger.info("[SBD] Game splits: %d games fetched", len(game_df))

        # 2. Player-level prop splits
        prop_records = _fetch_player_props_splits(self._session)
        prop_df = pd.DataFrame(prop_records) if prop_records else pd.DataFrame(columns=[
            "player_name", "team", "prop_type", "line",
            "prop_over_bets_pct", "prop_over_money_pct",
            "prop_under_bets_pct", "prop_under_money_pct",
        ])

        # Cache results
        if not game_df.empty:
            self._save_cache(game_df, "games")
        if not prop_df.empty:
            self._save_cache(prop_df, "props")
        else:
            # Save empty to suppress re-fetching
            self._save_cache(prop_df, "props")

        return game_df, prop_df


# ── Drop-in enrichment hook ────────────────────────────────────────────────────

def enrich_props_with_public_trends(
    props: pd.DataFrame,
    game_df: pd.DataFrame | None = None,
    prop_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Enrich a props DataFrame with SportsBettingDime public betting percentages.

    Adds columns to props:
        sbd_game_over_bets_pct   -- % tickets on game total Over
        sbd_game_over_money_pct  -- % money on game total Over
        sbd_game_under_bets_pct  -- % tickets on game total Under
        sbd_game_under_money_pct -- % money on game total Under
        sbd_home_ml_bets_pct     -- % tickets on home team moneyline
        sbd_home_ml_money_pct    -- % money on home team moneyline
        sbd_prop_over_bets_pct   -- % tickets on player's specific prop Over (when available)
        sbd_prop_over_money_pct  -- % money on player's specific prop Over (when available)

    Args:
        props: DataFrame with at minimum 'player_name' (or 'player') and 'team' columns.
        game_df: Pre-fetched game splits (optional, fetches if None).
        prop_df: Pre-fetched prop splits (optional, fetches if None).
    """
    if props.empty:
        return props

    if game_df is None or prop_df is None:
        scraper = PublicTrendsScraper()
        fetched_game, fetched_prop = scraper.fetch()
        game_df = fetched_game if game_df is None else game_df
        prop_df = fetched_prop if prop_df is None else prop_df

    # Normalise player name column
    player_col = "player_name" if "player_name" in props.columns else "player"
    props = props.copy()

    # ── Merge game-level splits via team ──────────────────────────────────────
    if not game_df.empty and "home_team" in game_df.columns:
        team_col = "team" if "team" in props.columns else None

        # Build team -> game lookup (home and away)
        home_lookup: dict[str, dict] = {}
        away_lookup: dict[str, dict] = {}
        for _, row in game_df.iterrows():
            ht = str(row.get("home_team", "")).upper()
            at = str(row.get("away_team", "")).upper()
            game_info = {
                "sbd_game_over_bets_pct":   row.get("total_over_bets"),
                "sbd_game_over_money_pct":  row.get("total_over_money"),
                "sbd_game_under_bets_pct":  row.get("total_under_bets"),
                "sbd_game_under_money_pct": row.get("total_under_money"),
                "sbd_home_ml_bets_pct":     row.get("moneyline_home_bets"),
                "sbd_home_ml_money_pct":    row.get("moneyline_home_money"),
            }
            if ht:
                home_lookup[ht] = game_info
            if at:
                away_lookup[at] = game_info

        sbd_cols = [
            "sbd_game_over_bets_pct", "sbd_game_over_money_pct",
            "sbd_game_under_bets_pct", "sbd_game_under_money_pct",
            "sbd_home_ml_bets_pct", "sbd_home_ml_money_pct",
        ]
        for col in sbd_cols:
            props[col] = None

        if team_col:
            for idx, row in props.iterrows():
                team = str(row.get(team_col, "")).upper()
                game_info = home_lookup.get(team) or away_lookup.get(team)
                if game_info:
                    for col, val in game_info.items():
                        props.at[idx, col] = val

    # ── Merge player-level prop splits ────────────────────────────────────────
    props["sbd_prop_over_bets_pct"] = None
    props["sbd_prop_over_money_pct"] = None

    if not prop_df.empty and player_col in props.columns:
        prop_type_col = "prop_type" if "prop_type" in props.columns else None

        for idx, row in props.iterrows():
            player = str(row.get(player_col, "")).lower()
            if not player:
                continue

            # Match on last name (fuzzy) + prop_type if available
            matches = prop_df[
                prop_df["player_name"].str.lower().str.contains(
                    player.split()[-1] if player.split() else player,
                    na=False
                )
            ]
            if prop_type_col and not matches.empty:
                prop_t = str(row.get(prop_type_col, "")).lower()
                type_matches = matches[
                    matches["prop_type"].str.lower().str.contains(prop_t[:4], na=False)
                ]
                if not type_matches.empty:
                    matches = type_matches

            if not matches.empty:
                best = matches.iloc[0]
                props.at[idx, "sbd_prop_over_bets_pct"] = best.get("prop_over_bets_pct")
                props.at[idx, "sbd_prop_over_money_pct"] = best.get("prop_over_money_pct")

    enriched = sum(
        1 for _, r in props.iterrows()
        if r.get("sbd_game_over_bets_pct") is not None
        or r.get("sbd_prop_over_bets_pct") is not None
    )
    logger.info(
        "[SBD] Enriched %d/%d props with public trends data",
        enriched, len(props),
    )
    return props


# ── FadeAgent-specific helper ──────────────────────────────────────────────────

def get_fade_signal(
    player: str,
    team: str,
    prop_type: str,
    game_df: pd.DataFrame,
    prop_df: pd.DataFrame,
    threshold: float = 65.0,
) -> tuple[float, str]:
    """Return (public_over_pct, signal_source) for FadeAgent decisions.

    Precedence:
      1. Player-level prop bets% (most precise)
      2. Game-level total Over bets% (fallback)
      3. 0.0, 'none' (no data)

    FadeAgent should fade when public_over_pct >= threshold.
    """
    player_lower = player.lower()
    last_name = player_lower.split()[-1] if player_lower.split() else player_lower

    # 1. Player-level prop match
    if not prop_df.empty:
        pt_short = prop_type.lower()[:4]
        matches = prop_df[
            prop_df["player_name"].str.lower().str.contains(last_name, na=False)
            & prop_df["prop_type"].str.lower().str.contains(pt_short, na=False)
        ]
        if not matches.empty:
            val = matches.iloc[0].get("prop_over_bets_pct")
            if val is not None:
                return float(val), "player_prop"

    # 2. Game-level total Over match
    if not game_df.empty:
        team_upper = team.upper()
        mask = (
            (game_df["home_team"].str.upper() == team_upper)
            | (game_df["away_team"].str.upper() == team_upper)
        )
        rows = game_df[mask]
        if not rows.empty:
            val = rows.iloc[0].get("total_over_bets")
            if val is not None:
                return float(val), "game_total"

    return 0.0, "none"


# ── CLI smoke test ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("PropIQ -- SportsBettingDime Public Trends Scraper")
    print("=" * 60)

    scraper = PublicTrendsScraper()
    game_df, prop_df = scraper.fetch()

    print(f"\n✅ Game splits: {len(game_df)} games")
    if not game_df.empty:
        display_cols = [
            "home_team", "away_team",
            "total_over_bets", "total_over_money",
            "moneyline_home_bets", "moneyline_home_money",
        ]
        available = [c for c in display_cols if c in game_df.columns]
        print(game_df[available].to_string(index=False))

    print(f"\n✅ Player prop splits: {len(prop_df)} records")
    if not prop_df.empty:
        print(prop_df.head(10).to_string(index=False))
    else:
        print("   (Props populate day-of when books post player lines)")

    # Test FadeAgent helper with sample data
    print("\n--- FadeAgent signal test ---")
    test_players = [
        ("Shohei Ohtani", "LAD", "hits"),
        ("Aaron Judge",   "NYY", "home_runs"),
        ("Freddie Freeman", "LAD", "rbi"),
    ]
    for player, team, pt in test_players:
        pct, source = get_fade_signal(player, team, pt, game_df, prop_df)
        flag = "🚩 FADE" if pct >= 65.0 else "✅ pass"
        print(f"  {flag}  {player:<20} {pt:<12} over_bets={pct:.1f}%  source={source}")
