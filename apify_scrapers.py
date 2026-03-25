"""apify_scrapers.py — PropIQ Analytics Data Enrichment Layer

Scrapes all external data sources and populates PropEdge context fields.
Results are written to Redis and consumed by ExecutionSquad._parse_prop_edge
at message-processing time.

Architecture
------------
                ┌─────────────────────────────────────────────────────┐
                │                  EnrichmentService                  │
                │  (staggered 5–15 min pre-match schedule)            │
                └────────────┬──────────────────────────┬────────────┘
                             │                          │
               ┌─────────────▼───────────┐  ┌──────────▼──────────────┐
               │      DataEnricher        │  │  SportsDataScheduleScraper│
               │  orchestrates all below  │  │  (rest / travel / TZ)    │
               └──┬──────┬────────┬──────┘  └─────────────────────────┘
                  │      │        │
     ┌────────────▼─┐  ┌─▼──────┐ ┌▼──────────────┐
     │ BaseballSavant│  │RotoWire│ │ ActionNetwork  │
     │   Client      │  │Scraper │ │   Scraper      │
     │ (direct CSV)  │  │(Apify) │ │  (Apify)       │
     └───────────────┘  └────────┘ └────────────────┘
                  │
        ┌─────────▼──────────────────────────────────────┐
        │               RedisEnrichmentCache              │
        │  All data stored at propiq:* keys with TTLs     │
        └────────────────────────────────────────────────┘

Redis Cache Keys & TTLs
-----------------------
  propiq:arsenal:{player_id}        3600s  — pitcher pitch-type arsenal
  propiq:whiff:{player_id}          3600s  — batter whiff% by pitch type
  propiq:framing:{player_id}        3600s  — catcher framing runs
  propiq:pop_time:{player_id}       3600s  — catcher pop time to 2B
  propiq:delivery:{player_id}       3600s  — pitcher time to plate
  propiq:wrc_splits:{player_id}     3600s  — batter wRC+ vL / vR / overall
  propiq:handedness:{player_id}     3600s  — pitcher handedness (L/R)
  propiq:lineup:{team_abbr}:{date}   600s  — confirmed batting order + batter hand
  propiq:starters:{date}             600s  — starting pitchers per game
  propiq:pa_avg:{player_id}         3600s  — 14-day rolling PA average
  propiq:team_total:{game_id}        300s  — implied team total runs from odds
  propiq:odds:{game_id}              300s  — live over/under odds
  propiq:public_bet:{game_id}        600s  — ticket % vs money %
  propiq:sharp:{game_id}             600s  — steam velocity + book count
  propiq:schedule:{team_abbr}       3600s  — rest hours, TZ change, prev innings
  propiq:umpire:{umpire_id}         3600s  — CS%, K-rate, run-environment mult
  propiq:weather:{game_id}           600s  — wind speed, direction, temp
  propiq:fatigue:{team_abbr}:{date}  600s  — bullpen fatigue index per pitcher
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import redis
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APIFY_API_KEY: str = os.getenv("APIFY_API_KEY", "")
SPORTSDATA_KEY: str = os.getenv("SPORTSDATA_KEY", "")
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

APIFY_BASE: str = "https://api.apify.com/v2"
SAVANT_BASE: str = "https://baseballsavant.mlb.com"
SPORTSDATA_BASE: str = "https://api.sportsdata.io/v3/mlb"

# Apify actor IDs
CHEERIO_ACTOR: str = "apify/cheerio-scraper"
WEB_ACTOR: str = "apify/web-scraper"

# HTTP timeouts
APIFY_RUN_TIMEOUT: int = 120   # seconds to wait for an Apify run to complete
HTTP_TIMEOUT: int = 30

# Minimal Stuff+ / whiff thresholds surfaced in ArsenalAgent
ARSENAL_PITCH_USAGE_THRESHOLD: float = 0.35   # 35% usage
BATTER_WHIFF_THRESHOLD: float = 0.28           # 28% whiff rate

# Catcher tier thresholds
FRAMING_ELITE_THRESHOLD: float = 2.0
POP_TIME_WEAK_THRESHOLD: float = 2.00          # seconds
DELIVERY_SLOW_THRESHOLD: float = 1.40          # seconds

CURRENT_SEASON: int = datetime.now(timezone.utc).year


# ===========================================================================
# 1. Redis Enrichment Cache
# ===========================================================================

class RedisEnrichmentCache:
    """Thin Redis wrapper for PropIQ enrichment keys.

    All writes use ``SET … EX`` to enforce TTLs.  All reads return
    ``None`` on cache miss so callers can fall back gracefully.
    """

    def __init__(self, redis_url: str = REDIS_URL) -> None:
        self._client: redis.Redis = redis.Redis.from_url(
            redis_url, decode_responses=True
        )

    # ------------------------------------------------------------------
    def set(self, key: str, value: Any, ttl: int) -> None:
        """Serialise *value* to JSON and store with *ttl* seconds TTL."""
        try:
            self._client.set(key, json.dumps(value), ex=ttl)
        except redis.RedisError as exc:
            logger.warning("RedisEnrichmentCache.set %s: %s", key, exc)

    def get(self, key: str) -> Optional[Any]:
        """Return deserialised value or ``None`` on miss / error."""
        try:
            raw = self._client.get(key)
            return json.loads(raw) if raw is not None else None
        except (redis.RedisError, json.JSONDecodeError) as exc:
            logger.warning("RedisEnrichmentCache.get %s: %s", key, exc)
            return None

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def arsenal(self, player_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:arsenal:{player_id}")

    def whiff(self, player_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:whiff:{player_id}")

    def framing(self, player_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:framing:{player_id}")

    def pop_time(self, player_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:pop_time:{player_id}")

    def delivery(self, player_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:delivery:{player_id}")

    def wrc_splits(self, player_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:wrc_splits:{player_id}")

    def handedness(self, player_id: str) -> Optional[str]:
        return self.get(f"propiq:handedness:{player_id}")

    def lineup(self, team_abbr: str, game_date: str) -> Optional[List[Dict]]:
        return self.get(f"propiq:lineup:{team_abbr}:{game_date}")

    def starters(self, game_date: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:starters:{game_date}")

    def pa_avg(self, player_id: str) -> Optional[float]:
        return self.get(f"propiq:pa_avg:{player_id}")

    def team_total(self, game_id: str) -> Optional[float]:
        return self.get(f"propiq:team_total:{game_id}")

    def odds(self, game_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:odds:{game_id}")

    def public_bet(self, game_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:public_bet:{game_id}")

    def sharp(self, game_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:sharp:{game_id}")

    def schedule(self, team_abbr: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:schedule:{team_abbr}")

    def umpire(self, umpire_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:umpire:{umpire_id}")

    def weather(self, game_id: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:weather:{game_id}")

    def fatigue(self, team_abbr: str, game_date: str) -> Optional[Dict[str, Any]]:
        return self.get(f"propiq:fatigue:{team_abbr}:{game_date}")


# ===========================================================================
# 2. Apify Client
# ===========================================================================

class ApifyClient:
    """Synchronous Apify web scraper client.

    Launches an actor run, polls until ``SUCCEEDED`` or ``FAILED``,
    then returns the dataset items.

    Args:
        api_key: Apify API token.  Defaults to ``APIFY_API_KEY`` env var.
        run_timeout: Maximum seconds to wait for a run to finish.
    """

    def __init__(
        self,
        api_key: str = APIFY_API_KEY,
        run_timeout: int = APIFY_RUN_TIMEOUT,
    ) -> None:
        self._api_key = api_key
        self._run_timeout = run_timeout
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    # ------------------------------------------------------------------
    def scrape(
        self,
        actor_id: str,
        start_urls: List[str],
        page_function: str,
        extra_input: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Run *actor_id* against *start_urls* and return scraped items.

        Args:
            actor_id:      Apify actor ID (e.g. ``"apify/cheerio-scraper"``).
            start_urls:    List of URLs to scrape.
            page_function: JavaScript page-function string for the actor.
            extra_input:   Additional actor input fields.

        Returns:
            List of item dicts from the actor dataset.
        """
        run_id = self._launch_run(actor_id, start_urls, page_function, extra_input)
        if not run_id:
            return []
        status = self._poll_run(run_id)
        if status != "SUCCEEDED":
            logger.error("Apify run %s finished with status: %s", run_id, status)
            return []
        return self._fetch_dataset(run_id)

    # ------------------------------------------------------------------
    def _launch_run(
        self,
        actor_id: str,
        start_urls: List[str],
        page_function: str,
        extra_input: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        url = f"{APIFY_BASE}/acts/{actor_id.replace('/', '~')}/runs"
        payload: Dict[str, Any] = {
            "startUrls": [{"url": u} for u in start_urls],
            "pageFunction": page_function,
        }
        if extra_input:
            payload.update(extra_input)
        try:
            resp = self._session.post(
                url,
                json=payload,
                params={"token": self._api_key},
                timeout=HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()["data"]["id"]
        except (requests.RequestException, KeyError) as exc:
            logger.error("Apify launch error for %s: %s", actor_id, exc)
            return None

    def _poll_run(self, run_id: str) -> str:
        """Poll run status until terminal state or timeout."""
        deadline = time.time() + self._run_timeout
        url = f"{APIFY_BASE}/actor-runs/{run_id}"
        while time.time() < deadline:
            try:
                resp = self._session.get(
                    url,
                    params={"token": self._api_key},
                    timeout=HTTP_TIMEOUT,
                )
                resp.raise_for_status()
                status: str = resp.json()["data"]["status"]
                if status in {"SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"}:
                    return status
            except (requests.RequestException, KeyError) as exc:
                logger.warning("Apify poll error for run %s: %s", run_id, exc)
            time.sleep(5)
        logger.error("Apify run %s timed out after %ds", run_id, self._run_timeout)
        return "TIMED-OUT"

    def _fetch_dataset(self, run_id: str) -> List[Dict[str, Any]]:
        url = f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items"
        try:
            resp = self._session.get(
                url,
                params={"token": self._api_key, "format": "json"},
                timeout=HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.error("Apify dataset fetch error run %s: %s", run_id, exc)
            return []


# ===========================================================================
# 3. Baseball Savant Client (direct CSV — no Apify needed)
# ===========================================================================

class BaseballSavantClient:
    """Direct HTTP client for Baseball Savant CSV export endpoints.

    Baseball Savant's CSV endpoints do not require JavaScript rendering
    so we hit them directly with ``requests`` instead of Apify.
    """

    ARSENAL_PITCHER_URL = (
        f"{SAVANT_BASE}/leaderboard/pitch-arsenal-stats"
        f"?type=pitcher&pitchType=&year={CURRENT_SEASON}&position=SP,RP"
        f"&team=&min=10&csv=true"
    )
    ARSENAL_BATTER_URL = (
        f"{SAVANT_BASE}/leaderboard/pitch-arsenal-stats"
        f"?type=batter&pitchType=&year={CURRENT_SEASON}&position="
        f"&team=&min=10&csv=true"
    )
    FRAMING_URL = (
        f"{SAVANT_BASE}/leaderboard/framing?min=20&sort=8&sortDir=desc&csv=true"
    )
    POP_TIME_URL = (
        f"{SAVANT_BASE}/catcher_throwing?min_att=1&csv=true"
    )
    # Pitcher pace / time-to-plate — Statcast pitcher leaderboard
    PITCHER_PACE_URL = (
        f"{SAVANT_BASE}/leaderboard/statcast"
        f"?type=pitcher&year={CURRENT_SEASON}&position=SP,RP"
        f"&team=&min_pa=10&csv=true"
    )

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        })

    def fetch_csv(self, url: str) -> List[Dict[str, str]]:
        """Download a CSV URL and return list of row dicts."""
        try:
            resp = self._session.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            return [dict(row) for row in reader]
        except (requests.RequestException, csv.Error) as exc:
            logger.error("BaseballSavant fetch error %s: %s", url, exc)
            return []


# ===========================================================================
# 4. Arsenal Scraper  (pitcher arsenal + batter whiff rates)
# ===========================================================================

class ArsenalScraper:
    """Populates ``propiq:arsenal:{pitcher_id}`` and ``propiq:whiff:{batter_id}``.

    Pitcher arsenal cache format::

        {
          "SL": {"usage_rate": 0.40, "stuff_plus": 118, "whiff_pct": 0.32},
          "FF": {"usage_rate": 0.50, "stuff_plus": 105, "whiff_pct": 0.22},
        }

    Batter whiff cache format::

        {
          "SL": {"whiff_rate": 0.31},
          "CH": {"whiff_rate": 0.27},
        }
    """

    # Baseball Savant pitch-type abbreviation → human label
    PITCH_ABBR = {
        "FF": "4-Seam Fastball",
        "SI": "Sinker",
        "FC": "Cutter",
        "SL": "Slider",
        "SW": "Sweeper",
        "ST": "Sweeping Curve",
        "CU": "Curveball",
        "CH": "Changeup",
        "FS": "Splitter",
        "FO": "Forkball",
        "KN": "Knuckleball",
    }

    def __init__(
        self, savant: BaseballSavantClient, cache: RedisEnrichmentCache
    ) -> None:
        self._savant = savant
        self._cache = cache

    # ------------------------------------------------------------------
    def refresh_pitcher_arsenal(self) -> int:
        """Download pitcher arsenal CSV and write all players to Redis."""
        rows = self._savant.fetch_csv(BaseballSavantClient.ARSENAL_PITCHER_URL)
        if not rows:
            return 0
        # Group by pitcher
        pitcher_map: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            pid = row.get("pitcher_id") or row.get("player_id", "")
            pitch = row.get("pitch_type", "").upper()
            if not pid or not pitch:
                continue
            if pid not in pitcher_map:
                pitcher_map[pid] = {}
            pitcher_map[pid][pitch] = {
                "usage_rate": _safe_float(row.get("pitch_usage") or row.get("pitcher_break_z")),
                "stuff_plus": _safe_float(row.get("stuff_plus")),
                "whiff_pct": _safe_float(row.get("whiff_percent")),
            }
        # Normalise usage_rate to 0–1 if stored as percentage
        for pid, arsenal in pitcher_map.items():
            for pitch_data in arsenal.values():
                if pitch_data["usage_rate"] and pitch_data["usage_rate"] > 1.0:
                    pitch_data["usage_rate"] /= 100.0
                if pitch_data["whiff_pct"] and pitch_data["whiff_pct"] > 1.0:
                    pitch_data["whiff_pct"] /= 100.0
            self._cache.set(f"propiq:arsenal:{pid}", arsenal, ttl=3600)
        logger.info("ArsenalScraper: wrote arsenal for %d pitchers", len(pitcher_map))
        return len(pitcher_map)

    def refresh_batter_whiff(self) -> int:
        """Download batter-side arsenal CSV and write whiff rates to Redis."""
        rows = self._savant.fetch_csv(BaseballSavantClient.ARSENAL_BATTER_URL)
        if not rows:
            return 0
        batter_map: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            pid = row.get("batter_id") or row.get("player_id", "")
            pitch = row.get("pitch_type", "").upper()
            if not pid or not pitch:
                continue
            if pid not in batter_map:
                batter_map[pid] = {}
            whiff = _safe_float(row.get("whiff_percent"))
            if whiff and whiff > 1.0:
                whiff /= 100.0
            batter_map[pid][pitch] = {"whiff_rate": whiff or 0.0}
        for pid, whiff_data in batter_map.items():
            self._cache.set(f"propiq:whiff:{pid}", whiff_data, ttl=3600)
        logger.info("ArsenalScraper: wrote whiff data for %d batters", len(batter_map))
        return len(batter_map)


# ===========================================================================
# 5. Catcher Metrics Scraper  (framing + pop time + pitcher delivery)
# ===========================================================================

class CatcherMetricsScraper:
    """Populates catcher framing, pop time, and pitcher delivery Redis keys."""

    def __init__(
        self, savant: BaseballSavantClient, cache: RedisEnrichmentCache
    ) -> None:
        self._savant = savant
        self._cache = cache

    def refresh_framing(self) -> int:
        """Framing Runs leaderboard → ``propiq:framing:{player_id}``."""
        rows = self._savant.fetch_csv(BaseballSavantClient.FRAMING_URL)
        if not rows:
            return 0
        count = 0
        for row in rows:
            pid = row.get("player_id", "")
            if not pid:
                continue
            data = {
                "framing_runs": _safe_float(row.get("framing_runs") or row.get("runs_extra_strikes")),
                "blocking_runs": _safe_float(row.get("blocking_runs")),
                "throwing_runs": _safe_float(row.get("throwing_runs")),
                "n_total": _safe_int(row.get("n_total")),
                "team": row.get("team_name_abbrev", ""),
            }
            self._cache.set(f"propiq:framing:{pid}", data, ttl=3600)
            count += 1
        logger.info("CatcherMetricsScraper: framing written for %d catchers", count)
        return count

    def refresh_pop_time(self) -> int:
        """Catcher throwing / pop time → ``propiq:pop_time:{player_id}``."""
        rows = self._savant.fetch_csv(BaseballSavantClient.POP_TIME_URL)
        if not rows:
            return 0
        count = 0
        for row in rows:
            pid = row.get("catcher_id") or row.get("player_id", "")
            if not pid:
                continue
            data = {
                "pop_time_2b": _safe_float(row.get("pop_2b_sba") or row.get("pop_time_2b_sba")),
                "exchange_time": _safe_float(row.get("exchange_2b_3b_sba")),
                "cs_pct": _safe_float(row.get("cs_pct")),
                "n_attempts": _safe_int(row.get("n_2b_3b_sba")),
            }
            self._cache.set(f"propiq:pop_time:{pid}", data, ttl=3600)
            count += 1
        logger.info("CatcherMetricsScraper: pop time written for %d catchers", count)
        return count

    def refresh_pitcher_delivery(self) -> int:
        """Pitcher pace / time-to-plate → ``propiq:delivery:{player_id}``.

        Baseball Savant's statcast pitcher leaderboard includes
        ``pace_per_pa`` (seconds per pitch).  We normalise this to an
        approximate time-to-plate by applying the league-average fraction
        (≈ 0.65 of pace_per_pa represents delivery time).
        """
        rows = self._savant.fetch_csv(BaseballSavantClient.PITCHER_PACE_URL)
        if not rows:
            return 0
        count = 0
        for row in rows:
            pid = row.get("pitcher_id") or row.get("player_id", "")
            if not pid:
                continue
            # pace_per_pa in seconds; delivery time ≈ pace × 0.65
            pace = _safe_float(row.get("pace") or row.get("pace_per_pa"))
            delivery_time = round(pace * 0.65, 2) if pace else 1.30
            data = {
                "time_to_plate": delivery_time,
                "pace_per_pa": pace,
                "pitcher_hand": row.get("p_throws", ""),
            }
            self._cache.set(f"propiq:delivery:{pid}", data, ttl=3600)
            # Also cache pitcher handedness
            if data["pitcher_hand"]:
                self._cache.set(f"propiq:handedness:{pid}", data["pitcher_hand"], ttl=3600)
            count += 1
        logger.info("CatcherMetricsScraper: delivery written for %d pitchers", count)
        return count


# ===========================================================================
# 6. RotoWire Scraper  (lineups, starters, splits, weather, umpires)
# ===========================================================================

_ROTOWIRE_PAGE_FN = """
async function pageFunction(context) {
    const { $, request } = context;
    const url = request.url;
    const items = [];

    // ── Batting Orders ──────────────────────────────────────────────
    if (url.includes('batting-orders')) {
        $('.lineup__list').each((_, teamBlock) => {
            const teamAbbr = $(teamBlock).closest('.lineup__main')
                                .find('.lineup__abbr').first().text().trim();
            $(teamBlock).find('.lineup__player').each((pos, player) => {
                const name = $(player).find('.lineup__name').text().trim();
                const hand = $(player).find('.lineup__bats').text().trim();
                items.push({ type: 'lineup', team: teamAbbr,
                             position: pos + 1, name, handedness: hand });
            });
        });
    }

    // ── Projected Starters ─────────────────────────────────────────
    if (url.includes('projected-starters')) {
        $('.starting-pitchers__player').each((_, el) => {
            const name = $(el).find('.starting-pitchers__name').text().trim();
            const team = $(el).find('.starting-pitchers__team').text().trim();
            const hand = $(el).find('.starting-pitchers__bats').text().trim()
                              .replace('Throws: ', '');
            const era  = $(el).find('.starting-pitchers__stat--era').text().trim();
            const avg_ip = $(el).find('.starting-pitchers__stat--ip').text().trim();
            items.push({ type: 'starter', name, team, hand, era, avg_ip });
        });
    }

    // ── Weather ────────────────────────────────────────────────────
    if (url.includes('weather.php')) {
        $('.weather-table__row').each((_, row) => {
            const game    = $(row).find('.weather-table__team').text().trim();
            const wind    = $(row).find('.weather-table__wind').text().trim();
            const temp    = $(row).find('.weather-table__temp').text().trim();
            const precip  = $(row).find('.weather-table__precip').text().trim();
            items.push({ type: 'weather', game, wind, temp, precip });
        });
    }

    // ── Umpire Stats ────────────────────────────────────────────────
    if (url.includes('umpire-stats-daily')) {
        $('table tbody tr').each((_, row) => {
            const cells = $(row).find('td');
            if (cells.length < 6) return;
            items.push({
                type:           'umpire',
                name:           $(cells[0]).text().trim(),
                game:           $(cells[1]).text().trim(),
                k_rate:         $(cells[2]).text().trim(),
                bb_rate:        $(cells[3]).text().trim(),
                run_env:        $(cells[4]).text().trim(),
                cs_pct:         $(cells[5]).text().trim(),
            });
        });
    }

    // ── Advanced Stats (wRC+/splits) ────────────────────────────────
    if (url.includes('stats-advanced') || url.includes('stats-second-half')) {
        $('table tbody tr').each((_, row) => {
            const cells = $(row).find('td');
            if (cells.length < 8) return;
            items.push({
                type:           'wrc_splits',
                name:           $(cells[0]).text().trim(),
                team:           $(cells[1]).text().trim(),
                wrc_plus_vl:    $(cells).filter((_, c) => $(c).data('col') === 'wRC+vL').text().trim()
                                || $(cells[5]).text().trim(),
                wrc_plus_vr:    $(cells).filter((_, c) => $(c).data('col') === 'wRC+vR').text().trim()
                                || $(cells[6]).text().trim(),
                wrc_plus:       $(cells[7]).text().trim(),
                pa:             $(cells[2]).text().trim(),
            });
        });
    }

    // ── BvP (Batter vs Pitcher) ─────────────────────────────────────
    if (url.includes('stats-bvp')) {
        $('table tbody tr').each((_, row) => {
            const cells = $(row).find('td');
            if (cells.length < 5) return;
            items.push({
                type:       'bvp',
                batter:     $(cells[0]).text().trim(),
                pitcher:    $(cells[1]).text().trim(),
                pa:         $(cells[2]).text().trim(),
                avg:        $(cells[3]).text().trim(),
                ops:        $(cells[4]).text().trim(),
            });
        });
    }

    return items;
}
"""

_ROTOWIRE_UNDERDOG_PAGE_FN = """
async function pageFunction(context) {
    const { $, request } = context;
    const items = [];
    $('[class*="pick"], [class*="prop"], [class*="player"]').each((_, el) => {
        const name  = $(el).find('[class*="name"]').first().text().trim();
        const stat  = $(el).find('[class*="stat"], [class*="prop"]').first().text().trim();
        const line  = $(el).find('[class*="line"], [class*="value"]').first().text().trim();
        const pa_avg = $(el).find('[class*="pa"], [class*="avg"]').first().text().trim();
        if (name) items.push({ type: 'dfs_pick', name, stat, line, pa_avg });
    });
    return items;
}
"""


class RotoWireScraper:
    """Scrapes RotoWire via Apify Cheerio and writes enrichment data to Redis."""

    URLS = {
        "batting_orders": "https://www.rotowire.com/baseball/batting-orders.php",
        "starters": "https://www.rotowire.com/baseball/projected-starters.php",
        "weather": "https://www.rotowire.com/baseball/weather.php",
        "umpires": "https://www.rotowire.com/baseball/umpire-stats-daily.php",
        "advanced": "https://www.rotowire.com/baseball/stats-advanced.php",
        "bvp": "https://www.rotowire.com/baseball/stats-bvp.php",
        "batted_ball": "https://www.rotowire.com/baseball/stats-batted-ball.php",
        "second_half": "https://www.rotowire.com/baseball/stats-second-half.php",
        "underdog_picks": "https://www.rotowire.com/picks/underdog/",
        "prizepicks": "https://www.rotowire.com/picks/prizepicks/",
        "sleeper": "https://www.rotowire.com/picks/sleeper/",
    }

    def __init__(
        self,
        apify: ApifyClient,
        cache: RedisEnrichmentCache,
    ) -> None:
        self._apify = apify
        self._cache = cache

    # ------------------------------------------------------------------
    def refresh_lineups(self, game_date: Optional[str] = None) -> int:
        """Scrape batting orders and write to ``propiq:lineup:{team}:{date}``."""
        today = game_date or date.today().isoformat()
        items = self._apify.scrape(
            CHEERIO_ACTOR,
            [self.URLS["batting_orders"]],
            _ROTOWIRE_PAGE_FN,
        )
        lineup_by_team: Dict[str, List[Dict]] = {}
        for item in items:
            if item.get("type") != "lineup":
                continue
            team = item.get("team", "").upper()
            if not team:
                continue
            if team not in lineup_by_team:
                lineup_by_team[team] = []
            lineup_by_team[team].append({
                "position": item.get("position", 5),
                "name": item.get("name", ""),
                "handedness": item.get("handedness", ""),
            })
        for team, order in lineup_by_team.items():
            self._cache.set(f"propiq:lineup:{team}:{today}", order, ttl=600)
        logger.info("RotoWireScraper: lineups written for %d teams", len(lineup_by_team))
        return len(lineup_by_team)

    def refresh_starters(self, game_date: Optional[str] = None) -> int:
        """Scrape projected starters and write to ``propiq:starters:{date}``."""
        today = game_date or date.today().isoformat()
        items = self._apify.scrape(
            CHEERIO_ACTOR,
            [self.URLS["starters"]],
            _ROTOWIRE_PAGE_FN,
        )
        starters: Dict[str, Dict] = {}
        for item in items:
            if item.get("type") != "starter":
                continue
            team = item.get("team", "").upper()
            if not team:
                continue
            avg_ip_str = item.get("avg_ip", "5.0")
            avg_ip = _safe_float(avg_ip_str) or 5.0
            # Expected starter PAs ≈ (avg_ip / 9) × 27 × (1/3) ≈ avg_ip
            pa_starter = round(avg_ip * 27 / 9 / 3, 2)
            starters[team] = {
                "name": item.get("name", ""),
                "hand": item.get("hand", ""),
                "era": _safe_float(item.get("era")),
                "avg_ip": avg_ip,
                "pa_starter": pa_starter,
            }
            # Also cache pitcher handedness by name key (resolver maps name→id separately)
            name_key = item.get("name", "").replace(" ", "_").lower()
            self._cache.set(f"propiq:starter_hand:{name_key}", item.get("hand", ""), ttl=600)
        self._cache.set(f"propiq:starters:{today}", starters, ttl=600)
        logger.info("RotoWireScraper: %d starters written for %s", len(starters), today)
        return len(starters)

    def refresh_weather(self, game_date: Optional[str] = None) -> int:
        """Scrape weather and write to ``propiq:weather:{matchup}``."""
        items = self._apify.scrape(
            CHEERIO_ACTOR,
            [self.URLS["weather"]],
            _ROTOWIRE_PAGE_FN,
        )
        count = 0
        for item in items:
            if item.get("type") != "weather":
                continue
            game = item.get("game", "")
            if not game:
                continue
            wind_str = item.get("wind", "0 mph N")
            speed, direction = _parse_wind(wind_str)
            temp_str = item.get("temp", "72")
            temp = _safe_float(temp_str.replace("°", "").replace("F", "").strip()) or 72.0
            data = {
                "wind_speed": speed,
                "wind_direction": direction,
                "temp_f": temp,
                "precip": item.get("precip", ""),
            }
            self._cache.set(f"propiq:weather:{game}", data, ttl=600)
            count += 1
        logger.info("RotoWireScraper: weather written for %d games", count)
        return count

    def refresh_umpires(self) -> int:
        """Scrape umpire stats and write to ``propiq:umpire:{name}``."""
        items = self._apify.scrape(
            CHEERIO_ACTOR,
            [self.URLS["umpires"]],
            _ROTOWIRE_PAGE_FN,
        )
        count = 0
        for item in items:
            if item.get("type") != "umpire":
                continue
            name = item.get("name", "")
            if not name:
                continue
            cs_raw = item.get("cs_pct", "32.5").replace("%", "")
            run_raw = item.get("run_env", "1.00")
            k_raw = item.get("k_rate", "")
            bb_raw = item.get("bb_rate", "")
            data = {
                "cs_pct": (_safe_float(cs_raw) or 32.5) / 100.0,
                "run_environment": _safe_float(run_raw) or 1.0,
                "k_rate": _safe_float(k_raw.replace("%", "")) or 0.0,
                "bb_rate": _safe_float(bb_raw.replace("%", "")) or 0.0,
                "game": item.get("game", ""),
            }
            key = name.replace(" ", "_").lower()
            self._cache.set(f"propiq:umpire:{key}", data, ttl=3600)
            count += 1
        logger.info("RotoWireScraper: umpire stats written for %d umpires", count)
        return count

    def refresh_wrc_splits(self) -> int:
        """Scrape wRC+ splits from advanced stats and write to Redis."""
        items = self._apify.scrape(
            CHEERIO_ACTOR,
            [self.URLS["advanced"], self.URLS["second_half"]],
            _ROTOWIRE_PAGE_FN,
        )
        count = 0
        for item in items:
            if item.get("type") != "wrc_splits":
                continue
            name = item.get("name", "")
            if not name:
                continue
            data = {
                "wrc_plus_vl": _safe_float(item.get("wrc_plus_vl")) or 100.0,
                "wrc_plus_vr": _safe_float(item.get("wrc_plus_vr")) or 100.0,
                "wrc_plus_overall": _safe_float(item.get("wrc_plus")) or 100.0,
            }
            name_key = name.replace(" ", "_").lower()
            self._cache.set(f"propiq:wrc_splits:{name_key}", data, ttl=3600)
            count += 1
        logger.info("RotoWireScraper: wRC+ splits written for %d players", count)
        return count

    def refresh_dfs_pa_avg(self) -> int:
        """Scrape DFS pick pages for 14-day PA rolling averages."""
        items = self._apify.scrape(
            CHEERIO_ACTOR,
            [self.URLS["underdog_picks"], self.URLS["prizepicks"]],
            _ROTOWIRE_UNDERDOG_PAGE_FN,
        )
        count = 0
        for item in items:
            if item.get("type") != "dfs_pick":
                continue
            name = item.get("name", "")
            pa_raw = item.get("pa_avg", "")
            pa = _safe_float(pa_raw)
            if name and pa:
                name_key = name.replace(" ", "_").lower()
                self._cache.set(f"propiq:pa_avg:{name_key}", pa, ttl=3600)
                count += 1
        logger.info("RotoWireScraper: PA averages written for %d players", count)
        return count


# ===========================================================================
# 7. Action Network Scraper  (public betting, sharp report, live odds)
# ===========================================================================

_ACTION_NET_PAGE_FN = """
async function pageFunction(context) {
    const { page, request } = context;
    await page.waitForSelector('[class*="public-betting"], [class*="game-odds"], table', { timeout: 15000 }).catch(() => {});
    const html = await page.evaluate(() => document.body.innerText);
    const url  = request.url;
    const items = [];

    if (url.includes('public-betting')) {
        const rows = document.querySelectorAll('[class*="public-betting__row"], tr');
        rows.forEach(row => {
            const cells = row.querySelectorAll('td, [class*="cell"]');
            if (cells.length >= 4) {
                items.push({
                    type:       'public_bet',
                    game:       cells[0]?.innerText?.trim() || '',
                    prop:       cells[1]?.innerText?.trim() || '',
                    ticket_pct: cells[2]?.innerText?.trim() || '',
                    money_pct:  cells[3]?.innerText?.trim() || '',
                });
            }
        });
    }

    if (url.includes('sharp-report')) {
        const rows = document.querySelectorAll('tr, [class*="sharp__row"]');
        rows.forEach(row => {
            const cells = row.querySelectorAll('td, [class*="cell"]');
            if (cells.length >= 4) {
                items.push({
                    type:          'sharp',
                    game:          cells[0]?.innerText?.trim() || '',
                    prop:          cells[1]?.innerText?.trim() || '',
                    steam_move:    cells[2]?.innerText?.trim() || '',
                    book_count:    cells[3]?.innerText?.trim() || '',
                });
            }
        });
    }

    if (url.includes('/odds')) {
        const rows = document.querySelectorAll('[class*="odds__row"], tr');
        rows.forEach(row => {
            const cells = row.querySelectorAll('td, [class*="cell"]');
            if (cells.length >= 5) {
                items.push({
                    type:       'odds',
                    game:       cells[0]?.innerText?.trim() || '',
                    prop:       cells[1]?.innerText?.trim() || '',
                    odds_over:  cells[2]?.innerText?.trim() || '',
                    odds_under: cells[3]?.innerText?.trim() || '',
                    team_total: cells[4]?.innerText?.trim() || '',
                });
            }
        });
    }

    return items;
}
"""


class ActionNetworkScraper:
    """Scrapes Action Network via Apify Web Scraper and writes to Redis.

    Uses ``apify/web-scraper`` (Puppeteer) because Action Network is
    heavily JavaScript-rendered and does not work with Cheerio.
    """

    URLS = {
        "public_betting": "https://www.actionnetwork.com/mlb/public-betting",
        "sharp_report": "https://www.actionnetwork.com/mlb/sharp-report",
        "odds": "https://www.actionnetwork.com/mlb/odds",
        "prop_projections": "https://www.actionnetwork.com/mlb/prop-projections",
    }

    def __init__(
        self,
        apify: ApifyClient,
        cache: RedisEnrichmentCache,
    ) -> None:
        self._apify = apify
        self._cache = cache

    # ------------------------------------------------------------------
    def refresh_public_betting(self) -> int:
        """Scrape ticket% / money% and write to ``propiq:public_bet:{game}``."""
        items = self._apify.scrape(
            WEB_ACTOR,
            [self.URLS["public_betting"]],
            _ACTION_NET_PAGE_FN,
        )
        count = 0
        for item in items:
            if item.get("type") != "public_bet":
                continue
            game = item.get("game", "")
            if not game:
                continue
            ticket = _safe_float(item.get("ticket_pct", "").replace("%", ""))
            money = _safe_float(item.get("money_pct", "").replace("%", ""))
            if ticket is None or money is None:
                continue
            data = {
                "ticket_pct": ticket / 100.0 if ticket > 1.0 else ticket,
                "money_pct": money / 100.0 if money > 1.0 else money,
                "prop": item.get("prop", ""),
            }
            game_key = game.replace(" ", "_").lower()
            self._cache.set(f"propiq:public_bet:{game_key}", data, ttl=600)
            count += 1
        logger.info("ActionNetworkScraper: public betting written for %d lines", count)
        return count

    def refresh_sharp_report(self) -> int:
        """Scrape sharp-report steam moves and write to ``propiq:sharp:{game}``."""
        items = self._apify.scrape(
            WEB_ACTOR,
            [self.URLS["sharp_report"]],
            _ACTION_NET_PAGE_FN,
        )
        count = 0
        for item in items:
            if item.get("type") != "sharp":
                continue
            game = item.get("game", "")
            if not game:
                continue
            # steam_move format: "±2.5 pts/min" or "2.5"
            steam_raw = item.get("steam_move", "0").split()[0].replace("+", "")
            book_ct = _safe_int(item.get("book_count", "1")) or 1
            data = {
                "steam_velocity": _safe_float(steam_raw) or 0.0,
                "steam_book_count": book_ct,
                "prop": item.get("prop", ""),
            }
            game_key = game.replace(" ", "_").lower()
            self._cache.set(f"propiq:sharp:{game_key}", data, ttl=600)
            count += 1
        logger.info("ActionNetworkScraper: sharp report written for %d lines", count)
        return count

    def refresh_odds(self) -> int:
        """Scrape live odds and team totals → ``propiq:odds:{game}`` +
        ``propiq:team_total:{game}``."""
        items = self._apify.scrape(
            WEB_ACTOR,
            [self.URLS["odds"]],
            _ACTION_NET_PAGE_FN,
        )
        count = 0
        for item in items:
            if item.get("type") != "odds":
                continue
            game = item.get("game", "")
            if not game:
                continue
            game_key = game.replace(" ", "_").lower()
            # Parse American odds strings like "-110" or "+150"
            over_str = item.get("odds_over", "-110").strip()
            under_str = item.get("odds_under", "-110").strip()
            odds_data = {
                "odds_over": _parse_american_odds(over_str),
                "odds_under": _parse_american_odds(under_str),
                "game": game,
            }
            self._cache.set(f"propiq:odds:{game_key}", odds_data, ttl=300)
            # Team total
            tt_str = item.get("team_total", "4.5").strip()
            tt = _safe_float(tt_str) or 4.5
            self._cache.set(f"propiq:team_total:{game_key}", tt, ttl=300)
            count += 1
        logger.info("ActionNetworkScraper: odds written for %d games", count)
        return count


# ===========================================================================
# 8. SportsData.io Schedule Scraper  (rest, travel, time-zone change)
# ===========================================================================

# MLB team city → UTC offset (standard time; DST auto-adjust not modelled for simplicity)
TEAM_TIMEZONE_OFFSET: Dict[str, int] = {
    "NYY": -5, "NYM": -5, "BOS": -5, "TOR": -5, "BAL": -5, "TB": -5,
    "PHI": -5, "ATL": -5, "MIA": -5, "WSH": -5,
    "CHC": -6, "CWS": -6, "MIL": -6, "MIN": -6, "STL": -6, "KC": -6,
    "DET": -5, "CLE": -5, "PIT": -5, "CIN": -5,
    "HOU": -6, "TEX": -6,
    "LAD": -8, "LAA": -8, "SF": -8, "OAK": -8, "SEA": -8, "SD": -8,
    "ARI": -7, "COL": -7,
}


class SportsDataScheduleScraper:
    """Fetches MLB schedule from SportsData.io to compute rest / travel data.

    Writes ``propiq:schedule:{team_abbr}`` with::

        {
            "hours_rest": 21.5,
            "time_zone_change": 3,
            "previous_game_innings": 9,
            "previous_game_date": "2025-06-10",
        }
    """

    def __init__(
        self,
        api_key: str = SPORTSDATA_KEY,
        cache: Optional[RedisEnrichmentCache] = None,
    ) -> None:
        self._api_key = api_key
        self._cache = cache or RedisEnrichmentCache()
        self._session = requests.Session()

    def _fetch_games(self, game_date: str) -> List[Dict[str, Any]]:
        url = f"{SPORTSDATA_BASE}/scores/json/GamesByDate/{game_date}"
        try:
            resp = self._session.get(
                url,
                params={"key": self._api_key},
                timeout=HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.error("SportsDataScheduleScraper: fetch error %s: %s", game_date, exc)
            return []

    def refresh_schedule(
        self,
        game_date: Optional[str] = None,
        lookback_days: int = 3,
    ) -> int:
        """Compute rest / TZ data for each team playing today.

        Args:
            game_date:     ISO date string (default: today).
            lookback_days: How many past days to fetch for rest calculation.

        Returns:
            Number of teams written to Redis.
        """
        from datetime import timedelta

        today_dt = date.fromisoformat(game_date) if game_date else date.today()
        today_str = today_dt.isoformat()

        today_games = self._fetch_games(today_str)
        if not today_games:
            return 0

        # Build last-game lookup from past N days
        past_games: List[Dict[str, Any]] = []
        for days_back in range(1, lookback_days + 1):
            past_dt = (today_dt - timedelta(days=days_back)).isoformat()
            past_games.extend(self._fetch_games(past_dt))

        # Index past games by team: team_abbr → latest game played
        last_game_by_team: Dict[str, Dict[str, Any]] = {}
        for g in sorted(past_games, key=lambda x: x.get("DateTime", "")):
            for side in ("HomeTeam", "AwayTeam"):
                team = g.get(side, "")
                if team:
                    last_game_by_team[team] = g

        count = 0
        for game in today_games:
            today_dt_str = game.get("DateTime", "")
            home_team = game.get("HomeTeam", "")
            away_team = game.get("AwayTeam", "")
            today_ts = _parse_sportsdata_dt(today_dt_str)

            for team in (home_team, away_team):
                if not team:
                    continue
                last_g = last_game_by_team.get(team)
                if not last_g:
                    # No recent game found — safe default
                    self._cache.set(
                        f"propiq:schedule:{team}",
                        {
                            "hours_rest": 48.0,
                            "time_zone_change": 0,
                            "previous_game_innings": 9,
                            "previous_game_date": "",
                        },
                        ttl=3600,
                    )
                    continue

                last_ts = _parse_sportsdata_dt(last_g.get("DateTime", ""))
                hours_rest = (today_ts - last_ts) / 3600.0 if (today_ts and last_ts) else 24.0

                # Determine previous location: home team of the last game played
                prev_venue_team = last_g.get("HomeTeam", "")
                prev_tz = TEAM_TIMEZONE_OFFSET.get(prev_venue_team, -6)
                curr_venue_team = game.get("HomeTeam", "")
                curr_tz = TEAM_TIMEZONE_OFFSET.get(curr_venue_team, -6)
                tz_change = abs(curr_tz - prev_tz)

                prev_innings = int(last_g.get("Innings", 9) or 9)

                data = {
                    "hours_rest": round(hours_rest, 1),
                    "time_zone_change": tz_change,
                    "previous_game_innings": prev_innings,
                    "previous_game_date": last_g.get("Day", ""),
                }
                self._cache.set(f"propiq:schedule:{team}", data, ttl=3600)
                count += 1

        logger.info(
            "SportsDataScheduleScraper: schedule written for %d team-slots", count
        )
        return count


# ===========================================================================
# 9. DataEnricher  — orchestrates all scrapers, single public interface
# ===========================================================================

class DataEnricher:
    """Single public interface consumed by ``ExecutionSquad._parse_prop_edge``.

    Reads pre-populated Redis cache and returns a flat enrichment dict
    that maps directly to ``PropEdge`` field names.

    Usage::

        enricher = DataEnricher()

        # In ExecutionSquad._parse_prop_edge:
        enrichment = enricher.enrich(
            player_id="660670",
            player_name="Aaron Judge",
            team_abbr="NYY",
            game_id="nyy_bos_2025-06-15",
            game_date="2025-06-15",
            pitcher_id="477132",
        )
        # Returns dict merged into PropEdge constructor kwargs
    """

    def __init__(self, redis_url: str = REDIS_URL) -> None:
        self._cache = RedisEnrichmentCache(redis_url)

    # ------------------------------------------------------------------
    def enrich(
        self,
        player_id: str,
        player_name: str = "",
        team_abbr: str = "",
        game_id: str = "",
        game_date: str = "",
        pitcher_id: str = "",
        is_pitcher: bool = False,
        catcher_id: str = "",
    ) -> Dict[str, Any]:
        """Return enrichment dict keyed to ``PropEdge`` field names.

        All fields have safe defaults so callers can merge without
        checking for ``None``.
        """
        enrichment: Dict[str, Any] = {}
        name_key = player_name.replace(" ", "_").lower()
        today = game_date or date.today().isoformat()

        # ── ArsenalAgent fields ──────────────────────────────────────
        if is_pitcher:
            arsenal = self._cache.arsenal(player_id) or {}
        else:
            arsenal = self._cache.arsenal(pitcher_id) or {} if pitcher_id else {}
        enrichment["pitcher_arsenal_json"] = json.dumps(arsenal) if arsenal else "{}"

        whiff = self._cache.whiff(player_id) if not is_pitcher else {}
        enrichment["batter_whiff_json"] = json.dumps(whiff) if whiff else "{}"

        # ── PlatoonAgent fields ──────────────────────────────────────
        splits = (
            self._cache.wrc_splits(player_id)
            or self._cache.wrc_splits(name_key)
            or {}
        )
        enrichment["wrc_plus_vl"] = splits.get("wrc_plus_vl", 100.0)
        enrichment["wrc_plus_vr"] = splits.get("wrc_plus_vr", 100.0)
        enrichment["wrc_plus_overall"] = splits.get("wrc_plus_overall", 100.0)

        # Batter handedness from lineup cache
        lineup = self._cache.lineup(team_abbr, today) or []
        batter_hand = ""
        for entry in lineup:
            if name_key in (entry.get("name") or "").replace(" ", "_").lower():
                batter_hand = entry.get("handedness", "")
                break
        enrichment["batter_handedness"] = batter_hand

        # Pitcher handedness from delivery cache or starter hand cache
        delivery = self._cache.delivery(pitcher_id) if pitcher_id else {}
        pitcher_hand = (delivery or {}).get("pitcher_hand", "") if delivery else ""
        if not pitcher_hand:
            pitcher_hand = self._cache.handedness(pitcher_id) or "" if pitcher_id else ""
        enrichment["pitcher_handedness"] = pitcher_hand

        # PA projections from starters cache
        starters = self._cache.starters(today) or {}
        starter_info = starters.get(team_abbr.upper(), {}) or {}
        enrichment["pa_starter"] = float(starter_info.get("pa_starter", 2.5))

        # Bullpen handedness probabilities: simple heuristic 30/70 LHP/RHP default
        enrichment["p_lhp_bullpen"] = 0.30
        enrichment["p_rhp_bullpen"] = 0.70

        # Pinch-hit risk: elevated if batter is LHB and wRC+_vL < 70
        wrc_vl = enrichment["wrc_plus_vl"]
        bh = enrichment["batter_handedness"].upper()
        enrichment["pinch_hit_risk"] = 0.25 if (bh == "L" and wrc_vl < 70) else 0.0

        # PA total: project from lineup position and team total
        enrichment["pa_total"] = 4.0  # overridden by LineupAgent from lineup pos

        # ── CatcherAgent fields ───────────────────────────────────────
        framing_data = self._cache.framing(catcher_id) if catcher_id else None
        enrichment["catcher_framing_runs"] = (
            float(framing_data.get("framing_runs") or 0.0) if framing_data else 0.0
        )
        pop_data = self._cache.pop_time(catcher_id) if catcher_id else None
        enrichment["catcher_pop_time"] = (
            float(pop_data.get("pop_time_2b") or 1.90) if pop_data else 1.90
        )
        delivery_data = self._cache.delivery(pitcher_id) if pitcher_id else None
        enrichment["pitcher_time_to_plate"] = (
            float(delivery_data.get("time_to_plate") or 1.30) if delivery_data else 1.30
        )

        # ── LineupAgent fields ─────────────────────────────────────────
        lineup_pos = 5
        for entry in lineup:
            if name_key in (entry.get("name") or "").replace(" ", "_").lower():
                lineup_pos = int(entry.get("position", 5))
                break
        enrichment["lineup_position"] = lineup_pos

        tt = self._cache.team_total(game_id) or 4.5
        enrichment["team_total_runs"] = float(tt)

        pa_avg = self._cache.pa_avg(player_id) or self._cache.pa_avg(name_key) or 3.8
        enrichment["pa_average"] = float(pa_avg)

        # ── GetawayAgent fields ────────────────────────────────────────
        sched = self._cache.schedule(team_abbr) or {}
        enrichment["hours_rest"] = float(sched.get("hours_rest", 24.0))
        enrichment["time_zone_change"] = int(sched.get("time_zone_change", 0))
        enrichment["previous_game_innings"] = int(sched.get("previous_game_innings", 9))

        # ── WeatherAgent / existing fields ────────────────────────────
        wx = self._cache.weather(game_id) or {}
        enrichment["wind_speed"] = float(wx.get("wind_speed", 0.0))
        enrichment["wind_direction"] = str(wx.get("wind_direction", ""))

        # ── Umpire ────────────────────────────────────────────────────
        # Umpire name-to-id resolution is done via propiq:umpire:{name_key}
        # The umpire name is not in the PropEdge message, so we leave umpire_cs_pct
        # at its message value (populated by UmpireRunEnvironment context modifier)

        # ── FadeAgent / SteamAgent via Action Network ──────────────────
        public = self._cache.public_bet(game_id) or {}
        enrichment["ticket_pct"] = float(public.get("ticket_pct", 0.0))
        enrichment["money_pct"] = float(public.get("money_pct", 0.0))

        sharp = self._cache.sharp(game_id) or {}
        enrichment["steam_velocity"] = float(sharp.get("steam_velocity", 0.0))
        enrichment["steam_book_count"] = int(sharp.get("steam_book_count", 0))

        # ── Odds (for vig-stripping in odds_math) ──────────────────────
        odds = self._cache.odds(game_id) or {}
        enrichment["odds_over"] = float(odds.get("odds_over", -110.0))
        enrichment["odds_under"] = float(odds.get("odds_under", -110.0))

        return enrichment


# ===========================================================================
# 10. EnrichmentService — orchestrates all scrapers on a staggered schedule
# ===========================================================================

class EnrichmentService:
    """Runs all data enrichment scrapers and populates Redis before first pitch.

    Called by the Spring Boot ``DataHubTasklet`` pre-match trigger or run
    directly via Railway's scheduled job.  Staggered intervals prevent
    API hammering:

        T-0:   Arsenal + Catcher metrics (Baseball Savant CSV)
        T+2m:  Lineups + Starters + wRC+ splits (RotoWire via Apify)
        T+5m:  Weather + Umpires (RotoWire via Apify)
        T+8m:  Public betting + Sharp report + Odds (Action Network via Apify)
        T+12m: Schedule / rest / travel (SportsData.io)
    """

    def __init__(
        self,
        redis_url: str = REDIS_URL,
        apify_key: str = APIFY_API_KEY,
        sportsdata_key: str = SPORTSDATA_KEY,
    ) -> None:
        self._cache = RedisEnrichmentCache(redis_url)
        self._savant = BaseballSavantClient()
        self._apify = ApifyClient(api_key=apify_key)

        self._arsenal_scraper = ArsenalScraper(self._savant, self._cache)
        self._catcher_scraper = CatcherMetricsScraper(self._savant, self._cache)
        self._rotowire_scraper = RotoWireScraper(self._apify, self._cache)
        self._action_scraper = ActionNetworkScraper(self._apify, self._cache)
        self._schedule_scraper = SportsDataScheduleScraper(
            api_key=sportsdata_key, cache=self._cache
        )

    # ------------------------------------------------------------------
    def run_full_enrichment(self, game_date: Optional[str] = None) -> Dict[str, int]:
        """Execute all enrichment scrapers in staggered order.

        Args:
            game_date: ISO date string (default: today).

        Returns:
            Summary dict of records written per scraper.
        """
        today = game_date or date.today().isoformat()
        summary: Dict[str, int] = {}

        logger.info("EnrichmentService: starting full enrichment cycle for %s", today)

        # T+0: Baseball Savant (direct CSV — fast, no JS)
        logger.info("EnrichmentService [T+0]: Baseball Savant...")
        summary["pitcher_arsenal"] = self._arsenal_scraper.refresh_pitcher_arsenal()
        summary["batter_whiff"] = self._arsenal_scraper.refresh_batter_whiff()
        summary["catcher_framing"] = self._catcher_scraper.refresh_framing()
        summary["catcher_pop_time"] = self._catcher_scraper.refresh_pop_time()
        summary["pitcher_delivery"] = self._catcher_scraper.refresh_pitcher_delivery()
        time.sleep(30)  # 30s gap

        # T+2m: RotoWire lineups + starters + wRC+ splits
        logger.info("EnrichmentService [T+2m]: RotoWire lineups + starters + splits...")
        summary["lineups"] = self._rotowire_scraper.refresh_lineups(today)
        summary["starters"] = self._rotowire_scraper.refresh_starters(today)
        summary["wrc_splits"] = self._rotowire_scraper.refresh_wrc_splits()
        summary["dfs_pa_avg"] = self._rotowire_scraper.refresh_dfs_pa_avg()
        time.sleep(90)  # 1.5m gap

        # T+5m: RotoWire weather + umpires
        logger.info("EnrichmentService [T+5m]: RotoWire weather + umpires...")
        summary["weather"] = self._rotowire_scraper.refresh_weather(today)
        summary["umpires"] = self._rotowire_scraper.refresh_umpires()
        time.sleep(90)

        # T+8m: Action Network public betting + sharp + odds
        logger.info("EnrichmentService [T+8m]: Action Network...")
        summary["public_betting"] = self._action_scraper.refresh_public_betting()
        summary["sharp_report"] = self._action_scraper.refresh_sharp_report()
        summary["odds"] = self._action_scraper.refresh_odds()
        time.sleep(60)

        # T+12m: SportsData.io schedule
        logger.info("EnrichmentService [T+12m]: SportsData.io schedule...")
        summary["schedule"] = self._schedule_scraper.refresh_schedule(today)

        total = sum(summary.values())
        logger.info(
            "EnrichmentService: enrichment complete. %d total records written. %s",
            total, summary,
        )
        return summary

    def run_pre_match_refresh(self, game_date: Optional[str] = None) -> Dict[str, int]:
        """Fast refresh cycle (odds + lineups + weather only — run 15m before first pitch)."""
        today = game_date or date.today().isoformat()
        summary: Dict[str, int] = {}
        logger.info("EnrichmentService: pre-match fast refresh for %s", today)
        summary["lineups"] = self._rotowire_scraper.refresh_lineups(today)
        summary["starters"] = self._rotowire_scraper.refresh_starters(today)
        summary["weather"] = self._rotowire_scraper.refresh_weather(today)
        summary["public_betting"] = self._action_scraper.refresh_public_betting()
        summary["sharp_report"] = self._action_scraper.refresh_sharp_report()
        summary["odds"] = self._action_scraper.refresh_odds()
        logger.info("EnrichmentService: fast refresh complete. %s", summary)
        return summary


# ===========================================================================
# Utilities
# ===========================================================================

def _safe_float(val: Any) -> Optional[float]:
    """Return float or None."""
    try:
        return float(str(val).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """Return int or None."""
    try:
        return int(str(val).strip().split(".")[0].replace(",", ""))
    except (TypeError, ValueError):
        return None


def _parse_wind(wind_str: str) -> Tuple[float, str]:
    """Parse '15 mph Out to CF' → (15.0, 'Out to CF').

    Args:
        wind_str: Raw wind string from RotoWire.

    Returns:
        Tuple of (speed_mph, direction_label).
    """
    parts = wind_str.split()
    speed = 0.0
    direction = ""
    for i, part in enumerate(parts):
        try:
            speed = float(part)
            direction = " ".join(parts[i + 2:])  # skip 'mph'
            break
        except (ValueError, IndexError):
            continue
    return speed, direction.strip()


def _parse_american_odds(odds_str: str) -> float:
    """Parse American odds string to float.

    Handles formats: '-110', '+150', '110', 'EVEN', 'PK'.
    """
    s = odds_str.strip().upper().replace(",", "")
    if s in {"EVEN", "PK", "±0", "0"}:
        return 100.0
    try:
        return float(s)
    except ValueError:
        return -110.0


def _parse_sportsdata_dt(dt_str: str) -> Optional[float]:
    """Parse SportsData.io ISO timestamp to Unix epoch float."""
    if not dt_str:
        return None
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(dt_str[:19], fmt[:len(fmt)]).timestamp()
        except ValueError:
            continue
    return None


# ===========================================================================
# Entrypoint
# ===========================================================================

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="PropIQ Enrichment Service")
    parser.add_argument("--date", help="Game date (YYYY-MM-DD), default: today")
    parser.add_argument(
        "--fast", action="store_true",
        help="Run fast pre-match refresh only (odds + lineups + weather)"
    )
    args = parser.parse_args()

    svc = EnrichmentService()
    if args.fast:
        result = svc.run_pre_match_refresh(args.date)
    else:
        result = svc.run_full_enrichment(args.date)

    print(json.dumps(result, indent=2))
