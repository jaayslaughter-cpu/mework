"""
DataHubTasklet — Every 15s, pulls from 20 data sources → Redis "mlb_hub".

Data sources (20 total):
  API Sources (no scraping):
    1. SportsData.io v3    — games, player stats, bullpen pitch counts
    2. The Odds API v4     — live odds (DK/FD/BetMGM/bet365)
    3. Tank01 RapidAPI     — player stats, props, real-time scores
    4. MLB Stats API       — official boxscores, rosters, standings
    5. ESPN MLB API        — scores, news, injury updates

  Scraped Sources (anti-ban via BaseScraper):
    6.  RotoWire umpire-stats-daily  → umpire K%, called strike%, tight zone
    7.  RotoWire weather             → wind speed/dir, temp, precip
    8.  RotoWire stats-bvp           → batter vs pitcher xwOBA by hand
    9.  RotoWire stats-advanced      → FIP, SwStr%, CSW%, wRC+
    10. RotoWire batting-orders      → confirmed lineups, batting positions
    11. RotoWire news?injuries=all   → injury status, scratch probability
    12. RotoWire projected-starters  → starter name, hand, rest days
    13. RotoWire stats-batted-ball   → barrel%, hard-hit%, exit velo
    14. Action Network public-betting → public bet %, sharp money
    15. Action Network odds           → line movement, RLM detection
    16. Baseball Savant (Apify)       → pitch arsenal: whiff%, usage, zone%

  Computed / ML:
    17. No-vig prices         — true fair value across all books
    18. Wind/park adjustments — HR/hits boost by direction + park
    19. Bullpen fatigue score — 0-4 scale per team
    20. Spring training mode  — 0-0 records baseline until Opening Day

Redis Structure (mlb_hub):
  mlb_hub:games          — today's game slate
  mlb_hub:odds           — live odds per market
  mlb_hub:umpires        — today's ump assignments + K%
  mlb_hub:weather        — per-game weather conditions
  mlb_hub:lineups        — confirmed batting orders
  mlb_hub:injuries       — injury status per player
  mlb_hub:pitchers       — FIP/SwStr%/K9 per starter
  mlb_hub:bvp            — batter vs pitcher matchups
  mlb_hub:public_betting — public % + sharp money
  mlb_hub:line_movement  — RLM + steam moves
  mlb_hub:arsenal        — pitch arsenal (Savant)
  mlb_hub:batted_ball    — barrel%/hard-hit%/exit velo
  mlb_hub:bullpen_fatigue — team fatigue scores
  mlb_hub:no_vig_prices  — vig-removed fair prices
  mlb_hub:wind_adjustments — HR/hits boosts per game
  mlb_hub:spring_training — ST mode flag + records
  mlb_hub:last_updated   — ISO timestamp of last successful run
"""

import json
import logging
import os
import time
from datetime import datetime, date
from typing import Optional

import redis
import requests

from ..scrapers import RotoWireScraper, ActionNetworkScraper, BaseballSavantScraper
from ..analytics import NoVigCalculator, WindParkCalculator, BullpenFatigueScorer
from ..analytics.bullpen_fatigue import RelieverData
from ..analytics.wind_park_calculator import WindCondition

logger = logging.getLogger(__name__)

# ── Config from env ────────────────────────────────────────────────────────────
SPORTSDATA_KEY = os.getenv("SPORTSDATA_KEY", "")
ODDS_API_KEY   = os.getenv("ODDS_API_KEY", "")
TANK01_KEY     = os.getenv("TANK01_KEY", "")
REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")

HUB_TTL         = 30      # seconds — hub data expires after 30s (refreshed every 15s)
HUB_CACHE_TTL   = 900     # 15 min cache for scraped data to reduce scrape frequency

OPENING_DAY_2026 = date(2026, 3, 26)

# ── California offshore books ──────────────────────────────────────────────────
BOOKS = ["draftkings", "fanduel", "betmgm", "bet365"]
BOOK_DISPLAY = {"draftkings": "DraftKings", "fanduel": "FanDuel", "betmgm": "BetMGM", "bet365": "bet365"}

# ── MLB prop markets for The Odds API ─────────────────────────────────────────
PROP_MARKETS = [
    "batter_hits", "batter_home_runs", "batter_total_bases",
    "batter_rbis", "batter_strikeouts", "pitcher_strikeouts",
    "batter_walks", "pitcher_hits_allowed",
]


class DataHubTasklet:
    """
    Runs every 15 seconds. Pulls from 20 sources → populates Redis mlb_hub.
    All agents read from Redis, never call APIs directly.
    """

    def __init__(self):
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)
        self.roto  = RotoWireScraper(redis_client=self.redis)
        self.action = ActionNetworkScraper(redis_client=self.redis)
        self.savant = BaseballSavantScraper(redis_client=self.redis)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PropIQ/1.0 (prop betting system)",
            "Accept": "application/json",
        })
        self._spring_training_mode = date.today() < OPENING_DAY_2026

    # ── Main execute (scheduled every 15s) ────────────────────────────────────
    def execute(self):
        start = time.time()
        logger.info("[DATAHUB] ⚡ Starting 15s data refresh cycle")

        hub = {}

        try:
            # ── Fast API calls (run first, highest priority) ──────────────────
            hub["todays_games"]    = self._fetch_todays_games()
            hub["odds"]            = self._fetch_odds()
            hub["player_stats"]    = self._fetch_player_stats()
            hub["scores"]          = self._fetch_live_scores()
            hub["bullpen_usage"]   = self._fetch_bullpen_usage()

            # ── Scraped data (cached 15 min, polite rate) ─────────────────────
            hub["umpires"]         = self.roto.get_umpire_stats()
            hub["weather"]         = self.roto.get_weather()
            hub["lineups"]         = self.roto.get_lineups()
            hub["injuries"]        = self.roto.get_injuries()
            hub["pitchers"]        = self.roto.get_advanced_pitching_stats()
            hub["bvp"]             = self.roto.get_bvp_stats()
            hub["batted_ball"]     = self.roto.get_batted_ball_stats()
            hub["projected_starters"] = self.roto.get_projected_starters()
            hub["public_betting"]  = self.action.get_public_betting()
            hub["line_movement"]   = self.action.get_line_movement()
            hub["sharp_report"]    = self.action.get_sharp_report()
            hub["action_projections"] = self.action.get_projections()

            # ── Baseball Savant (daily, cached 1hr) ───────────────────────────
            hub["savant_arsenal"]  = self.savant.get_pitch_arsenal()
            hub["savant_statcast"] = self.savant.get_statcast_leaders()

            # ── Computed analytics ────────────────────────────────────────────
            hub["no_vig_prices"]   = self._compute_no_vig_prices(hub["odds"])
            hub["wind_adjustments"] = self._compute_wind_adjustments(hub["weather"], hub["todays_games"])
            hub["bullpen_fatigue"] = self._compute_bullpen_fatigue(hub["bullpen_usage"])
            hub["spring_training"] = self._build_spring_training_context()

        except Exception as e:
            logger.error(f"[DATAHUB ERROR] {e}", exc_info=True)

        # ── Persist to Redis ──────────────────────────────────────────────────
        hub["last_updated"] = datetime.utcnow().isoformat()
        self._write_to_redis(hub)

        elapsed = round(time.time() - start, 2)
        game_count = len(hub.get("todays_games", []))
        prop_count = len(hub.get("odds", {}).get("props", []))
        logger.info(
            f"[DATAHUB] ✅ Done in {elapsed}s | "
            f"{game_count} games | {prop_count} props | "
            f"{'🌱 ST MODE' if self._spring_training_mode else '⚾ SEASON'}"
        )
        return hub

    # ── 1. SportsData.io — Today's games ─────────────────────────────────────
    def _fetch_todays_games(self) -> list:
        today = datetime.utcnow().strftime("%Y-%b-%d").upper()
        url = f"https://api.sportsdata.io/v3/mlb/scores/json/GamesByDate/{today}"
        data = self._api_get(url, params={"key": SPORTSDATA_KEY})
        if not data:
            return []

        games = []
        for g in data:
            games.append({
                "game_id": g.get("GameID"),
                "home": g.get("HomeTeam"),
                "away": g.get("AwayTeam"),
                "park": g.get("StadiumID", g.get("HomeTeam")),
                "status": g.get("Status", "Scheduled"),
                "date_time": g.get("DateTime"),
                "matchup_str": f"{g.get('AwayTeam')}@{g.get('HomeTeam')}",
            })
        return games

    # ── 2. The Odds API — Live odds ───────────────────────────────────────────
    def _fetch_odds(self) -> dict:
        """Fetch game lines + player props for all today's games."""
        # Game lines
        url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/"
        game_odds = self._api_get(url, params={
            "apiKey": ODDS_API_KEY,
            "regions": "us",
            "markets": "h2h,spreads,totals",
            "bookmakers": ",".join(BOOKS),
        }) or []

        # Player props (per event — rate limited, batch carefully)
        props = []
        events_url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/events"
        events = self._api_get(events_url, params={"apiKey": ODDS_API_KEY}) or []

        for event in events[:8]:  # Max 8 events to stay within rate limits
            event_id = event.get("id")
            if not event_id:
                continue
            prop_url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds"
            prop_data = self._api_get(prop_url, params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": ",".join(PROP_MARKETS),
                "bookmakers": ",".join(BOOKS),
                "oddsFormat": "american",
            })
            if prop_data:
                props.extend(self._parse_props(prop_data))
            time.sleep(1.2)  # 1.2s between prop calls (rate limit = 500/month)

        return {"game_lines": game_odds, "props": props}

    # ── 3. Tank01 — Player stats ──────────────────────────────────────────────
    def _fetch_player_stats(self) -> list:
        url = "https://tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com/getMLBPlayerList"
        headers = {
            "X-RapidAPI-Key": TANK01_KEY,
            "X-RapidAPI-Host": "tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com",
        }
        data = self._api_get(url, headers_override=headers)
        if not data:
            return []
        return data.get("body", []) if isinstance(data, dict) else data

    # ── 4. SportsData.io — Bullpen pitch counts ───────────────────────────────
    def _fetch_bullpen_usage(self) -> dict:
        """Get yesterday's pitcher pitch counts to compute bullpen fatigue."""
        from datetime import timedelta
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%b-%d").upper()
        url = f"https://api.sportsdata.io/v3/mlb/stats/json/BoxScores/{yesterday}"
        data = self._api_get(url, params={"key": SPORTSDATA_KEY}) or []

        team_relievers: dict[str, list] = {}
        for game in data:
            for side in ["HomeTeamPlayerStats", "AwayTeamPlayerStats"]:
                for player in game.get(side, []):
                    if player.get("PitchingInningsPitched", 0) > 0:
                        team = player.get("Team", "UNK")
                        team_relievers.setdefault(team, []).append({
                            "name": player.get("Name", ""),
                            "pitches": player.get("PitchingPitches", 0),
                            "innings": float(player.get("PitchingInningsPitched", 0)),
                            "is_starter": player.get("PitchingGamesPitched", 0) == 1 and
                                          float(player.get("PitchingInningsPitched", 0)) >= 3.0,
                        })
        return team_relievers

    # ── 5. Live scores (Tank01) ───────────────────────────────────────────────
    def _fetch_live_scores(self) -> list:
        url = "https://tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com/getMLBScoresOnly"
        headers = {
            "X-RapidAPI-Key": TANK01_KEY,
            "X-RapidAPI-Host": "tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com",
        }
        data = self._api_get(url, params={"gameDate": datetime.utcnow().strftime("%Y%m%d")},
                             headers_override=headers)
        if isinstance(data, dict):
            return list(data.get("body", {}).values())
        return []

    # ── Analytics: No-vig prices ──────────────────────────────────────────────
    def _compute_no_vig_prices(self, odds_data: dict) -> list:
        """For every prop, compute true no-vig probability and fair line."""
        no_vig = []
        for prop in odds_data.get("props", []):
            over_odds = prop.get("over_american")
            under_odds = prop.get("under_american")
            if over_odds and under_odds:
                try:
                    result = NoVigCalculator.remove_vig_two_outcome(
                        int(over_odds), int(under_odds)
                    )
                    no_vig.append({
                        "player": prop.get("player"),
                        "prop_type": prop.get("prop_type"),
                        "line": prop.get("line"),
                        "over_fair_american": result["over_fair_american"],
                        "over_true_prob": result["over_true_prob"],
                        "vig_pct": result["vig_pct"],
                    })
                except Exception:
                    pass
        return no_vig

    # ── Analytics: Wind adjustments ───────────────────────────────────────────
    def _compute_wind_adjustments(self, weather_list: list, games: list) -> dict:
        """Map each game to its wind adjustment for HR/hits props."""
        adjustments = {}
        game_park_map = {g.get("matchup_str", ""): g.get("park", "") for g in games}

        for w in weather_list:
            game = w.get("game", "")
            park = game_park_map.get(game, "")
            if not park or len(park) > 5:
                continue

            wind = WindCondition(
                speed_mph=float(w.get("wind_speed", 0)),
                direction=str(w.get("wind_dir", "calm")),
                temp_f=float(w.get("temp_f", 72)),
            )

            adj = WindParkCalculator.calculate(park, wind, prop_type="HR")
            adjustments[game] = {
                "park": park,
                "wind_speed": wind.speed_mph,
                "wind_dir": wind.direction,
                "hr_boost_pct": adj.hr_boost_pct,
                "hit_boost_pct": adj.hit_boost_pct,
                "under_boost_pct": adj.under_boost_pct,
                "trigger_flag": adj.trigger_flag,
                "summary": WindParkCalculator.summarize(park, wind),
            }
        return adjustments

    # ── Analytics: Bullpen fatigue ────────────────────────────────────────────
    def _compute_bullpen_fatigue(self, bullpen_usage: dict) -> dict:
        """Score each team's bullpen fatigue 0-4."""
        fatigue_scores = {}
        for team, pitchers in bullpen_usage.items():
            relievers = []
            total_innings = 0.0
            for p in pitchers:
                if not p.get("is_starter", False):
                    pc = p.get("pitches", 0)
                    innings = p.get("innings", 0.0)
                    total_innings += innings
                    relievers.append(RelieverData(
                        name=p.get("name", ""),
                        pitches_last_outing=pc,
                        pitches_l3=pc,   # approximation (single day data)
                        days_rest=0 if pc > 0 else 1,
                        innings_l2=innings,
                    ))

            result = BullpenFatigueScorer.score(relievers, total_innings)
            fatigue_scores[team] = {
                "score": result.score,
                "label": result.label,
                "hit_boost_pct": result.hit_prop_boost_pct,
                "starter_k_boost": result.starter_k_boost_pct,
                "total_over_boost": result.total_over_boost_pct,
                "triggers": result.triggers,
                "summary": BullpenFatigueScorer.under_threshold_summary(result, team),
            }
        return fatigue_scores

    # ── Spring Training context ───────────────────────────────────────────────
    def _build_spring_training_context(self) -> dict:
        today = date.today()
        is_st = today < OPENING_DAY_2026
        days_until = (OPENING_DAY_2026 - today).days if is_st else 0
        return {
            "mode": "spring_training" if is_st else "regular_season",
            "all_records": "0-0" if is_st else "live",
            "stat_weight": 0.30 if is_st else 1.00,
            "prior_weight": 0.70 if is_st else 0.00,
            "days_until_opening": max(0, days_until),
            "opening_day": str(OPENING_DAY_2026),
        }

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _parse_props(self, event_odds: dict) -> list:
        props = []
        try:
            for bookmaker in event_odds.get("bookmakers", []):
                book = bookmaker.get("key", "")
                if book not in BOOKS:
                    continue
                for market in bookmaker.get("markets", []):
                    market_key = market.get("key", "")
                    for outcome in market.get("outcomes", []):
                        props.append({
                            "player": outcome.get("description", ""),
                            "prop_type": market_key,
                            "over_under": outcome.get("name", ""),
                            "line": float(outcome.get("point", 0)),
                            "over_american": outcome.get("price") if outcome.get("name") == "Over" else None,
                            "under_american": outcome.get("price") if outcome.get("name") == "Under" else None,
                            "book": BOOK_DISPLAY.get(book, book),
                        })
        except Exception as e:
            logger.debug(f"[PROPS PARSE] {e}")
        return props

    def _api_get(self, url: str, params: Optional[dict] = None,
                 headers_override: Optional[dict] = None) -> Optional[any]:
        try:
            headers = {"Accept": "application/json"}
            if headers_override:
                headers.update(headers_override)
            resp = self.session.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                logger.warning(f"[RATE LIMIT] {url} — waiting 60s")
                time.sleep(62)
            else:
                logger.error(f"[API ERROR] {url}: {e}")
        except Exception as e:
            logger.error(f"[API ERROR] {url}: {e}")
        return None

    def _write_to_redis(self, hub: dict):
        """Write each hub section to its own Redis key for granular TTLs."""
        sections = {
            "mlb_hub:games":         ("todays_games",   HUB_TTL),
            "mlb_hub:odds":          ("odds",           HUB_TTL),
            "mlb_hub:umpires":       ("umpires",        HUB_CACHE_TTL),
            "mlb_hub:weather":       ("weather",        HUB_CACHE_TTL),
            "mlb_hub:lineups":       ("lineups",        HUB_CACHE_TTL),
            "mlb_hub:injuries":      ("injuries",       HUB_CACHE_TTL),
            "mlb_hub:pitchers":      ("pitchers",       HUB_CACHE_TTL),
            "mlb_hub:bvp":           ("bvp",            HUB_CACHE_TTL),
            "mlb_hub:public_betting":("public_betting", 120),
            "mlb_hub:line_movement": ("line_movement",  120),
            "mlb_hub:arsenal":       ("savant_arsenal", 3600),
            "mlb_hub:batted_ball":   ("batted_ball",    HUB_CACHE_TTL),
            "mlb_hub:bullpen_fatigue":("bullpen_fatigue",HUB_TTL),
            "mlb_hub:no_vig_prices": ("no_vig_prices",  HUB_TTL),
            "mlb_hub:wind_adjustments":("wind_adjustments",HUB_CACHE_TTL),
            "mlb_hub:spring_training":("spring_training",3600),
        }

        for redis_key, (hub_key, ttl) in sections.items():
            val = hub.get(hub_key)
            if val is not None:
                try:
                    self.redis.setex(redis_key, ttl, json.dumps(val))
                except Exception as e:
                    logger.error(f"[REDIS WRITE ERROR] {redis_key}: {e}")

        # Master hub snapshot
        try:
            self.redis.setex("mlb_hub", HUB_TTL, json.dumps(hub))
            self.redis.set("mlb_hub:last_updated", hub.get("last_updated", ""))
        except Exception as e:
            logger.error(f"[REDIS WRITE ERROR] mlb_hub: {e}")
