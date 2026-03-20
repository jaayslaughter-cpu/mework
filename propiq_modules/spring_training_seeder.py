"""
SpringTrainingSeeder
Initializes all player/team records at 0-0 for the 2026 season.
Pulls Spring Training stats from SportsData.io and weights them at 30%
against 2025 full-season priors until Opening Day (2026-03-26).

Run once on startup. Re-runs every Sunday 2AM alongside XGBoostTasklet
to incorporate new ST at-bats/innings.
"""

import os
import json
import redis
import logging
import datetime
import requests

logger = logging.getLogger(__name__)

OPENING_DAY        = datetime.date(2026, 3, 26)
ST_WEIGHT          = 0.30   # spring training stats weight
PRIOR_WEIGHT       = 0.70   # 2025 season priors weight
SPORTSDATA_KEY     = os.getenv("SPORTSDATA_API_KEY")
SPORTSDATA_BASE    = "https://api.sportsdata.io/v3/mlb"

# League-average priors (2025 full season baselines)
LEAGUE_AVG_PRIORS = {
    "batting_avg":        0.250,
    "obp":                0.318,
    "slg":                0.420,
    "ops":                0.738,
    "hr_per_ab":          0.034,    # ~1 HR per 29 AB
    "hits_per_ab":        0.250,
    "strikeout_rate":     0.225,
    "era":                4.20,
    "whip":               1.28,
    "k_per_9":            9.1,
    "bb_per_9":           3.1,
}

# Key players to seed (expanded at runtime from SportsData roster)
STAR_PLAYERS = [
    "Aaron Judge", "Rafael Devers", "Juan Soto", "Shohei Ohtani",
    "Fernando Tatis Jr.", "Mookie Betts", "Freddie Freeman",
    "Yordan Alvarez", "Vladimir Guerrero Jr.", "Bo Bichette",
    "Pete Alonso", "Austin Riley", "Matt Olson", "Trea Turner",
    "Bryce Harper", "Kyle Tucker", "Jose Altuve", "Jeremy Pena",
    "Gerrit Cole", "Spencer Strider", "Corbin Burnes",
    "Dylan Cease", "Zack Wheeler", "Kevin Gausman",
    "Logan Webb", "Framber Valdez", "Tyler Glasnow",
]


class SpringTrainingSeeder:
    def __init__(self):
        self.r = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True,
        )

    @staticmethod
    def is_spring_training() -> bool:
        return datetime.date.today() < OPENING_DAY

    def seed_all(self):
        """Main entry — seed all records."""
        logger.info("🌱 SpringTrainingSeeder: initializing 2026 records...")

        # 1. Reset all team records to 0-0
        self._seed_team_records()

        # 2. Seed player baselines
        self._seed_player_baselines()

        # 3. Pull Spring Training stats (if available)
        if self.is_spring_training():
            self._pull_spring_training_stats()

        # 4. Write meta
        seeded_meta = {
            "seeded_at":       datetime.datetime.utcnow().isoformat(),
            "mode":            "spring_training" if self.is_spring_training() else "regular_season",
            "all_records":     "0-0",
            "stat_weight":     ST_WEIGHT if self.is_spring_training() else 1.0,
            "opening_day":     str(OPENING_DAY),
            "days_remaining":  max(0, (OPENING_DAY - datetime.date.today()).days),
        }
        self.r.set("spring_training_meta", json.dumps(seeded_meta))
        logger.info("✅ Seeding complete. Mode=%s", seeded_meta["mode"])
        return seeded_meta

    # ── team records ───────────────────────────────────────────────────────
    def _seed_team_records(self):
        """All 30 MLB teams start 0-0."""
        teams = [
            "ARI","ATL","BAL","BOS","CHC","CWS","CIN","CLE","COL","DET",
            "HOU","KC", "LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK",
            "PHI","PIT","SD", "SEA","SF", "STL","TB", "TEX","TOR","WSH",
        ]
        records = {}
        for t in teams:
            records[t] = {
                "wins": 0, "losses": 0,
                "run_diff": 0, "games_played": 0,
                "record_str": "0-0",
                "source": "spring_training_init_2026",
            }
        self.r.set("team_records", json.dumps(records))
        logger.info("Seeded 0-0 records for %d teams", len(teams))

    # ── player baselines ───────────────────────────────────────────────────
    def _seed_player_baselines(self):
        """
        Each player gets a weighted baseline:
        70% 2025 full-season priors + 30% league average (since ST hasn't started).
        """
        baselines = {}
        for player in STAR_PLAYERS:
            baselines[player] = {
                **LEAGUE_AVG_PRIORS,
                "games_played_2026": 0,
                "ab_2026": 0,
                "hits_2026": 0,
                "hr_2026": 0,
                "season_record": "0-0",
                "st_weight": ST_WEIGHT,
                "prior_weight": PRIOR_WEIGHT,
                "note": "2026 ST baseline — full weight activates Opening Day",
            }

        self.r.set("player_baselines", json.dumps(baselines))
        logger.info("Seeded %d player baselines", len(STAR_PLAYERS))

    # ── spring training API pull ───────────────────────────────────────────
    def _pull_spring_training_stats(self):
        """
        Pull Spring Training game logs from SportsData.io.
        Endpoint: GET /v3/mlb/stats/{format}/PlayerSeasonStats/{season}
        Season: 2026ST (SportsData Spring Training season code)
        """
        if not SPORTSDATA_KEY:
            logger.warning("No SPORTSDATA_API_KEY — skipping ST stat pull")
            return

        try:
            url = f"{SPORTSDATA_BASE}/stats/json/PlayerSeasonStats/2026ST"
            headers = {"Ocp-Apim-Subscription-Key": SPORTSDATA_KEY}
            resp = requests.get(url, headers=headers, timeout=10)

            if resp.status_code == 404:
                logger.info("ST stats not yet posted (season code 2026ST) — using priors only")
                return

            resp.raise_for_status()
            st_stats = resp.json()  # list of player stat objects
            logger.info("Pulled %d ST player stat rows", len(st_stats))

            # blend with baselines
            baselines_raw = self.r.get("player_baselines")
            baselines = json.loads(baselines_raw) if baselines_raw else {}

            for row in st_stats:
                name = f"{row.get('FirstName','')} {row.get('LastName','')}".strip()
                if not name:
                    continue

                st_avg  = row.get("BattingAverage", 0) or 0
                st_hrs  = row.get("HomeRuns", 0) or 0
                st_abs  = row.get("AtBats", 0) or 0

                if name not in baselines:
                    baselines[name] = {**LEAGUE_AVG_PRIORS, "games_played_2026": 0}

                # weighted blend: 30% ST actual + 70% prior
                prior_avg = baselines[name].get("batting_avg", LEAGUE_AVG_PRIORS["batting_avg"])
                if st_abs > 0:
                    blended_avg = ST_WEIGHT * st_avg + PRIOR_WEIGHT * prior_avg
                    baselines[name]["batting_avg_blended"] = round(blended_avg, 3)
                    baselines[name]["ab_2026"]             = st_abs
                    baselines[name]["hits_2026"]           = row.get("Hits", 0) or 0
                    baselines[name]["hr_2026"]             = st_hrs
                    baselines[name]["st_abs"]              = st_abs

            self.r.set("player_baselines", json.dumps(baselines))
            logger.info("Blended ST stats for %d players", len(st_stats))

        except requests.RequestException as e:
            logger.warning("ST stat pull failed: %s", e)

    # ── convenience: get player baseline ──────────────────────────────────
    def get_player_baseline(self, player_name: str) -> dict:
        raw = self.r.get("player_baselines")
        if not raw:
            return LEAGUE_AVG_PRIORS
        baselines = json.loads(raw)
        return baselines.get(player_name, LEAGUE_AVG_PRIORS)

    # ── convenience: get team record ───────────────────────────────────────
    def get_team_record(self, team_abbr: str) -> dict:
        raw = self.r.get("team_records")
        if not raw:
            return {"wins": 0, "losses": 0, "record_str": "0-0"}
        records = json.loads(raw)
        return records.get(team_abbr.upper(), {"wins": 0, "losses": 0, "record_str": "0-0"})


# ── standalone run ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seeder = SpringTrainingSeeder()
    meta = seeder.seed_all()
    print(json.dumps(meta, indent=2))
