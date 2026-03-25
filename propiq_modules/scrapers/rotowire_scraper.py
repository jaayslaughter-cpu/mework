"""
RotoWireScraper — Scrapes 8 RotoWire MLB endpoints with anti-ban protection.

Endpoints:
  1. umpire-stats-daily.php  → called_strike_pct, K%, accuracy
  2. weather.php             → wind_speed, wind_dir, temp, precip
  3. stats-bvp.php           → batter vs pitcher (xwOBA by hand)
  4. stats-advanced.php      → FIP, SwStr%, CSW%, wRC+
  5. batting-orders.php      → confirmed lineups, batting position
  6. news.php?injuries=all   → injury status, days_since_return
  7. projected-starters.php  → starter name, handedness, rest days
  8. stats-batted-ball.php   → barrel%, hard-hit%, exit velo

IMPORTANT: All data is publicly available on RotoWire.
  - Scrape rate: max once per 15 minutes per endpoint (enforced via Redis cache)
  - User-agent rotation via BaseScraper
  - Delay: 4-9 seconds between requests (configured in BaseScraper DOMAIN_DELAYS)
  - Never scrape same endpoint more than 4x/hour
"""

import re
import logging
from typing import Optional
from datetime import datetime
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.rotowire.com"

# Ump hand size heuristics from K% data
UMP_TIGHTNESS = {
    "Pat Hoberg": {"tight": False, "k_pct": 24.1, "accuracy": 93.7},
    "CB Bucknor": {"tight": True,  "k_pct": 22.3, "accuracy": 88.2},
    "Angel Hernandez": {"tight": False, "k_pct": 19.8, "accuracy": 86.1},
    "Doug Eddings": {"tight": True,  "k_pct": 23.8, "accuracy": 91.2},
    "Dan Iassogna": {"tight": False, "k_pct": 21.4, "accuracy": 90.5},
    "Ted Barrett": {"tight": False, "k_pct": 20.9, "accuracy": 89.8},
    "Joe West": {"tight": False, "k_pct": 20.1, "accuracy": 87.3},
    "Stu Scheurwater": {"tight": True, "k_pct": 23.1, "accuracy": 92.4},
    "Mark Carlson": {"tight": False, "k_pct": 21.7, "accuracy": 90.1},
    "Jim Reynolds": {"tight": True,  "k_pct": 22.9, "accuracy": 91.8},
}


class RotoWireScraper(BaseScraper):

    def __init__(self, redis_client=None):
        super().__init__(redis_client=redis_client, domain_override="rotowire.com")

    # ── 1. Umpire Stats ───────────────────────────────────────────────────────
    def get_umpire_stats(self) -> list[dict]:
        """
        Scrape daily umpire assignments + historical K% stats.
        Returns: [{"umpire": "Pat Hoberg", "game": "NYY@BOS", "k_pct": 24.1,
                   "called_strike_pct": 68.2, "accuracy": 93.7, "home_bias": 1.2,
                   "tight_zone": False}]
        """
        url = f"{BASE_URL}/baseball/umpire-stats-daily.php"
        html = self.get(url)
        if not html:
            return self._umpire_fallback()

        soup = BeautifulSoup(html, "html.parser")
        umps = []

        try:
            table = soup.find("table", class_=re.compile("RW-table|umpire"))
            if not table:
                # Try any table with umpire data
                tables = soup.find_all("table")
                table = tables[0] if tables else None

            if table:
                rows = table.find_all("tr")[1:]  # skip header
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 5:
                        ump_name = cells[0].get_text(strip=True)
                        game = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                        # Pull historical data from our lookup if scrape is sparse
                        hist = UMP_TIGHTNESS.get(ump_name, {})
                        k_pct = float(cells[2].get_text(strip=True).replace("%", "") or hist.get("k_pct", 21.0))
                        called_k = float(cells[3].get_text(strip=True).replace("%", "") or 67.0)
                        accuracy = float(cells[4].get_text(strip=True).replace("%", "") or hist.get("accuracy", 90.0))

                        umps.append({
                            "umpire": ump_name,
                            "game": game,
                            "k_pct": k_pct,
                            "called_strike_pct": called_k,
                            "accuracy": accuracy,
                            "home_bias": hist.get("home_bias", 0.0),
                            "tight_zone": called_k < 66.0 or k_pct > 22.0,
                        })
        except Exception as e:
            logger.error(f"[UMPIRE PARSE ERROR] {e}")
            return self._umpire_fallback()

        return umps if umps else self._umpire_fallback()

    def _umpire_fallback(self) -> list[dict]:
        """Return cached baseline umpire data if scrape fails."""
        return [
            {"umpire": k, "game": "TBD", "k_pct": v["k_pct"],
             "called_strike_pct": 67.0 if not v["tight"] else 64.8,
             "accuracy": v["accuracy"], "home_bias": 0.0,
             "tight_zone": v["tight"]}
            for k, v in list(UMP_TIGHTNESS.items())[:5]
        ]

    # ── 2. Weather ────────────────────────────────────────────────────────────
    def get_weather(self) -> list[dict]:
        """
        Scrape today's game weather conditions.
        Returns: [{"game": "NYY@BOS", "temp_f": 68, "wind_speed": 12,
                   "wind_dir": "out_to_LF", "precip_pct": 10,
                   "dome": False, "trigger_flag": True}]
        """
        url = f"{BASE_URL}/baseball/weather.php"
        html = self.get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        games = []

        try:
            weather_blocks = soup.find_all("div", class_=re.compile("weather|game-weather|rw-weather"))
            if not weather_blocks:
                weather_blocks = soup.find_all("div", class_=lambda c: c and "game" in c.lower())

            for block in weather_blocks:
                text = block.get_text(separator=" ", strip=True)

                game_match = re.search(r"([A-Z]{2,3})\s*[@vs]+\s*([A-Z]{2,3})", text)
                game = game_match.group(0) if game_match else "Unknown"

                temp_match = re.search(r"(\d{2,3})[°\s]*[Ff]", text)
                temp = int(temp_match.group(1)) if temp_match else 72

                wind_match = re.search(r"(\d+)\s*mph", text, re.I)
                wind_speed = int(wind_match.group(1)) if wind_match else 0

                # Parse wind direction
                wind_dir = "calm"
                text_lower = text.lower()
                if "out to lf" in text_lower or "out to left" in text_lower:
                    wind_dir = "out_to_LF"
                elif "out to rf" in text_lower or "out to right" in text_lower:
                    wind_dir = "out_to_RF"
                elif "out to cf" in text_lower or "out to center" in text_lower:
                    wind_dir = "out_to_CF"
                elif "in from lf" in text_lower or "in from left" in text_lower:
                    wind_dir = "in_from_LF"
                elif "in from rf" in text_lower or "in from right" in text_lower:
                    wind_dir = "in_from_RF"
                elif "r to l" in text_lower:
                    wind_dir = "R_to_L"
                elif "l to r" in text_lower:
                    wind_dir = "L_to_R"
                elif wind_speed > 3:
                    wind_dir = "variable"

                precip_match = re.search(r"(\d+)%?\s*(rain|precip|chance)", text, re.I)
                precip = int(precip_match.group(1)) if precip_match else 0

                dome = any(w in text_lower for w in ["dome", "retractable", "indoor", "roof closed"])

                games.append({
                    "game": game,
                    "temp_f": temp,
                    "wind_speed": wind_speed,
                    "wind_dir": wind_dir,
                    "precip_pct": precip,
                    "dome": dome,
                    "trigger_flag": wind_speed >= 8 and any(
                        d in wind_dir for d in ["out_to", "R_to_L", "L_to_R"]
                    ),
                })

        except Exception as e:
            logger.error(f"[WEATHER PARSE ERROR] {e}")

        return games

    # ── 3. Batter vs Pitcher (BVP) ────────────────────────────────────────────
    def get_bvp_stats(self, batter: Optional[str] = None) -> list[dict]:
        """
        Scrape batter vs pitcher matchup stats.
        Returns: [{"batter": "Judge", "pitcher": "Sale", "ab": 18, "avg": .333,
                   "xwoba": .412, "k_pct": 22.2, "pitch_hand": "LHP"}]
        """
        url = f"{BASE_URL}/baseball/stats-bvp.php"
        html = self.get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        records = []

        try:
            table = soup.find("table", id=re.compile("stats|bvp", re.I))
            if not table:
                tables = soup.find_all("table")
                table = next((t for t in tables if t.find("tr")), None)

            if table:
                headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
                rows = table.find_all("tr")[1:]
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 6:
                        record = {
                            "batter": cells[0].get_text(strip=True),
                            "pitcher": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                            "ab": int(cells[2].get_text(strip=True) or 0) if len(cells) > 2 else 0,
                            "avg": float(cells[3].get_text(strip=True) or 0) if len(cells) > 3 else 0.0,
                            "xwoba": float(cells[4].get_text(strip=True) or 0.320) if len(cells) > 4 else 0.320,
                            "pitch_hand": "RHP",
                        }
                        if batter and batter.lower() not in record["batter"].lower():
                            continue
                        records.append(record)
        except Exception as e:
            logger.error(f"[BVP PARSE ERROR] {e}")

        return records

    # ── 4. Advanced Stats ─────────────────────────────────────────────────────
    def get_advanced_pitching_stats(self) -> list[dict]:
        """
        Scrape pitcher advanced stats: FIP, SwStr%, CSW%, SIERA.
        Returns: [{"pitcher": "Gerrit Cole", "fip": 3.12, "swstr_pct": 13.4,
                   "csw_pct": 28.9, "siera": 3.08, "k9": 11.2, "hand": "R"}]
        """
        url = f"{BASE_URL}/baseball/stats-advanced.php"
        html = self.get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        pitchers = []

        try:
            tables = soup.find_all("table")
            for table in tables:
                headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
                if not any(h in headers for h in ["fip", "swstr", "csw"]):
                    continue

                rows = table.find_all("tr")[1:]
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 5:
                        try:
                            pitchers.append({
                                "pitcher": cells[0].get_text(strip=True),
                                "fip": float(cells[3].get_text(strip=True) or 4.0),
                                "swstr_pct": float(cells[5].get_text(strip=True).replace("%","") or 10.0),
                                "csw_pct": float(cells[6].get_text(strip=True).replace("%","") or 27.0),
                                "siera": float(cells[4].get_text(strip=True) or 4.0),
                                "k9": float(cells[2].get_text(strip=True) or 8.0),
                                "hand": "R",
                            })
                        except (ValueError, IndexError):
                            continue
        except Exception as e:
            logger.error(f"[ADVANCED STATS PARSE ERROR] {e}")

        return pitchers

    # ── 5. Batting Orders / Confirmed Lineups ─────────────────────────────────
    def get_lineups(self, team_code: str = "") -> list[dict]:
        """
        Scrape today's confirmed batting orders.
        Returns: [{"team": "NYY", "players": [
                    {"name": "Judge", "position": 2, "confirmed": True}
                  ]}]
        """
        url = f"{BASE_URL}/baseball/batting-orders.php"
        if team_code:
            url += f"?team={team_code.upper()}"
        html = self.get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        lineups = []

        try:
            lineup_blocks = soup.find_all("div", class_=re.compile("lineup|batting-order", re.I))
            for block in lineup_blocks:
                team_tag = block.find(class_=re.compile("team|abbrev", re.I))
                team = team_tag.get_text(strip=True) if team_tag else "UNK"
                confirmed = "confirmed" in block.get_text().lower()
                players = []
                rows = block.find_all("li") or block.find_all("tr")
                for i, row in enumerate(rows[:9], 1):
                    name = row.get_text(strip=True).split("\n")[0]
                    if name and len(name) > 2:
                        players.append({"name": name, "position": i, "confirmed": confirmed})
                if players:
                    lineups.append({"team": team, "players": players, "confirmed": confirmed})
        except Exception as e:
            logger.error(f"[LINEUP PARSE ERROR] {e}")

        return lineups

    # ── 6. Injury Report ──────────────────────────────────────────────────────
    def get_injuries(self) -> list[dict]:
        """
        Scrape MLB injury news.
        Returns: [{"player": "Judge", "status": "day-to-day",
                   "injury": "toe sprain", "team": "NYY",
                   "scratch_prob": 0.40, "days_since_return": -1}]
        """
        url = f"{BASE_URL}/baseball/news.php?injuries=all"
        html = self.get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        injuries = []

        try:
            items = soup.find_all("div", class_=re.compile("news-item|player-news|injury", re.I))
            for item in items:
                text = item.get_text(separator=" ", strip=True)
                # Extract status
                status = "active"
                for s in ["day-to-day", "dtd", "out", "doubtful", "questionable", "probable", "IL", "IL-10", "IL-15", "IL-60"]:
                    if s.lower() in text.lower():
                        status = s.lower()
                        break

                # Scratch probability heuristic
                scratch_prob = {
                    "out": 1.0, "il": 1.0, "il-10": 1.0, "il-15": 1.0, "il-60": 1.0,
                    "doubtful": 0.75, "questionable": 0.50,
                    "day-to-day": 0.30, "dtd": 0.30, "probable": 0.10,
                }.get(status.lower(), 0.05)

                player_tag = item.find(class_=re.compile("player|name", re.I))
                player_name = player_tag.get_text(strip=True) if player_tag else ""

                team_tag = item.find(class_=re.compile("team|abbrev", re.I))
                team = team_tag.get_text(strip=True) if team_tag else ""

                # Injury type
                injury_keywords = ["sprain", "strain", "fracture", "surgery", "oblique",
                                   "hamstring", "knee", "elbow", "shoulder", "wrist", "toe",
                                   "back", "hamate", "blister", "illness", "concussion"]
                injury_type = next(
                    (kw for kw in injury_keywords if kw in text.lower()), "unknown"
                )

                injuries.append({
                    "player": player_name,
                    "team": team,
                    "status": status,
                    "injury": injury_type,
                    "scratch_prob": scratch_prob,
                    "days_since_return": -1,  # enriched by XGBoost feature store
                    "raw_text": text[:200],
                })
        except Exception as e:
            logger.error(f"[INJURY PARSE ERROR] {e}")

        return injuries

    # ── 7. Projected Starters ─────────────────────────────────────────────────
    def get_projected_starters(self) -> list[dict]:
        """
        Scrape projected starting pitchers for today's slate.
        Returns: [{"game": "NYY@BOS", "home_starter": "Cole", "away_starter": "Sale",
                   "home_hand": "R", "away_hand": "L",
                   "home_rest_days": 5, "away_rest_days": 4}]
        """
        url = f"{BASE_URL}/baseball/projected-starters.php"
        html = self.get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        starters = []

        try:
            blocks = soup.find_all("div", class_=re.compile("game|starter|matchup", re.I))
            for block in blocks:
                text = block.get_text(separator="|", strip=True)
                game_match = re.search(r"([A-Z]{2,3})\s*[@vs]+\s*([A-Z]{2,3})", text)
                if game_match:
                    home_hand_match = re.search(r"\(([RL]HP)\)", text)
                    starters.append({
                        "game": game_match.group(0),
                        "home_starter": "TBD",
                        "away_starter": "TBD",
                        "home_hand": home_hand_match.group(1) if home_hand_match else "RHP",
                        "away_hand": "RHP",
                        "home_rest_days": 5,
                        "away_rest_days": 5,
                    })
        except Exception as e:
            logger.error(f"[STARTERS PARSE ERROR] {e}")

        return starters

    # ── 8. Batted Ball Stats ──────────────────────────────────────────────────
    def get_batted_ball_stats(self) -> list[dict]:
        """
        Scrape Barrel%, HardHit%, AvgExitVelo for all batters.
        Returns: [{"batter": "Judge", "barrel_pct": 22.1, "hard_hit_pct": 54.3,
                   "avg_exit_velo": 95.2, "xslg": .652}]
        """
        url = f"{BASE_URL}/baseball/stats-batted-ball.php"
        html = self.get(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        batters = []

        try:
            tables = soup.find_all("table")
            for table in tables:
                headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
                if not any(h in headers for h in ["barrel", "hard hit", "exit velo"]):
                    continue
                rows = table.find_all("tr")[1:]
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 5:
                        try:
                            batters.append({
                                "batter": cells[0].get_text(strip=True),
                                "barrel_pct": float(cells[3].get_text(strip=True).replace("%","") or 8.0),
                                "hard_hit_pct": float(cells[4].get_text(strip=True).replace("%","") or 40.0),
                                "avg_exit_velo": float(cells[2].get_text(strip=True) or 88.0),
                                "xslg": float(cells[5].get_text(strip=True) or 0.400) if len(cells) > 5 else 0.400,
                            })
                        except (ValueError, IndexError):
                            continue
        except Exception as e:
            logger.error(f"[BATTED BALL PARSE ERROR] {e}")

        return batters
