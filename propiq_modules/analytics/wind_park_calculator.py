"""
WindParkCalculator — Park-specific wind impact on HR/hits props.

Logic:
  - Pull hitter (RHH) hits to LEFT field → benefits from R→L or out-to-LF winds
  - Pull hitter (LHH) hits to RIGHT field → benefits from L→R or out-to-RF winds
  - Headwinds suppress HR by 8-15%
  - Wind speed + direction combined with park factor
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ── Park configurations ────────────────────────────────────────────────────────
# orientation: where "out" wind blows (LF = toward left field fence)
PARK_CONFIGS = {
    # Hitter-friendly
    "COL": {"name": "Coors Field",       "factor": 1.38, "lf_dist": 347, "rf_dist": 350, "cf_dist": 415, "orientation": "LF",  "altitude_ft": 5200},
    "CIN": {"name": "Great American",    "factor": 1.18, "lf_dist": 328, "rf_dist": 325, "cf_dist": 404, "orientation": "RF",  "altitude_ft": 490},
    "TEX": {"name": "Globe Life",        "factor": 1.12, "lf_dist": 334, "rf_dist": 325, "cf_dist": 407, "orientation": "CF",  "altitude_ft": 551},
    "PHI": {"name": "Citizens Bank",     "factor": 1.10, "lf_dist": 329, "rf_dist": 330, "cf_dist": 401, "orientation": "RF",  "altitude_ft": 20},
    "NYY": {"name": "Yankee Stadium",    "factor": 1.08, "lf_dist": 318, "rf_dist": 314, "cf_dist": 408, "orientation": "RF",  "altitude_ft": 55},
    "BOS": {"name": "Fenway Park",       "factor": 1.06, "lf_dist": 310, "rf_dist": 302, "cf_dist": 420, "orientation": "LF",  "altitude_ft": 20},
    "CHC": {"name": "Wrigley Field",     "factor": 1.05, "lf_dist": 355, "rf_dist": 353, "cf_dist": 400, "orientation": "LF",  "altitude_ft": 595},
    # Neutral
    "ATL": {"name": "Truist Park",       "factor": 1.02, "lf_dist": 335, "rf_dist": 325, "cf_dist": 400, "orientation": "CF",  "altitude_ft": 1050},
    "MIL": {"name": "American Family",  "factor": 1.01, "lf_dist": 344, "rf_dist": 345, "cf_dist": 400, "orientation": "CF",  "altitude_ft": 635},
    "HOU": {"name": "Minute Maid",       "factor": 0.99, "lf_dist": 315, "rf_dist": 326, "cf_dist": 409, "orientation": "LF",  "altitude_ft": 43},
    "TOR": {"name": "Rogers Centre",     "factor": 0.98, "lf_dist": 328, "rf_dist": 328, "cf_dist": 400, "orientation": "CF",  "altitude_ft": 249},
    # Pitcher-friendly
    "SFG": {"name": "Oracle Park",       "factor": 0.87, "lf_dist": 339, "rf_dist": 309, "cf_dist": 399, "orientation": "CF",  "altitude_ft": 10},
    "OAK": {"name": "Oakland Coliseum",  "factor": 0.88, "lf_dist": 330, "rf_dist": 330, "cf_dist": 400, "orientation": "CF",  "altitude_ft": 25},
    "MIA": {"name": "LoanDepot Park",    "factor": 0.89, "lf_dist": 344, "rf_dist": 335, "cf_dist": 418, "orientation": "CF",  "altitude_ft": 6},
    "SEA": {"name": "T-Mobile Park",     "factor": 0.91, "lf_dist": 331, "rf_dist": 326, "cf_dist": 401, "orientation": "CF",  "altitude_ft": 0},
    "NYM": {"name": "Citi Field",        "factor": 0.93, "lf_dist": 335, "rf_dist": 330, "cf_dist": 408, "orientation": "LF",  "altitude_ft": 20},
    "LAD": {"name": "Dodger Stadium",    "factor": 0.96, "lf_dist": 330, "rf_dist": 330, "cf_dist": 395, "orientation": "CF",  "altitude_ft": 514},
    "CWS": {"name": "Guaranteed Rate",   "factor": 1.00, "lf_dist": 330, "rf_dist": 335, "cf_dist": 400, "orientation": "CF",  "altitude_ft": 595},
    "CLE": {"name": "Progressive Field", "factor": 0.95, "lf_dist": 325, "rf_dist": 325, "cf_dist": 405, "orientation": "CF",  "altitude_ft": 650},
    "DET": {"name": "Comerica Park",     "factor": 0.94, "lf_dist": 345, "rf_dist": 330, "cf_dist": 420, "orientation": "CF",  "altitude_ft": 585},
    "MIN": {"name": "Target Field",      "factor": 0.97, "lf_dist": 339, "rf_dist": 328, "cf_dist": 404, "orientation": "CF",  "altitude_ft": 841},
    "KC":  {"name": "Kauffman Stadium",  "factor": 0.96, "lf_dist": 330, "rf_dist": 330, "cf_dist": 410, "orientation": "CF",  "altitude_ft": 1050},
    "LAA": {"name": "Angel Stadium",     "factor": 0.97, "lf_dist": 330, "rf_dist": 330, "cf_dist": 400, "orientation": "CF",  "altitude_ft": 160},
    "SD":  {"name": "Petco Park",        "factor": 0.90, "lf_dist": 336, "rf_dist": 322, "cf_dist": 396, "orientation": "RF",  "altitude_ft": 20},
    "STL": {"name": "Busch Stadium",     "factor": 0.98, "lf_dist": 336, "rf_dist": 335, "cf_dist": 400, "orientation": "CF",  "altitude_ft": 465},
    "PIT": {"name": "PNC Park",          "factor": 0.94, "lf_dist": 325, "rf_dist": 320, "cf_dist": 399, "orientation": "RF",  "altitude_ft": 730},
    "WSH": {"name": "Nationals Park",    "factor": 1.04, "lf_dist": 336, "rf_dist": 335, "cf_dist": 402, "orientation": "CF",  "altitude_ft": 25},
    "BAL": {"name": "Camden Yards",      "factor": 1.03, "lf_dist": 333, "rf_dist": 318, "cf_dist": 400, "orientation": "RF",  "altitude_ft": 18},
    "TB":  {"name": "Tropicana Field",   "factor": 0.97, "lf_dist": 315, "rf_dist": 322, "cf_dist": 404, "orientation": "CF",  "altitude_ft": 0},
    "ARI": {"name": "Chase Field",       "factor": 1.05, "lf_dist": 330, "rf_dist": 334, "cf_dist": 407, "orientation": "CF",  "altitude_ft": 1090},
}

# Known pull-hitter tendencies (RHH = pulls to LF, LHH = pulls to RF)
PULL_HITTERS = {
    # RHH power pull hitters
    "Aaron Judge": "RHH", "Mookie Betts": "RHH", "Yordan Alvarez": "LHH",
    "Rafael Devers": "LHH", "Pete Alonso": "RHH", "Giancarlo Stanton": "RHH",
    "Matt Olson": "LHH", "Trea Turner": "RHH", "Juan Soto": "LHH",
    "Freddie Freeman": "LHH", "Corey Seager": "LHH", "Austin Riley": "RHH",
    "Kyle Tucker": "LHH", "Vladimir Guerrero Jr.": "RHH", "Jose Ramirez": "SHH",
    "Gunnar Henderson": "LHH", "Bobby Witt Jr.": "RHH", "Julio Rodriguez": "RHH",
    "Fernando Tatis Jr.": "RHH", "Bryce Harper": "LHH", "Mike Trout": "RHH",
    "Shohei Ohtani": "LHH", "Ronald Acuna Jr.": "RHH", "Jose Altuve": "RHH",
    "Dansby Swanson": "RHH", "Wander Franco": "SHH", "Adley Rutschman": "SHH",
}


@dataclass
class WindCondition:
    speed_mph: float
    direction: str          # "out_to_LF", "out_to_RF", "out_to_CF", "in_from_LF", "in_from_RF", "L_to_R", "R_to_L", "calm"
    temp_f: float = 72.0
    humidity_pct: float = 50.0


@dataclass
class WindAdjustment:
    hr_boost_pct: float = 0.0
    hit_boost_pct: float = 0.0
    under_boost_pct: float = 0.0
    total_boost_pct: float = 0.0
    note: str = ""
    trigger_flag: bool = False      # True if meets agent threshold (8+ mph out)


class WindParkCalculator:

    @staticmethod
    def get_park(team_code: str) -> Optional[dict]:
        return PARK_CONFIGS.get(team_code.upper())

    @staticmethod
    def get_batter_hand(player_name: str) -> str:
        return PULL_HITTERS.get(player_name, "RHH")  # Default RHH

    @classmethod
    def calculate(
        cls,
        team_code: str,
        wind: WindCondition,
        batter_name: Optional[str] = None,
        prop_type: str = "HR",
    ) -> WindAdjustment:
        """
        Calculate wind-park adjustment for a specific prop.
        prop_type: "HR", "hits", "total_runs", "strikeouts"
        """
        park = cls.get_park(team_code)
        if not park:
            return WindAdjustment(note=f"Unknown park: {team_code}")

        park_factor = park["factor"]
        orientation = park["orientation"]
        speed = wind.speed_mph
        direction = wind.direction.lower()

        adj = WindAdjustment()

        # ── CALM: minimal effect ──────────────────────────────────────────────
        if speed < 5 or direction == "calm":
            adj.note = "Calm wind — no adjustment"
            return adj

        # ── BASE WIND BOOST TABLE ─────────────────────────────────────────────
        # out_to_LF / out_to_RF boosts HRs, in suppresses
        is_out = any(x in direction for x in ["out_to", "out to"])
        is_in = any(x in direction for x in ["in_from", "in from"])
        is_crosswind = direction in ["l_to_r", "r_to_l", "l to r", "r to l"]

        # HR boost from tailwind
        if is_out and prop_type in ("HR", "total_runs"):
            base_boost = 0.0
            if speed >= 15:
                base_boost = 0.14
            elif speed >= 12:
                base_boost = 0.10
            elif speed >= 8:
                base_boost = 0.06

            # Park amplifier
            base_boost *= park_factor
            adj.hr_boost_pct = round(base_boost * 100, 1)
            adj.hit_boost_pct = round(base_boost * 0.5 * 100, 1)
            adj.total_boost_pct = round(base_boost * 0.7 * 100, 1)
            adj.trigger_flag = speed >= 8

        # HR suppression from headwind
        elif is_in and prop_type in ("HR", "total_runs"):
            base_suppress = 0.0
            if speed >= 15:
                base_suppress = -0.12
            elif speed >= 10:
                base_suppress = -0.08
            elif speed >= 6:
                base_suppress = -0.05

            adj.hr_boost_pct = round(base_suppress * 100, 1)
            adj.under_boost_pct = round(abs(base_suppress) * 100, 1)

        # Crosswind — side-dependent
        elif is_crosswind and prop_type == "HR":
            # L_to_R helps LHH (pulls to RF), R_to_L helps RHH (pulls to LF)
            batter_hand = cls.get_batter_hand(batter_name) if batter_name else "RHH"
            helps = (
                (direction in ["r_to_l", "r to l"] and batter_hand == "RHH") or
                (direction in ["l_to_r", "l to r"] and batter_hand == "LHH")
            )
            if helps and speed >= 8:
                crosswind_boost = 0.05 if speed < 12 else 0.08
                adj.hr_boost_pct = round(crosswind_boost * 100, 1)
                adj.trigger_flag = True
                adj.note = f"Crosswind favors {batter_hand} pull HR"

        # ── PULL HITTER SPECIFIC BOOST ────────────────────────────────────────
        if batter_name and prop_type == "HR":
            batter_hand = cls.get_batter_hand(batter_name)
            # Check wind + park orientation alignment
            park_out_dir = park["orientation"].lower()

            favorable = False
            if batter_hand == "RHH" and park_out_dir == "lf":
                # RHH pulls to LF, park has out-to-LF wind
                favorable = "lf" in direction or "r_to_l" in direction or "r to l" in direction
            elif batter_hand == "LHH" and park_out_dir == "rf":
                favorable = "rf" in direction or "l_to_r" in direction or "l to r" in direction
            elif batter_hand == "LHH" and park_out_dir == "lf":
                # LHH hits oppo to LF sometimes but mostly RF — less favorable
                favorable = False

            if favorable and speed >= 8 and adj.hr_boost_pct >= 0:
                pull_bonus = 0.03 if speed < 12 else 0.05
                adj.hr_boost_pct = round(adj.hr_boost_pct + pull_bonus * 100, 1)
                adj.note += f" | Pull hitter {batter_hand} + favorable wind"
                adj.trigger_flag = True

        # ── TEMPERATURE BOOST ─────────────────────────────────────────────────
        if prop_type in ("HR", "total_runs"):
            if wind.temp_f > 85:
                temp_boost = (wind.temp_f - 75) * 0.002  # +0.2% per degree above 75
                adj.hr_boost_pct = round(adj.hr_boost_pct + temp_boost * 100, 1)
            elif wind.temp_f < 50:
                temp_penalty = (50 - wind.temp_f) * 0.003
                adj.hr_boost_pct = round(adj.hr_boost_pct - temp_penalty * 100, 1)

        # ── COORS ALTITUDE SPECIAL ────────────────────────────────────────────
        if team_code.upper() == "COL":
            altitude_boost = 0.06  # Ball travels ~6% farther at 5200ft
            adj.hr_boost_pct = round(adj.hr_boost_pct + altitude_boost * 100, 1)
            adj.note += " | Coors altitude +6% base"

        if not adj.note:
            adj.note = f"{speed}mph {direction} @ {park['name']} (PF={park_factor})"

        return adj

    @classmethod
    def agent_threshold_met(
        cls, team_code: str, wind: WindCondition, prop_type: str = "HR"
    ) -> bool:
        """
        Returns True if wind conditions meet PropIQ agent threshold:
        Wind > 8mph out to LF → HR overs (or RF for LHH parks)
        """
        if wind.speed_mph < 8:
            return False
        direction = wind.direction.lower()
        is_out = any(x in direction for x in ["out_to", "out to", "r_to_l", "l_to_r"])
        return is_out

    @classmethod
    def summarize(cls, team_code: str, wind: WindCondition, batter_name: Optional[str] = None) -> str:
        """Human-readable wind summary for the Analyze tab."""
        adj = cls.calculate(team_code, wind, batter_name, "HR")
        park = cls.get_park(team_code) or {}
        park_name = park.get("name", team_code)
        symbol = "🌬️" if adj.trigger_flag else "💨"
        return (
            f"{symbol} {wind.speed_mph}mph {wind.direction} @ {park_name} "
            f"| HR boost: {adj.hr_boost_pct:+.0f}% "
            f"{'⚡ THRESHOLD MET' if adj.trigger_flag else ''}"
        )
