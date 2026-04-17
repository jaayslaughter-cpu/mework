"""
PropIQ Analytics — DFS Tracker + Backtester
=============================================
Tracks DFS lineup performance and backtests prop model
against historical sportsbook lines.

Drop this into: api/services/dfs_tracker.py
"""

import os
import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("PROPIQ_DATA_DIR", "data"))
DFS_LOG_FILE = DATA_DIR / "dfs_results.jsonl"
BACKTEST_RESULTS_FILE = DATA_DIR / "backtest_results.json"


# ─────────────────────────────────────────────
# DFS Scoring
# ─────────────────────────────────────────────
# FanDuel MLB scoring
FD_SCORING = {
    "single": 3.0,
    "double": 6.0,
    "triple": 9.0,
    "home_run": 12.0,
    "rbi": 3.5,
    "run": 3.2,
    "walk": 3.0,
    "hbp": 3.0,
    "stolen_base": 6.0,
    "strikeout_batter": -0.0,  # FD doesn't penalize
    # Pitchers
    "pitcher_inning": 3.0,
    "pitcher_strikeout": 3.0,
    "pitcher_win": 6.0,
    "pitcher_quality_start": 4.0,
    "pitcher_er": -3.0,
    "pitcher_hit_allowed": -0.6,
    "pitcher_walk_allowed": -0.6,
    "pitcher_hbp_allowed": -0.6,
    "pitcher_complete_game": 3.0,
    "pitcher_shutout_bonus": 3.0,
    "pitcher_no_hitter": 10.0,
}

# DraftKings MLB scoring
DK_SCORING = {
    "single": 3.0,
    "double": 5.0,
    "triple": 8.0,
    "home_run": 10.0,
    "rbi": 2.0,
    "run": 2.0,
    "walk": 2.0,
    "hbp": 2.0,
    "stolen_base": 5.0,
    "strikeout_batter": -0.5,  # DK penalizes K
    # Pitchers
    "pitcher_inning": 2.25,
    "pitcher_strikeout": 2.0,
    "pitcher_win": 4.0,
    "pitcher_quality_start": 4.0,
    "pitcher_er": -2.0,
    "pitcher_hit_allowed": -0.6,
    "pitcher_walk_allowed": -0.6,
    "pitcher_hbp_allowed": -0.6,
    "pitcher_complete_game": 2.5,
    "pitcher_shutout_bonus": 2.5,
    "pitcher_no_hitter": 5.0,
}


class DFSScorer:
    """Calculate DFS fantasy points from stat lines."""

    def __init__(self, platform: str = "fd"):
        self.scoring = FD_SCORING if platform == "fd" else DK_SCORING
        self.platform = platform

    def score_batter(self, stats: Dict) -> float:
        """
        Calculate DFS points for a batter.
        stats: {hits, doubles, triples, home_runs, rbi, runs, walks, hbp, stolen_bases, strikeouts, at_bats}
        """
        s = self.scoring
        hits = stats.get("hits", 0)
        doubles = stats.get("doubles", 0)
        triples = stats.get("triples", 0)
        homers = stats.get("home_runs", 0)
        singles = hits - doubles - triples - homers

        pts = (
            singles * s["single"]
            + doubles * s["double"]
            + triples * s["triple"]
            + homers * s["home_run"]
            + stats.get("rbi", 0) * s["rbi"]
            + stats.get("runs", 0) * s["run"]
            + stats.get("walks", 0) * s["walk"]
            + stats.get("hbp", 0) * s["hbp"]
            + stats.get("stolen_bases", 0) * s["stolen_base"]
            + stats.get("strikeouts", 0) * s["strikeout_batter"]
        )
        return round(pts, 2)

    def score_pitcher(self, stats: Dict) -> float:
        """
        Calculate DFS points for a SP.
        stats: {outs, strikeouts, wins, earned_runs, hits_allowed, walks_allowed, hbp, quality_start}
        """
        s = self.scoring
        outs = stats.get("outs", 0)
        innings = outs / 3.0

        pts = (
            innings * s["pitcher_inning"]
            + stats.get("strikeouts", 0) * s["pitcher_strikeout"]
            + stats.get("wins", 0) * s["pitcher_win"]
            + stats.get("quality_start", 0) * s["pitcher_quality_start"]
            + stats.get("earned_runs", 0) * s["pitcher_er"]
            + stats.get("hits_allowed", 0) * s["pitcher_hit_allowed"]
            + stats.get("walks_allowed", 0) * s["pitcher_walk_allowed"]
            + stats.get("hbp", 0) * s["pitcher_hbp_allowed"]
        )

        # Bonuses
        if outs >= 27:
            pts += s.get("pitcher_complete_game", 0)
            if stats.get("earned_runs", 0) == 0:
                pts += s.get("pitcher_shutout_bonus", 0)

        return round(pts, 2)

    @staticmethod
    def salary_value(dfs_points: float, salary: int) -> float:
        """Value = points per $1000 salary."""
        return round(dfs_points / (salary / 1000), 2) if salary else 0.0


# ─────────────────────────────────────────────
# DFS Tracker
# ─────────────────────────────────────────────
class DFSTracker:
    """
    Records DFS lineup entries and outcomes.
    Measures model accuracy from a DFS-value perspective.
    """

    def __init__(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def log_entry(
        self,
        platform: str,
        player: str,
        prop_type: str,
        projection: float,
        salary: int,
        actual_points: Optional[float] = None,
        lineup_id: Optional[str] = None,
        game_date: Optional[str] = None,
    ):
        scorer = DFSScorer(platform)
        record = {
            "ts": datetime.utcnow().isoformat(),
            "game_date": game_date or str(date.today()),
            "platform": platform,
            "player": player,
            "prop_type": prop_type,
            "projection": projection,
            "salary": salary,
            "actual_points": actual_points,
            "proj_value": scorer.salary_value(projection, salary),
            "actual_value": scorer.salary_value(actual_points or 0, salary),
            "lineup_id": lineup_id,
        }
        with open(DFS_LOG_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")
        return record

    def get_value_plays(self, min_value: float = 4.0, platform: str = "fd") -> List[Dict]:
        """Return historical plays that exceeded value threshold."""
        if not DFS_LOG_FILE.exists():
            return []
        plays = []
        with open(DFS_LOG_FILE) as f:
            for line in f:
                try:
                    r = json.loads(line)
                    if r.get("platform") == platform and r.get("actual_value", 0) >= min_value:
                        plays.append(r)
                except json.JSONDecodeError:
                    continue
        return plays


# ─────────────────────────────────────────────
# Backtester
# ─────────────────────────────────────────────
class PropBacktester:
    """
    Backtest the PropModel against historical sportsbook lines.
    Requires: historical DataFrame with columns:
      player, prop_type, line_value, book_prob, model_prob, actual_result, game_date
    """

    def __init__(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def run_backtest(
        self,
        df: pd.DataFrame,
        bet_threshold: float = 0.05,
        stake: float = 100.0,
    ) -> Dict:
        """
        Simulate flat-stake betting on every edge above threshold.

        Args:
            df: historical predictions DataFrame
            bet_threshold: minimum edge (model_prob - book_prob) to bet
            stake: flat stake per bet in $

        Returns:
            backtest results dict
        """
        required = ["player", "prop_type", "line_value", "book_prob", "model_prob", "actual_result"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing column: {col}")

        df = df.copy().dropna(subset=required)
        df["edge"] = df["model_prob"] - df["book_prob"]
        df["bet"] = df["edge"] >= bet_threshold

        bets = df[df["bet"]].copy()
        if bets.empty:
            return {"status": "no_bets", "threshold": bet_threshold}

        # Convert American odds from book_prob to payout
        def prob_to_payout(prob: float) -> float:
            """Return net profit per $1 staked (on the over)."""
            if prob >= 0.5:
                return (100 / (prob / (1 - prob))) / 100
            else:
                return ((1 - prob) / prob)

        bets["payout_multiplier"] = bets["book_prob"].apply(prob_to_payout)
        bets["pnl"] = bets.apply(
            lambda r: stake * r["payout_multiplier"] if r["actual_result"] == 1 else -stake,
            axis=1,
        )
        bets["cumulative_pnl"] = bets["pnl"].cumsum()

        total_bets = len(bets)
        wins = int((bets["actual_result"] == 1).sum())
        total_pnl = float(bets["pnl"].sum())
        roi = total_pnl / (total_bets * stake) * 100

        # By prop type breakdown
        by_type = bets.groupby("prop_type").agg(
            bets=("pnl", "count"),
            wins=("actual_result", "sum"),
            pnl=("pnl", "sum"),
        ).reset_index()
        by_type["win_rate"] = by_type["wins"] / by_type["bets"]
        by_type["roi"] = by_type["pnl"] / (by_type["bets"] * stake) * 100

        results = {
            "status": "complete",
            "total_bets": total_bets,
            "wins": wins,
            "losses": total_bets - wins,
            "win_rate": round(wins / total_bets, 4),
            "total_pnl": round(total_pnl, 2),
            "roi_pct": round(roi, 2),
            "avg_edge": round(float(bets["edge"].mean()), 4),
            "bet_threshold": bet_threshold,
            "stake": stake,
            "by_prop_type": by_type.to_dict("records"),
            "run_date": datetime.utcnow().isoformat(),
        }

        # Save
        with open(BACKTEST_RESULTS_FILE, "w") as f:
            json.dump(results, f, indent=2)

        logger.info("Backtest: %d bets, %d wins (%.1f%%), ROI %.1f%%", total_bets, wins, wins/total_bets * 100, roi)
        return results

    @staticmethod
    def kelly_criterion(model_prob: float, book_prob: float, max_fraction: float = 0.25) -> float:
        """
        Full Kelly Criterion bet size (as fraction of bankroll).
        Capped at max_fraction for safety.
        """
        b = (1 - book_prob) / book_prob  # decimal odds - 1
        p = model_prob
        q = 1 - p
        kelly = (b * p - q) / b
        return round(max(0, min(kelly, max_fraction)), 4)

    def half_kelly(self, model_prob: float, book_prob: float) -> float:
        return self.kelly_criterion(model_prob, book_prob) / 2

    @staticmethod
    def load_results() -> Optional[Dict]:
        if not BACKTEST_RESULTS_FILE.exists():
            return None
        with open(BACKTEST_RESULTS_FILE) as f:
            return json.load(f)


# Context7 integration: configure via CONTEXT7_API_KEY env var in Railway. Never hardcode.
