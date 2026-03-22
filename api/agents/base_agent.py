"""
PropIQ Base Agent
-----------------
Abstract base class for all 7 betting agents.
Handles: bet tracking, error logging, capital allocation, performance stats.
"""
from __future__ import annotations
import abc
import json
import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "agent_army.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class Leg:
    player: str
    prop_type: str       # e.g. "strikeouts", "hits", "home_runs", "total_bases"
    line: float
    direction: str       # "over" | "under"
    book: str
    american_odds: int
    decimal_odds: float
    book_prob: float
    model_prob: float
    edge: float          # model_prob - book_prob
    market: str = "player_props"


@dataclass
class BetSlip:
    bet_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    agent_name: str = ""
    strategy: str = ""
    legs: list[Leg] = field(default_factory=list)
    stake_units: float = 1.0
    combined_odds: float = 1.0
    expected_value: float = 0.0
    confidence: float = 0.0
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    settled_at: Optional[str] = None
    outcome: Optional[str] = None  # "win" | "loss" | "push" | "pending"
    profit_units: float = 0.0
    game_date: str = field(default_factory=lambda: date.today().isoformat())
    metadata: dict = field(default_factory=dict)

    @property
    def num_legs(self) -> int:
        return len(self.legs)

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


class AgentDB:
    """SQLite persistence layer for all agents."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_schema()

    def _init_schema(self):
        conn = sqlite3.connect(self.db_path)
        with conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS bets (
                    bet_id TEXT PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    strategy TEXT,
                    num_legs INTEGER,
                    stake_units REAL DEFAULT 1.0,
                    combined_odds REAL,
                    expected_value REAL,
                    confidence REAL,
                    created_at TEXT,
                    settled_at TEXT,
                    outcome TEXT DEFAULT 'pending',
                    profit_units REAL DEFAULT 0.0,
                    game_date TEXT,
                    legs_json TEXT,
                    metadata_json TEXT
                );

                CREATE TABLE IF NOT EXISTS agent_stats (
                    agent_name TEXT PRIMARY KEY,
                    total_bets INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    pushes INTEGER DEFAULT 0,
                    total_units_wagered REAL DEFAULT 0.0,
                    total_profit_units REAL DEFAULT 0.0,
                    roi_pct REAL DEFAULT 0.0,
                    win_rate_pct REAL DEFAULT 0.0,
                    current_capital REAL DEFAULT 100.0,
                    base_capital REAL DEFAULT 100.0,
                    last_updated TEXT
                );

                CREATE TABLE IF NOT EXISTS agent_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_name TEXT,
                    error_type TEXT,
                    message TEXT,
                    traceback TEXT,
                    timestamp TEXT DEFAULT (datetime('now')),
                    resolved INTEGER DEFAULT 0,
                    resolution_note TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_bets_agent ON bets(agent_name);
                CREATE INDEX IF NOT EXISTS idx_bets_date ON bets(game_date);
                CREATE INDEX IF NOT EXISTS idx_bets_outcome ON bets(outcome);
            """)
        conn.close()

    def save_bet(self, slip: BetSlip):
        conn = sqlite3.connect(self.db_path)
        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO bets
                (bet_id, agent_name, strategy, num_legs, stake_units, combined_odds,
                 expected_value, confidence, created_at, settled_at, outcome,
                 profit_units, game_date, legs_json, metadata_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                slip.bet_id, slip.agent_name, slip.strategy, slip.num_legs,
                slip.stake_units, slip.combined_odds, slip.expected_value,
                slip.confidence, slip.created_at, slip.settled_at, slip.outcome,
                slip.profit_units, slip.game_date,
                json.dumps([asdict(l) for l in slip.legs]),
                json.dumps(slip.metadata)
            ))
        conn.close()

    def settle_bet(self, bet_id: str, outcome: str, profit_units: float):
        conn = sqlite3.connect(self.db_path)
        with conn:
            conn.execute("""
                UPDATE bets SET outcome=?, profit_units=?, settled_at=?
                WHERE bet_id=?
            """, (outcome, profit_units, datetime.utcnow().isoformat(), bet_id))
        conn.close()

    def get_agent_stats(self, agent_name: str) -> dict:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM agent_stats WHERE agent_name=?", (agent_name,)
        ).fetchone()
        conn.close()
        if row:
            return dict(row)
        return {
            "agent_name": agent_name, "total_bets": 0, "wins": 0,
            "losses": 0, "pushes": 0, "total_units_wagered": 0.0,
            "total_profit_units": 0.0, "roi_pct": 0.0, "win_rate_pct": 0.0,
            "current_capital": 100.0, "base_capital": 100.0, "last_updated": None
        }

    def update_agent_stats(self, agent_name: str):
        """Recalculate stats from settled bets."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT outcome, stake_units, profit_units
            FROM bets WHERE agent_name=? AND outcome != 'pending'
        """, (agent_name,)).fetchall()

        wins = sum(1 for r in rows if r["outcome"] == "win")
        losses = sum(1 for r in rows if r["outcome"] == "loss")
        pushes = sum(1 for r in rows if r["outcome"] == "push")
        total = wins + losses + pushes
        total_wagered = sum(r["stake_units"] for r in rows)
        total_profit = sum(r["profit_units"] for r in rows)
        roi = (total_profit / total_wagered * 100) if total_wagered > 0 else 0.0
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0

        # Get current capital from stats table or default
        existing = conn.execute(
            "SELECT current_capital, base_capital FROM agent_stats WHERE agent_name=?",
            (agent_name,)
        ).fetchone()
        base_capital = existing["base_capital"] if existing else 100.0
        current_capital = base_capital + total_profit

        with conn:
            conn.execute("""
                INSERT OR REPLACE INTO agent_stats
                (agent_name, total_bets, wins, losses, pushes, total_units_wagered,
                 total_profit_units, roi_pct, win_rate_pct, current_capital, base_capital, last_updated)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                agent_name, total, wins, losses, pushes, total_wagered,
                total_profit, roi, win_rate, current_capital, base_capital,
                datetime.utcnow().isoformat()
            ))
        conn.close()

    def log_error(self, agent_name: str, error_type: str, message: str, tb: str = ""):
        conn = sqlite3.connect(self.db_path)
        with conn:
            conn.execute("""
                INSERT INTO agent_errors (agent_name, error_type, message, traceback)
                VALUES (?,?,?,?)
            """, (agent_name, error_type, message, tb))
        conn.close()

    def get_all_stats(self) -> list[dict]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM agent_stats ORDER BY roi_pct DESC"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_pending_bets(self, agent_name: Optional[str] = None) -> list[dict]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        if agent_name:
            rows = conn.execute(
                "SELECT * FROM bets WHERE outcome='pending' AND agent_name=?",
                (agent_name,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM bets WHERE outcome='pending'"
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]


# Singleton DB
_db: Optional[AgentDB] = None


def get_db() -> AgentDB:
    global _db
    if _db is None:
        _db = AgentDB()
    return _db


class BaseAgent(abc.ABC):
    """
    Abstract base for all 7 PropIQ agents.
    Each agent must implement: analyze() -> list[BetSlip]
    """

    name: str = "base"
    strategy: str = "base"
    max_legs: int = 1
    min_legs: int = 1
    ev_threshold: float = 0.05      # 5% minimum edge
    confidence_threshold: float = 0.55

    def __init__(self):
        self.db = get_db()
        self.log = logging.getLogger(f"propiq.agent.{self.name}")
        self._ensure_stats_row()

    def _ensure_stats_row(self):
        stats = self.db.get_agent_stats(self.name)
        if stats["last_updated"] is None:
            # Initialize with defaults
            self.db.update_agent_stats(self.name)

    @abc.abstractmethod
    def analyze(self, hub_data: dict) -> list[BetSlip]:
        """
        Core analysis. Given hub_data dict (from DataHubTasklet),
        return a list of BetSlip recommendations.
        """
        ...

    def run(self, hub_data: dict) -> list[BetSlip]:
        """Safe wrapper around analyze() with error logging."""
        start = time.time()
        try:
            slips = self.analyze(hub_data)
            # Validate constraints
            valid = []
            for slip in slips:
                if self.min_legs <= slip.num_legs <= self.max_legs and slip.expected_value >= self.ev_threshold:
                    slip.agent_name = self.name
                    slip.strategy = self.strategy
                    self.db.save_bet(slip)
                    valid.append(slip)
                    self.log.info(
                        f"[{self.name}] Queued {slip.num_legs}-leg bet | "
                        f"EV={slip.expected_value:.1%} | odds={slip.combined_odds:.2f}"
                    )
            elapsed = time.time() - start
            self.log.info(f"[{self.name}] {len(valid)} bets queued in {elapsed:.2f}s")
            return valid
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.log.error(f"[{self.name}] AGENT ERROR: {e}")
            self.db.log_error(self.name, type(e).__name__, str(e), tb)
            return []

    def record_result(self, bet_id: str, outcome: str, profit_units: float):
        self.db.settle_bet(bet_id, outcome, profit_units)
        self.db.update_agent_stats(self.name)

    @property
    def stats(self) -> dict:
        return self.db.get_agent_stats(self.name)

    def set_capital(self, capital: float):
        """Called by LeaderboardTasklet to update capital allocation."""
        conn = sqlite3.connect(self.db.db_path)
        with conn:
            conn.execute(
                "UPDATE agent_stats SET current_capital=? WHERE agent_name=?",
                (capital, self.name)
            )
        conn.close()

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def american_to_decimal(american: int) -> float:
        if american > 0:
            return round(american / 100 + 1, 4)
        return round(100 / abs(american) + 1, 4)

    @staticmethod
    def decimal_to_prob(decimal: float) -> float:
        return round(1 / decimal, 4) if decimal > 0 else 0.0

    @staticmethod
    def calculate_ev(model_prob: float, decimal_odds: float) -> float:
        """Expected value as a fraction of stake."""
        return round(model_prob * (decimal_odds - 1) - (1 - model_prob), 4)

    @staticmethod
    def kelly_fraction(model_prob: float, decimal_odds: float, kelly_pct: float = 0.25) -> float:
        """Quarter-Kelly stake sizing."""
        b = decimal_odds - 1
        q = 1 - model_prob
        k = (model_prob * b - q) / b
        return max(0.0, round(k * kelly_pct, 4))

    @staticmethod
    def parlay_odds(legs: list[Leg]) -> float:
        combined = 1.0
        for leg in legs:
            combined *= leg.decimal_odds
        return round(combined, 4)
