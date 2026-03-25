"""
risk_manager.py — PropIQ Risk Management & Exposure Control
============================================================
Enforces:
  - Max daily stake per agent (3% of bankroll default)
  - Max aggregate daily stake (15% of bankroll default)
  - Auto cool-down: pauses agent for N days if 30-day ROI or CLV drops below threshold
  - Config version stamped on every action for full audit trail

Called by live_dispatcher.py BEFORE building parlays.

Usage:
    from risk_manager import RiskManager
    rm = RiskManager()
    active = rm.get_active_agents()          # Returns list of enabled, non-cooled agents
    ok = rm.check_stake(agent_name, stake)   # True if within daily cap
    rm.record_stake(agent_name, stake)       # Log stake taken
"""

from __future__ import annotations

import logging
import os

import psycopg2
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [RISK] %(message)s")
logger = logging.getLogger(__name__)

_DB_URL = os.environ.get("DATABASE_URL", "")
_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "agent_config.yaml")


def _load_config() -> dict:
    try:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except Exception as exc:
        logger.error("Could not load agent_config.yaml: %s", exc)
        return {}


def _get_conn():
    return psycopg2.connect(_DB_URL)


def _ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_exposure (
                id SERIAL PRIMARY KEY,
                exposure_date DATE NOT NULL DEFAULT CURRENT_DATE,
                agent_name TEXT NOT NULL,
                stake FLOAT NOT NULL,
                parlay_id INT REFERENCES propiq_season_record(id) ON DELETE SET NULL,
                config_version TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_cool_down (
                agent_name TEXT PRIMARY KEY,
                cool_down_until DATE NOT NULL,
                reason TEXT,
                triggered_at TIMESTAMPTZ DEFAULT NOW(),
                config_version TEXT
            )
        """)
        conn.commit()


class RiskManager:
    def __init__(self):
        self.cfg = _load_config()
        self.config_version = self.cfg.get("version", "unknown")
        self.risk_cfg = self.cfg.get("risk", {})
        self.agents_cfg = self.cfg.get("agents", {})
        self.bankroll = self.risk_cfg.get("bankroll", 1000.0)
        self.max_daily_pct = self.risk_cfg.get("max_daily_stake_pct", 0.15)
        self.max_agent_pct = self.risk_cfg.get("max_agent_stake_pct", 0.03)
        self.cool_down_days = self.risk_cfg.get("cool_down_days", 3)
        self.cool_down_roi = self.risk_cfg.get("cool_down_roi_threshold", -0.20)
        self.cool_down_clv = self.risk_cfg.get("cool_down_clv_threshold", -0.030)
        self.half_kelly_cap = self.risk_cfg.get("half_kelly_cap", 0.10)
        self._today_exposure: dict[str, float] = {}  # in-memory cache
        self._cool_down_cache: dict[str, date] = {}
        self._load_today_exposure()
        self._load_cool_downs()

    # ------------------------------------------------------------------
    # Load today's exposure from DB into memory
    # ------------------------------------------------------------------
    def _load_today_exposure(self) -> None:
        try:
            with _get_conn() as conn:
                _ensure_tables(conn)
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT agent_name, SUM(stake)
                        FROM daily_exposure
                        WHERE exposure_date = %s
                        GROUP BY agent_name
                    """, (date.today(),))
                    for row in cur.fetchall():
                        self._today_exposure[row[0]] = float(row[1])
        except Exception as exc:
            logger.warning("Could not load today's exposure: %s", exc)

    def _load_cool_downs(self) -> None:
        try:
            with _get_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                        SELECT agent_name, cool_down_until
                        FROM agent_cool_down
                        WHERE cool_down_until >= %s
                    """, (date.today(),))
                for row in cur.fetchall():
                    self._cool_down_cache[row[0]] = row[1]
        except Exception as exc:
            logger.warning("Could not load cool-downs: %s", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_active_agents(self) -> list[str]:
        """
        Returns list of agents that are:
          1. Enabled in agent_config.yaml
          2. Not in cool-down (DB or config)
        """
        active = []
        today = date.today()
        for agent, cfg in self.agents_cfg.items():
            if not cfg.get("enabled", True):
                logger.info("[SKIP] %s — disabled in config", agent)
                continue
            if cfg.get("cool_down_active", False):
                until = cfg.get("cool_down_until")
                if until and date.fromisoformat(str(until)) >= today:
                    logger.info("[SKIP] %s — cool_down_active in config until %s", agent, until)
                    continue
            if agent in self._cool_down_cache and self._cool_down_cache[agent] >= today:
                logger.info("[SKIP] %s — auto cool-down until %s", agent, self._cool_down_cache[agent])
                continue
            active.append(agent)
        return active

    def check_stake(self, agent_name: str, stake: float) -> bool:
        """
        Returns True if this stake fits within:
          - Per-agent daily cap
          - Aggregate daily cap
        """
        agent_cap = self.bankroll * self.max_agent_pct
        aggregate_cap = self.bankroll * self.max_daily_pct

        agent_spent = self._today_exposure.get(agent_name, 0.0)
        total_spent = sum(self._today_exposure.values())

        if agent_spent + stake > agent_cap:
            logger.warning(
                "[RISK] %s: agent cap exceeded — spent $%.2f, cap $%.2f, requested $%.2f",
                agent_name, agent_spent, agent_cap, stake,
            )
            return False

        if total_spent + stake > aggregate_cap:
            logger.warning(
                "[RISK] Aggregate cap exceeded — spent $%.2f, cap $%.2f, requested $%.2f",
                total_spent, aggregate_cap, stake,
            )
            return False

        return True

    def record_stake(self, agent_name: str, stake: float, parlay_id: int | None = None) -> None:
        """Log a stake taken to DB and update in-memory cache."""
        self._today_exposure[agent_name] = self._today_exposure.get(agent_name, 0.0) + stake
        try:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO daily_exposure (agent_name, stake, parlay_id, config_version)
                        VALUES (%s, %s, %s, %s)
                    """, (agent_name, stake, parlay_id, self.config_version))
                conn.commit()
        except Exception as exc:
            logger.error("Failed to record stake: %s", exc)

    def apply_cool_down(self, agent_name: str, reason: str) -> None:
        """Pause an agent for cool_down_days days. Logged to DB + Discord warning."""
        until = date.today() + timedelta(days=self.cool_down_days)
        self._cool_down_cache[agent_name] = until
        logger.warning("[COOL-DOWN] %s paused until %s — reason: %s", agent_name, until, reason)
        try:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO agent_cool_down (agent_name, cool_down_until, reason, config_version)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (agent_name) DO UPDATE
                            SET cool_down_until = EXCLUDED.cool_down_until,
                                reason = EXCLUDED.reason,
                                triggered_at = NOW(),
                                config_version = EXCLUDED.config_version
                    """, (agent_name, until.isoformat(), reason, self.config_version))
                conn.commit()
        except Exception as exc:
            logger.error("Failed to write cool-down: %s", exc)

    def check_and_apply_cool_downs(self, edge_metrics: dict) -> None:
        """
        Called post-settlement with rolling 30-day metrics per agent.
        edge_metrics: { agent_name: { roi_30d, clv_30d, brier_30d } }
        Applies cool-down if thresholds breached.
        """
        for agent, m in edge_metrics.items():
            roi = m.get("roi_30d", 0.0)
            clv = m.get("clv_30d", 0.0)
            brier = m.get("brier_30d", 0.0)

            reasons = []
            if roi < self.cool_down_roi:
                reasons.append(f"30d ROI {roi:.1%} < threshold {self.cool_down_roi:.1%}")
            if clv < self.cool_down_clv:
                reasons.append(f"30d CLV {clv:.3f} < threshold {self.cool_down_clv:.3f}")
            brier_thresh = self.risk_cfg.get("cool_down_brier_threshold", 0.28)
            if brier > brier_thresh:
                reasons.append(f"30d Brier {brier:.4f} > threshold {brier_thresh:.4f}")

            if reasons:
                self.apply_cool_down(agent, "; ".join(reasons))

    def daily_summary(self) -> dict:
        """Returns today's exposure summary for Discord footer."""
        agent_cap = self.bankroll * self.max_agent_pct
        aggregate_cap = self.bankroll * self.max_daily_pct
        total_spent = sum(self._today_exposure.values())
        return {
            "total_spent": round(total_spent, 2),
            "aggregate_cap": round(aggregate_cap, 2),
            "utilization_pct": round(total_spent / aggregate_cap * 100, 1) if aggregate_cap else 0,
            "per_agent": {k: round(v, 2) for k, v in self._today_exposure.items()},
            "agent_cap": round(agent_cap, 2),
        }
