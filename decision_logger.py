"""
decision_logger.py — PropIQ Structured Decision Logger
========================================================
Logs every leg evaluation with full feature context:
  - Which agent evaluated it
  - All 5 layer probability contributions (base, DraftEdge, Statcast, SBD, form, FanGraphs)
  - Final probability after all layers
  - Whether it was INCLUDED or REJECTED and why
  - Config version for full reproducibility

This makes debugging trivial:
  "Why did BullpenAgent pick Yordan Alvarez hits on 3/25?"
  → Query decision_log WHERE player = 'Yordan Alvarez' AND date = '2026-03-25'

Used by replay_tool.py to reconstruct any day's decisions.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

import psycopg2
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [DECISION-LOG] %(message)s")
logger = logging.getLogger(__name__)

_DB_URL = os.environ.get("DATABASE_URL", "")
_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "agent_config.yaml")
_BUFFER: list[dict] = []   # In-memory buffer — flushed in batch at end of dispatch


def _load_config_version() -> str:
    try:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f).get("version", "unknown")
    except Exception:
        return "unknown"


def _get_conn():
    return psycopg2.connect(_DB_URL)


def _ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS decision_log (
                id SERIAL PRIMARY KEY,
                log_date DATE NOT NULL DEFAULT CURRENT_DATE,
                agent_name TEXT NOT NULL,
                player_name TEXT,
                prop_type TEXT,
                direction TEXT,
                line FLOAT,
                platform TEXT,
                prob_base FLOAT,
                prob_draftedge FLOAT,
                prob_statcast FLOAT,
                prob_sbd FLOAT,
                prob_form FLOAT,
                prob_fangraphs FLOAT,
                prob_final FLOAT,
                edge_pct FLOAT,
                decision TEXT NOT NULL,    -- INCLUDED or REJECTED
                reject_reason TEXT,        -- NULL if INCLUDED
                features JSONB,            -- Full feature dict for debugging
                config_version TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_decision_log_date
            ON decision_log (log_date, agent_name)
        """)
        conn.commit()


def log_leg(
    agent_name: str,
    player_name: str,
    prop_type: str,
    direction: str,
    line: float,
    platform: str,
    prob_base: float,
    prob_draftedge: float,
    prob_statcast: float,
    prob_sbd: float,
    prob_form: float,
    prob_fangraphs: float,
    prob_final: float,
    edge_pct: float,
    decision: str,              # "INCLUDED" or "REJECTED"
    reject_reason: str = "",
    features: dict | None = None,
) -> None:
    """
    Buffer a single leg decision. Call flush_buffer() at end of dispatch run.
    Buffering avoids N individual DB round-trips during the hot dispatch loop.
    """
    _BUFFER.append({
        "log_date": datetime.now(ZoneInfo("America/Los_Angeles")).date().isoformat(),
        "agent_name": agent_name,
        "player_name": player_name,
        "prop_type": prop_type,
        "direction": direction,
        "line": line,
        "platform": platform,
        "prob_base": round(prob_base, 4),
        "prob_draftedge": round(prob_draftedge, 4),
        "prob_statcast": round(prob_statcast, 4),
        "prob_sbd": round(prob_sbd, 4),
        "prob_form": round(prob_form, 4),
        "prob_fangraphs": round(prob_fangraphs, 4),
        "prob_final": round(prob_final, 4),
        "edge_pct": round(edge_pct, 4),
        "decision": decision,
        "reject_reason": reject_reason or None,
        "features": json.dumps(features or {}),
        "config_version": _load_config_version(),
    })


def flush_buffer() -> int:
    """
    Write all buffered leg decisions to DB in a single batch INSERT.
    Returns number of rows written.
    """
    if not _BUFFER:
        return 0
    try:
        with _get_conn() as conn:
            _ensure_table(conn)
            with conn.cursor() as cur:
                for row in _BUFFER:
                    cur.execute("""
                        INSERT INTO decision_log (
                            log_date, agent_name, player_name, prop_type, direction, line,
                            platform, prob_base, prob_draftedge, prob_statcast, prob_sbd,
                            prob_form, prob_fangraphs, prob_final, edge_pct,
                            decision, reject_reason, features, config_version
                        ) VALUES (
                            %(log_date)s, %(agent_name)s, %(player_name)s, %(prop_type)s,
                            %(direction)s, %(line)s, %(platform)s, %(prob_base)s,
                            %(prob_draftedge)s, %(prob_statcast)s, %(prob_sbd)s,
                            %(prob_form)s, %(prob_fangraphs)s, %(prob_final)s,
                            %(edge_pct)s, %(decision)s, %(reject_reason)s,
                            %(features)s, %(config_version)s
                        )
                    """, row)
            conn.commit()
        n = len(_BUFFER)
        _BUFFER.clear()
        logger.info("Flushed %d leg decisions to decision_log", n)
        return n
    except Exception as exc:
        logger.error("Failed to flush decision log: %s", exc)
        return 0
