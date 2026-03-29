"""
clv_feedback.py — PropIQ CLV (Closing Line Value) Feedback Loop
================================================================
Reads ``prediction_results.csv`` after each settled game slate,
compares model pre-game probability to the closing line, and updates
``reliability_config.json`` so future picks use improved reliability scores.

Run via ``nightly_recap.py`` after settlement is complete.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

PREDICTION_RESULTS_PATH = os.getenv("PREDICTION_RESULTS_PATH", "prediction_results.csv")
RELIABILITY_CONFIG_PATH = os.getenv("RELIABILITY_CONFIG_PATH", "reliability_config.json")


class CLVFeedbackLoop:
    """Update tier reliability scores based on CLV and actual outcomes.

    Lead indicator: CLV (did the market move in our direction after we bet?)
    Lag indicator:  Brier-style outcome accuracy (was the predicted prob right?)

    Both signals update the ``reliability_score`` for the specific
    ``(volatility_tier, prop_type)`` bucket.  Scores are clamped to [0.1, 0.9].
    """

    def __init__(
        self,
        logs_path: str = PREDICTION_RESULTS_PATH,
        config_path: str = RELIABILITY_CONFIG_PATH,
    ) -> None:
        self.logs_path = logs_path
        self.config_path = config_path
        self._config: Optional[dict] = None

    # ── I/O ──────────────────────────────────────────────────────────────────

    def _load_config(self) -> dict:
        try:
            with open(self.config_path) as fh:
                self._config = json.load(fh)
        except Exception:
            self._config = {"tiers": {}}
        return self._config  # type: ignore[return-value]

    def _save_config(self) -> None:
        with open(self.config_path, "w") as fh:
            json.dump(self._config, fh, indent=4)

    # ── CLV Math ─────────────────────────────────────────────────────────────

    @staticmethod
    def calculate_clv_score(p_open: float, p_close: float) -> float:
        """Positive = market moved in our direction after we bet."""
        return round(p_close - p_open, 4)

    # ── Main Update ──────────────────────────────────────────────────────────

    def update_reliability(self) -> None:
        """Read unprocessed rows from prediction_results.csv and update config."""
        try:
            import pandas as pd  # noqa: PLC0415
        except ImportError:
            logger.warning("[CLV] pandas not available — skipping feedback update")
            return

        try:
            df = pd.read_csv(self.logs_path)
        except Exception as exc:
            logger.warning("[CLV] Cannot load %s: %s", self.logs_path, exc)
            return

        cfg = self._load_config()

        processed_col = "processed"
        if processed_col not in df.columns:
            df[processed_col] = False

        recent = df[df[processed_col] == False]  # noqa: E712
        if recent.empty:
            logger.info("[CLV] No unprocessed rows — nothing to update")
            return

        updated = 0
        for idx, row in recent.iterrows():
            tier = str(row.get("volatility_tier", "mid_rotation"))
            prop = str(row.get("prop_type", "strikeouts"))

            # Ensure nested structure exists
            cfg.setdefault("tiers", {}).setdefault(tier, {}).setdefault(
                prop, {"reliability_score": 0.50}
            )

            # Lead indicator: CLV
            clv = self.calculate_clv_score(
                float(row.get("p_mkt_open", 0.524)),
                float(row.get("p_mkt_close", 0.524)),
            )

            # Lag indicator: Brier-style outcome error
            outcome = int(row.get("outcome", 0))
            p_final = float(row.get("p_final", 0.524))
            error = (outcome - p_final) ** 2

            current = cfg["tiers"][tier][prop].get("reliability_score", 0.50)
            new_r = current + (clv * 0.05)
            new_r += 0.01 if error < 0.20 else -0.01
            cfg["tiers"][tier][prop]["reliability_score"] = max(0.10, min(0.90, round(new_r, 4)))

            df.at[idx, processed_col] = True
            updated += 1

        self._save_config()
        try:
            df.to_csv(self.logs_path, index=False)
        except Exception as exc:
            logger.warning("[CLV] Could not save updated CSV: %s", exc)

        logger.info("[CLV] Updated reliability scores for %d rows", updated)

    # ── Log a new prediction ─────────────────────────────────────────────────

    def log_prediction(
        self,
        bet_id: str,
        player: str,
        prop_type: str,
        volatility_tier: str,
        model_prob: float,
        p_mkt_open: float,
        p_final: float,
    ) -> None:
        """Append a new prediction row to prediction_results.csv.

        Called at dispatch time (before outcome is known).
        ``outcome`` and ``p_mkt_close`` are filled at settlement.
        """
        import csv  # noqa: PLC0415
        import os   # noqa: PLC0415

        row = {
            "bet_id": bet_id,
            "player": player,
            "prop_type": prop_type,
            "volatility_tier": volatility_tier,
            "model_prob": round(model_prob, 4),
            "p_mkt_open": round(p_mkt_open, 4),
            "p_final": round(p_final, 4),
            "p_mkt_close": "",
            "outcome": "",
            "processed": False,
        }
        file_exists = os.path.isfile(self.logs_path)
        with open(self.logs_path, "a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
