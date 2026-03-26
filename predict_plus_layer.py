
from __future__ import annotations

import csv
import io
import json
import logging
import math
import os
import time
import tempfile
from collections import defaultdict
from datetime import datetime, timezone

import requests

logger = logging.getLogger("propiq.predict_plus")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MIN_TEST_PITCHES: int = 80    # minimum test-set pitches to produce a score
_MIN_TRAIN_PITCHES: int = 200  # minimum training pitches to fit the full model
_MIN_CLASS_FREQ: float = 0.03  # pitch types below 3% of training set are dropped

_SAVANT_CSV_URL: str = "https://baseballsavant.mlb.com/statcast_search/csv"

_HEADERS: dict = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://baseballsavant.mlb.com/statcast_search",
}

_REQUEST_DELAY: float = 1.5  # seconds between Savant requests (IP protection)

# Pitch-type family groupings — reduces class count for MLR stability.
# Same pitcher with 90% FF / 10% FT shouldn't be penalized for one variant.
_PITCH_FAMILIES: dict[str, str] = {
    "FF": "FB",  # 4-seam fastball
    "FA": "FB",  # generic fastball
    "FT": "FB",  # 2-seam fastball (older label)
    "SI": "SI",  # sinker — keep separate (different movement profile)
    "FC": "CT",  # cutter
    "SL": "SL",  # slider
    "ST": "SL",  # sweeper → slider family
}
    "SV": "SL",  # slurve → slider family
    "CU": "CB",  # curveball
    "KC": "CB",  # knuckle-curve
    "CS": "CB",  # slow curve
    "CH": "CH",  # changeup
    "FS": "CH",  # splitter → changeup family
    "FO": "CH",  # forkball → changeup family
    "SC": "CH",  # screwball → changeup family
    "KN": "KN",  # knuckleball (intentionally separate — unique profile)
    "EP": "CH",  # eephus → changeup family
}


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _get_cache_path(season: int) -> str:
    """Weekly cache path — same file used all week, refreshes on Monday."""
    today = datetime.now(timezone.utc)
    iso = today.isocalendar()  # (year, week, weekday)
    return os.path.join(_CACHE_DIR, f"predict_plus_{season}_{iso[0]}w{iso[1]}.json")


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def _fetch_pitcher_pitches(mlbam_id: int, season: int) -> list[dict]:
    """
    Download pitch-level Statcast data for one pitcher from Baseball Savant.
    Returns a list of row dicts from the CSV (empty list on any failure).
    """
    params = {
        "all": "true",
        "player_type": "pitcher",
        "pitchers_lookup[]": str(mlbam_id),
        "hfSea": f"{season}|",
        "type": "details",
    }
    try:
        resp = requests.get(
            _SAVANT_CSV_URL,
            params=params,
            headers=_HEADERS,
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning(
                "[PredictPlus] Savant HTTP %d for pitcher mlbam=%d season=%d",
                resp.status_code, mlbam_id, season,
            )
            return []

        reader = csv.DictReader(io.StringIO(resp.text))
        pitches = []
        for row in reader:
            pt = (row.get("pitch_type") or "").strip().upper()
            # Skip non-pitch events (intentional balls, hit-by-pitch outcomes, etc.)
            if pt in ("", "PO", "IN", "AB", "UN", "EP") or not pt:
                continue
            pitches.append(row)
        logger.debug(
            "[PredictPlus] Fetched %d pitches for mlbam=%d season=%d",
            len(pitches), mlbam_id, season,
        )
        return pitches

    except Exception as exc:
        logger.warning(
            "[PredictPlus] Fetch failed — mlbam=%d: %s", mlbam_id, exc
        )
        return []


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def _parse_int(val, default: int = 0) -> int:
    """Safe integer parse from Savant CSV string."""
    try:
        return int(val) if val not in (None, "", "null", "None") else default
    except (ValueError, TypeError):
        return default


def _build_features(pitches: list[dict]) -> list[dict]:
    """
    Build model-ready feature rows from raw Savant pitch data.

    Adds:
      - pitch_type (grouped by family)
      - last_pitch_type (previous pitch in same at-bat; "NONE" on first pitch)
      - base_state (0-7 bitmask: on_1b * 1 + on_2b * 2 + on_3b * 4)
      - two_strikes, is_risp flags
      - score_diff (batter team score - fielding team score)

    Sorts chronologically by (game_pk, at_bat_number, pitch_number).
    """
    try:
        pitches.sort(key=lambda r: (
            _parse_int(r.get("game_pk")),
            _parse_int(r.get("at_bat_number")),
            _parse_int(r.get("pitch_number")),
        ))
    except Exception:
        pass  # unsortable data → proceed with original order

    # Build per-at-bat pitch family sequences for last_pitch_type feature
    ab_sequence: dict[tuple, list[str]] = defaultdict(list)
    for p in pitches:
        ab_key = (
            _parse_int(p.get("game_pk")),
            _parse_int(p.get("at_bat_number")),
        )
        raw_pt = (p.get("pitch_type") or "").strip().upper()
        family = _PITCH_FAMILIES.get(raw_pt, raw_pt)
        ab_sequence[ab_key].append(family)

    feature_rows: list[dict] = []
    ab_idx: dict[tuple, int] = defaultdict(int)

    for p in pitches:
        raw_pt = (p.get("pitch_type") or "").strip().upper()
        family = _PITCH_FAMILIES.get(raw_pt, raw_pt)
        if not family:
            continue

        ab_key = (
            _parse_int(p.get("game_pk")),
            _parse_int(p.get("at_bat_number")),
        )
        idx = ab_idx[ab_key]
        last_pt = ab_sequence[ab_key][idx - 1] if idx > 0 else "NONE"
        ab_idx[ab_key] += 1

        # Runners bitmask (0 = empty, 7 = bases loaded)
        def _on(key: str, p=p) -> int:
            v = p.get(key, "")
            return 0 if v in (None, "", "null", "None") else 1

        on_1b = _on("on_1b")
        on_2b = _on("on_2b")
        on_3b = _on("on_3b")
        base_state = on_1b * 1 + on_2b * 2 + on_3b * 4

        balls   = _parse_int(p.get("balls"))
        strikes = _parse_int(p.get("strikes"))
        bat_s   = _parse_int(p.get("bat_score"))
        fld_s   = _parse_int(p.get("fld_score"))

        feature_rows.append({
            "pitch_type":      family,
            "balls":           min(balls, 3),
            "strikes":         min(strikes, 2),
            "two_strikes":     1 if strikes >= 2 else 0,
            "outs":            _parse_int(p.get("outs_when_up")),
            "stand":           1 if p.get("stand") == "L" else 0,  # 1=LHB, 0=RHB
            "base_state":      base_state,
            "is_risp":         1 if (on_2b or on_3b) else 0,
            "last_pitch_type": last_pt,
            "score_diff":      bat_s - fld_s,
        })

    return feature_rows


# ---------------------------------------------------------------------------
# Surprise (negative log-likelihood) computation
# ---------------------------------------------------------------------------

def _compute_predict_plus_ratio(feature_rows: list[dict]) -> float | None:
    """
    Compute the raw unpredictability ratio for a pitcher:
      ratio = mean_surprise(full model) / mean_surprise(baseline model)

    ratio > 1.0  → complex model MORE surprised than baseline
                  → context/sequencing doesn't help prediction
                  → HIGH unpredictability

    ratio ≈ 1.0  → both models equally surprised
                  → count-based rules explain pitch selection

    ratio < 1.0  → complex model LESS surprised (better predictions)
                  → pitcher follows learnable patterns
                  → LOW unpredictability

    Returns None if insufficient data.
    """
    try:
        import numpy as np
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import LabelEncoder
    except ImportError as exc:
        logger.error("[PredictPlus] scikit-learn unavailable: %s", exc)
        return None

    n = len(feature_rows)
    if n < _MIN_TRAIN_PITCHES + _MIN_TEST_PITCHES:
        return None

    # Chronological 80/20 split — no data leakage
    split = int(n * 0.80)
    train_rows = feature_rows[:split]
    test_rows  = feature_rows[split:]

    if len(test_rows) < _MIN_TEST_PITCHES:
        return None

    # Determine valid pitch-type classes (must appear ≥ 3% in training)
    class_counts: dict[str, int] = defaultdict(int)
    for r in train_rows:
        class_counts[r["pitch_type"]] += 1
    total_train = len(train_rows)
    valid_classes = {
        pt for pt, cnt in class_counts.items()
        if cnt / total_train >= _MIN_CLASS_FREQ
    }
    if len(valid_classes) < 2:
        return None

    train_rows = [r for r in train_rows if r["pitch_type"] in valid_classes]
    test_rows  = [r for r in test_rows  if r["pitch_type"] in valid_classes]
    if len(test_rows) < _MIN_TEST_PITCHES:
        return None

    # Build last_pitch_type integer encoding (consistent across train/test)
    all_lpt = sorted({r["last_pitch_type"] for r in train_rows} | {"NONE"})
    lpt_enc = {lpt: i for i, lpt in enumerate(all_lpt)}

    def _to_arrays(rows: list[dict], full: bool) -> tuple:
        """
        Convert feature rows to (X, y_str) numpy arrays.
        full=True  → include last_pitch_type and base_state (full model features)
        full=False → only balls/strikes/two_strikes/stand (baseline features)
        """
        y = np.array([r["pitch_type"] for r in rows])
        # Shared numerics: balls, strikes, two_strikes, stand
        shared = np.column_stack([
            [r["balls"]       for r in rows],
            [r["strikes"]     for r in rows],
            [r["two_strikes"] for r in rows],
            [r["stand"]       for r in rows],
        ]).astype(float)

        if full:
            extra = np.column_stack([
                [r["outs"]            for r in rows],
                [r["is_risp"]         for r in rows],
                [r["base_state"]      for r in rows],
                [r["score_diff"]      for r in rows],
                [lpt_enc.get(r["last_pitch_type"], 0) for r in rows],
            ]).astype(float)
            X = np.hstack([shared, extra])
        else:
            X = shared
        return X, y

    X_full_tr, y_tr = _to_arrays(train_rows, full=True)
    X_base_tr, _    = _to_arrays(train_rows, full=False)
    X_full_te, y_te = _to_arrays(test_rows,  full=True)
    X_base_te, _    = _to_arrays(test_rows,  full=False)

    # Label encode target
    le = LabelEncoder()
    le.fit(sorted(valid_classes))

    def _fit(X_tr: "np.ndarray", y_labels: "np.ndarray") -> "LogisticRegression | None":
        if len(set(y_labels)) < 2:
            return None
        try:
            clf = LogisticRegression(
                solver="lbfgs",
                max_iter=600,
                C=1.0,
                random_state=42,
            )
            clf.fit(X_tr, le.transform(y_labels))
            return clf
        except Exception as exc:
            logger.debug("[PredictPlus] LR fit error: %s", exc)
            return None

    full_model = _fit(X_full_tr, y_tr)
    base_model = _fit(X_base_tr, y_tr)
    if full_model is None or base_model is None:
        return None

    # Per-pitch surprise on test set
    P_full = full_model.predict_proba(X_full_te)
    P_base = base_model.predict_proba(X_base_te)
    y_te_enc = le.transform(y_te)

    eps = 1e-12
    s_full: list[float] = []
    s_base: list[float] = []
    for i, actual_class in enumerate(y_te_enc):
        s_full.append(-math.log(max(P_full[i, actual_class], eps)))
        s_base.append(-math.log(max(P_base[i, actual_class], eps)))

    mean_full = sum(s_full) / len(s_full)
    mean_base = sum(s_base) / len(s_base)
    if mean_base == 0.0:
        return None

    return mean_full / mean_base  # raw ratio; normalised downstream


# ---------------------------------------------------------------------------
# PredictPlusLayer class
# ---------------------------------------------------------------------------

class PredictPlusLayer:
    """
    Compute and serve Predict+ scores for today's starting pitchers.

    Weekly cache prevents re-fetching/re-computing during the season.
    Falls back gracefully if scikit-learn is unavailable or data is sparse.

    Usage:
        layer = PredictPlusLayer(season=2026)
        layer.prefetch([(543272, "Spencer Strider"), (656756, "Paul Skenes")])
        score = layer.get_score(543272)   # 112.4  (100 = league avg, SD = 10)
    """

    def __init__(self, season: int | None = None) -> None:
        self._season     = season or datetime.now(timezone.utc).year
        self._cache_path = _get_cache_path(self._season)
        self._cache:     dict[str, float] = {}   # str(mlbam_id) → Predict+ score
        self._loaded:    bool = False

    # ── cache I/O ──────────────────────────────────────────────────────────

    def _load_cache(self) -> bool:
        if os.path.exists(self._cache_path):
            try:
                with open(self._cache_path) as f:
                    raw = json.load(f)
                self._cache  = {str(k): float(v) for k, v in raw.items()}
                self._loaded = True
                logger.info(
                    "[PredictPlus] Cache loaded: %d pitchers (%s)",
                    len(self._cache), os.path.basename(self._cache_path),
                )
                return True
            except Exception as exc:
                logger.warning("[PredictPlus] Cache load failed: %s", exc)
        return False

    def _save_cache(self) -> None:
        try:
            with open(self._cache_path, "w") as f:
                json.dump(self._cache, f, indent=2)
            logger.info("[PredictPlus] Cache saved (%d pitchers)", len(self._cache))
        except Exception as exc:
            logger.warning("[PredictPlus] Cache save failed: %s", exc)

    # ── public API ─────────────────────────────────────────────────────────

    def prefetch(self, pitcher_ids: list[tuple[int, str]]) -> None:
        """
        Pre-compute Predict+ scores for a list of (mlbam_id, player_name) tuples.
        Uses weekly cache; only computes for uncached pitchers.

        Uses the prior full season (season - 1) for training — current season
        may have too few pitches early in the year.
        """
        if not self._loaded:
            self._load_cache()

        missing = [
            (mid, name) for mid, name in pitcher_ids
            if str(mid) not in self._cache and mid > 0
        ]
        if not missing:
            logger.info(
                "[PredictPlus] All %d pitchers already in cache.", len(pitcher_ids)
            )
            return

        train_season = self._season - 1  # prior full season for complete data
        logger.info(
            "[PredictPlus] Computing scores for %d pitchers (season=%d)...",
            len(missing), train_season,
        )

        raw_ratios: dict[str, float] = {}
        for mlbam_id, name in missing:
            pitches = _fetch_pitcher_pitches(mlbam_id, train_season)
            required = _MIN_TRAIN_PITCHES + _MIN_TEST_PITCHES
            if len(pitches) < required:
                logger.debug(
                    "[PredictPlus] %s: %d pitches < %d minimum — skipped",
                    name, len(pitches), required,
                )
                time.sleep(_REQUEST_DELAY)
                continue
            features = _build_features(pitches)
            ratio = _compute_predict_plus_ratio(features)
            if ratio is not None:
                raw_ratios[str(mlbam_id)] = ratio
                logger.debug("[PredictPlus] %s: raw_ratio=%.4f", name, ratio)
            else:
                logger.debug("[PredictPlus] %s: insufficient variance — skipped", name)
            time.sleep(_REQUEST_DELAY)

        if not raw_ratios:
            logger.warning(
                "[PredictPlus] No ratios computed — all pitchers below threshold."
            )
            return

        # Normalise to Predict+ scale: mean=100, SD=10 (like wRC+, ERA+, etc.)
        ratios = list(raw_ratios.values())
        if len(ratios) >= 3:
            mu     = sum(ratios) / len(ratios)
            sigma  = math.sqrt(sum((r - mu) ** 2 for r in ratios) / len(ratios))
            sigma  = sigma if sigma > 0 else 0.10  # avoid div-by-zero for identical scores
        else:
            # Single pitcher day — use fixed reference points from the methodology
            mu, sigma = 1.0, 0.10

        for mid_str, ratio in raw_ratios.items():
            self._cache[mid_str] = round(100.0 + 10.0 * (ratio - mu) / sigma, 1)

        self._save_cache()

    def get_score(self, mlbam_id: int, name: str = "") -> float:
        """
        Return Predict+ score (100 = league avg, SD = 10).
        Returns 0.0 if no data available for this pitcher.
        """
        if not self._loaded:
            self._load_cache()
        return self._cache.get(str(mlbam_id), 0.0)


# ---------------------------------------------------------------------------
# Probability adjustment function
# ---------------------------------------------------------------------------

def predict_plus_adjustment(
    prop_type: str,
    side: str,
    predict_plus_score: float,
) -> float:
    """
    Return probability delta based on Predict+ score.

    Only applies to strikeout props:
      Over  + high Predict+ (>110) → batters can't sit on a pitch → more Ks
      Over  + low  Predict+ (<90)  → batters read the arsenal → fewer Ks
      Under + low  Predict+ (<90)  → predictable = more contact = K Under value
      Under + high Predict+ (>110) → unpredictable = fewer "easy" contact ABs

    Scale: +0.010 at 110, +0.020 at 120+  (capped at ±0.020)
    """
    if prop_type != "strikeouts" or predict_plus_score <= 0:
        return 0.0

    deviation = predict_plus_score - 100.0  # positive = unpredictable

    if side == "Over":
        if deviation >= 10:        # score ≥ 110
            adj = min(0.020, deviation / 100.0 * 0.020)
            return round(adj, 4)
        elif deviation <= -10:     # score ≤ 90
            adj = min(0.010, abs(deviation) / 100.0 * 0.010)
            return round(-adj, 4)

    elif side == "Under":
        if deviation <= -10:       # score ≤ 90 — predictable pitcher, more contact
            adj = min(0.015, abs(deviation) / 100.0 * 0.015)
            return round(adj, 4)
        elif deviation >= 10:      # score ≥ 110 — unpredictable = fewer "free" contact ABs
            adj = min(0.010, deviation / 100.0 * 0.010)
            return round(-adj, 4)

    return 0.0
