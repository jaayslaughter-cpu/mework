"""
propiq_constants.py — Single source of truth for all PropIQ constants.

Any file that defines multipliers, gates, or thresholds should import from here.
This prevents the copy-paste drift that hit calibration_layer.py (fixed PR #589).

Usage:
    from propiq_constants import UD_MULTIPLIERS, PP_MULTIPLIERS, MIN_PROB
"""

# ── Underdog PowerPlay multipliers ──────────────────────────────────────────
# PR #584 confirmed: 2=3.5x, 3=6x, 4=10x
# PR #589 confirmed: 5=10x (FlexPlay) — NOT 20x
UD_MULTIPLIERS: dict = {
    2: 3.5,
    3: 6.0,
    4: 10.0,
    5: 10.0,  # 5-leg FlexPlay — PR #589 corrected from 20.0
}

# ── PrizePicks Power multipliers ─────────────────────────────────────────────
# PR #332 confirmed: 2=3x, 3=5x, 4=10x, 5=20x
# PR #589 corrected: 3=5x (was 6x)
PP_MULTIPLIERS: dict = {
    2: 3.0,
    3: 5.0,   # PR #589 corrected from 6.0
    4: 10.0,
    5: 20.0,
}

# ── PrizePicks Flex multipliers: (all_correct, one_miss) ─────────────────────
# PR #332: 4/4=6x, 3/4=1.5x, 2/3=1x
PP_FLEX_MULTIPLIERS: dict = {
    4: (6.0, 1.5),
    3: (3.0, 1.0),
    2: (1.0, 0.0),
}

# ── Dispatch gates ────────────────────────────────────────────────────────────
MIN_PROB: float = 0.60           # PR #572: raised from 0.57
MIN_CONFIDENCE: int = 6          # commit cdea446
MIN_EV_PCT: float = 3.0          # combined EV floor, PR #332
MAX_LEGS_PER_TEAM: int = 3       # team concentration cap, PR #312

# ── Streak agent gates ────────────────────────────────────────────────────────
STREAK_CONF_MIN: float = 6.0
STREAK_PROB_MIN: float = 0.62
STREAK_EV_MIN: float = 8.0
STREAK_MIN_LINE: float = 0.5
STREAK_MIN_SIGNALS: int = 2
STREAK_EV_BREAK_EVEN: float = 0.50
DEFAULT_ENTRY: int = 10          # $10 entry → $10,000 prize
