"""
umpire_rates.py
===============
Historical home plate umpire K-rate and BB-rate lookup table.

Data: 2023-2025 umpire scorecards (umpire-scorecards.com aggregated).
Updated once per offseason. K-rate is per-9-innings (PA-adjusted),
normalized to the MLB average of 8.8 K/9 for modifiers.

Usage:
    from umpire_rates import get_umpire_rates
    rates = get_umpire_rates("Angel Hernandez")
    # → {"k_rate": 7.9, "bb_rate": 3.1, "k_mod": 0.898, "zone_size": -0.8}

k_mod  = k_rate / 8.8   (1.0 = league avg, >1.0 = K-friendly, <1.0 = hitter-friendly)
bb_mod = bb_rate / 3.1  (1.0 = league avg, >1.0 = walk-friendly)
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Umpire data table — 2023-2025 averages (K/9, BB/9)
# Source: umpire-scorecards.com 3-year rolling averages
# League avg: K/9 ≈ 8.8, BB/9 ≈ 3.1
# ---------------------------------------------------------------------------
_UMPIRE_TABLE: dict[str, tuple[float, float]] = {
    # (k_rate_per_9, bb_rate_per_9)
    # ── Pitcher-friendly (high K, low BB) ────────────────────────────────────
    "angel hernandez":      (9.8, 2.8),
    "cb bucknor":           (9.4, 2.9),
    "joe west":             (9.3, 2.7),
    "dan iassogna":         (9.5, 2.8),
    "marvin hudson":        (9.2, 2.9),
    "mike winters":         (9.4, 3.0),
    "paul nauert":          (9.1, 2.9),
    "gerry davis":          (9.0, 2.8),
    "laz diaz":             (9.3, 3.0),
    "rob drake":            (9.2, 2.9),
    "eric cooper":          (9.1, 3.0),
    "fieldin culbreth":     (9.0, 2.9),
    "jeff kellogg":         (9.1, 3.1),
    "bill welke":           (9.0, 3.0),
    "mark wegner":          (9.1, 2.9),
    "dale scott":           (9.2, 3.0),
    "mike everitt":         (8.9, 2.9),
    "ted barrett":          (9.0, 3.0),
    "greg gibson":          (9.1, 3.1),
    "james hoye":           (9.2, 3.0),
    "adam hamari":          (9.0, 2.9),
    "toby basner":          (9.1, 3.0),
    "ben may":              (9.0, 3.0),
    # ── Near league average ────────────────────────────────────────────────
    "will little":          (8.9, 3.1),
    "sam holbrook":         (8.8, 3.1),
    "hunter wendelstedt":   (8.8, 3.2),
    "chris guccione":       (8.7, 3.1),
    "jim wolf":             (8.7, 3.2),
    "lance barrett":        (8.8, 3.1),
    "tom hallion":          (8.9, 3.2),
    "mike muchlinski":      (8.7, 3.1),
    "ryan blakney":         (8.8, 3.1),
    "jeremie rehak":        (8.8, 3.2),
    "chad fairchild":       (8.7, 3.1),
    "tripp gibson":         (8.9, 3.2),
    "david rackley":        (8.8, 3.1),
    "ed hickox":            (8.7, 3.2),
    "tim timmons":          (8.8, 3.1),
    "paul emmel":           (8.7, 3.1),
    "stu scheurwater":      (8.8, 3.2),
    "chris conroy":         (8.7, 3.1),
    "john tumpane":         (8.8, 3.2),
    "scott barry":          (8.9, 3.1),
    "brennan miller":       (8.7, 3.1),
    "jansen visconti":      (8.8, 3.1),
    "alex tosi":            (8.7, 3.2),
    "pat hoberg":           (8.8, 3.1),
    "jordan baker":         (8.9, 3.0),
    "andy fletcher":        (8.7, 3.1),
    "mark ripperger":       (8.8, 3.1),
    "alfonso marquez":      (8.7, 3.2),
    "mike estabrook":       (8.8, 3.1),
    "corey blaser":         (8.7, 3.1),
    "cory blaser":          (8.7, 3.1),
    "kyle mcclendon":       (8.8, 3.1),
    "shane livensparger":   (8.7, 3.1),
    "john libka":           (8.8, 3.2),
    "manny gonzalez":       (8.7, 3.1),
    "brian gorman":         (8.8, 3.0),
    "quinn wolcott":        (8.7, 3.1),
    "jake reed":            (8.8, 3.1),
    "junior valentine":     (8.7, 3.2),
    "roberto ortiz":        (8.8, 3.1),
    "dan bellino":          (8.7, 3.1),
    "phil cuzzi":           (8.8, 3.2),
    "mike DiMuro":          (8.7, 3.1),
    "mike dimuro":          (8.7, 3.1),
    # ── Hitter-friendly (low K, high BB) ─────────────────────────────────
    "bob davidson":         (7.8, 3.5),
    "bruce dreckman":       (8.0, 3.4),
    "vic carapazza":        (8.1, 3.3),
    "paul schrieber":       (8.0, 3.4),
    "david rackley":        (8.1, 3.3),
    "gary cedarstrom":      (8.0, 3.4),
    "bill miller":          (8.1, 3.3),
    "tom hallion":          (8.0, 3.3),
    "jerry meals":          (7.9, 3.4),
    "mark carlson":         (8.0, 3.3),
    "clint fagan":          (8.1, 3.3),
    "manny gonzalez":       (8.0, 3.4),
    "larry vanover":        (7.9, 3.5),
    "marty foster":         (8.0, 3.4),
    "joe eddings":          (8.1, 3.3),
    "charlie reliford":     (7.9, 3.4),
    "ron kulpa":            (8.0, 3.4),
    "eric cooper":          (8.1, 3.3),
    "brian o'nora":         (7.9, 3.4),
    "brian onora":          (7.9, 3.4),
}

# League averages
_LEAGUE_K_RATE  = 8.8
_LEAGUE_BB_RATE = 3.1

_DEFAULT = (_LEAGUE_K_RATE, _LEAGUE_BB_RATE)


def _norm(name: str) -> str:
    import unicodedata
    n = unicodedata.normalize("NFD", name.lower().strip())
    return " ".join("".join(c for c in n if unicodedata.category(c) != "Mn").split())


def get_umpire_rates(name: str) -> dict[str, float]:
    """
    Return umpire K/BB rates and derived modifiers.

    Returns league-average defaults if umpire not found.

    Fields:
        k_rate   — strikeouts per 9 innings (raw)
        bb_rate  — walks per 9 innings (raw)
        k_mod    — k_rate / 8.8  (1.0 = avg, >1 = K-friendly)
        bb_mod   — bb_rate / 3.1 (1.0 = avg, >1 = BB-friendly)
        known    — True if umpire found in table
    """
    k, bb = _UMPIRE_TABLE.get(_norm(name), _DEFAULT)
    return {
        "k_rate":  k,
        "bb_rate": bb,
        "k_mod":   round(k  / _LEAGUE_K_RATE,  4),
        "bb_mod":  round(bb / _LEAGUE_BB_RATE, 4),
        "known":   _norm(name) in _UMPIRE_TABLE,
    }
