"""
Agent 6: Arbitrage Agent
-------------------------
Strategy: Cross-book arbitrage — guaranteed profit when sum(implied probs) < 1.
Minimum 1% guaranteed ROI after juice. Exactly 2 legs across different books.
Books: DraftKings, FanDuel, BetMGM, bet365.
"""
from __future__ import annotations
import logging
from .base_agent import BaseAgent, BetSlip, Leg

logger = logging.getLogger("propiq.agent.arb")

ARB_BOOKS = {"draftkings", "fanduel", "betmgm", "bet365"}
MIN_ARB_PCT = 0.01    # 1% guaranteed profit
MAX_ARB_PCT = 0.08    # > 8% arb is likely a data error


def _arb_stake_split(
    prob_a: float, prob_b: float, total_units: float = 10.0
) -> tuple[float, float, float]:
    """
    Returns (stake_a, stake_b, guaranteed_profit_pct) for a 2-way arb.
    prob_a, prob_b are decimal odds (not probabilities).
    """
    # stake_a / stake_b = decimal_b / decimal_a  → equal payout
    total_implied = 1 / prob_a + 1 / prob_b
    if total_implied >= 1.0:
        return 0, 0, 0.0   # No arb
    profit_pct = (1 - total_implied) / total_implied
    stake_a = total_units / (prob_a * (1 / prob_a + 1 / prob_b))
    stake_b = total_units / (prob_b * (1 / prob_a + 1 / prob_b))
    return round(stake_a, 2), round(stake_b, 2), round(profit_pct, 4)


class ArbAgent(BaseAgent):
    name = "arb"
    strategy = "Cross-Book Arbitrage"
    max_legs = 2
    min_legs = 2
    ev_threshold = MIN_ARB_PCT

    def analyze(self, hub_data: dict) -> list[BetSlip]:
        # Need multi-book odds for same prop
        multi_book_props: dict[str, list[dict]] = hub_data.get("multi_book_props", {})
        props: list[dict] = hub_data.get("player_props", [])

        # Build multi-book index if not pre-computed
        if not multi_book_props:
            multi_book_props = {}
            for prop in props:
                key = (
                    f"{prop.get('player_name','')}|"
                    f"{prop.get('prop_type','')}|"
                    f"{prop.get('line','')}"
                )
                book = prop.get("bookmaker", "").lower()
                if book not in ARB_BOOKS:
                    continue
                multi_book_props.setdefault(key, []).append(prop)

        slips: list[BetSlip] = []

        for prop_key, book_list in multi_book_props.items():
            if len(book_list) < 2:
                continue

            player, prop_type, line_str = prop_key.split("|", 2)
            line = float(line_str) if line_str else 0.0

            # Find best OVER across all books
            best_over = max(
                (b for b in book_list if b.get("over_odds")),
                key=lambda b: self.american_to_decimal(int(b["over_odds"])),
                default=None
            )
            # Find best UNDER across all books
            best_under = max(
                (b for b in book_list if b.get("under_odds")),
                key=lambda b: self.american_to_decimal(int(b["under_odds"])),
                default=None
            )

            if not best_over or not best_under:
                continue

            book_over = best_over.get("bookmaker", "").lower()
            book_under = best_under.get("bookmaker", "").lower()

            if book_over == book_under:
                # Need different books for true arb
                # Try next-best under from different book
                for b in sorted(
                    (x for x in book_list if x.get("under_odds") and x.get("bookmaker", "").lower() != book_over),
                    key=lambda x: self.american_to_decimal(int(x["under_odds"])),
                    reverse=True
                ):
                    best_under = b
                    book_under = b.get("bookmaker", "").lower()
                    break

            if book_over == book_under:
                continue

            dec_over = self.american_to_decimal(int(best_over["over_odds"]))
            dec_under = self.american_to_decimal(int(best_under["under_odds"]))

            # Check arb
            total_implied = 1 / dec_over + 1 / dec_under
            if total_implied >= 1.0:
                continue  # No arb

            profit_pct = (1 - total_implied)
            if not (MIN_ARB_PCT <= profit_pct <= MAX_ARB_PCT):
                continue

            stake_over, stake_under, guaranteed_pct = _arb_stake_split(
                dec_over, dec_under, total_units=10.0
            )

            over_leg = Leg(
                player=player, prop_type=prop_type, line=line,
                direction="over", book=book_over,
                american_odds=int(best_over["over_odds"]),
                decimal_odds=dec_over,
                book_prob=round(1 / dec_over, 4),
                model_prob=round(1 / dec_over, 4),
                edge=0.0,   # Pure arb — no model needed
            )
            under_leg = Leg(
                player=player, prop_type=prop_type, line=line,
                direction="under", book=book_under,
                american_odds=int(best_under["under_odds"]),
                decimal_odds=dec_under,
                book_prob=round(1 / dec_under, 4),
                model_prob=round(1 / dec_under, 4),
                edge=0.0,
            )

            slips.append(BetSlip(
                agent_name=self.name,
                strategy=f"Arb {book_over.upper()} vs {book_under.upper()}",
                legs=[over_leg, under_leg],
                stake_units=stake_over + stake_under,
                combined_odds=1 + guaranteed_pct,
                expected_value=guaranteed_pct,
                confidence=1.0,   # Guaranteed
                metadata={
                    "arb_pct": guaranteed_pct,
                    "stake_over": stake_over,
                    "stake_under": stake_under,
                    "book_over": book_over,
                    "book_under": book_under,
                    "total_implied": total_implied,
                }
            ))

        # Sort by arb_pct
        slips.sort(key=lambda x: x.metadata.get("arb_pct", 0), reverse=True)
        logger.info(f"[arb] Found {len(slips)} arbitrage opportunities")
        return slips[:10]
