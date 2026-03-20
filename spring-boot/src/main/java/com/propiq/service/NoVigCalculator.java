package com.propiq.service;

import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Strips the bookmaker's juice to reveal the true implied probability.
 *
 * Formula:
 *  1. Convert each American odds line to decimal
 *  2. Convert to implied probability: 1 / decimal
 *  3. Sum all implied probs (the "overround", always > 1.0)
 *  4. Divide each side's implied prob by the sum → true no-vig probability
 *
 * EV Formula:
 *  EV% = (modelProb * (decimalOdds - 1) - (1 - modelProb)) * 100
 */
@Service
public class NoVigCalculator {

    /**
     * Convert American odds string to decimal odds.
     * "-115" → 1.8696   "+105" → 2.05
     */
    public double parseAmerican(String americanOdds) {
        if (americanOdds == null || americanOdds.isBlank()) return 1.9;
        try {
            int odds = Integer.parseInt(americanOdds.replace("+", "").trim());
            if (odds > 0) {
                return 1.0 + (odds / 100.0);
            } else {
                return 1.0 + (100.0 / Math.abs(odds));
            }
        } catch (NumberFormatException e) {
            return 1.9; // default -110 equivalent
        }
    }

    /**
     * Convert American odds to implied probability (includes vig).
     */
    public double americanToImplied(int americanOdds) {
        if (americanOdds < 0) {
            return Math.abs(americanOdds) / (Math.abs(americanOdds) + 100.0);
        } else {
            return 100.0 / (americanOdds + 100.0);
        }
    }

    /**
     * Calculate the true no-vig probability for the OVER side.
     * marketOdds: book → American odds for OVER (over and under implied to 100)
     */
    public double calculateTrueProb(Map<String, Integer> marketOdds) {
        if (marketOdds == null || marketOdds.isEmpty()) return 0.50;

        // Take the best (highest decimal) odds across all books
        double bestDecimalOdds = marketOdds.values().stream()
                .mapToDouble(o -> parseAmerican(String.valueOf(o)))
                .max()
                .orElse(1.909);

        double impliedOver  = 1.0 / bestDecimalOdds;
        // Assume standard ~-110 for both sides if we only have over
        double impliedUnder = 1.0 / 1.909;

        double overround = impliedOver + impliedUnder;
        return impliedOver / overround;   // true probability stripped of vig
    }

    /**
     * True probability when we have both sides explicitly.
     */
    public double calculateTrueProbBothSides(int overAmerican, int underAmerican) {
        double impliedOver  = americanToImplied(overAmerican);
        double impliedUnder = americanToImplied(underAmerican);
        double overround    = impliedOver + impliedUnder;
        return impliedOver / overround;
    }

    /**
     * Calculate cross-book arbitrage edge.
     * Returns the guaranteed profit % if > 0.
     *
     * @param bestOverOdds   Best OVER decimal odds across all 4 books
     * @param bestUnderOdds  Best UNDER decimal odds across all 4 books
     */
    public double calculateArbEdge(double bestOverOdds, double bestUnderOdds) {
        double impliedOver  = 1.0 / bestOverOdds;
        double impliedUnder = 1.0 / bestUnderOdds;
        // If sum < 1.0, there is a guaranteed profit
        double arbMargin = 1.0 - (impliedOver + impliedUnder);
        return arbMargin * 100.0; // Return as percentage
    }

    public double calculateArbEdge(Map<String, Integer> marketOdds) {
        if (marketOdds == null || marketOdds.isEmpty()) return 0.0;
        double bestDecimal = marketOdds.values().stream()
                .mapToDouble(o -> parseAmerican(String.valueOf(o)))
                .max().orElse(1.9);
        // Simplified: use best over vs estimated best under
        return calculateArbEdge(bestDecimal, 1.9);
    }

    /**
     * Expected Value calculation.
     *
     * @param xgbProb     XGBoost probability 0-100
     * @param decimalOdds Decimal odds e.g. 1.87
     */
    public double calculateEv(double xgbProb, double decimalOdds) {
        double p = xgbProb / 100.0;
        double q = 1.0 - p;
        return ((p * (decimalOdds - 1)) - q) * 100.0;
    }

    /**
     * Quarter-Kelly fraction: f* = (b*p - q) / b × 0.25
     */
    public double calculateQuarterKelly(double xgbProb, double decimalOdds) {
        double p = xgbProb / 100.0;
        double q = 1.0 - p;
        double b = decimalOdds - 1.0;
        if (b <= 0) return 0.0;
        double fullKelly = (b * p - q) / b;
        return Math.max(0.0, fullKelly * 0.25);
    }

    /**
     * Convert decimal odds to American.
     */
    public int decimalToAmerican(double decimalOdds) {
        if (decimalOdds >= 2.0) {
            return (int) Math.round((decimalOdds - 1) * 100);
        } else {
            return (int) Math.round(-100 / (decimalOdds - 1));
        }
    }

    /**
     * Fair value American odds derived from no-vig probability.
     */
    public int fairValueAmerican(double noVigProb) {
        if (noVigProb <= 0 || noVigProb >= 1) return -110;
        return decimalToAmerican(1.0 / noVigProb);
    }
}
