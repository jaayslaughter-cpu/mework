package com.propiq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Immutable bet record sent to the Kafka bet_queue.
 *
 * Field naming follows TelegramAlertService expectations.
 * Alias getters provide backward-compatibility for AgentTasklet Builder calls.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Bet {

    // ── Core identity ─────────────────────────────────────────────────────────
    private String betId;
    private String agentName;
    private String player;           // Display name: "Aaron Judge"
    private String propKey;          // "Aaron Judge_Hits_OVER_1.5"
    private String propType;         // "Hits", "Strikeouts", etc.
    private double targetLine;       // e.g. 1.5
    private String direction;        // "OVER" or "UNDER"
    private Instant placedAt;

    // ── Odds & EV ─────────────────────────────────────────────────────────────
    private String odds;             // Best American odds: "-115"
    private String bestOddsBook;     // Which book has best odds
    private Map<String, Integer> marketOdds; // book → American odds for OVER
    private double edgePct;          // Expected value %
    private double calculatedEv;     // Legacy alias (same as edgePct)
    private double noVigProb;        // No-vig fair probability (0-100)
    private double xgboostProb;      // XGBoost model probability (0-100)
    private double modelProb;        // Alias for xgboostProb (used by Telegram)

    // ── Sizing ────────────────────────────────────────────────────────────────
    private double kellyFraction;    // Raw Kelly fraction (0.0-1.0)
    private double kellySizePct;     // Kelly as % of bankroll (kellyFraction * 100)
    private double unitSizing;       // Final units to risk
    private int    requiredLegs;

    // ── DFS platform ─────────────────────────────────────────────────────────
    private String dfsPlatform;          // "PrizePicks", "Underdog", "Sleeper"
    private String recommendedPlatform;  // Explicit platform label (same as dfsPlatform)

    // ── Contextual checklist fields ───────────────────────────────────────────
    private double pitcherFip;
    private double umpireKPct;
    private double windSpeed;
    private String windDirection;
    private int    bullpenFatigue;     // 0-4 score
    private double publicBetPct;
    private int    agentsAgreeing;     // How many of 10 agents agreed
    private Map<String, Boolean> checklistFlags; // 7-point checklist results

    // ── Alias getters (backward compat with Telegram formatters) ─────────────

    /** @return player display name (alias for getPlayer()) */
    public String getPlayerName()        { return player; }

    /** @return "OVER" or "UNDER" (alias for getDirection()) */
    public String getSide()              { return direction; }

    /** @return target line as double (alias for getTargetLine()) */
    public double getLine()              { return targetLine; }
    public double getTargetLineDouble()  { return targetLine; }
    public double getUnitsRiskedDouble() { return unitSizing; }
    public double getKellyFractionDouble() { return kellyFraction; }

    /** @return edge % — prefers edgePct, falls back to calculatedEv */
    public double getEdgePct() {
        return edgePct != 0 ? edgePct : calculatedEv;
    }

    /** @return model prob — prefers modelProb, falls back to xgboostProb */
    public double getModelProb() {
        return modelProb != 0 ? modelProb : xgboostProb;
    }

    /** @return kelly as % of bankroll — prefers kellySizePct, derives from kellyFraction */
    public double getKellySizePct() {
        return kellySizePct != 0 ? kellySizePct : kellyFraction * 100.0;
    }

    /** @return recommended DFS platform — prefers recommendedPlatform, falls back to dfsPlatform */
    public String getRecommendedPlatform() {
        return recommendedPlatform != null ? recommendedPlatform : dfsPlatform;
    }

    // ── 7-point checklist boolean convenience methods ─────────────────────────

    public boolean isPitcherFipOk() {
        Boolean v = checklistFlags != null ? checklistFlags.get("pitcher") : null;
        return v != null ? v : (pitcherFip > 0 && pitcherFip < 3.80);
    }

    public boolean isMatchupXwobaOk() {
        Boolean v = checklistFlags != null ? checklistFlags.get("matchup") : null;
        return v != null && v;
    }

    public boolean isParkFactorOk() {
        Boolean v = checklistFlags != null ? checklistFlags.get("park") : null;
        return v != null && v;
    }

    public boolean isUmpireOk() {
        Boolean v = checklistFlags != null ? checklistFlags.get("umpire") : null;
        return v != null ? v : (umpireKPct > 22.0);
    }

    public boolean isPublicBettingOk() {
        Boolean v = checklistFlags != null ? checklistFlags.get("public") : null;
        return v != null && v;
    }

    public boolean isLineupConfirmedOk() {
        Boolean v = checklistFlags != null ? checklistFlags.get("lineup") : null;
        return v != null && v;
    }

    public boolean isBullpenOk() {
        Boolean v = checklistFlags != null ? checklistFlags.get("bullpen") : null;
        return v != null ? v : (bullpenFatigue < 3);
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static class Builder {
        private final Bet bet = new Bet();

        public Builder() {
            bet.betId    = UUID.randomUUID().toString();
            bet.placedAt = Instant.now();
            bet.checklistFlags = new HashMap<>();
        }

        // Core
        public Builder withAgent(String agent)           { bet.agentName = agent;           return this; }
        public Builder withPlayer(String player)         { bet.player = player;              return this; }
        public Builder withPropType(String pt)           { bet.propType = pt;                return this; }
        public Builder withTargetLine(double line)       { bet.targetLine = line;            return this; }
        public Builder withDirection(String dir)         { bet.direction = dir;              return this; }
        public Builder withOdds(String odds)             { bet.odds = odds;                  return this; }

        // EV / probability
        public Builder withCalculatedEv(double ev)       { bet.calculatedEv = ev; bet.edgePct = ev;        return this; }
        public Builder withEdgePct(double ev)            { bet.edgePct = ev; bet.calculatedEv = ev;        return this; }
        public Builder withNoVigProb(double p)           { bet.noVigProb = p;                return this; }
        public Builder withXgboostProb(double p)         { bet.xgboostProb = p; bet.modelProb = p;         return this; }
        public Builder withModelProb(double p)           { bet.modelProb = p; bet.xgboostProb = p;         return this; }

        // Sizing
        public Builder withKellyFraction(double k)       { bet.kellyFraction = k; bet.kellySizePct = k * 100.0; return this; }
        public Builder withKellySizePct(double k)        { bet.kellySizePct = k; bet.kellyFraction = k / 100.0; return this; }
        public Builder withUnitSizing(double units)      { bet.unitSizing = units;           return this; }
        public Builder withRequiredLegs(int legs)        { bet.requiredLegs = legs;          return this; }

        // Platform
        public Builder withDfsPlatform(String plat)      { bet.dfsPlatform = plat; bet.recommendedPlatform = plat; return this; }
        public Builder withRecommendedPlatform(String p) { bet.recommendedPlatform = p; bet.dfsPlatform = p;        return this; }

        // Odds detail
        public Builder withBestOddsBook(String book)     { bet.bestOddsBook = book;          return this; }
        public Builder withMarketOdds(Map<String, Integer> odds) { bet.marketOdds = odds;    return this; }

        // Context
        public Builder withPropKey(String key)           { bet.propKey = key;                return this; }
        public Builder withPitcherFip(double fip)        { bet.pitcherFip = fip;             return this; }
        public Builder withUmpireKPct(double k)          { bet.umpireKPct = k;               return this; }
        public Builder withWindSpeed(double s)           { bet.windSpeed = s;                return this; }
        public Builder withWindDirection(String d)       { bet.windDirection = d;            return this; }
        public Builder withBullpenFatigue(int f)         { bet.bullpenFatigue = f;           return this; }
        public Builder withPublicBetPct(double p)        { bet.publicBetPct = p;             return this; }
        public Builder withAgentsAgreeing(int n)         { bet.agentsAgreeing = n;           return this; }
        public Builder withChecklistFlag(String key, boolean val) {
            if (bet.checklistFlags == null) bet.checklistFlags = new HashMap<>();
            bet.checklistFlags.put(key, val);
            return this;
        }
        public Builder withChecklistFlags(Map<String, Boolean> flags) {
            bet.checklistFlags = flags; return this;
        }

        public Bet build() { return bet; }
    }
}
