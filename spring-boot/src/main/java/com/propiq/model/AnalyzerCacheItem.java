package com.propiq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pre-computed analysis item stored in Redis "bet_analyzer_cache" (10s TTL).
 * Populated every 5s by BetAnalyzerTasklet so the REST endpoint + Telegram chatbot
 * return instantly without re-running the ML math.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AnalyzerCacheItem {

    private double modelProb;           // XGBoost probability (0-100)
    private double noVigProb;           // Market true probability (0-100)
    private double evPct;               // Expected value %
    private String matchupContext;      // e.g. "FIP 2.91 | SwStr% 14.2% | Ump K% 24.1%..."
    private int    agentsAgreeing;      // How many of 10 agents agree
    private boolean sharpMoney;         // True if public > 70% but money < 50% (RLM)
    private double publicBetPct;
    private double moneyPct;
    private int    bullpenFatigue;      // 0-4 score
    private boolean lineupTop4;         // Player confirmed in top-4 batting order
    private double umpireKPct;          // Called strike %
    private double windBoostFactor;     // 1.0 = neutral, >1.0 = wind boosts this prop
    private String checklistSummary;    // "5/7 checks passed"
    private String recommendation;      // "GREEN - BET NOW", "YELLOW - MARGINAL", "RED - AVOID"

    // Fields used by TelegramChatbotService
    private String recommendedPlatform; // "PrizePicks", "Underdog", "Sleeper"
    private double kellySizePct;        // Kelly sizing as % of bankroll

    // ── Alias getter: TelegramChatbotService expects getEdgePct() ─────────────
    public double getEdgePct() {
        return evPct;
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static class Builder {
        private final AnalyzerCacheItem item = new AnalyzerCacheItem();

        public Builder withModelProb(double p)          { item.modelProb = p;              return this; }
        public Builder withNoVigProb(double p)          { item.noVigProb = p;              return this; }
        public Builder withEvPct(double ev)             { item.evPct = ev;                 return this; }
        public Builder withMatchupContext(String ctx)   { item.matchupContext = ctx;        return this; }
        public Builder withAgentsAgreeing(int n)        { item.agentsAgreeing = n;         return this; }
        public Builder withSharpMoney(boolean sm)       { item.sharpMoney = sm;            return this; }
        public Builder withPublicBetPct(double p)       { item.publicBetPct = p;           return this; }
        public Builder withMoneyPct(double m)           { item.moneyPct = m;               return this; }
        public Builder withBullpenFatigue(int f)        { item.bullpenFatigue = f;         return this; }
        public Builder withLineupTop4(boolean l)        { item.lineupTop4 = l;             return this; }
        public Builder withUmpireKPct(double k)         { item.umpireKPct = k;             return this; }
        public Builder withWindBoostFactor(double w)    { item.windBoostFactor = w;        return this; }
        public Builder withRecommendedPlatform(String p){ item.recommendedPlatform = p;    return this; }
        public Builder withKellySizePct(double k)       { item.kellySizePct = k;           return this; }

        public AnalyzerCacheItem build() {
            // Derive recommendation from EV
            if (item.evPct >= 5.0) {
                item.recommendation = "GREEN - BET NOW";
            } else if (item.evPct > 0.0) {
                item.recommendation = "YELLOW - MARGINAL";
            } else {
                item.recommendation = "RED - AVOID";
            }

            // Derive recommended platform if not set
            if (item.recommendedPlatform == null) {
                item.recommendedPlatform = "PrizePicks"; // Default
            }

            // Derive Kelly size if not set
            if (item.kellySizePct == 0 && item.evPct > 0) {
                // Quarter-Kelly approximation from EV
                item.kellySizePct = Math.min(item.evPct * 0.5, 5.0); // cap at 5% bankroll
            }

            // 7-point checklist summary
            int checks = 0;
            if (item.modelProb > 55.0)        checks++;
            if (item.evPct > 5.0)             checks++;
            if (item.sharpMoney)              checks++;
            if (item.lineupTop4)              checks++;
            if (item.umpireKPct > 22.0)       checks++;
            if (item.bullpenFatigue < 2)       checks++;
            if (item.windBoostFactor > 1.05)  checks++;
            item.checklistSummary = checks + "/7 checks passed";

            return item;
        }
    }
}
