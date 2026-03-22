package com.propiq.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.propiq.kafka.TelegramAlertService;   // FIX: missing import added
import com.propiq.model.AnalyzerCacheItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * TelegramChatbotService — 2-Way Intelligence Chatbot
 *
 * Polls Telegram getUpdates every 3 seconds. When the user texts a player name,
 * it reads the pre-computed bet_analyzer_cache (written by BetAnalyzerTasklet every 5s)
 * and returns a full Intelligence Report with:
 *   - Model probability vs No-Vig fair probability
 *   - Matchup context (weather, umpire, pitcher stats)
 *   - Confidence Score 1–10 (derived from XGBoost probability)
 *   - Agent consensus count
 *   - DFS platform recommendation
 *
 * FIX: Uses fixedDelay (not fixedRate) to prevent overlapping poll requests.
 * The Telegram long-poll waits up to 2 seconds for a response, so fixedRate=3000
 * could fire a new request before the previous one finishes. fixedDelay=3000
 * waits 3 seconds AFTER each response completes before the next poll.
 */
@Service
public class TelegramChatbotService {

    private static final Logger logger = LoggerFactory.getLogger(TelegramChatbotService.class);

    private final RestTemplate restTemplate = new RestTemplate();

    /** Tracks the last processed update_id to prevent double-replies */
    private long lastUpdateId = 0;

    @Value("${propiq.telegram.bot-token}")
    private String botToken;

    @Value("${propiq.telegram.chat-id}")
    private String ownerChatId;

    @Autowired
    private RedisCacheManager redisCache;

    @Autowired
    private TelegramAlertService alertService;

    // ─────────────────────────────────────────────────────────
    //  LONG POLL — fires 3 seconds AFTER each response completes
    //  (fixedDelay prevents overlapping requests with the 2s server timeout)
    // ─────────────────────────────────────────────────────────

    @Scheduled(fixedDelay = 3000)  // FIX: was fixedRate=3000, now fixedDelay=3000
    public void pollTelegramForQuestions() {
        if (botToken == null || botToken.isBlank()) return;

        String url = String.format(
            "https://api.telegram.org/bot%s/getUpdates?offset=%d&timeout=2",
            botToken, lastUpdateId + 1
        );

        try {
            TelegramUpdateResponse response = restTemplate.getForObject(url, TelegramUpdateResponse.class);

            if (response == null || !response.isOk() || response.getResult() == null) return;

            for (TelegramUpdate update : response.getResult()) {
                lastUpdateId = update.getUpdateId();

                TelegramMessage msg = update.getMessage();
                if (msg == null || msg.getText() == null || msg.getText().isBlank()) continue;

                String text   = msg.getText().trim();
                String chatId = String.valueOf(msg.getChat().getId());

                logger.info("Telegram query [chatId={}]: {}", chatId, text);
                processUserQuery(text, chatId);
            }

        } catch (Exception e) {
            logger.error("Telegram poll error: {}", e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────
    //  QUERY DISPATCH
    // ─────────────────────────────────────────────────────────

    private void processUserQuery(String query, String chatId) {

        if (query.equalsIgnoreCase("/start") || query.equalsIgnoreCase("/help")) {
            alertService.sendTelegramMessage(chatId, buildHelpMessage());
            return;
        }

        if (query.equalsIgnoreCase("/top")) {
            handleTopPlays(chatId);
            return;
        }

        if (query.equalsIgnoreCase("/status")) {
            handleSystemStatus(chatId);
            return;
        }

        // Default: player name lookup
        handlePlayerLookup(query, chatId);
    }

    // ─────────────────────────────────────────────────────────
    //  PLAYER LOOKUP — reads bet_analyzer_cache
    // ─────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void handlePlayerLookup(String query, String chatId) {
        Map<String, AnalyzerCacheItem> cache = redisCache.get("bet_analyzer_cache", Map.class);

        if (cache == null || cache.isEmpty()) {
            alertService.sendTelegramMessage(chatId,
                "⚠️ *System Rebuilding*\n\nMLB Hub cache is empty — BetAnalyzerTasklet is warming up. Try again in 30 seconds.");
            return;
        }

        List<Map.Entry<String, AnalyzerCacheItem>> matches = cache.entrySet().stream()
            .filter(e -> e.getKey().toLowerCase().contains(query.toLowerCase()))
            .sorted((a, b) -> Double.compare(b.getValue().getEdgePct(), a.getValue().getEdgePct()))
            .limit(3)
            .toList();

        if (matches.isEmpty()) {
            alertService.sendTelegramMessage(chatId,
                "❌ *No active edges found* for '" + query + "' in today's market.\n\n" +
                "Try a last name only (e.g. `Judge`, `Ohtani`, `Cole`).");
            return;
        }

        alertService.sendTelegramMessage(chatId,
                formatIntelligenceReport(matches.get(0).getKey(), matches.get(0).getValue()));

        if (matches.size() > 1) {
            StringBuilder extras = new StringBuilder("📋 *Also found:*\n");
            for (int i = 1; i < matches.size(); i++) {
                AnalyzerCacheItem item = matches.get(i).getValue();
                extras.append(String.format("• %s → %.1f%% EV | %s\n",
                    matches.get(i).getKey().replace("_", " | "),
                    item.getEdgePct(),
                    item.getRecommendedPlatform()));
            }
            alertService.sendTelegramMessage(chatId, extras.toString());
        }
    }

    // ─────────────────────────────────────────────────────────
    //  /top — Top 5 plays by EV right now
    // ─────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void handleTopPlays(String chatId) {
        Map<String, AnalyzerCacheItem> cache = redisCache.get("bet_analyzer_cache", Map.class);

        if (cache == null || cache.isEmpty()) {
            alertService.sendTelegramMessage(chatId, "⚠️ Cache empty — try again in 30s.");
            return;
        }

        StringBuilder sb = new StringBuilder("🏆 *TOP 5 PLAYS RIGHT NOW*\n\n");

        cache.entrySet().stream()
            .filter(e -> e.getValue().getEdgePct() >= 3.0)
            .sorted((a, b) -> Double.compare(b.getValue().getEdgePct(), a.getValue().getEdgePct()))
            .limit(5)
            .forEach(e -> {
                AnalyzerCacheItem item = e.getValue();
                sb.append(String.format(
                    "• *%s*\n  EV: +%.1f%% | Conf: %d/10 | 📱 %s\n\n",
                    e.getKey().replace("_", " | "),
                    item.getEdgePct(),
                    confidenceScore(item.getModelProb()),
                    item.getRecommendedPlatform()
                ));
            });

        if (sb.toString().equals("🏆 *TOP 5 PLAYS RIGHT NOW*\n\n")) {
            sb.append("No plays above 3% EV threshold currently. Market may be efficient right now.");
        }

        alertService.sendTelegramMessage(chatId, sb.toString());
    }

    // ─────────────────────────────────────────────────────────
    //  /status — System health check
    // ─────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void handleSystemStatus(String chatId) {
        Map<String, AnalyzerCacheItem> cache = redisCache.get("bet_analyzer_cache", Map.class);
        Object hubState = redisCache.get("mlb_hub", Object.class);

        int propCount = (cache != null) ? cache.size() : 0;
        boolean hubUp = (hubState != null);

        String status = String.format(
            "🖥️ *PROPIQ SYSTEM STATUS*\n\n" +
            "%s MLB Hub Cache: %s\n" +
            "%s Analyzer Cache: %d active props\n" +
            "%s Telegram Bot: Online ✅\n\n" +
            "Commands: /top · /status · <player name>",
            hubUp        ? "✅" : "❌", hubUp ? "Online" : "Offline",
            propCount > 0 ? "✅" : "⚠️", propCount
        );

        alertService.sendTelegramMessage(chatId, status);
    }

    // ─────────────────────────────────────────────────────────
    //  INTELLIGENCE REPORT FORMATTER
    // ─────────────────────────────────────────────────────────

    private String formatIntelligenceReport(String propKey, AnalyzerCacheItem item) {
        int score       = confidenceScore(item.getModelProb());
        String bars     = "🟩".repeat(score) + "⬜".repeat(10 - score);
        String edgeSign = item.getEdgePct() >= 0 ? "+" : "";

        return String.format(
            "🧠 *PROPIQ INTELLIGENCE REPORT*\n" +
            "━━━━━━━━━━━━━━━━━━━━━━\n\n" +
            "👤 *Target:* %s\n" +
            "🎯 *Model Probability:* %.1f%%\n" +
            "⚡ *No-Vig Fair Prob:* %.1f%%\n" +
            "📈 *Edge:* %s%.1f%%\n\n" +
            "📝 *Matchup Context:*\n%s\n\n" +
            "📊 *Confidence: %d/10*\n%s\n\n" +
            "🤖 *Agent Consensus:* %d/10 agents approve\n\n" +
            "📱 *Platform:* %s\n" +
            "💰 *Kelly Sizing:* %.1f%% bankroll\n\n" +
            "%s",
            propKey.replace("_", " | "),
            item.getModelProb(),
            item.getNoVigProb(),
            edgeSign, item.getEdgePct(),
            formatMatchupContext(item.getMatchupContext()),
            score, bars,
            item.getAgentsAgreeing(),
            item.getRecommendedPlatform(),
            item.getKellySizePct(),
            buildRecommendation(item)
        );
    }

    // ─────────────────────────────────────────────────────────
    //  HELPERS
    // ─────────────────────────────────────────────────────────

    private int confidenceScore(double modelProb) {
        return (int) Math.min(10, Math.max(1, Math.round((modelProb - 50.0) / 5.0)));
    }

    private String formatMatchupContext(String raw) {
        if (raw == null || raw.isBlank()) return "_No context data available_";
        return raw.replace("Weather:", "🌤 Weather:")
                  .replace("Umpire:",  "⚖️ Umpire:")
                  .replace("Pitcher:", "⚾ Pitcher:")
                  .replace("Matchup:", "🆚 Matchup:")
                  .replace("Bullpen:", "💪 Bullpen:")
                  .replace("Wind:",    "💨 Wind:")
                  .replace("Lineup:",  "📋 Lineup:");
    }

    private String buildRecommendation(AnalyzerCacheItem item) {
        if (item.getEdgePct() >= 8.0) return "🔥 *STRONG PLAY* — High conviction. Execute on open.";
        if (item.getEdgePct() >= 5.0) return "✅ *SOLID PLAY* — Good edge. Standard Kelly sizing.";
        if (item.getEdgePct() >= 3.0) return "🟡 *MARGINAL PLAY* — Edge exists. Min sizing only.";
        return "⛔ *PASS* — Edge below 3% threshold. No action.";
    }

    private String buildHelpMessage() {
        return "🤖 *PROPIQ CHATBOT*\n\n" +
               "Send me a player name to get a full Intelligence Report.\n\n" +
               "*Commands:*\n" +
               "• `/top` — Top 5 plays by EV right now\n" +
               "• `/status` — System health check\n" +
               "• `/help` — This message\n\n" +
               "*Examples:*\n" +
               "`Judge` → Aaron Judge Hits report\n" +
               "`Cole strikeouts` → Gerrit Cole Ks report\n" +
               "`Ohtani` → All active Ohtani props";
    }

    // ─────────────────────────────────────────────────────────
    //  INNER CLASSES — Telegram API response DTOs
    // ─────────────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TelegramUpdateResponse {
        private boolean ok;
        private List<TelegramUpdate> result;
        public boolean isOk() { return ok; }
        public void setOk(boolean ok) { this.ok = ok; }
        public List<TelegramUpdate> getResult() { return result; }
        public void setResult(List<TelegramUpdate> result) { this.result = result; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TelegramUpdate {
        @JsonProperty("update_id") private long updateId;
        private TelegramMessage message;
        public long getUpdateId() { return updateId; }
        public void setUpdateId(long updateId) { this.updateId = updateId; }
        public TelegramMessage getMessage() { return message; }
        public void setMessage(TelegramMessage message) { this.message = message; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TelegramMessage {
        private String text;
        private TelegramChat chat;
        public String getText() { return text; }
        public void setText(String text) { this.text = text; }
        public TelegramChat getChat() { return chat; }
        public void setChat(TelegramChat chat) { this.chat = chat; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TelegramChat {
        private long id;
        public long getId() { return id; }
        public void setId(long id) { this.id = id; }
    }
}
