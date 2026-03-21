package com.propiq.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.propiq.model.Bet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * TelegramAlertService
 *
 * Kafka consumer for bet_queue and abort_queue.
 * Every outbound message EXPLICITLY states which DFS app to open.
 * Also used by TelegramChatbotService to send chatbot replies.
 *
 * Platform routing logic:
 *   - UNDERDOG  → best for higher-limit alternate lines & fantasy scoring
 *   - PRIZEPICKS → best for standard over/under, widest prop menu
 *   - SLEEPER    → best for player points/assists/rebounds combos
 *   Decision stored on Bet.recommendedPlatform (set by DFSOptimizerAgent)
 */
@Service
public class TelegramAlertService {

    private static final Logger logger = LoggerFactory.getLogger(TelegramAlertService.class);
    private static final String TELEGRAM_API = "https://api.telegram.org/bot%s/sendMessage";

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper  = new ObjectMapper();

    @Value("${propiq.telegram.bot-token}")
    private String botToken;

    @Value("${propiq.telegram.chat-id}")
    private String defaultChatId;

    // ─────────────────────────────────────────────────────────
    //  KAFKA CONSUMERS
    // ─────────────────────────────────────────────────────────

    @KafkaListener(topics = "bet_queue", groupId = "propiq-telegram")
    public void onBetQueued(ConsumerRecord<String, String> record) {
        try {
            Bet bet = objectMapper.readValue(record.value(), Bet.class);
            logger.info("Bet received from Kafka: {} | EV={}", bet.getPropKey(), bet.getEdgePct());
            String message = formatBetAlert(bet);
            sendTelegramMessage(defaultChatId, message);
        } catch (Exception e) {
            logger.error("Failed to process bet_queue message: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "abort_queue", groupId = "propiq-telegram")
    public void onAbortSignal(ConsumerRecord<String, String> record) {
        try {
            Map<?, ?> payload = objectMapper.readValue(record.value(), Map.class);
            logger.warn("ABORT signal received: {}", payload);
            String message = formatAbortAlert(payload);
            sendTelegramMessage(defaultChatId, message);
        } catch (Exception e) {
            logger.error("Failed to process abort_queue message: {}", e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────
    //  BET ALERT FORMATTER  — explicit platform stamp
    // ─────────────────────────────────────────────────────────

    private String formatBetAlert(Bet bet) {
        String platform    = resolvePlatformLabel(bet.getRecommendedPlatform());
        String platformEmoji = resolvePlatformEmoji(bet.getRecommendedPlatform());
        String edgeStr     = String.format("+%.1f%%", bet.getEdgePct());
        String kellyStr    = String.format("%.1f%%", bet.getKellySizePct());
        String confBars    = buildConfidenceBars(bet.getModelProb());

        return String.format(
            "🚨 *NEW DFS PLAY* 🚨\n" +
            "━━━━━━━━━━━━━━━━━━━━━━\n\n" +
            "👤 *Player:* %s\n" +
            "📊 *Prop:* %s %s %s\n\n" +
            "%s *OPEN APP: %s*\n" +
            "━━━━━━━━━━━━━━━━━━━━━━\n\n" +
            "⚡ *Edge:* %s | Fair Prob: %.1f%%\n" +
            "📈 *Model Confidence:* %.1f%%\n" +
            "%s\n\n" +
            "💰 *Sizing:* %s of bankroll\n" +
            "🤖 *Agents:* %d/10 agree\n\n" +
            "📋 *Checklist:*\n%s\n\n" +
            "⏰ *Act before lineup lock!*",
            bet.getPlayerName(),
            bet.getPropType(), bet.getSide(), bet.getLine(),
            platformEmoji, platform,
            edgeStr, bet.getNoVigProb(),
            bet.getModelProb(), confBars,
            kellyStr,
            bet.getAgentsAgreeing(),
            formatChecklist(bet)
        );
    }

    // ─────────────────────────────────────────────────────────
    //  ABORT ALERT FORMATTER
    // ─────────────────────────────────────────────────────────

    private String formatAbortAlert(Map<?, ?> payload) {
        String player   = String.valueOf(payload.getOrDefault("player",  "Unknown"));
        String reason   = String.valueOf(payload.getOrDefault("reason",  "Late Scratch"));
        String platform = String.valueOf(payload.getOrDefault("platform","ALL APPS"));
        String propKey  = String.valueOf(payload.getOrDefault("propKey", ""));

        return String.format(
            "🚨🚨 *EMERGENCY ABORT* 🚨🚨\n" +
            "━━━━━━━━━━━━━━━━━━━━━━\n\n" +
            "⛔ *CANCEL IMMEDIATELY*\n\n" +
            "👤 *Player:* %s\n" +
            "❌ *Reason:* %s\n" +
            "📱 *App:* %s\n" +
            "🔑 *Prop:* %s\n\n" +
            "━━━━━━━━━━━━━━━━━━━━━━\n" +
            "✅ Open app now and cancel any unmatched entries for this player.\n" +
            "✅ Do NOT re-enter until confirmation from PropIQ.",
            player, reason, platform, propKey
        );
    }

    // ─────────────────────────────────────────────────────────
    //  7-POINT CHECKLIST  (emoji indicators on each point)
    // ─────────────────────────────────────────────────────────

    private String formatChecklist(Bet bet) {
        StringBuilder sb = new StringBuilder();
        Map<String, Boolean> checks = bet.getChecklistFlags();
        if (checks == null) checks = new HashMap<>();

        sb.append(flag(checks, "pitcher")   ).append(" Pitcher FIP/SwStr%\n");
        sb.append(flag(checks, "matchup")   ).append(" Batter vs Pitcher xwOBA\n");
        sb.append(flag(checks, "park")      ).append(" Park Factor + Wind\n");
        sb.append(flag(checks, "umpire")    ).append(" Umpire K% Tendency\n");
        sb.append(flag(checks, "public")    ).append(" Sharp vs Public Money\n");
        sb.append(flag(checks, "lineup")    ).append(" Lineup Confirmed (Top-4)\n");
        sb.append(flag(checks, "bullpen")   ).append(" Bullpen Fatigue Score");

        return sb.toString();
    }

    private String flag(Map<String, Boolean> checks, String key) {
        Boolean val = checks.get(key);
        return (val != null && val) ? "✅" : "⬜";
    }

    // ─────────────────────────────────────────────────────────
    //  PLATFORM ROUTING HELPERS
    // ─────────────────────────────────────────────────────────

    private String resolvePlatformLabel(String platform) {
        if (platform == null) return "PrizePicks";
        return switch (platform.toUpperCase()) {
            case "UNDERDOG"   -> "Underdog Fantasy";
            case "SLEEPER"    -> "Sleeper Fantasy";
            case "PRIZEPICKS" -> "PrizePicks";
            default           -> platform;
        };
    }

    private String resolvePlatformEmoji(String platform) {
        if (platform == null) return "📱";
        return switch (platform.toUpperCase()) {
            case "UNDERDOG"   -> "🐶";
            case "SLEEPER"    -> "😴";
            case "PRIZEPICKS" -> "🏆";
            default           -> "📱";
        };
    }

    private String buildConfidenceBars(double modelProb) {
        int score = (int) Math.min(10, Math.max(1, Math.round((modelProb - 50.0) / 5.0)));
        return "🟩".repeat(score) + "⬜".repeat(10 - score) + " " + score + "/10";
    }

    // ─────────────────────────────────────────────────────────
    //  PUBLIC SEND — used by TelegramChatbotService for replies
    // ─────────────────────────────────────────────────────────

    public void sendTelegramMessage(String chatId, String text) {
        if (botToken == null || botToken.isBlank() || chatId == null || chatId.isBlank()) {
            logger.warn("Telegram credentials not configured — skipping message");
            return;
        }

        try {
            String url = String.format(TELEGRAM_API, botToken);

            Map<String, Object> body = new HashMap<>();
            body.put("chat_id",    chatId);
            body.put("text",       text);
            body.put("parse_mode", "Markdown");

            restTemplate.postForObject(url, body, String.class);
            logger.info("Telegram message sent to chatId={}", chatId);

        } catch (Exception e) {
            logger.error("Failed to send Telegram message: {}", e.getMessage());
        }
    }
}
