package com.propiq.discord;

import com.propiq.model.Bet;
import com.propiq.model.BetRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * DiscordAlertService — Outbound webhook bridge between PropIQ and Discord.
 *
 * Replaces TelegramAlertService. Listens on Kafka bet_queue and abort_queue,
 * then POSTs formatted messages to the configured Discord webhook URL.
 *
 * NOTE: Discord webhooks are outbound-only. Two-way queries (player lookups)
 * require a full Discord bot token with MESSAGE_CONTENT intent — out of scope
 * for the current webhook integration.
 *
 * Config (application.yml / environment):
 *   DISCORD_WEBHOOK_URL — full webhook URL from Discord channel settings
 */
@Slf4j
@Service
public class DiscordAlertService {

    private static final int DISCORD_MAX_CHARS = 2000;
    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("MMMM d, yyyy");

    @Value("${propiq.discord.webhook-url}")
    private String webhookUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    // ── Kafka Listeners ───────────────────────────────────────────────────────

    /**
     * Consumes from bet_queue — fires every time AgentTasklet dispatches a +EV play.
     */
    @KafkaListener(topics = "bet_queue", groupId = "discord-alert-consumer")
    public void onBetQueued(Bet bet) {
        sendBetAlert(bet);
    }

    /**
     * Consumes from abort_queue — fires on late scratch or roof-state change.
     */
    @KafkaListener(topics = "abort_queue", groupId = "discord-abort-consumer")
    public void onAbortQueued(Bet bet) {
        sendAbortAlert(bet);
    }

    // ── Public Methods (called directly by GradingTasklet) ────────────────────

    /**
     * +EV play alert — fired for every qualified bet that exits the AgentTasklet.
     */
    public void sendBetAlert(Bet bet) {
        if (bet == null) return;

        String platform     = bet.getRecommendedPlatform() != null ? bet.getRecommendedPlatform() : "PrizePicks";
        String platformEmoji = getPlatformEmoji(platform);
        String direction    = bet.getDirection() != null ? bet.getDirection() : "OVER";
        String dirEmoji     = "OVER".equals(direction) ? "📈" : "📉";
        String player       = bet.getPlayerName() != null ? bet.getPlayerName() : "Unknown";
        String propType     = bet.getPropType() != null ? bet.getPropType() : "Prop";
        String odds         = formatOdds(bet.getOdds());

        String message = String.format(
                "🎯 **PropIQ +EV Alert**\n" +
                "━━━━━━━━━━━━━━━━━━━━━━\n" +
                "**%s %s** %s **%.1f** @ %s\n\n" +
                "📊 XGBoost: **%.1f%%** | No-Vig: **%.1f%%**\n" +
                "💰 Edge: **+%.1f%%** | Size: **%.2fu**\n" +
                "🤖 Agent: `%s`\n\n" +
                "%s **OPEN APP: %s**\n" +
                "━━━━━━━━━━━━━━━━━━━━━━",
                player, propType, dirEmoji, bet.getTargetLine(), odds,
                bet.getXgboostProb(), bet.getNoVigProb(),
                bet.getEdgePct(), bet.getUnitSizing(),
                bet.getAgent() != null ? bet.getAgent() : "PropIQ",
                platformEmoji, platform
        );

        postToWebhook(message);
    }

    /**
     * Emergency abort alert — late scratch or stadium roof state change.
     */
    public void sendAbortAlert(Bet bet) {
        if (bet == null) return;

        String player   = bet.getPlayerName() != null ? bet.getPlayerName() : "Unknown";
        String propType = bet.getPropType() != null ? bet.getPropType() : "Prop";
        String dir      = bet.getDirection() != null ? bet.getDirection() : "";

        String message = String.format(
                "🚨 **ABORT SIGNAL — PropIQ**\n" +
                "━━━━━━━━━━━━━━━━━━━━━━\n" +
                "**%s** — %s %s\n" +
                "⚠️ Late scratch or roof state change detected.\n" +
                "**DO NOT enter this play.**\n" +
                "━━━━━━━━━━━━━━━━━━━━━━",
                player, propType, dir
        );

        postToWebhook(message);
    }

    /**
     * End-of-day settlement recap — called by GradingTasklet at 11:30 PM PT.
     *
     * Shows date, W/L/P record, total units, and a line-by-line breakdown.
     */
    public void sendDailyRecap(List<BetRecord> settledBets, double totalProfit) {
        if (settledBets == null || settledBets.isEmpty()) {
            log.info("No settled bets for Discord recap — skipping.");
            return;
        }

        long wins   = settledBets.stream().filter(b -> "WIN".equalsIgnoreCase(b.getStatus())).count();
        long losses = settledBets.stream().filter(b -> "LOSS".equalsIgnoreCase(b.getStatus())).count();
        long pushes = settledBets.stream().filter(b -> "PUSH".equalsIgnoreCase(b.getStatus())).count();

        String profitEmoji = totalProfit >= 0 ? "📈" : "📉";
        String dateStr     = LocalDate.now().format(DATE_FMT);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("📊 **PropIQ Daily Recap — %s**\n", dateStr));
        sb.append("━━━━━━━━━━━━━━━━━━━━━━\n\n");
        sb.append(String.format("%s Units: **%+.2fu**\n", profitEmoji, totalProfit));
        sb.append(String.format("🏆 Record: **%d-%d-%d** (W-L-P)\n\n", wins, losses, pushes));
        sb.append("━━━━━━━━━━━━━━━━━━━━━━\n");

        for (BetRecord rec : settledBets) {
            String emoji = switch (rec.getStatus() != null ? rec.getStatus().toUpperCase() : "") {
                case "WIN"  -> "✅";
                case "LOSS" -> "❌";
                case "PUSH" -> "➖";
                default     -> "⏳";
            };
            String recOdds = formatOdds(rec.getOdds());
            sb.append(String.format("%s **%s** — %s %s @ %s | %+.2fu\n",
                    emoji,
                    rec.getPlayerName() != null ? rec.getPlayerName() : "Unknown",
                    rec.getPropType()   != null ? rec.getPropType()   : "Prop",
                    rec.getDirection()  != null ? rec.getDirection()  : "",
                    recOdds,
                    rec.getProfitLoss()
            ));
        }

        sb.append("━━━━━━━━━━━━━━━━━━━━━━\n");
        sb.append("_Powered by PropIQ Analytics_ 🤖");

        postToWebhook(sb.toString());
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private void postToWebhook(String content) {
        try {
            // Discord hard limit: 2000 chars per message
            if (content.length() > DISCORD_MAX_CHARS) {
                content = content.substring(0, DISCORD_MAX_CHARS - 3) + "...";
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, String> body = Map.of("content", content);
            HttpEntity<Map<String, String>> entity = new HttpEntity<>(body, headers);

            ResponseEntity<String> response =
                    restTemplate.postForEntity(webhookUrl, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("✅ Discord webhook message delivered.");
            } else {
                log.warn("⚠️ Discord webhook returned non-2xx: {}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.error("❌ Discord webhook POST failed: {}", e.getMessage());
        }
    }

    /**
     * Convert decimal odds string to American format (e.g. "+110", "-115").
     * If already American format, just ensures the sign is present.
     */
    private String formatOdds(String odds) {
        if (odds == null || odds.isBlank()) return "N/A";
        try {
            double d = Double.parseDouble(odds);
            // Looks like decimal odds (e.g. 1.91)
            if (d > 0 && d < 30) {
                int american = d >= 2.0
                        ? (int) Math.round((d - 1) * 100)
                        : (int) Math.round(-100.0 / (d - 1));
                return american > 0 ? "+" + american : String.valueOf(american);
            }
            // Already American (e.g. 110, -115, -110)
            int i = (int) d;
            return i > 0 ? "+" + i : String.valueOf(i);
        } catch (NumberFormatException e) {
            return odds; // Already formatted string
        }
    }

    private String getPlatformEmoji(String platform) {
        if (platform == null) return "🎯";
        return switch (platform.toUpperCase().replace(" ", "_")) {
            case "PRIZEPICKS"       -> "🏆";
            case "UNDERDOG_FANTASY",
                 "UNDERDOG"        -> "🐶";
            case "SLEEPER"         -> "😴";
            default                -> "🎯";
        };
    }
}
