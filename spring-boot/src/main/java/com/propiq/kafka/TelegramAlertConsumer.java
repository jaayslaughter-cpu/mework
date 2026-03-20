package com.propiq.kafka;

import com.propiq.model.Bet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * TelegramAlertConsumer — Formatted DFS push notifications.
 *
 * Listens to two Kafka topics:
 *
 *   bet_queue    → Formatted DFS slip for manual entry on PrizePicks/Underdog
 *   abort_queue  → 🚨 Emergency abort (late scratch, roof change, injury)
 *
 * Message format is designed for 1-tap DFS execution:
 * ┌────────────────────────────────────────┐
 * │ 🎯 PROPIQ PICK — EV_Hunter             │
 * │ Aaron Judge  OVER 1.5 Hits             │
 * │ ─────────────────────────────────────  │
 * │ XGBoost: 68.2%  EV: +7.4%             │
 * │ Books: DK -115 | FD -112 | BM -118    │
 * │ Line: 1.5 | Best: FanDuel -112         │
 * │ Units: 1.25u  Kelly: 0.018             │
 * │ Platform: PrizePicks [More/Less]       │
 * │ Ump K%: 24.1% | FIP: 3.22 | Pub: 62% │
 * │ Bullpen: 1/4 | Wind: Out-LF 12mph     │
 * └────────────────────────────────────────┘
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TelegramAlertConsumer {

    @Value("${propiq.telegram.bot-token}")
    private String botToken;

    @Value("${propiq.telegram.chat-id}")
    private String chatId;

    private final RestTemplate restTemplate;

    // ── Bet Queue (DFS pick alerts) ───────────────────────────────────────────

    @KafkaListener(topics = "bet_queue", groupId = "propiq-telegram")
    public void consumeBetAlert(Bet bet) {
        try {
            String message = formatBetAlert(bet);
            sendTelegramMessage(message);
            log.info("📱 Telegram: Sent DFS alert for {}", bet.getPlayer());
        } catch (Exception e) {
            log.error("Failed to send Telegram bet alert: {}", e.getMessage());
        }
    }

    // ── Abort Queue (emergency alerts) ───────────────────────────────────────

    @KafkaListener(topics = "abort_queue", groupId = "propiq-telegram")
    public void consumeAbortAlert(Bet bet) {
        try {
            String message = formatAbortAlert(bet);
            sendTelegramMessage(message);
            log.warn("🚨 Telegram: Emergency abort alert sent for {}", bet.getPlayer());
        } catch (Exception e) {
            log.error("Failed to send Telegram abort alert: {}", e.getMessage());
        }
    }

    // ── DFS Pick Formatter ─────────────────────────────────────────────────────

    private String formatBetAlert(Bet bet) {
        String platform = bet.getDfsPlatform() != null ? bet.getDfsPlatform() : "PrizePicks";
        String pickType = "PrizePicks".equalsIgnoreCase(platform) ? "More/Less" : "Higher/Lower";

        String evStr     = bet.getEvPct() > 0
                ? String.format("+%.1f%%", bet.getEvPct())
                : String.format("%.1f%%", bet.getEvPct());

        String umpireStr = bet.getUmpireKPct() > 0
                ? String.format("%.1f%%", bet.getUmpireKPct())
                : "N/A";

        String windStr   = bet.getWindSpeed() > 0
                ? String.format("%s %.0fmph", bet.getWindDirection(), bet.getWindSpeed())
                : "None";

        String bullpenStr = bet.getBullpenFatigue() >= 0
                ? bet.getBullpenFatigue() + "/4"
                : "N/A";

        return String.format("""
                🎯 *PROPIQ PICK — %s*
                *%s  %s  %.1f*
                ─────────────────────────────
                XGBoost: *%.1f%%*  EV: *%s*
                Books: %s
                Best: *%s  %s*
                Units: *%.2fu*  Kelly: %.3f
                Platform: *%s* [%s]
                ─────────────────────────────
                Ump K%%: %s | FIP: %.2f | Pub: %.0f%%
                Bullpen: %s | Wind: %s
                7-point: %s
                """,
                bet.getAgentName(),
                bet.getPlayer(), bet.getDirection().equals("OVER") ? "OVER" : "UNDER",
                bet.getTargetLineDouble(),
                bet.getXgboostProb(), evStr,
                formatBooksOdds(bet),
                bet.getBestOddsBook(), bet.getBestOdds(),
                bet.getUnitsRiskedDouble(), bet.getKellyFractionDouble(),
                platform, pickType,
                umpireStr, bet.getPitcherFip(), bet.getPublicBetPct(),
                bullpenStr, windStr,
                formatChecklistEmoji(bet)
        );
    }

    private String formatAbortAlert(Bet bet) {
        return String.format("""
                🚨 *EMERGENCY ABORT*
                ─────────────────────────────
                Player: *%s*  →  SCRATCHED
                Prop: %s  %.1f
                Agent: %s
                ─────────────────────────────
                ⚠️ If you already placed this on %s, attempt to CANCEL immediately.
                Bet ID: `%s`
                """,
                bet.getPlayer(), bet.getPropType(), bet.getTargetLineDouble(),
                bet.getAgentName(),
                bet.getDfsPlatform() != null ? bet.getDfsPlatform() : "DFS platform",
                bet.getBetId()
        );
    }

    private String formatBooksOdds(Bet bet) {
        if (bet.getMarketOdds() == null || bet.getMarketOdds().isEmpty()) return "Odds N/A";
        StringBuilder sb = new StringBuilder();
        bet.getMarketOdds().forEach((book, odds) ->
                sb.append(abbreviateBook(book)).append(": ").append(formatAmericanOdds(odds)).append(" | "));
        if (sb.length() > 3) sb.setLength(sb.length() - 3);
        return sb.toString();
    }

    private String abbreviateBook(String book) {
        return switch (book.toLowerCase()) {
            case "draftkings" -> "DK";
            case "fanduel"    -> "FD";
            case "betmgm"     -> "BM";
            case "bet365"     -> "B365";
            default           -> book.substring(0, Math.min(4, book.length())).toUpperCase();
        };
    }

    private String formatAmericanOdds(int odds) {
        return odds > 0 ? "+" + odds : String.valueOf(odds);
    }

    /** 7 green/red circles for the pro checklist */
    private String formatChecklistEmoji(Bet bet) {
        return String.format("%s%s%s%s%s%s%s",
                bet.isPitcherFipOk()        ? "🟢" : "🔴",  // 1. Pitcher FIP
                bet.isMatchupXwobaOk()      ? "🟢" : "🔴",  // 2. xwOBA matchup
                bet.isParkFactorOk()        ? "🟢" : "🔴",  // 3. Park factors
                bet.isUmpireOk()            ? "🟢" : "🔴",  // 4. Umpire K%
                bet.isPublicBettingOk()     ? "🟢" : "🔴",  // 5. Public % + RLM
                bet.isLineupConfirmedOk()   ? "🟢" : "🔴",  // 6. Lineup confirmed top-4
                bet.isBullpenOk()           ? "🟢" : "🔴"   // 7. Bullpen fatigue < 3
        );
    }

    // ── Telegram API sender ───────────────────────────────────────────────────

    private void sendTelegramMessage(String text) {
        String url = "https://api.telegram.org/bot" + botToken + "/sendMessage";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> body = new HashMap<>();
        body.put("chat_id",    chatId);
        body.put("text",       text);
        body.put("parse_mode", "Markdown");

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                log.error("Telegram send failed: {}", response.getBody());
            }
        } catch (Exception e) {
            log.error("Telegram HTTP call failed: {}", e.getMessage());
        }
    }
}
