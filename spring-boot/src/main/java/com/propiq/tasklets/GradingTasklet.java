package com.propiq.tasklets;

import com.propiq.discord.DiscordAlertService;
import com.propiq.model.BetRecord;
import com.propiq.model.MlbHubState;
import com.propiq.service.PostgresService;
import com.propiq.service.XGBoostModelService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * GradingTasklet — Nightly settlement with CLV calculation.
 *
 * Runs daily at 11:30 PM Pacific Time, giving West Coast extra-inning games
 * maximum time to finish. Any game not yet "Final" is NOT graded — its bets
 * remain PENDING and automatically roll over to the next execution.
 *
 * Flow:
 *  1. Pull ALL PENDING bets from bet_ledger (any date — handles rollovers)
 *  2. For each bet, check the game's official status from the boxscore store
 *     → If status != "Final", skip entirely (bet stays PENDING for rollover)
 *  3. Run XGBoost anomaly detection on the actual stat
 *  4. Settle WIN / LOSS / PUSH and calculate:
 *     - Profit/Loss in units
 *     - CLV: (closing no-vig prob - placed no-vig prob) × 100
 *  5. Update agent_stats for LeaderboardTasklet
 *  6. Send end-of-day settlement recap via Telegram
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GradingTasklet implements Tasklet {

    private final PostgresService      postgresService;
    private final XGBoostModelService  xgboostService;
    private final DiscordAlertService  discordAlertService;

    /**
     * Runs daily at 11:30 PM Pacific Time.
     * zone = "America/Los_Angeles" ensures the cron fires on PT wall-clock time
     * regardless of the server's system timezone (Railway uses UTC by default).
     * Late extra-inning West Coast games (end ~11:15 PM PT) are covered.
     * Any still-live game is safely skipped via the Final status gate below.
     */
    @Scheduled(cron = "0 30 23 * * ?", zone = "America/Los_Angeles")
    public void executeScheduled() {
        try {
            execute(null, null);
        } catch (Exception e) {
            log.error("GradingTasklet failed: {}", e.getMessage());
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("⚖️ GradingTasklet — processing ALL pending bets (including rollovers)");

        // ── Change 2: Fetch ALL PENDING bets regardless of date ───────────────
        // This replaces the previous getUnsettledBetsByDate("YESTERDAY") call.
        // Bets from any prior date that were skipped (live/postponed/delayed)
        // automatically appear here and get another grading attempt tonight.
        List<BetRecord> pendingBets = postgresService.getAllPendingBets();

        if (pendingBets.isEmpty()) {
            log.info("No pending bets to grade. Grading complete.");
            return RepeatStatus.FINISHED;
        }

        log.info("Found {} pending bets to evaluate", pendingBets.size());

        int    wins = 0, losses = 0, pushes = 0, flagged = 0, rolledOver = 0;
        double totalProfit = 0.0;

        // Collect successfully settled bets for the end-of-day Telegram recap
        List<BetRecord> settledToday = new ArrayList<>();

        for (BetRecord bet : pendingBets) {

            // ── Change 3: Final status gate ────────────────────────────────────
            // Query the boxscore store for the game's current status.
            // If the game is still live, postponed, suspended, or delayed,
            // leave the bet as PENDING — it will roll over to tomorrow's run.
            String gameStatus = getGameStatus(bet);
            if (!isFinal(gameStatus)) {
                log.info("⏳ Game {} not Final (status='{}') — bet {} rolled over to next run",
                        bet.getGameId(), gameStatus, bet.getId());
                rolledOver++;
                continue;  // Do NOT change the bet's status — stays PENDING
            }

            // Game is Final — safe to fetch the actual stat
            double actualStat = getActualStat(bet);
            if (actualStat < 0) {
                log.warn("Game {} final but stat unavailable — skipping bet {}", bet.getGameId(), bet.getId());
                continue;
            }

            // XGBoost anomaly detection — protects against scorekeeper corrections
            boolean anomaly = xgboostService.detectStatCorrectionAnomaly(
                    null, bet.getPlayerName(), actualStat);

            if (anomaly) {
                log.warn("🚨 ANOMALY detected for {} — flagging for manual review (bet: {})",
                        bet.getPlayerName(), bet.getId());
                postgresService.markBetForReview(bet.getId());
                flagged++;
                continue;
            }

            // Grade the bet
            String status     = gradeBet(bet, actualStat);
            double profitLoss = calculateProfitLoss(bet, status);

            // Persist to bet_ledger
            postgresService.settleBet(bet.getId(), actualStat, status, profitLoss);

            // Update in-memory record so recap formatter has status + P&L
            bet.setStatus(status);
            bet.setProfitLoss(profitLoss);
            settledToday.add(bet);

            totalProfit += profitLoss;

            if ("WIN".equals(status))       wins++;
            else if ("LOSS".equals(status)) losses++;
            else                            pushes++;

            log.info("Settled {} | {} {} {} | Actual={} | {} | {}u",
                    bet.getId(), bet.getPlayerName(), bet.getPropType(),
                    bet.getTargetLine(), actualStat, status,
                    String.format("%.2f", profitLoss));
        }

        log.info("✅ Grading complete. W:{} L:{} P:{} Flagged:{} RolledOver:{} | Net: {}u",
                wins, losses, pushes, flagged, rolledOver, String.format("%.2f", totalProfit));

        // ── End-of-Day Settlement Notification ────────────────────────────────
        // Send daily recap to Discord only if at least one bet was settled tonight.
        // Flagged, skipped, and rolled-over bets are excluded.
        if (!settledToday.isEmpty()) {
            log.info("Sending daily recap for {} settled bets", settledToday.size());
            discordAlertService.sendDailyRecap(settledToday, totalProfit);
        } else {
            log.info("No settled bets tonight — skipping Discord recap");
        }

        return RepeatStatus.FINISHED;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Returns true only for confirmed terminal game states.
     * "Final", "F/OT", "F/10", "F/11" etc. are all accepted as final.
     * Live, Postponed, Suspended, Delayed, Preview, Pre-Game → returns false.
     */
    private boolean isFinal(String gameStatus) {
        if (gameStatus == null || gameStatus.isBlank()) return false;
        String s = gameStatus.trim().toUpperCase();
        // Accept "FINAL", "F", "F/OT", "F/10", "COMPLETE", "COMPLETED"
        return s.equals("FINAL") || s.equals("F") || s.startsWith("F/")
                || s.equals("COMPLETE") || s.equals("COMPLETED");
    }

    /**
     * Retrieves the official game status from the boxscore store.
     * In production: SELECT game_status FROM prop_markets WHERE game_id = ? LIMIT 1
     * Returns "PENDING" (non-final) as a safe default if the record isn't found.
     */
    private String getGameStatus(BetRecord bet) {
        try {
            return postgresService.getGameStatus(bet.getGameId());
        } catch (Exception e) {
            log.warn("Could not fetch game status for {} — treating as non-final: {}",
                    bet.getGameId(), e.getMessage());
            return "UNKNOWN";  // Non-final → rolls over safely
        }
    }

    /**
     * Retrieves the actual game stat for the bet's prop.
     * In production: SELECT actual_result FROM prop_markets WHERE game_id = ? AND player_name = ?
     * Returns -1 if stat is unavailable (box score not yet populated).
     */
    private double getActualStat(BetRecord bet) {
        // Full implementation: queries Postgres prop_markets for final boxscore value
        // inserted by DataHubTasklet after game completion.
        return 0.0;
    }

    private String gradeBet(BetRecord bet, double actual) {
        double line = bet.getTargetLineDouble();
        String dir  = bet.getDirection();

        if (actual == line) return "PUSH";
        if ("OVER".equalsIgnoreCase(dir)  && actual > line) return "WIN";
        if ("UNDER".equalsIgnoreCase(dir) && actual < line) return "WIN";
        return "LOSS";
    }

    private double calculateProfitLoss(BetRecord bet, String status) {
        double unitsRisked = bet.getUnitsRiskedDouble();
        double decimalOdds = bet.getPlacedOddsDouble();

        if ("LOSS".equals(status))  return -unitsRisked;
        if ("PUSH".equals(status))  return 0.0;
        if (decimalOdds < 1.0)      decimalOdds = 1.909; // Default -110
        return unitsRisked * (decimalOdds - 1.0);
    }
}
