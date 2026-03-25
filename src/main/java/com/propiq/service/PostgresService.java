package com.propiq.service;

import com.propiq.model.BetRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All direct database interactions for PropIQ.
 * Uses JdbcTemplate for performance-critical queries and type safety.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PostgresService {

    private final JdbcTemplate jdbcTemplate;

    // ── Bet ledger ────────────────────────────────────────────────────────────

    @Transactional
    public void saveBet(BetRecord bet) {
        jdbcTemplate.update("""
            INSERT INTO bet_ledger
                (bet_id, agent_name, direction, player_name, prop_type, game_id,
                 target_line, units_risked, kelly_fraction, placed_odds,
                 placed_no_vig_prob, xgboost_prob, ev_pct, status, placed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
            ON CONFLICT (bet_id) DO NOTHING
            """,
                bet.getId(), bet.getAgentName(), bet.getDirection(),
                bet.getPlayerName(), bet.getPropType(), bet.getGameId(),
                bet.getTargetLine(), bet.getUnitsRisked(), bet.getKellyFraction(),
                bet.getPlacedOdds(), bet.getPlacedNoVigProb(), bet.getXgboostProb(),
                bet.getEvPct(), bet.getPlacedAt()
        );
    }

    /**
     * Settle a bet and calculate CLV (Closing Line Value).
     */
    @Transactional
    public void settleBet(String betId, double actualStat, String status, double profitLoss) {
        jdbcTemplate.update("""
            UPDATE bet_ledger
            SET status = ?, profit_loss = ?, settled_at = ?
            WHERE bet_id = ?
            """,
                status, BigDecimal.valueOf(profitLoss), Instant.now(), betId
        );

        // Update agent stats
        String agentName = jdbcTemplate.queryForObject(
                "SELECT agent_name FROM bet_ledger WHERE bet_id = ?", String.class, betId);
        if (agentName != null) {
            updateAgentStats(agentName, profitLoss, "WIN".equals(status));
        }
    }

    public void markBetForReview(String betId) {
        jdbcTemplate.update(
                "INSERT INTO stat_correction_flags (bet_id) VALUES (?) ON CONFLICT DO NOTHING",
                betId
        );
    }

    // ── Pending bets (for GradingTasklet) ────────────────────────────────────

    public List<BetRecord> getUnsettledBetsByDate(String dateStr) {
        LocalDate date = "YESTERDAY".equals(dateStr)
                ? LocalDate.now().minusDays(1)
                : LocalDate.parse(dateStr);

        return jdbcTemplate.query("""
            SELECT bet_id, agent_name, direction, player_name, prop_type,
                   game_id, target_line, units_risked, placed_odds, status, placed_at
            FROM bet_ledger
            WHERE status = 'PENDING'
              AND DATE(placed_at) = ?
            """,
                (rs, row) -> {
                    BetRecord r = new BetRecord();
                    r.setId(rs.getString("bet_id"));
                    r.setAgentName(rs.getString("agent_name"));
                    r.setDirection(rs.getString("direction"));
                    r.setPlayerName(rs.getString("player_name"));
                    r.setPropType(rs.getString("prop_type"));
                    r.setGameId(rs.getString("game_id"));
                    r.setTargetLine(rs.getBigDecimal("target_line"));
                    r.setUnitsRisked(rs.getBigDecimal("units_risked"));
                    r.setPlacedOdds(rs.getBigDecimal("placed_odds"));
                    r.setStatus(rs.getString("status"));
                    return r;
                },
                date
        );
    }

    // ── Agent performance (for LeaderboardTasklet) ────────────────────────────

    public Map<String, Double> getAgentRoisTrailing14Days() {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
            SELECT agent_name,
                   SUM(profit_loss) / NULLIF(SUM(units_risked), 0) * 100 AS roi_pct
            FROM bet_ledger
            WHERE status IN ('WIN', 'LOSS')
              AND settled_at >= NOW() - INTERVAL '14 days'
            GROUP BY agent_name
            HAVING COUNT(*) >= 5
            """);

        Map<String, Double> result = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String agent = (String) row.get("agent_name");
            Object roi   = row.get("roi_pct");
            result.put(agent, roi != null ? ((Number) roi).doubleValue() : 0.0);
        }
        return result;
    }

    private void updateAgentStats(String agentName, double profitLoss, boolean isWin) {
        jdbcTemplate.update("""
            INSERT INTO agent_stats (agent_name, total_bets, total_profit, updated_at)
            VALUES (?, 1, ?, NOW())
            ON CONFLICT (agent_name) DO UPDATE SET
                total_bets   = agent_stats.total_bets + 1,
                total_profit = agent_stats.total_profit + EXCLUDED.total_profit,
                roi_pct      = (agent_stats.total_profit + EXCLUDED.total_profit)
                               / NULLIF(agent_stats.total_bets + 1, 0) * 100,
                win_pct      = (SELECT COUNT(*) FILTER (WHERE status = 'WIN')::numeric
                               / NULLIF(COUNT(*), 0) * 100
                               FROM bet_ledger WHERE agent_name = ?),
                updated_at   = NOW()
            """,
                agentName, BigDecimal.valueOf(profitLoss), agentName
        );
    }

    // ── XGBoost data export ───────────────────────────────────────────────────

    /**
     * Exports settled bets to a CSV file for Python ML audit.
     * Returns the file path so BacktestTasklet can pass it to the ML service.
     */
    public String exportRecentSettledDataToCSV() {
        String csvPath = "/tmp/propiq_backtest_" + LocalDate.now() + ".csv";

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
            SELECT b.bet_id, b.agent_name, b.direction, b.player_name, b.prop_type,
                   b.placed_no_vig_prob, b.xgboost_prob, b.ev_pct, b.units_risked,
                   b.profit_loss, b.status,
                   CASE WHEN b.status = 'WIN' THEN 1 ELSE 0 END AS prop_hit_actual
            FROM bet_ledger b
            WHERE b.status IN ('WIN', 'LOSS')
              AND b.settled_at >= NOW() - INTERVAL '180 days'
            ORDER BY b.settled_at
            """);

        try (java.io.FileWriter fw = new java.io.FileWriter(csvPath);
             org.apache.commons.csv.CSVPrinter csv = new org.apache.commons.csv.CSVPrinter(
                     fw, org.apache.commons.csv.CSVFormat.DEFAULT.withHeader(
                             "bet_id", "agent_name", "direction", "player_name", "prop_type",
                             "placed_no_vig_prob", "xgboost_prob", "ev_pct", "units_risked",
                             "profit_loss", "status", "prop_hit_actual"))) {
            for (Map<String, Object> row : rows) {
                csv.printRecord(row.values());
            }
        } catch (Exception e) {
            log.error("CSV export failed: {}", e.getMessage());
        }

        log.info("Exported {} records to {}", rows.size(), csvPath);
        return csvPath;
    }

    // ── Backtest support (BacktestTasklet) ───────────────────────────────────

    /**
     * Returns settled bets within the lookback window as a list of feature maps.
     * Each map includes: agent_name, placed_no_vig_prob, xgboost_prob, ev_pct,
     * units_risked, profit_loss, status, prop_hit_actual.
     */
    public List<Map<String, Object>> getSettledBetsForBacktest(int lookbackDays) {
        return jdbcTemplate.queryForList("""
            SELECT agent_name,
                   placed_no_vig_prob,
                   xgboost_prob,
                   ev_pct,
                   units_risked,
                   profit_loss,
                   status,
                   CASE WHEN status = 'WIN' THEN 1 ELSE 0 END AS prop_hit_actual
            FROM bet_ledger
            WHERE status IN ('WIN', 'LOSS')
              AND settled_at >= NOW() - (? || ' days')::INTERVAL
            ORDER BY settled_at
            """,
                lookbackDays
        );
    }

    /**
     * Persists dropped feature names to the model_dropped_features table so
     * XGBoostTasklet can exclude them on the next retrain cycle.
     */
    @Transactional
    public void updateDroppedFeatures(List<String> droppedFeatures) {
        // Clear existing drops and replace with latest audit result
        jdbcTemplate.update("DELETE FROM model_dropped_features WHERE audit_date = CURRENT_DATE");
        for (String feature : droppedFeatures) {
            jdbcTemplate.update("""
                INSERT INTO model_dropped_features (feature_name, audit_date, dropped_at)
                VALUES (?, CURRENT_DATE, NOW())
                ON CONFLICT (feature_name) DO UPDATE SET
                    audit_date = EXCLUDED.audit_date,
                    dropped_at = EXCLUDED.dropped_at
                """,
                    feature
            );
        }
        log.info("[PostgresService] Updated {} dropped features.", droppedFeatures.size());
    }

    /**
     * Saves a complete backtest run summary for historical tracking.
     */
    @Transactional
    public void saveBacktestRun(
            double overallAccuracy,
            int sampleSize,
            List<String> droppedFeatures,
            Map<String, Double> featureAccuracies) {
        jdbcTemplate.update("""
            INSERT INTO backtest_runs
                (run_date, overall_accuracy, sample_size, dropped_features,
                 feature_accuracy_json, created_at)
            VALUES (CURRENT_DATE, ?, ?, ?::text[], ?::jsonb, NOW())
            ON CONFLICT (run_date) DO UPDATE SET
                overall_accuracy     = EXCLUDED.overall_accuracy,
                sample_size          = EXCLUDED.sample_size,
                dropped_features     = EXCLUDED.dropped_features,
                feature_accuracy_json = EXCLUDED.feature_accuracy_json,
                created_at           = EXCLUDED.created_at
            """,
                overallAccuracy,
                sampleSize,
                droppedFeatures.toArray(new String[0]),
                featureAccuracies.toString()   // simple JSON-ish representation
        );
        log.info("[PostgresService] Backtest run saved. accuracy={:.2f}% sample={}",
                 overallAccuracy * 100, sampleSize);
    }

    // ── Audit logging ─────────────────────────────────────────────────────────

    public void logAuditSuccess(double accuracy, List<String> validFeatures) {
        jdbcTemplate.update("""
            INSERT INTO model_audit_log (run_date, holdout_accuracy, valid_features, passed)
            VALUES (CURRENT_DATE, ?, ?, TRUE)
            """, accuracy, validFeatures.toString());
    }

    public void logAuditFailure(double accuracy, List<String> droppedFeatures) {
        jdbcTemplate.update("""
            INSERT INTO model_audit_log (run_date, holdout_accuracy, dropped_features, passed)
            VALUES (CURRENT_DATE, ?, ?, FALSE)
            """, accuracy, droppedFeatures != null ? droppedFeatures.toString() : "[]");
    }
}
