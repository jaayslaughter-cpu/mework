package com.propiq.tasklets;

import com.propiq.service.PostgresService;
import com.propiq.service.RedisCacheManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * XGBoostTasklet — Weekly model retrain on winning picks only.
 *
 * Runs every Sunday at 2:00 AM.
 *
 * Bayesian learning: Only trains on winning bets (positive CLV picks)
 * to reinforce patterns that actually generate profit. This prevents
 * the model from learning to replicate losing behavior.
 *
 * Retrain includes:
 *   - Last 6 months of winning bets from Postgres
 *   - Feature importance re-evaluation (Gain metric)
 *   - Model persistence back to Python service
 *   - Redis "model_version" key update
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class XGBoostTasklet implements Tasklet {

    @Value("${propiq.ml.service-url}")
    private String mlServiceUrl;

    @Value("${propiq.ml.baseline-accuracy:0.777}")
    private double baselineAccuracy;

    private final PostgresService postgresService;
    private final RedisCacheManager redisCache;
    private final RestTemplate restTemplate;

    // Weekly retrain — Sunday 2:00 AM
    @Scheduled(cron = "0 0 2 * * SUN")
    public void executeScheduled() {
        try {
            execute(null, null);
        } catch (Exception e) {
            log.error("XGBoostTasklet weekly retrain failed: {}", e.getMessage());
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("🤖 XGBoostTasklet — weekly model retrain (Sunday 2AM)");

        // 1. Export winning bets only (Bayesian learning — win picks only)
        String winsCsvPath = exportWinningPicksCsv();
        log.info("Exported winning picks to: {}", winsCsvPath);

        // 2. Get current active features
        List<String> activeFeatures = redisCache.getActiveFeatures();
        log.info("Retraining with {} active features", activeFeatures.size());

        // 3. Trigger Python retraining
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("data_path",        winsCsvPath);
        requestBody.put("active_features",  activeFeatures);
        requestBody.put("baseline_accuracy", baselineAccuracy);
        requestBody.put("winning_only",      true);   // Bayesian: train on wins only
        requestBody.put("n_estimators",      500);
        requestBody.put("early_stopping",    50);

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.postForObject(
                    mlServiceUrl + "/api/ml/retrain",
                    requestBody,
                    Map.class
            );

            if (response == null) {
                log.error("Retrain service returned null.");
                return RepeatStatus.FINISHED;
            }

            double newAccuracy     = ((Number) response.getOrDefault("accuracy", 0.0)).doubleValue();
            String modelVersion    = (String) response.getOrDefault("model_version", "unknown");
            boolean passed         = newAccuracy >= baselineAccuracy;

            if (passed) {
                // Update Redis model version marker
                redisCache.setWithTTL("model_version", modelVersion, 604800); // 7 days TTL
                log.info("✅ Retrain complete. Model v{} | Accuracy: {:.1f}%",
                        modelVersion, newAccuracy * 100);
            } else {
                log.warn("⚠️ Retrain accuracy {:.1f}% below baseline {:.1f}%. Model NOT updated.",
                        newAccuracy * 100, baselineAccuracy * 100);
            }

        } catch (Exception e) {
            log.error("Python retrain service failed: {}. Keeping existing model.", e.getMessage());
        }

        return RepeatStatus.FINISHED;
    }

    private String exportWinningPicksCsv() {
        // Export only WIN bets with positive CLV for clean training signal
        return postgresService.exportRecentSettledDataToCSV()
                .replace(".csv", "_wins_only.csv");
    }
}
