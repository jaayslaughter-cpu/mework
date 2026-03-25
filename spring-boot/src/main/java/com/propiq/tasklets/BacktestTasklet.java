package com.propiq.tasklets;

import com.propiq.model.AuditResponse;
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
 * BacktestTasklet — Daily XGBoost feature importance audit.
 *
 * Runs daily at 12:01 AM.
 *
 * The self-healing immune system of PropIQ Analytics:
 *  1. Exports yesterday's settled data to CSV
 *  2. Sends to Python ML microservice for feature audit
 *  3. Checks accuracy against 77.7% baseline
 *  4. If passed → updates Redis feature set
 *  5. If failed → rolls back to last known good feature set
 *
 * Target: 77.7% → 84.2% accuracy through continuous learning.
 *
 * The 2% rule: Features with < 1.5% relative gain impact are dropped
 * automatically by the Python audit script.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BacktestTasklet implements Tasklet {

    private static final double BASELINE_ACCURACY = 0.777;
    private static final double TARGET_ACCURACY   = 0.842;

    @Value("${propiq.ml.service-url}")
    private String mlServiceUrl;

    private final PostgresService postgresService;
    private final RedisCacheManager redisCache;
    private final RestTemplate restTemplate;

    // Runs daily at 12:01 AM
    @Scheduled(cron = "0 1 0 * * ?")
    public void executeScheduled() {
        try {
            execute(null, null);
        } catch (Exception e) {
            log.error("BacktestTasklet failed: {}", e.getMessage());
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("🛡️ BacktestTasklet — daily XGBoost feature audit");

        // 1. Export settled data to CSV for Python
        String csvPath = postgresService.exportRecentSettledDataToCSV();
        log.info("Exported backtest data to: {}", csvPath);

        // 2. Get active features from Redis
        List<String> activeFeatures = redisCache.getActiveFeatures();

        // 3. Send to Python ML audit endpoint
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("dataFilePath",  csvPath);
        requestBody.put("targetCol",     "prop_hit_actual");
        requestBody.put("features",      activeFeatures);

        AuditResponse response;
        try {
            response = restTemplate.postForObject(
                    mlServiceUrl + "/api/ml/audit-features",
                    requestBody,
                    AuditResponse.class
            );
        } catch (Exception e) {
            log.error("ML audit service unreachable: {}. Skipping audit.", e.getMessage());
            return RepeatStatus.FINISHED;
        }

        if (response == null) {
            log.error("ML audit returned null response. Skipping.");
            return RepeatStatus.FINISHED;
        }

        double accuracy = response.getHoldoutAccuracy();
        log.info("📊 Out-of-sample holdout accuracy: {:.1f}%", accuracy * 100);

        // 4. Enforce accuracy gate
        if (accuracy < BASELINE_ACCURACY) {
            log.warn("⚠️ AUDIT FAILED: {:.1f}% < {:.1f}% baseline. Rolling back.",
                    accuracy * 100, BASELINE_ACCURACY * 100);
            redisCache.rollbackFeatureSet();
            postgresService.logAuditFailure(accuracy, response.getDroppedFeatures());
            log.warn("Dropped features: {}", response.getDroppedFeatures());
        } else {
            log.info("✅ AUDIT PASSED: {:.1f}% accuracy. Updating feature set.", accuracy * 100);
            redisCache.updateActiveFeatures(response.getValidFeatures());
            postgresService.logAuditSuccess(accuracy, response.getValidFeatures());

            if (accuracy >= TARGET_ACCURACY) {
                log.info("🚀 TARGET REACHED: Model hit {:.1f}%+ accuracy!", accuracy * 100);
            }
        }

        return RepeatStatus.FINISHED;
    }
}
