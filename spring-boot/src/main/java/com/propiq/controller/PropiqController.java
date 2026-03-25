package com.propiq.controller;

import com.propiq.model.AnalyzerCacheItem;
import com.propiq.model.MlbHubState;
import com.propiq.service.RedisCacheManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * PropIQ Internal REST API — Headless monitoring only.
 *
 * These endpoints are for ops visibility — NOT for a web UI.
 * Read from Redis pre-computed cache for sub-100ms responses.
 *
 * Endpoints:
 *   GET /api/v1/hub            — Full mlb_hub snapshot (debug)
 *   GET /api/v1/analyzer       — All pre-computed EV picks
 *   GET /api/v1/analyzer/{key} — Single prop EV  (e.g., "Aaron Judge_Hits")
 *   GET /api/v1/leaderboard    — Agent ROI rankings
 *   GET /api/v1/health         — Redis+Kafka connectivity check
 */
@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class PropiqController {

    private final RedisCacheManager redisCache;

    @GetMapping("/hub")
    public ResponseEntity<MlbHubState> getHub() {
        MlbHubState hub = redisCache.get("mlb_hub", MlbHubState.class);
        if (hub == null) return ResponseEntity.noContent().build();
        return ResponseEntity.ok(hub);
    }

    @GetMapping("/analyzer")
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, AnalyzerCacheItem>> getAnalyzerCache() {
        Map<String, AnalyzerCacheItem> cache = redisCache.get("bet_analyzer_cache", Map.class);
        if (cache == null || cache.isEmpty()) return ResponseEntity.noContent().build();
        return ResponseEntity.ok(cache);
    }

    @GetMapping("/analyzer/{playerProp}")
    @SuppressWarnings("unchecked")
    public ResponseEntity<AnalyzerCacheItem> getAnalyzerItem(@PathVariable String playerProp) {
        Map<String, AnalyzerCacheItem> cache = redisCache.get("bet_analyzer_cache", Map.class);
        if (cache == null) return ResponseEntity.noContent().build();
        AnalyzerCacheItem item = cache.get(playerProp);
        return item != null ? ResponseEntity.ok(item) : ResponseEntity.notFound().build();
    }

    @GetMapping("/leaderboard")
    public ResponseEntity<Map<String, Double>> getLeaderboard() {
        @SuppressWarnings("unchecked")
        Map<String, Double> weights = redisCache.get("agent_capital_weights", Map.class);
        return weights != null ? ResponseEntity.ok(weights) : ResponseEntity.noContent().build();
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        boolean redisOk = redisCache.ping();
        return ResponseEntity.ok(Map.of(
                "status",    redisOk ? "UP" : "DEGRADED",
                "redis",     redisOk ? "OK" : "FAIL",
                "version",   "propiq-spring-boot-1.0"
        ));
    }
}
