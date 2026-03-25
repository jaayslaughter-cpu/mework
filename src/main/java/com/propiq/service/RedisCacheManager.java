package com.propiq.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.propiq.model.Bet;
import com.propiq.model.ActiveSlip;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Central Redis abstraction.
 * All tasklets read/write through this class — never touch RedisTemplate directly.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisCacheManager {

    private static final String AGENT_WEIGHT_PREFIX = "agent_weight:";
    private static final String PENDING_BETS_KEY    = "pending_bets";
    private static final String ACTIVE_SLIPS_KEY    = "active_dfs_slips";
    private static final String FEATURE_SET_KEY     = "active_features";
    private static final String FEATURE_BACKUP_KEY  = "active_features_backup";

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper redisObjectMapper;

    // ── TtlBucket — maps semantic bucket names to concrete TTL seconds ────────

    /**
     * TTL buckets for DataHubTasklet group-based caching.
     *
     * <ul>
     *   <li>{@code PHYSICS}  — pitch arsenal, advanced stats (30 min stable)</li>
     *   <li>{@code CONTEXT}  — weather, lineups, injuries (10 min)</li>
     *   <li>{@code MARKET}   — odds, public betting, sharp steam (7 min)</li>
     *   <li>{@code DFS}      — DFS picks, prop lines (5 min — fastest cadence)</li>
     *   <li>{@code GRADING}  — graded slip history (24 h)</li>
     * </ul>
     */
    public enum TtlBucket {
        PHYSICS(1800),
        CONTEXT(600),
        MARKET(420),
        DFS(300),
        GRADING(86400);

        private final long seconds;

        TtlBucket(long seconds) { this.seconds = seconds; }

        public long getSeconds() { return seconds; }
    }

    // ── Generic get/set ───────────────────────────────────────────────────────

    public <T> void setWithTTL(String key, T value, long ttlSeconds) {
        try {
            redisTemplate.opsForValue().set(key, value, ttlSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Redis SET failed for key={}: {}", key, e.getMessage());
        }
    }

    /**
     * Store {@code value} with the TTL defined by {@code bucket}.
     * Called by DataHubTasklet — each scraper group has its own bucket.
     *
     * @param key    Redis key
     * @param value  Serializable value
     * @param bucket TtlBucket enum constant defining the TTL
     */
    public void set(String key, Object value, TtlBucket bucket) {
        setWithTTL(key, value, bucket.getSeconds());
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> type) {
        try {
            Object val = redisTemplate.opsForValue().get(key);
            if (val == null) return null;
            if (type.isInstance(val)) return type.cast(val);
            // Re-serialize if Jackson returned LinkedHashMap
            return redisObjectMapper.convertValue(val, type);
        } catch (Exception e) {
            log.error("Redis GET failed for key={}: {}", key, e.getMessage());
            return null;
        }
    }

    // ── Agent capital weights ─────────────────────────────────────────────────

    public void updateAgentCapitalWeight(String agentName, double multiplier) {
        redisTemplate.opsForValue().set(AGENT_WEIGHT_PREFIX + agentName, multiplier);
        log.debug("Capital weight updated: {} → {}x", agentName, multiplier);
    }

    public double getAgentCapitalWeight(String agentName) {
        Object val = redisTemplate.opsForValue().get(AGENT_WEIGHT_PREFIX + agentName);
        if (val == null) return 1.0;
        return val instanceof Number ? ((Number) val).doubleValue() : 1.0;
    }

    // ── Pending bets (abort detection) ───────────────────────────────────────

    public void addPendingBet(Bet bet) {
        redisTemplate.opsForHash().put(PENDING_BETS_KEY, bet.getBetId(), bet);
    }

    @SuppressWarnings("unchecked")
    public List<Bet> getPendingBets() {
        try {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(PENDING_BETS_KEY);
            List<Bet> bets = new ArrayList<>();
            for (Object v : entries.values()) {
                bets.add(redisObjectMapper.convertValue(v, Bet.class));
            }
            return bets;
        } catch (Exception e) {
            log.error("Failed to get pending bets: {}", e.getMessage());
            return List.of();
        }
    }

    public void removePendingBet(String betId) {
        redisTemplate.opsForHash().delete(PENDING_BETS_KEY, betId);
    }

    // ── Active DFS slips ─────────────────────────────────────────────────────

    public void addActiveDfsSlip(ActiveSlip slip) {
        redisTemplate.opsForHash().put(ACTIVE_SLIPS_KEY, slip.getSlipId(), slip);
    }

    @SuppressWarnings("unchecked")
    public List<ActiveSlip> getActiveDfsSlips() {
        try {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(ACTIVE_SLIPS_KEY);
            List<ActiveSlip> slips = new ArrayList<>();
            for (Object v : entries.values()) {
                slips.add(redisObjectMapper.convertValue(v, ActiveSlip.class));
            }
            return slips;
        } catch (Exception e) {
            log.error("Failed to get active DFS slips: {}", e.getMessage());
            return List.of();
        }
    }

    // ── XGBoost feature management ────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    public List<String> getActiveFeatures() {
        Object val = redisTemplate.opsForValue().get(FEATURE_SET_KEY);
        if (val == null) return getDefaultFeatures();
        return redisObjectMapper.convertValue(val, List.class);
    }

    public void updateActiveFeatures(List<String> features) {
        // Backup current before overwriting
        Object current = redisTemplate.opsForValue().get(FEATURE_SET_KEY);
        if (current != null) {
            redisTemplate.opsForValue().set(FEATURE_BACKUP_KEY, current);
        }
        redisTemplate.opsForValue().set(FEATURE_SET_KEY, features);
        log.info("Active XGBoost features updated: {} features", features.size());
    }

    public void rollbackFeatureSet() {
        Object backup = redisTemplate.opsForValue().get(FEATURE_BACKUP_KEY);
        if (backup != null) {
            redisTemplate.opsForValue().set(FEATURE_SET_KEY, backup);
            log.warn("⚠️ Feature set rolled back to last known good state.");
        }
    }

    // ── Analyzer cache helper ─────────────────────────────────────────────────

    public void updateAnalyzerCache(Object masterState) {
        setWithTTL("bet_analyzer_cache_meta", masterState, 10);
    }

    private List<String> getDefaultFeatures() {
        return List.of(
            "pitcher_fip", "pitcher_swstr_pct", "batter_xwoba", "implied_odds",
            "umpire_called_strike_pct", "wind_speed_outfield", "bullpen_fatigue_score",
            "public_bet_pct", "days_since_injury", "rest_days", "bvp_xwoba"
        );
    }
}
