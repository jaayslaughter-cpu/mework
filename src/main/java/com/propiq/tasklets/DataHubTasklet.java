package com.propiq.tasklets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.propiq.model.MlbHubState;
import com.propiq.service.ApifyScraperService;
import com.propiq.service.RedisCacheManager;
import com.propiq.service.SportsDataService;
import com.propiq.service.TankApiService;
import com.propiq.service.TheOddsApiService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DataHubTasklet — Phase 1 of the PropIQ pipeline.
 *
 * CRITICAL RULES (DO NOT DEVIATE):
 * 1. PRE-MATCH ONLY — if a game is LIVE or FINAL, skip all scraping for that game.
 * 2. STAGGERED POLLING — different scraper groups fire on different intervals.
 * 3. QUOTA PROTECTION — The Odds API: max once per 7 minutes. Tank01: max once per 5 minutes.
 *
 * Scraper Group Schedule:
 *   Group A — Physics/Arsenal         → every 30 min  (stable pitching data)
 *   Group B — Context/Environment     → every 10 min  (weather, lineups, injuries)
 *   Group C — Market/Sharp Steam      → every 7  min  (Odds API quota guard)
 *   Group D — DFS Targets             → every 5  min  (PrizePicks/Underdog/Sleeper lines)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataHubTasklet implements Tasklet {

    // ─── Services ───────────────────────────────────────────────────────────
    private final ApifyScraperService apifyScraper;
    private final RedisCacheManager   redisCache;
    private final SportsDataService   sportsDataService;
    private final TankApiService      tankApiService;
    private final TheOddsApiService   theOddsApiService;
    private final ObjectMapper        objectMapper;

    // ─── Config ─────────────────────────────────────────────────────────────
    @Value("${propiq.polling.physics-interval-minutes:30}")
    private int physicsIntervalMinutes;

    @Value("${propiq.polling.context-interval-minutes:10}")
    private int contextIntervalMinutes;

    @Value("${propiq.polling.market-interval-minutes:7}")
    private int marketIntervalMinutes;

    @Value("${propiq.polling.dfs-interval-minutes:5}")
    private int dfsIntervalMinutes;

    @Value("${propiq.spring-training:false}")
    private boolean springTrainingMode;

    // ─── Cycle counters for staggered scheduling ────────────────────────────
    // Base tick = 5 minutes. Each group fires when (tick % N == 0).
    private final AtomicInteger tickCounter = new AtomicInteger(0);

    // Track last-run timestamps per group for safety double-check
    private final Map<String, Instant> lastRunMap = new ConcurrentHashMap<>();

    // ─── URLs (all sourced from master blueprint, DO NOT change) ────────────
    // Group A — Physics / Arsenal
    private static final List<String> PHYSICS_URLS = List.of(
        "https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats",
        "https://www.rotowire.com/baseball/stats-advanced.php",
        "https://www.rotowire.com/baseball/stats-bvp.php",
        "https://www.rotowire.com/baseball/stats-batted-ball.php",
        "https://www.rotowire.com/baseball/stats-second-half.php"
    );

    // Group B — Context / Environment
    private static final List<String> CONTEXT_URLS = List.of(
        "https://www.rotowire.com/baseball/weather.php",
        "https://www.rotowire.com/baseball/umpire-stats-daily.php",
        "https://www.rotowire.com/baseball/news.php?injuries=all",
        "https://www.rotowire.com/baseball/batting-orders.php",
        "https://www.rotowire.com/baseball/projected-starters.php"
    );

    // Group C — Market / Sharp Steam
    private static final List<String> MARKET_URLS = List.of(
        "https://www.actionnetwork.com/mlb/public-betting",
        "https://www.actionnetwork.com/mlb/sharp-report",
        "https://www.actionnetwork.com/mlb/prop-projections",
        "https://www.actionnetwork.com/mlb/odds"
    );

    // Group D — DFS Targets (PrizePicks, Underdog, Sleeper, DraftKings + prop lines)
    private static final List<String> DFS_URLS = List.of(
        "https://www.rotowire.com/picks/underdog/",
        "https://www.rotowire.com/picks/prizepicks/",
        "https://www.rotowire.com/picks/sleeper/",
        "https://www.rotowire.com/picks/draftkings/",
        "https://www.rotowire.com/daily/mlb/optimizer.php",
        // Prop lines (retail DK odds per player, K/ER/TB/Runs) — 5 min TTL
        "https://www.rotowire.com/betting/mlb/player-props.php",
        // Prop projections (RW projection vs market line — edge signal) — 5 min TTL
        "https://www.rotowire.com/betting/mlb/player-props-plus-proj.php"
    );

    // ─── Scheduler entry point (every 5 minutes base tick) ──────────────────
    @Scheduled(fixedDelayString = "${propiq.polling.base-tick-ms:300000}")
    public void scheduledPoll() {
        int tick = tickCounter.incrementAndGet();
        log.info("[DataHub] Tick #{} fired at {}", tick, ZonedDateTime.now(ZoneId.of("America/Los_Angeles")));

        // Fetch today's pre-match game list — gate everything against this
        Set<String> preMatchGameIds = fetchPreMatchGameIds();

        if (preMatchGameIds.isEmpty() && !springTrainingMode) {
            log.info("[DataHub] No pre-match games found. Skipping all scraper groups.");
            return;
        }

        // Group D — DFS Targets — every tick (every 5 min)
        runGroupD(preMatchGameIds);

        // Group C — Market/Sharp Steam — every 7 min (tick % ~1.4 → use timestamp gate)
        if (shouldRun("MARKET", marketIntervalMinutes)) {
            runGroupC(preMatchGameIds);
        }

        // Group B — Context/Environment — every 10 min (tick % 2)
        if (shouldRun("CONTEXT", contextIntervalMinutes)) {
            runGroupB(preMatchGameIds);
        }

        // Group A — Physics/Arsenal — every 30 min (tick % 6)
        if (shouldRun("PHYSICS", physicsIntervalMinutes)) {
            runGroupA(preMatchGameIds);
        }

        // Always refresh fast APIs on every tick (5 min cadence respects quotas)
        runFastApis(preMatchGameIds);

        log.info("[DataHub] Tick #{} complete. Hub state pushed to Redis.", tick);
    }

    // ─── Spring Batch execute() for on-demand job runs ──────────────────────
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        log.info("[DataHub] Manual job execution triggered.");
        scheduledPoll();
        return RepeatStatus.FINISHED;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // PRE-MATCH GATE — CRITICAL LOGIC
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Fetches today's games from SportsData.io and returns ONLY game IDs
     * with status SCHEDULED or WARMUP. LIVE and FINAL games are excluded.
     * This is the master gate — no scraping happens for in-progress games.
     */
    private Set<String> fetchPreMatchGameIds() {
        try {
            List<Map<String, Object>> todayGames = sportsDataService.getTodayGames();
            Set<String> preMatchIds = new HashSet<>();

            for (Map<String, Object> game : todayGames) {
                String status = String.valueOf(game.getOrDefault("Status", ""));
                String gameId = String.valueOf(game.getOrDefault("GameID", ""));

                // PRE-MATCH GATE: Only allow SCHEDULED or WARMUP games
                if ("Scheduled".equalsIgnoreCase(status) || "Warmup".equalsIgnoreCase(status)) {
                    preMatchIds.add(gameId);
                    log.debug("[DataHub] Pre-match game admitted: {} ({})", gameId, status);
                } else {
                    log.debug("[DataHub] PRE-MATCH GATE BLOCKED: gameId={} status={} — no polling.", gameId, status);
                }
            }

            // Cache the pre-match game set in Redis for other tasklets to reference
            redisCache.set("mlb_hub:pre_match_game_ids", preMatchIds,
                           com.propiq.service.RedisCacheManager.TtlBucket.CONTEXT);

            log.info("[DataHub] Pre-match gate: {}/{} games admitted.", preMatchIds.size(), todayGames.size());
            return preMatchIds;

        } catch (Exception e) {
            log.error("[DataHub] Failed to fetch game list from SportsData.io: {}", e.getMessage());
            // Fail safe: return empty set — no scraping if we can't verify game status
            return Collections.emptySet();
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // GROUP A — PHYSICS / ARSENAL (every 30 min)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    private void runGroupA(Set<String> preMatchGameIds) {
        log.info("[DataHub] Group A — Physics/Arsenal scrape starting.");
        Map<String, Object> physicsData = new LinkedHashMap<>();

        for (String url : PHYSICS_URLS) {
            try {
                Map<String, Object> result = apifyScraper.scrape(url);
                // Filter result to only include props for pre-match games
                Map<String, Object> filtered = filterByPreMatch(result, preMatchGameIds);
                physicsData.put(urlKey(url), filtered);
                log.info("[DataHub] Group A scraped: {}", urlKey(url));
            } catch (Exception e) {
                log.warn("[DataHub] Group A scrape failed for {}: {}", url, e.getMessage());
            }
        }

        redisCache.set("mlb_hub:physics", physicsData,
                       com.propiq.service.RedisCacheManager.TtlBucket.PHYSICS);
        lastRunMap.put("PHYSICS", Instant.now());
        log.info("[DataHub] Group A complete — {} sources cached.", physicsData.size());
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // GROUP B — CONTEXT / ENVIRONMENT (every 10 min)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    private void runGroupB(Set<String> preMatchGameIds) {
        log.info("[DataHub] Group B — Context/Environment scrape starting.");
        Map<String, Object> contextData = new LinkedHashMap<>();

        for (String url : CONTEXT_URLS) {
            try {
                Map<String, Object> result = apifyScraper.scrape(url);
                Map<String, Object> filtered = filterByPreMatch(result, preMatchGameIds);
                contextData.put(urlKey(url), filtered);

                // Special case: injury scrape triggers late scratch detection
                if (url.contains("injuries")) {
                    detectLateScratches(filtered);
                }

                log.info("[DataHub] Group B scraped: {}", urlKey(url));
            } catch (Exception e) {
                log.warn("[DataHub] Group B scrape failed for {}: {}", url, e.getMessage());
            }
        }

        redisCache.set("mlb_hub:context", contextData,
                       com.propiq.service.RedisCacheManager.TtlBucket.CONTEXT);
        lastRunMap.put("CONTEXT", Instant.now());
        log.info("[DataHub] Group B complete — {} sources cached.", contextData.size());
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // GROUP C — MARKET / SHARP STEAM (every 7 min — Odds API quota guard)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    private void runGroupC(Set<String> preMatchGameIds) {
        log.info("[DataHub] Group C — Market/Sharp Steam scrape starting.");
        Map<String, Object> marketData = new LinkedHashMap<>();

        // Scrape Action Network (no quota concerns)
        for (String url : MARKET_URLS) {
            try {
                Map<String, Object> result = apifyScraper.scrape(url);
                Map<String, Object> filtered = filterByPreMatch(result, preMatchGameIds);
                marketData.put(urlKey(url), filtered);
                log.info("[DataHub] Group C scraped: {}", urlKey(url));
            } catch (Exception e) {
                log.warn("[DataHub] Group C scrape failed for {}: {}", url, e.getMessage());
            }
        }

        // The Odds API — QUOTA PROTECTED — only call if interval respected
        try {
            Map<String, Object> oddsData = theOddsApiService.getMlbOdds();
            Map<String, Object> filteredOdds = filterByPreMatch(oddsData, preMatchGameIds);
            marketData.put("the_odds_api", filteredOdds);
            log.info("[DataHub] The Odds API quota-safe call successful.");
        } catch (Exception e) {
            log.warn("[DataHub] The Odds API call failed (quota?): {}", e.getMessage());
        }

        redisCache.set("mlb_hub:market", marketData,
                       com.propiq.service.RedisCacheManager.TtlBucket.MARKET);
        lastRunMap.put("MARKET", Instant.now());
        log.info("[DataHub] Group C complete — {} sources cached.", marketData.size());
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // GROUP D — DFS TARGETS (every 5 min — highest priority)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    private void runGroupD(Set<String> preMatchGameIds) {
        log.info("[DataHub] Group D — DFS Targets scrape starting.");
        Map<String, Object> dfsData = new LinkedHashMap<>();

        for (String url : DFS_URLS) {
            try {
                Map<String, Object> result = apifyScraper.scrape(url);
                Map<String, Object> filtered = filterByPreMatch(result, preMatchGameIds);
                dfsData.put(urlKey(url), filtered);
                log.info("[DataHub] Group D scraped: {}", urlKey(url));
            } catch (Exception e) {
                log.warn("[DataHub] Group D scrape failed for {}: {}", url, e.getMessage());
            }
        }

        redisCache.set("mlb_hub:dfs", dfsData,
                       com.propiq.service.RedisCacheManager.TtlBucket.DFS);
        lastRunMap.put("DFS", Instant.now());
        log.info("[DataHub] Group D complete — {} DFS sources cached.", dfsData.size());
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // FAST APIS — SportsData.io + Tank01 (every 5 min base tick)
    // Tank01 rate limit: 5-min minimum between calls (enforced here)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    private void runFastApis(Set<String> preMatchGameIds) {
        // SportsData.io — player props + projections
        try {
            Map<String, Object> sdProps = sportsDataService.getPlayerProjections();
            Map<String, Object> filtered = filterByPreMatch(sdProps, preMatchGameIds);
            redisCache.set("mlb_hub:sportsdata_projections", filtered,
                           com.propiq.service.RedisCacheManager.TtlBucket.CONTEXT);
            log.info("[DataHub] SportsData.io projections cached ({} entries).", filtered.size());
        } catch (Exception e) {
            log.warn("[DataHub] SportsData.io call failed: {}", e.getMessage());
        }

        // Tank01 — live lineups/starters (pre-match only, 5-min quota)
        if (shouldRun("TANK01", 5)) {
            try {
                Map<String, Object> tankData = tankApiService.getBoxScores();
                Map<String, Object> filtered = filterByPreMatch(tankData, preMatchGameIds);
                redisCache.set("mlb_hub:tank01_lineups", filtered,
                               com.propiq.service.RedisCacheManager.TtlBucket.CONTEXT);
                lastRunMap.put("TANK01", Instant.now());
                log.info("[DataHub] Tank01 lineups cached.");
            } catch (Exception e) {
                log.warn("[DataHub] Tank01 call failed: {}", e.getMessage());
            }
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // LATE SCRATCH DETECTION — publishes to Kafka abort_queue if lineup changes
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    @SuppressWarnings("unchecked")
    private void detectLateScratches(Map<String, Object> injuryData) {
        try {
            // Pull last known lineup from Redis
            Object cachedLineups = redisCache.get("mlb_hub:last_confirmed_lineups");
            if (cachedLineups == null) {
                // First run — just cache current state, nothing to compare
                redisCache.set("mlb_hub:last_confirmed_lineups", injuryData,
                               com.propiq.service.RedisCacheManager.TtlBucket.DFS);
                return;
            }

            Map<String, Object> lastLineups = (Map<String, Object>) cachedLineups;
            List<String> scratched = new ArrayList<>();

            // Compare player statuses — flag any new OUT/Doubtful/Scratch entries
            for (Map.Entry<String, Object> entry : injuryData.entrySet()) {
                String playerId = entry.getKey();
                String currentStatus = String.valueOf(entry.getValue());
                String previousStatus = String.valueOf(lastLineups.getOrDefault(playerId, "Active"));

                if (!currentStatus.equals(previousStatus) &&
                    (currentStatus.contains("Out") || currentStatus.contains("Scratch") ||
                     currentStatus.contains("Doubtful"))) {
                    scratched.add(playerId);
                    log.warn("[DataHub] ⚠️ LATE SCRATCH DETECTED: player={} was={} now={}",
                             playerId, previousStatus, currentStatus);
                }
            }

            if (!scratched.isEmpty()) {
                // Push abort event to Kafka — TelegramAlertService will fire notification
                Map<String, Object> abortEvent = Map.of(
                    "event",      "LATE_SCRATCH",
                    "players",    scratched,
                    "timestamp",  Instant.now().toString(),
                    "source",     "DataHubTasklet"
                );
                redisCache.set("mlb_hub:abort_events", abortEvent,
                               com.propiq.service.RedisCacheManager.TtlBucket.DFS);
                log.warn("[DataHub] Abort event queued for {} scratched player(s).", scratched.size());
            }

            // Update cached lineups
            redisCache.set("mlb_hub:last_confirmed_lineups", injuryData,
                           com.propiq.service.RedisCacheManager.TtlBucket.DFS);

        } catch (Exception e) {
            log.error("[DataHub] Late scratch detection error: {}", e.getMessage());
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // UTILITIES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Timestamp-based interval gate. Returns true if the group hasn't run
     * within the last N minutes.
     */
    private boolean shouldRun(String groupKey, int intervalMinutes) {
        Instant last = lastRunMap.get(groupKey);
        if (last == null) return true;
        long elapsedMinutes = (Instant.now().toEpochMilli() - last.toEpochMilli()) / 60_000;
        return elapsedMinutes >= intervalMinutes;
    }

    /**
     * Filters a scraped data map to only include entries that correspond
     * to pre-match games. Keys that don't match any game ID pass through
     * (e.g., season-level stats like pitcher arsenals).
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> filterByPreMatch(Map<String, Object> raw, Set<String> preMatchIds) {
        if (preMatchIds.isEmpty()) return raw; // Spring training — no filter
        Map<String, Object> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            String key = entry.getKey();
            // Pass through if no game ID association or if game is pre-match
            boolean hasGameId = preMatchIds.stream().anyMatch(id -> key.contains(id));
            boolean isSeasonLevel = !key.matches(".*\\d{6,}.*"); // no long numeric game ID in key
            if (hasGameId || isSeasonLevel) {
                filtered.put(key, entry.getValue());
            }
        }
        return filtered;
    }

    /** Converts a URL to a short Redis-friendly key. */
    private String urlKey(String url) {
        return url.replaceAll("https?://(?:www\\.)?", "")
                  .replaceAll("[^a-zA-Z0-9]", "_")
                  .replaceAll("_+", "_")
                  .replaceAll("^_|_$", "")
                  .toLowerCase();
    }
}
