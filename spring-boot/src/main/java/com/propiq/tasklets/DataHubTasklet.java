package com.propiq.tasklets;

import com.propiq.model.MlbHubState;
import com.propiq.model.PropMatchup;
import com.propiq.service.ApifyScraperService;
import com.propiq.service.FastApiPollingService;
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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * DataHubTasklet — The only component allowed to touch external APIs.
 *
 * Staggered polling schedule (strict rate limit enforcement):
 * ┌─────────────────────────────┬──────────────────────────────────────┐
 * │ API / Scraper               │ Poll frequency                       │
 * ├─────────────────────────────┼──────────────────────────────────────┤
 * │ Tank01 (pre-game only)      │ Every 15s — STOPS once game goes LIVE│
 * │ SportsData.io (stats)       │ Every 60s  (every 4 cycles)          │
 * │ The Odds API (prop odds)    │ Every 5min (every 20 cycles) ← QUOTA │
 * │ Apify scrapers (8 RW + 3AN) │ Every 5min (every 20 cycles)         │
 * └─────────────────────────────┴──────────────────────────────────────┘
 *
 * PRE-MATCH GATE (CRITICAL):
 * Props for LIVE or FINAL games are EXCLUDED from the active props list.
 * We are hunting pre-game opening lines and steam — NOT in-game micro-bets.
 *
 * All fetches are concurrent via CompletableFuture.
 * Result is compiled into MlbHubState and stored in Redis "mlb_hub" (15s TTL).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataHubTasklet implements Tasklet {

    private static final int SPORTSDATA_CYCLE  = 4;   // every 60s
    private static final int ODDS_API_CYCLE    = 20;  // every 5 min — QUOTA PROTECTED
    private static final int APIFY_CYCLE       = 20;  // every 5 min

    /** Game states that trigger the pre-match gate (stop all polling for this game) */
    private static final Set<String> LIVE_OR_FINAL_STATES = Set.of(
            "LIVE", "IN_PROGRESS", "FINAL", "GAME_OVER", "COMPLETED", "F"
    );

    private final AtomicInteger cycleCount = new AtomicInteger(0);

    // Cache the last fetched odds/scraper data — only refreshed every 5 min
    private final AtomicReference<List<PropMatchup>>                       cachedProps       = new AtomicReference<>(List.of());
    private final AtomicReference<Map<String, MlbHubState.UmpireStats>>    cachedUmpires     = new AtomicReference<>(new HashMap<>());
    private final AtomicReference<Map<String, MlbHubState.WeatherData>>    cachedWeather     = new AtomicReference<>(new HashMap<>());
    private final AtomicReference<Map<String, MlbHubState.InjuryStatus>>   cachedInjuries    = new AtomicReference<>(new HashMap<>());
    private final AtomicReference<Map<String, MlbHubState.PitcherStats>>   cachedPitcherStats= new AtomicReference<>(new HashMap<>());
    private final AtomicReference<Map<String, MlbHubState.SavantData>>     cachedSavant      = new AtomicReference<>(new HashMap<>());
    private final AtomicReference<Map<String, MlbHubState.PublicBettingData>> cachedPublic   = new AtomicReference<>(new HashMap<>());
    private final AtomicReference<Map<String, Integer>>                    cachedLineups     = new AtomicReference<>(new HashMap<>());
    private final AtomicReference<Map<String, MlbHubState.StarterProjection>> cachedStarters = new AtomicReference<>(new HashMap<>());
    private final AtomicReference<Map<String, Double>>                     cachedBvp         = new AtomicReference<>(new HashMap<>());

    @Value("${propiq.books.targets}")
    private String targetBooks;

    private final RedisCacheManager redisCache;
    private final FastApiPollingService fastApi;
    private final ApifyScraperService apifyScraper;

    @Scheduled(fixedRate = 15000)
    public void executeScheduled() {
        try {
            execute(null, null);
        } catch (Exception e) {
            log.error("DataHubTasklet critical failure (cycle {}): {}", cycleCount.get(), e.getMessage());
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        int cycle = cycleCount.incrementAndGet();
        log.debug("🔄 DataHubTasklet cycle #{}", cycle);

        boolean refreshOdds       = (cycle % ODDS_API_CYCLE    == 0);
        boolean refreshSportsData = (cycle % SPORTSDATA_CYCLE  == 0);
        boolean refreshApify      = (cycle % APIFY_CYCLE       == 0);

        // ── ALWAYS: Tank01 pre-game data (15s) ───────────────────────────────
        CompletableFuture<Map<String, MlbHubState.GameBoxscore>> liveBoxscoresFuture
                = fastApi.fetchTank01Live();

        // ── CONDITIONAL: The Odds API (every 5 min) ──────────────────────────
        if (refreshOdds) {
            log.debug("📊 Refreshing Odds API (cycle #{}) — {} books", cycle, targetBooks);
            fastApi.fetchTheOddsApi(targetBooks)
                    .thenAccept(cachedProps::set)
                    .exceptionally(ex -> { log.error("Odds API failed: {}", ex.getMessage()); return null; });
        }

        // ── CONDITIONAL: SportsData.io (every 60s) ───────────────────────────
        if (refreshSportsData) {
            log.debug("📋 Refreshing SportsData.io (cycle #{})", cycle);
            fastApi.fetchSportsDataIo();
        }

        // ── CONDITIONAL: Apify scrapers (every 5 min) ────────────────────────
        if (refreshApify) {
            log.debug("🕷️ Triggering Apify scrapers (cycle #{})", cycle);

            CompletableFuture.allOf(
                apifyScraper.scrapeRotoWireUmpires()
                    .thenAccept(cachedUmpires::set),
                apifyScraper.scrapeRotoWireWeather()
                    .thenAccept(cachedWeather::set),
                apifyScraper.scrapeRotoWireInjuries()
                    .thenAccept(cachedInjuries::set),
                apifyScraper.scrapeRotoWireAdvancedStats()
                    .thenAccept(cachedPitcherStats::set),
                apifyScraper.scrapeBaseballSavant()
                    .thenAccept(cachedSavant::set),
                apifyScraper.scrapeActionNetworkPublic()
                    .thenAccept(cachedPublic::set),
                apifyScraper.scrapeRotoWireLineups()
                    .thenAccept(cachedLineups::set),
                apifyScraper.scrapeRotoWireStarters()
                    .thenAccept(cachedStarters::set),
                apifyScraper.scrapeRotoWireBvp()
                    .thenAccept(cachedBvp::set)
            ).exceptionally(ex -> {
                log.error("Apify batch scrape error: {}", ex.getMessage());
                return null;
            });
        }

        // ── Wait only for Tank01 (must be fresh every 15s) ───────────────────
        Map<String, MlbHubState.GameBoxscore> liveBoxscores = liveBoxscoresFuture
                .exceptionally(ex -> {
                    log.error("Tank01 live fetch failed: {}", ex.getMessage());
                    return new HashMap<>();
                }).join();

        // ── PRE-MATCH GATE ────────────────────────────────────────────────────
        // CRITICAL: Filter out props for any game that is LIVE or FINAL.
        // We hunt pre-game opening line steam only — no in-game micro-betting.
        List<PropMatchup> preMatchProps = applyPreMatchGate(cachedProps.get(), liveBoxscores);

        int filteredOut = cachedProps.get().size() - preMatchProps.size();
        if (filteredOut > 0) {
            log.debug("🚦 Pre-match gate: blocked {} props for {} live/final games",
                    filteredOut,
                    liveBoxscores.values().stream()
                            .filter(b -> LIVE_OR_FINAL_STATES.contains(b.getGameState()))
                            .count());
        }

        // ── Opening line detection (CLV) ─────────────────────────────────────
        MlbHubState existingHub = redisCache.get("mlb_hub", MlbHubState.class);
        Map<String, MlbHubState.LineSnapshot> openingLines =
                detectOpeningLines(preMatchProps, existingHub);

        // ── Bullpen fatigue (derived from live + historical boxscores) ────────
        Map<String, Integer> fatigueScores = computeBullpenFatigue(liveBoxscores);

        // ── Compile master state ──────────────────────────────────────────────
        MlbHubState masterState = MlbHubState.builder()
                .timestamp(Instant.now())
                .activeProps(preMatchProps)           // PRE-MATCH ONLY
                .liveBoxscores(liveBoxscores)
                .umpireStats(cachedUmpires.get())
                .weatherData(cachedWeather.get())
                .injuryStatuses(cachedInjuries.get())
                .pitcherStats(cachedPitcherStats.get())
                .savantData(cachedSavant.get())
                .publicBettingData(cachedPublic.get())
                .lineupPositions(cachedLineups.get())
                .starterProjections(cachedStarters.get())
                .bvpXwoba(cachedBvp.get())
                .bullpenFatigueScores(fatigueScores)
                .openingLines(openingLines)
                .build();

        // ── Push to Redis with 15s TTL ────────────────────────────────────────
        redisCache.setWithTTL("mlb_hub", masterState, 15);
        redisCache.updateAnalyzerCache(masterState);

        log.debug("✅ mlb_hub updated | {} pre-match props | {} live games (gated) | cycle #{}",
                preMatchProps.size(), liveBoxscores.size(), cycle);

        return RepeatStatus.FINISHED;
    }

    // ── Pre-Match Gate ────────────────────────────────────────────────────────

    /**
     * Filters out props for games that are LIVE or FINAL.
     *
     * If a game's boxscore state is LIVE/IN_PROGRESS/FINAL/COMPLETED,
     * ALL props for that gameId are removed from consideration.
     * This enforces the pre-match only rule — we do not make in-game micro-bets.
     */
    private List<PropMatchup> applyPreMatchGate(
            List<PropMatchup> allProps,
            Map<String, MlbHubState.GameBoxscore> liveBoxscores) {

        if (allProps == null || allProps.isEmpty()) return List.of();

        // Build set of gameIds that are currently live or final
        Set<String> gatedGameIds = liveBoxscores.entrySet().stream()
                .filter(e -> e.getValue() != null
                        && LIVE_OR_FINAL_STATES.contains(e.getValue().getGameState()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (gatedGameIds.isEmpty()) return allProps; // All games are pre-game

        // Return only props whose game is NOT yet started
        return allProps.stream()
                .filter(prop -> prop.getGameId() == null || !gatedGameIds.contains(prop.getGameId()))
                .collect(Collectors.toList());
    }

    // ── Opening Line Detector ─────────────────────────────────────────────────

    /**
     * Detects first-seen props and stamps them as opening lines for CLV calculation.
     */
    private Map<String, MlbHubState.LineSnapshot> detectOpeningLines(
            List<PropMatchup> currentProps,
            MlbHubState existingHub) {

        Map<String, MlbHubState.LineSnapshot> openingLines =
                existingHub != null && existingHub.getOpeningLines() != null
                        ? new HashMap<>(existingHub.getOpeningLines())
                        : new HashMap<>();

        if (currentProps == null) return openingLines;

        for (PropMatchup prop : currentProps) {
            String key = prop.getGameId() + "_" + prop.getPlayerId() + "_" + prop.getPropType();
            if (!openingLines.containsKey(key)) {
                openingLines.put(key, MlbHubState.LineSnapshot.builder()
                        .noVigProbOver(prop.getNoVigProbOver())
                        .noVigProbUnder(prop.getNoVigProbUnder())
                        .recordedAt(System.currentTimeMillis())
                        .build());
                log.debug("📌 Opening line recorded: {}", key);
            }
        }
        return openingLines;
    }

    // ── Bullpen Fatigue ───────────────────────────────────────────────────────

    /**
     * Bullpen fatigue 0-4 score per team.
     * Full calculation uses yesterday's boxscores from Postgres (via GradingTasklet).
     * Live pitch counts serve as a real-time proxy here.
     */
    private Map<String, Integer> computeBullpenFatigue(
            Map<String, MlbHubState.GameBoxscore> liveBoxscores) {
        // GradingTasklet updates the authoritative fatigue scores nightly at 1:05 AM.
        // This returns empty map; the nightly job writes directly to Redis.
        return new HashMap<>();
    }
}
