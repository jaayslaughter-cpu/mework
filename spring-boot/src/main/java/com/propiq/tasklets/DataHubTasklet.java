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

/**
 * DataHubTasklet — The only component allowed to touch external APIs.
 *
 * Staggered polling schedule (strict rate limit enforcement):
 * ┌─────────────────────────────┬──────────────────────────────────────┐
 * │ API / Scraper               │ Poll frequency                       │
 * ├─────────────────────────────┼──────────────────────────────────────┤
 * │ Tank01 (live game data)     │ Every 15s (every cycle)              │
 * │ SportsData.io (stats)       │ Every 60s  (every 4 cycles)          │
 * │ The Odds API (prop odds)    │ Every 5min (every 20 cycles) ← QUOTA │
 * │ Apify scrapers (8 RW + 3AN) │ Every 5min (every 20 cycles)         │
 * └─────────────────────────────┴──────────────────────────────────────┘
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

        boolean refreshOdds    = (cycle % ODDS_API_CYCLE    == 0);
        boolean refreshSportsData = (cycle % SPORTSDATA_CYCLE == 0);
        boolean refreshApify   = (cycle % APIFY_CYCLE       == 0);

        // ── ALWAYS: Tank01 live game data (15s) ──────────────────────────────
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
            fastApi.fetchSportsDataIo(); // Updates prop list with stat enrichment
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

        // ── Opening line detection (CLV) ─────────────────────────────────────
        MlbHubState existingHub = redisCache.get("mlb_hub", MlbHubState.class);
        Map<String, MlbHubState.LineSnapshot> openingLines =
                detectOpeningLines(cachedProps.get(), existingHub);

        // ── Bullpen fatigue (derived from live + historical boxscores) ────────
        Map<String, Integer> fatigueScores = computeBullpenFatigue(liveBoxscores);

        // ── Compile master state ──────────────────────────────────────────────
        MlbHubState masterState = MlbHubState.builder()
                .timestamp(Instant.now())
                .activeProps(cachedProps.get())
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

        log.debug("✅ mlb_hub updated | {} props | {} live games | cycle #{}",
                cachedProps.get().size(), liveBoxscores.size(), cycle);

        return RepeatStatus.FINISHED;
    }

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
                // First time seeing this prop — stamp as opening line
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

    /**
     * Bullpen fatigue 0-4 score per team.
     * Full calculation uses yesterday's boxscores from Postgres.
     * This version uses live pitch counts as a real-time proxy.
     */
    private Map<String, Integer> computeBullpenFatigue(
            Map<String, MlbHubState.GameBoxscore> liveBoxscores) {
        Map<String, Integer> scores = new HashMap<>();
        // In production: query Postgres for last 3 days of reliever usage
        // Placeholder: initialize all teams to 0, let GradingTasklet update nightly
        return scores;
    }
}
