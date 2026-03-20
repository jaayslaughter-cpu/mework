package com.propiq.tasklets;

import com.propiq.model.AnalyzerCacheItem;
import com.propiq.model.MlbHubState;
import com.propiq.model.PropMatchup;
import com.propiq.service.NoVigCalculator;
import com.propiq.service.RedisCacheManager;
import com.propiq.service.XGBoostModelService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BetAnalyzerTasklet — Pre-computation engine for the REST API.
 *
 * Runs every 5 seconds. Pre-computes XGBoost probabilities, no-vig EV,
 * and the 7-point checklist for ALL active props.
 *
 * Result stored in Redis "bet_analyzer_cache" (10s TTL).
 * The REST endpoint reads from this cache — never re-runs the ML math on demand.
 * This ensures sub-100ms API responses regardless of user load.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BetAnalyzerTasklet implements Tasklet {

    private final RedisCacheManager redisCache;
    private final XGBoostModelService xgboostService;
    private final NoVigCalculator noVigCalc;

    @Scheduled(fixedRate = 5000)
    public void executeScheduled() {
        try {
            execute(null, null);
        } catch (Exception e) {
            log.error("BetAnalyzerTasklet failure: {}", e.getMessage());
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        MlbHubState hub = redisCache.get("mlb_hub", MlbHubState.class);
        if (hub == null || hub.getActiveProps() == null) return RepeatStatus.FINISHED;

        Map<String, AnalyzerCacheItem> cache = new HashMap<>();
        List<PropMatchup> activeProps = hub.getActiveProps();

        for (PropMatchup prop : activeProps) {
            try {
                // 1. XGBoost probability
                double xgbProb   = xgboostService.predictProbability(prop, hub);
                double noVigProb = noVigCalc.calculateTrueProb(prop.getMarketOdds()) * 100.0;
                double decimalOdds = noVigCalc.parseAmerican(prop.getBestOdds());
                double ev = noVigCalc.calculateEv(xgbProb, decimalOdds);

                // 2. Sharp money / RLM detection
                MlbHubState.PublicBettingData pd = hub.getPublicBettingData() != null
                        ? hub.getPublicBettingData().get(prop.getTeam()) : null;
                double publicPct = pd != null ? pd.getPublicBetPct() : 50.0;
                double moneyPct  = pd != null ? pd.getMoneyPct() : 50.0;
                boolean sharpMoney = publicPct > 70.0 && moneyPct < 50.0;

                // 3. Umpire K%
                MlbHubState.UmpireStats us = hub.getUmpireStats() != null
                        ? hub.getUmpireStats().get(prop.getUmpireId()) : null;
                double umpireKPct = us != null ? us.getCalledStrikePct() : 68.0;

                // 4. Bullpen fatigue
                int bullpenFatigue = hub.getBullpenFatigueScores() != null
                        ? hub.getBullpenFatigueScores().getOrDefault(prop.getTeam(), 0) : 0;

                // 5. Lineup position (top-4 confirmed)
                int lineupPos = hub.getLineupPositions() != null
                        ? hub.getLineupPositions().getOrDefault(prop.getPlayerId(), 9) : 9;
                boolean lineupTop4 = lineupPos <= 4;

                // 6. Wind boost factor
                MlbHubState.WeatherData wd = hub.getWeatherData() != null
                        ? hub.getWeatherData().get(prop.getGameId()) : null;
                double windBoost = computeWindBoost(wd, prop);

                // 7. Agent consensus
                int agentsAgreeing = xgboostService.getAgentConsensusCount(prop, xgbProb);

                // Build cache item
                AnalyzerCacheItem item = new AnalyzerCacheItem.Builder()
                        .withModelProb(xgbProb)
                        .withNoVigProb(noVigProb)
                        .withEvPct(ev)
                        .withMatchupContext(hub.getMatchupContextString(prop))
                        .withAgentsAgreeing(agentsAgreeing)
                        .withSharpMoney(sharpMoney)
                        .withPublicBetPct(publicPct)
                        .withMoneyPct(moneyPct)
                        .withBullpenFatigue(bullpenFatigue)
                        .withLineupTop4(lineupTop4)
                        .withUmpireKPct(umpireKPct)
                        .withWindBoostFactor(windBoost)
                        .build();

                // Key: "Aaron Judge_Hits" or "Aaron Judge_O1.5H"
                String key = prop.getPlayer() + "_" + prop.getPropType();
                cache.put(key, item);

            } catch (Exception e) {
                log.debug("Failed to pre-compute for {}: {}", prop.getPlayer(), e.getMessage());
            }
        }

        redisCache.setWithTTL("bet_analyzer_cache", cache, 10);
        log.debug("📊 BetAnalyzer cache updated: {} props pre-computed", cache.size());

        return RepeatStatus.FINISHED;
    }

    private double computeWindBoost(MlbHubState.WeatherData wd, PropMatchup prop) {
        if (wd == null || wd.getWindSpeed() < 8) return 1.0;
        // Wind out > 8mph toward left field → boost HR/hits for LHH pull hitters
        String dir = wd.getWindDirection();
        if ("out_lf".equalsIgnoreCase(dir) && wd.getWindSpeed() > 8) return 1.15;
        if ("out_rf".equalsIgnoreCase(dir) && wd.getWindSpeed() > 8) return 1.10;
        if ("in".equalsIgnoreCase(dir))    return 0.90; // Wind in = suppress offense
        return 1.0;
    }
}
