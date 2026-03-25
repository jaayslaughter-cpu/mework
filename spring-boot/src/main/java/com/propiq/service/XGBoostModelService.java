package com.propiq.service;

import com.propiq.model.MlbHubState;
import com.propiq.model.PropMatchup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java proxy to the Python XGBoost microservice.
 *
 * Flow:
 *  1. Build feature vector from PropMatchup + MlbHubState
 *  2. POST to ml-engine:5000/predict
 *  3. Return probability 0-100
 *
 * Fallback: if ML service is down, returns a conservative 52.0
 * so agents don't fire on stale data.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class XGBoostModelService {

    private static final double FALLBACK_PROB    = 52.0;
    private static final double CORRELATION_MIN  = 0.72;

    @Value("${propiq.ml.service-url}")
    private String mlServiceUrl;

    private final RestTemplate restTemplate;
    private final RedisCacheManager redisCache;
    private final NoVigCalculator noVigCalc;

    /**
     * Calls the Python microservice to get XGBoost probability for a single prop.
     * Returns probability 0-100 that OVER hits.
     */
    @SuppressWarnings("unchecked")
    public double predictProbability(PropMatchup prop, MlbHubState hub) {
        try {
            Map<String, Object> features = buildFeatureVector(prop, hub);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("prop_id", prop.getId());
            requestBody.put("prop_type", prop.getPropType());
            requestBody.put("features", features);
            requestBody.put("active_features", redisCache.getActiveFeatures());

            Map<String, Object> response = restTemplate.postForObject(
                    mlServiceUrl + "/api/ml/predict",
                    requestBody,
                    Map.class
            );

            if (response == null || !response.containsKey("probability")) {
                log.warn("ML service returned null for prop: {}", prop.getId());
                return FALLBACK_PROB;
            }

            return ((Number) response.get("probability")).doubleValue();

        } catch (Exception e) {
            log.error("XGBoost prediction failed for {}: {}", prop.getId(), e.getMessage());
            return FALLBACK_PROB;
        }
    }

    /**
     * Predict live probability using in-game data (pitch count, score state).
     */
    @SuppressWarnings("unchecked")
    public double predictLiveProbability(PropMatchup prop, Object inGameData) {
        try {
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("prop_id", prop.getId());
            requestBody.put("in_game_data", inGameData);
            requestBody.put("live_mode", true);

            Map<String, Object> response = restTemplate.postForObject(
                    mlServiceUrl + "/api/ml/predict-live",
                    requestBody,
                    Map.class
            );

            if (response == null || !response.containsKey("probability")) return FALLBACK_PROB;
            return ((Number) response.get("probability")).doubleValue();

        } catch (Exception e) {
            log.error("Live XGBoost prediction failed: {}", e.getMessage());
            return FALLBACK_PROB;
        }
    }

    /**
     * Checks correlation factor between prop and other active props.
     * Used by Three_Leg_Correlated agent — e.g., pitcher K Over + batter Under = correlated.
     */
    @SuppressWarnings("unchecked")
    public double checkCorrelation(PropMatchup prop, MlbHubState hub) {
        try {
            Map<String, Object> requestBody = Map.of(
                    "prop_id",   prop.getId(),
                    "game_id",   prop.getGameId(),
                    "prop_type", prop.getPropType(),
                    "player",    prop.getPlayer()
            );
            Map<String, Object> response = restTemplate.postForObject(
                    mlServiceUrl + "/api/ml/correlation",
                    requestBody,
                    Map.class
            );
            if (response == null || !response.containsKey("correlation")) return 0.0;
            return ((Number) response.get("correlation")).doubleValue();
        } catch (Exception e) {
            return 0.0;
        }
    }

    /**
     * Probability that the team wins the game (for Standard_Parlay agent).
     */
    @SuppressWarnings("unchecked")
    public double getGameOutcomeProb(String gameId) {
        try {
            Map<String, Object> requestBody = Map.of("game_id", gameId);
            Map<String, Object> response = restTemplate.postForObject(
                    mlServiceUrl + "/api/ml/game-prob",
                    requestBody,
                    Map.class
            );
            if (response == null || !response.containsKey("win_prob")) return 50.0;
            return ((Number) response.get("win_prob")).doubleValue();
        } catch (Exception e) {
            return 50.0;
        }
    }

    /**
     * Count how many of the 10 agents would agree with a given probability.
     * Used by BetAnalyzerTasklet for the "X/10 agents agree" display.
     */
    public int getAgentConsensusCount(PropMatchup prop, double xgbProb) {
        int count = 0;
        // EV_Hunter: EV > 5%
        double bestDecimal = noVigCalc.parseAmerican(prop.getBestOdds());
        double ev = noVigCalc.calculateEv(xgbProb, bestDecimal);
        if (ev > 5.0) count++;
        // Under_Machine: > 58% and is under
        if (xgbProb > 58.0 && prop.isUnder()) count++;
        // Three_Leg / Standard_Parlay (conservative check)
        if (xgbProb > 62.0) count++;
        if (xgbProb > 55.0 && ev > 3.5) count++;
        // Live Agent: high probability
        if (xgbProb > 65.0) count++;
        // Arb Agent: EV > 0
        if (ev > 0) count++;
        // Fade: high public bet assumed
        if (xgbProb > 55.0) count++;
        // Umpire, F5, Fade specialists
        if (xgbProb > 60.0) count++;
        if (xgbProb > 57.0 && prop.isUnder()) count++;
        if (xgbProb > 55.0) count++;
        return Math.min(count, 10);
    }

    /**
     * Detects anomalous stat values that might indicate a pending correction.
     * Returns true if the stat is statistically improbable given historical distributions.
     */
    @SuppressWarnings("unchecked")
    public boolean detectStatCorrectionAnomaly(Object boxscore, String playerName, double actualStat) {
        try {
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("player_name", playerName);
            requestBody.put("actual_stat", actualStat);
            requestBody.put("boxscore", boxscore);

            Map<String, Object> response = restTemplate.postForObject(
                    mlServiceUrl + "/api/ml/anomaly-detect",
                    requestBody,
                    Map.class
            );
            if (response == null) return false;
            return Boolean.TRUE.equals(response.get("is_anomaly"));
        } catch (Exception e) {
            log.error("Anomaly detection failed for {}: {}", playerName, e.getMessage());
            return false; // Default: trust the stat
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private Map<String, Object> buildFeatureVector(PropMatchup prop, MlbHubState hub) {
        Map<String, Object> features = new HashMap<>();

        // Core model features
        features.put("pitcher_fip",           hub.getPitcherFip(prop.getOpposingPitcherId()));
        features.put("pitcher_swstr_pct",     hub.getPitcherSwStr(prop.getOpposingPitcherId()));
        features.put("batter_xwoba",          getSavantXwoba(hub, prop.getPlayerId()));
        features.put("implied_odds",          noVigCalc.calculateTrueProb(prop.getMarketOdds()));

        // Context features (7-point checklist)
        features.put("umpire_called_strike_pct", getUmpireKPct(hub, prop.getUmpireId()));
        features.put("wind_speed_outfield",      getWindSpeed(hub, prop.getGameId()));
        features.put("bullpen_fatigue_score",    getBullpenFatigue(hub, prop.getTeam()));
        features.put("public_bet_pct",           hub.getPublicBetPct(prop.getTeam()) / 100.0);

        // Injury / rest features
        features.put("days_since_injury",     getDaysSinceInjury(hub, prop.getPlayerId()));
        features.put("rest_days",             getRestDays(hub, prop.getOpposingPitcherId()));

        // BVP
        String bvpKey = prop.getPlayerId() + "_" + prop.getOpposingPitcherId();
        double bvpXwoba = hub.getBvpXwoba() != null ? hub.getBvpXwoba().getOrDefault(bvpKey, 0.320) : 0.320;
        features.put("bvp_xwoba", bvpXwoba);

        // Lineup position (top-4 = confirmed starts)
        int lineupPos = hub.getLineupPositions() != null
                ? hub.getLineupPositions().getOrDefault(prop.getPlayerId(), 9) : 9;
        features.put("lineup_position", lineupPos);

        return features;
    }

    private double getSavantXwoba(MlbHubState hub, String playerId) {
        MlbHubState.SavantData sd = hub.getSavantData() != null ? hub.getSavantData().get(playerId) : null;
        return sd != null ? sd.getXwoba() : 0.320;
    }

    private double getUmpireKPct(MlbHubState hub, String umpireId) {
        MlbHubState.UmpireStats us = hub.getUmpireStats() != null ? hub.getUmpireStats().get(umpireId) : null;
        return us != null ? us.getCalledStrikePct() : 68.0;
    }

    private int getWindSpeed(MlbHubState hub, String gameId) {
        MlbHubState.WeatherData wd = hub.getWeatherData() != null ? hub.getWeatherData().get(gameId) : null;
        return wd != null ? wd.getWindSpeed() : 0;
    }

    private int getBullpenFatigue(MlbHubState hub, String teamId) {
        return hub.getBullpenFatigueScores() != null
                ? hub.getBullpenFatigueScores().getOrDefault(teamId, 0) : 0;
    }

    private int getDaysSinceInjury(MlbHubState hub, String playerId) {
        MlbHubState.InjuryStatus is = hub.getInjuryStatuses() != null
                ? hub.getInjuryStatuses().get(playerId) : null;
        return is != null ? is.getDaysSinceInjury() : 999;
    }

    private int getRestDays(MlbHubState hub, String pitcherId) {
        MlbHubState.StarterProjection sp = hub.getStarterProjections() != null
                ? hub.getStarterProjections().get(pitcherId) : null;
        return sp != null ? sp.getDaysRest() : 4;
    }
}
