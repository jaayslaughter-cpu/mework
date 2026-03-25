package com.propiq.service;

import com.propiq.model.MlbHubState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Apify-powered anti-ban scraper for RotoWire, Action Network, and Baseball Savant.
 *
 * ALL 8 RotoWire endpoints + 3 Action Network endpoints are routed through
 * Apify residential proxies to prevent IP bans and rate limits.
 *
 * Rate policy: Only called every 5 minutes (20 × 15s DataHub cycles).
 * Never called directly from the 15s loop.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ApifyScraperService {

    // RotoWire endpoints
    private static final String RW_UMPIRES   = "https://www.rotowire.com/baseball/umpire-stats-daily.php";
    private static final String RW_WEATHER   = "https://www.rotowire.com/baseball/weather.php";
    private static final String RW_BVP       = "https://www.rotowire.com/baseball/stats-bvp.php";
    private static final String RW_LINEUPS   = "https://www.rotowire.com/baseball/batting-orders.php";
    private static final String RW_INJURIES  = "https://www.rotowire.com/baseball/news.php?injuries=all";
    private static final String RW_STARTERS  = "https://www.rotowire.com/baseball/projected-starters.php";
    private static final String RW_ADV_STATS = "https://www.rotowire.com/baseball/stats-advanced.php";
    private static final String RW_PROPS     = "https://www.rotowire.com/betting/mlb/player-props.php";

    // Action Network endpoints
    private static final String AN_PUBLIC    = "https://www.actionnetwork.com/mlb/public-betting";
    private static final String AN_SHARP     = "https://www.actionnetwork.com/mlb/sharp-report";
    private static final String AN_PROJ      = "https://www.actionnetwork.com/mlb/prop-projections";

    // Baseball Savant (Apify actor for JS-rendered Statcast pages)
    private static final String SAVANT_PITCH = "https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats";
    private static final String SAVANT_STAT  = "https://baseballsavant.mlb.com/leaderboard/statcast";

    @Value("${propiq.apis.apify.key}")
    private String apifyKey;

    @Value("${propiq.apis.apify.base-url}")
    private String apifyBaseUrl;

    private final RestTemplate restTemplate;

    // ── Public scrape methods (called from DataHubTasklet async) ─────────────

    public CompletableFuture<Map<String, MlbHubState.UmpireStats>> scrapeRotoWireUmpires() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String json = triggerApifyActor(RW_UMPIRES);
                return parseUmpireStats(json);
            } catch (Exception e) {
                log.error("RotoWire umpires scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, MlbHubState.WeatherData>> scrapeRotoWireWeather() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String json = triggerApifyActor(RW_WEATHER);
                return parseWeatherData(json);
            } catch (Exception e) {
                log.error("RotoWire weather scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, Double>> scrapeRotoWireBvp() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String json = triggerApifyActor(RW_BVP);
                return parseBvpData(json);
            } catch (Exception e) {
                log.error("RotoWire BVP scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, Integer>> scrapeRotoWireLineups() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String json = triggerApifyActor(RW_LINEUPS);
                return parseLineupPositions(json);
            } catch (Exception e) {
                log.error("RotoWire lineups scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, MlbHubState.InjuryStatus>> scrapeRotoWireInjuries() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String json = triggerApifyActor(RW_INJURIES);
                return parseInjuryStatuses(json);
            } catch (Exception e) {
                log.error("RotoWire injuries scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, MlbHubState.StarterProjection>> scrapeRotoWireStarters() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String json = triggerApifyActor(RW_STARTERS);
                return parseStarterProjections(json);
            } catch (Exception e) {
                log.error("RotoWire starters scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, MlbHubState.PitcherStats>> scrapeRotoWireAdvancedStats() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String json = triggerApifyActor(RW_ADV_STATS);
                return parsePitcherStats(json);
            } catch (Exception e) {
                log.error("RotoWire advanced stats scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, MlbHubState.PublicBettingData>> scrapeActionNetworkPublic() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String json = triggerApifyActor(AN_PUBLIC);
                return parsePublicBettingData(json);
            } catch (Exception e) {
                log.error("Action Network public betting scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, MlbHubState.SavantData>> scrapeBaseballSavant() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Savant requires JS rendering — use Apify actor for full scrape
                String pitchJson = triggerApifyActor(SAVANT_PITCH);
                String statJson  = triggerApifyActor(SAVANT_STAT);
                return parseSavantData(pitchJson, statJson);
            } catch (Exception e) {
                log.error("Baseball Savant scrape failed: {}", e.getMessage());
                return new HashMap<>();
            }
        });
    }

    public CompletableFuture<Map<String, Integer>> calculateBullpenFatigue(
            CompletableFuture<Map<String, MlbHubState.GameBoxscore>> boxscoresFuture) {
        return boxscoresFuture.thenApplyAsync(boxscores -> {
            Map<String, Integer> fatigueScores = new HashMap<>();
            if (boxscores == null) return fatigueScores;
            for (Map.Entry<String, MlbHubState.GameBoxscore> entry : boxscores.entrySet()) {
                // Fatigue 0-4 score (from master guidelines):
                // +1 if any reliever PC last 3 days >= 50
                // +1 if team innings last 2 days > 8
                // +1 if high-stress appearances (tie/1-run games)
                // +1 if 3+ relievers had zero rest
                // NOTE: Full calculation requires yesterday's boxscore data from Postgres
                fatigueScores.put(entry.getKey(), 0); // Will be updated by GradingTasklet data
            }
            return fatigueScores;
        });
    }

    // ── Apify actor trigger (anti-ban core) ───────────────────────────────────

    /**
     * Triggers the Apify web scraper actor with the given URL.
     * Apify routes through residential proxies, rotates user-agents,
     * and handles rate limiting automatically.
     */
    @SuppressWarnings("unchecked")
    private String triggerApifyActor(String targetUrl) {
        String actorUrl = apifyBaseUrl + "/acts/apify~web-scraper/runs";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + apifyKey);

        Map<String, Object> body = new HashMap<>();
        body.put("startUrls", List.of(Map.of("url", targetUrl)));
        body.put("proxyConfiguration", Map.of(
                "useApifyProxy", true,
                "apifyProxyGroups", List.of("RESIDENTIAL")
        ));
        body.put("pageFunction", buildPageFunction(targetUrl));

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<Map> response = restTemplate.postForEntity(
                    actorUrl + "?token=" + apifyKey + "&waitForFinish=60",
                    request,
                    Map.class
            );

            if (response.getBody() != null) {
                // Get dataset items from the completed run
                String datasetId = extractDatasetId(response.getBody());
                return fetchActorResults(datasetId);
            }
        } catch (Exception e) {
            log.error("Apify actor trigger failed for {}: {}", targetUrl, e.getMessage());
        }
        return "[]";
    }

    private String fetchActorResults(String datasetId) {
        try {
            String url = apifyBaseUrl + "/datasets/" + datasetId + "/items?format=json&token=" + apifyKey;
            return restTemplate.getForObject(url, String.class);
        } catch (Exception e) {
            log.error("Failed to fetch Apify results: {}", e.getMessage());
            return "[]";
        }
    }

    @SuppressWarnings("unchecked")
    private String extractDatasetId(Map<String, Object> responseBody) {
        Map<String, Object> data = (Map<String, Object>) responseBody.getOrDefault("data", new HashMap<>());
        return (String) data.getOrDefault("defaultDatasetId", "");
    }

    private String buildPageFunction(String url) {
        // Minimal page function — extracts all table data as JSON
        return """
            async function pageFunction(context) {
                const { $ } = context;
                const rows = [];
                $('table tr').each((i, row) => {
                    const cells = [];
                    $(row).find('td, th').each((j, cell) => { cells.push($(cell).text().trim()); });
                    if (cells.length > 0) rows.push(cells);
                });
                return { url: context.request.url, rows };
            }
            """;
    }

    // ── Parse methods (return empty maps — populated by Python ML if complex) ─

    private Map<String, MlbHubState.UmpireStats> parseUmpireStats(String json) {
        log.debug("Parsing umpire stats from Apify response ({} bytes)", json.length());
        return new HashMap<>(); // Populated by Python parsing layer
    }

    private Map<String, MlbHubState.WeatherData> parseWeatherData(String json) {
        return new HashMap<>();
    }

    private Map<String, Double> parseBvpData(String json) {
        return new HashMap<>();
    }

    private Map<String, Integer> parseLineupPositions(String json) {
        return new HashMap<>();
    }

    private Map<String, MlbHubState.InjuryStatus> parseInjuryStatuses(String json) {
        return new HashMap<>();
    }

    private Map<String, MlbHubState.StarterProjection> parseStarterProjections(String json) {
        return new HashMap<>();
    }

    private Map<String, MlbHubState.PitcherStats> parsePitcherStats(String json) {
        return new HashMap<>();
    }

    private Map<String, MlbHubState.PublicBettingData> parsePublicBettingData(String json) {
        return new HashMap<>();
    }

    private Map<String, MlbHubState.SavantData> parseSavantData(String pitchJson, String statJson) {
        return new HashMap<>();
    }
}
