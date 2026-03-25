package com.propiq.service;

import com.propiq.model.MlbHubState;
import com.propiq.model.PropMatchup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Polls the three JSON APIs:
 *
 *   1. The Odds API     — Player prop odds across 4 CA books
 *                          RATE LIMIT: Every 5 minutes (300s). DataHubTasklet
 *                          enforces this with a cycle counter.
 *
 *   2. SportsData.io    — Player stats, rosters, game schedules
 *                          Rate: Every 60s
 *
 *   3. Tank01 RapidAPI  — Live in-game data (pitch counts, score, inning)
 *                          Rate: Every 15s (game hours only)
 *
 * All methods return CompletableFuture for async aggregation in DataHubTasklet.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FastApiPollingService {

    @Value("${propiq.apis.theodds.key}")
    private String oddsApiKey;

    @Value("${propiq.apis.theodds.base-url}")
    private String oddsBaseUrl;

    @Value("${propiq.apis.sportsdata.key}")
    private String sportsDataKey;

    @Value("${propiq.apis.sportsdata.base-url}")
    private String sportsDataBaseUrl;

    @Value("${propiq.apis.tank01.key}")
    private String tank01Key;

    @Value("${propiq.apis.tank01.base-url}")
    private String tank01BaseUrl;

    @Value("${propiq.apis.tank01.host}")
    private String tank01Host;

    @Value("${propiq.books.targets}")
    private String targetBooks;    // "draftkings,fanduel,betmgm,bet365"

    private final RestTemplate restTemplate;

    // ── The Odds API (MUST poll max every 5 minutes) ──────────────────────────

    /**
     * Fetches player prop odds across DraftKings, FanDuel, BetMGM, bet365.
     *
     * IMPORTANT: This method is intentionally NOT called every 15s.
     * DataHubTasklet only calls it every 20 cycles (300s = 5 minutes)
     * to protect the monthly API quota.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<PropMatchup>> fetchTheOddsApi(String booksOverride) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String books = booksOverride != null ? booksOverride : targetBooks;
                String url = String.format(
                        "%s/sports/baseball_mlb/player-props/odds?apiKey=%s&regions=us&markets=batter_hits,batter_strikeouts,batter_total_bases,batter_home_runs&bookmakers=%s&oddsFormat=american",
                        oddsBaseUrl, oddsApiKey, books
                );

                List<Map<String, Object>> events = restTemplate.getForObject(url, List.class);
                if (events == null) return List.of();

                List<PropMatchup> props = new ArrayList<>();
                for (Map<String, Object> event : events) {
                    props.addAll(parseEventProps(event));
                }
                log.debug("The Odds API returned {} prop matchups", props.size());
                return props;

            } catch (Exception e) {
                log.error("The Odds API fetch failed: {}", e.getMessage());
                return List.of();
            }
        });
    }

    // ── SportsData.io (every 60s) ─────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<String, Object>> fetchSportsDataIo() {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, Object> result = new HashMap<>();
            try {
                // Today's games
                String gamesUrl = sportsDataBaseUrl + "/stats/json/GamesByDate/today?key=" + sportsDataKey;
                List<Map<String, Object>> games = restTemplate.getForObject(gamesUrl, List.class);
                result.put("games", games != null ? games : List.of());

                // Player props
                String propsUrl = sportsDataBaseUrl + "/odds/json/PlayerProps?key=" + sportsDataKey;
                List<Map<String, Object>> playerProps = restTemplate.getForObject(propsUrl, List.class);
                result.put("playerProps", playerProps != null ? playerProps : List.of());

            } catch (Exception e) {
                log.error("SportsData.io fetch failed: {}", e.getMessage());
            }
            return result;
        });
    }

    // ── Tank01 RapidAPI (every 15s — live games only) ─────────────────────────

    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<String, MlbHubState.GameBoxscore>> fetchTank01Live() {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, MlbHubState.GameBoxscore> boxscores = new HashMap<>();
            try {
                HttpHeaders headers = new HttpHeaders();
                headers.set("X-RapidAPI-Host", tank01Host);
                headers.set("X-RapidAPI-Key",  tank01Key);
                HttpEntity<String> entity = new HttpEntity<>(headers);

                String url = tank01BaseUrl + "/mlb/getLiveGameDay";
                ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.GET, entity, Map.class);

                if (response.getBody() != null) {
                    boxscores = parseTank01Boxscores(response.getBody());
                }
                log.debug("Tank01 live: {} active games", boxscores.size());

            } catch (Exception e) {
                log.error("Tank01 live fetch failed: {}", e.getMessage());
            }
            return boxscores;
        });
    }

    // ── Parse helpers ─────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private List<PropMatchup> parseEventProps(Map<String, Object> event) {
        List<PropMatchup> props = new ArrayList<>();
        String gameId    = (String) event.getOrDefault("id", "");
        String homeTeam  = (String) event.getOrDefault("home_team", "");
        String awayTeam  = (String) event.getOrDefault("away_team", "");

        List<Map<String, Object>> bookmakers =
                (List<Map<String, Object>>) event.getOrDefault("bookmakers", List.of());

        // Build a map: player+propType → best odds across books
        Map<String, Integer>  bestOddsMap   = new HashMap<>();
        Map<String, Double>   linesMap      = new HashMap<>();
        Map<String, Map<String, Integer>> allOddsMap = new HashMap<>();

        for (Map<String, Object> bookmaker : bookmakers) {
            String book = (String) bookmaker.getOrDefault("key", "");
            List<Map<String, Object>> markets =
                    (List<Map<String, Object>>) bookmaker.getOrDefault("markets", List.of());

            for (Map<String, Object> market : markets) {
                String marketKey = (String) market.getOrDefault("key", "");
                List<Map<String, Object>> outcomes =
                        (List<Map<String, Object>>) market.getOrDefault("outcomes", List.of());

                for (Map<String, Object> outcome : outcomes) {
                    String player    = (String) outcome.getOrDefault("description", "");
                    String name      = (String) outcome.getOrDefault("name", "");   // Over/Under
                    double point     = outcome.get("point") != null ? ((Number) outcome.get("point")).doubleValue() : 0.0;
                    int americanOdds = outcome.get("price") != null ? ((Number) outcome.get("price")).intValue() : -110;

                    if (!"Over".equalsIgnoreCase(name)) continue; // Only track over for now

                    String propKey = player + "_" + marketKey;
                    linesMap.put(propKey, point);

                    // Track best odds (highest decimal = most favorable for bettor)
                    int existingOdds = bestOddsMap.getOrDefault(propKey, -9999);
                    if (americanOdds > existingOdds) {
                        bestOddsMap.put(propKey, americanOdds);
                    }
                    allOddsMap.computeIfAbsent(propKey, k -> new HashMap<>()).put(book, americanOdds);
                }
            }
        }

        // Construct PropMatchup objects
        for (Map.Entry<String, Integer> entry : bestOddsMap.entrySet()) {
            String propKey = entry.getKey();
            String[] parts = propKey.split("_", 2);
            if (parts.length < 2) continue;

            String player    = parts[0];
            String propType  = mapMarketKeyToPropType(parts[1]);
            double line      = linesMap.getOrDefault(propKey, 0.5);
            int    bestOdds  = entry.getValue();

            PropMatchup pm = PropMatchup.builder()
                    .id(gameId + "_" + player.replaceAll(" ", "_") + "_" + propType)
                    .gameId(gameId)
                    .player(player)
                    .playerId(player.replaceAll(" ", "_").toLowerCase())
                    .team(homeTeam)   // Simplified — will be enriched by SportsData
                    .propType(propType)
                    .line(line)
                    .bestOdds(String.valueOf(bestOdds))
                    .bestOddsBook(getBestBook(allOddsMap.get(propKey), bestOdds))
                    .marketOdds(allOddsMap.getOrDefault(propKey, new HashMap<>()))
                    .dfsPlatform("PrizePicks")
                    .dfsPickType("OVER")
                    .build();
            props.add(pm);
        }
        return props;
    }

    private String mapMarketKeyToPropType(String marketKey) {
        return switch (marketKey) {
            case "batter_hits"         -> "Hits";
            case "batter_strikeouts"   -> "Strikeouts";
            case "batter_total_bases"  -> "TotalBases";
            case "batter_home_runs"    -> "HomeRuns";
            default                    -> marketKey;
        };
    }

    private String getBestBook(Map<String, Integer> bookOdds, int bestOddsVal) {
        if (bookOdds == null) return "unknown";
        return bookOdds.entrySet().stream()
                .filter(e -> e.getValue() == bestOddsVal)
                .map(Map.Entry::getKey)
                .findFirst().orElse("unknown");
    }

    @SuppressWarnings("unchecked")
    private Map<String, MlbHubState.GameBoxscore> parseTank01Boxscores(Map<String, Object> body) {
        Map<String, MlbHubState.GameBoxscore> result = new HashMap<>();
        Object gamesObj = body.get("body");
        if (!(gamesObj instanceof List)) return result;

        List<Map<String, Object>> games = (List<Map<String, Object>>) gamesObj;
        for (Map<String, Object> game : games) {
            String gameId = (String) game.getOrDefault("gameID", "");
            String status = (String) game.getOrDefault("gameStatus", "SCHEDULED");

            MlbHubState.GameBoxscore bs = MlbHubState.GameBoxscore.builder()
                    .gameId(gameId)
                    .status(status)
                    .isFinal("Final".equalsIgnoreCase(status))
                    .playerStats(new HashMap<>())
                    .build();
            result.put(gameId, bs);
        }
        return result;
    }
}
