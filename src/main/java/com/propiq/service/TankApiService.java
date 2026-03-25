package com.propiq.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * TankApiService — Tank01 MLB Real-Time Statistics (RapidAPI) client.
 *
 * Endpoints used:
 *   GET /getMLBBoxScore?gameID=...             — live/final box score for a specific game
 *   GET /getMLBGamesForDate?gameDate=YYYYMMDD  — all games for a given date
 *   GET /getMLBTeamRoster?teamAbv=LAD          — team roster (for handedness data)
 *   GET /getMLBPlayerInfo?playerID=...         — individual player bio/stats
 *
 * RapidAPI headers required on every request:
 *   X-RapidAPI-Key:  {key}
 *   X-RapidAPI-Host: tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com
 *
 * QUOTA PROTECTION:
 *   DataHubTasklet enforces a 5-minute minimum between Tank01 calls.
 *   This service does NOT enforce its own rate limit — the tasklet does.
 *   If called more frequently, RapidAPI will return 429 and the call will be skipped.
 *
 * PRE-MATCH ONLY:
 *   getBoxScores() returns raw data. DataHubTasklet filters to pre-match game IDs
 *   before caching to Redis. No in-game polling occurs here.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TankApiService {

    // ── Config ────────────────────────────────────────────────────────────────
    @Value("${propiq.apis.tank01.key}")
    private String apiKey;

    @Value("${propiq.apis.tank01.base-url}")
    private String baseUrl;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    // ── RapidAPI headers ──────────────────────────────────────────────────────
    private static final String RAPIDAPI_KEY_HEADER  = "X-RapidAPI-Key";
    private static final String RAPIDAPI_HOST_HEADER = "X-RapidAPI-Host";
    private static final String RAPIDAPI_HOST_VALUE  =
            "tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com";

    // ── Date format expected by Tank01: YYYYMMDD ──────────────────────────────
    private static final DateTimeFormatter TANK_DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BOX SCORES — used by DataHubTasklet for pre-match lineup confirmation
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns today's game summaries from Tank01 (lineup positions, starters, status).
     *
     * Returned map is keyed by Tank01 gameID string. Each value contains:
     *   gameID, gameDate, gameStatus (e.g. "scheduled"), homeTeam, awayTeam,
     *   lineups (nested: home/away player lists with battingOrder, playerID, name)
     *   startingPitchers (home/away pitcher IDs)
     *
     * DataHubTasklet filters this to pre-match game IDs before Redis cache.
     * Returns Map<String, Object> keyed by gameID for O(1) cross-reference.
     */
    public Map<String, Object> getBoxScores() {
        String today = LocalDate.now(ZoneId.of("America/Los_Angeles")).format(TANK_DATE_FMT);
        return getBoxScoresForDate(today);
    }

    /**
     * Returns game summaries for a specific date (YYYYMMDD format).
     * Used by GetawayAgent to look up previous_game_innings.
     */
    public Map<String, Object> getBoxScoresForDate(String yyyyMMdd) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl + "/getMLBGamesForDate")
                .queryParam("gameDate", yyyyMMdd)
                .toUriString();

        log.debug("[Tank01] GET getMLBGamesForDate?gameDate={}", yyyyMMdd);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<String, Object> raw = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<Map<String, Object>>() {});

                // Tank01 wraps the games list under "body" key
                Object body = raw.get("body");
                if (body instanceof List<?> games) {
                    Map<String, Object> byGameId = new LinkedHashMap<>();
                    for (Object gameObj : games) {
                        if (gameObj instanceof Map<?, ?> game) {
                            Object gameId = game.get("gameID");
                            if (gameId != null) {
                                byGameId.put(String.valueOf(gameId), game);
                            }
                        }
                    }
                    log.info("[Tank01] BoxScores for {}: {} games", yyyyMMdd, byGameId.size());
                    return byGameId;
                }
            }

        } catch (HttpClientErrorException.TooManyRequests e) {
            log.warn("[Tank01] 429 Rate limit — DataHub will skip this tick.");
        } catch (HttpClientErrorException.Unauthorized e) {
            log.error("[Tank01] 401 Unauthorized — check TANK01_API_KEY env var.");
        } catch (Exception e) {
            log.error("[Tank01] getMLBGamesForDate failed: {}", e.getMessage());
        }

        return Collections.emptyMap();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SINGLE GAME BOX SCORE — used for detailed pre-game lineup confirmation
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns the full box score for a specific Tank01 gameID.
     * Provides confirmed batting order, starting pitchers, and current game state.
     *
     * Called for individual games when a lineup change is detected by the late
     * scratch detector to re-validate PropEdge lineup_position values.
     */
    public Optional<Map<String, Object>> getBoxScoreForGame(String gameId) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl + "/getMLBBoxScore")
                .queryParam("gameID", gameId)
                .toUriString();

        log.debug("[Tank01] GET getMLBBoxScore?gameID={}", gameId);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<String, Object> raw = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<Map<String, Object>>() {});

                // Unwrap "body" wrapper if present
                Object body = raw.get("body");
                if (body instanceof Map<?, ?> bodyMap) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> boxScore = (Map<String, Object>) bodyMap;
                    log.debug("[Tank01] BoxScore fetched for gameID={}", gameId);
                    return Optional.of(boxScore);
                }
            }

        } catch (Exception e) {
            log.warn("[Tank01] getMLBBoxScore failed for gameID={}: {}", gameId, e.getMessage());
        }

        return Optional.empty();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // PLAYER INFO — batter handedness, pitcher delivery data
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns player bio and stats including:
     *   bats (L/R/S), throws (L/R), team, position, jerseyNum, height, weight
     *
     * Used by PlatoonAgent to confirm batter_handedness / pitcher_handedness
     * when the Apify RotoWire scrape hasn't populated those fields yet.
     */
    public Optional<Map<String, Object>> getPlayerInfo(String playerId) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl + "/getMLBPlayerInfo")
                .queryParam("playerID", playerId)
                .toUriString();

        log.debug("[Tank01] GET getMLBPlayerInfo?playerID={}", playerId);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<String, Object> raw = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<Map<String, Object>>() {});
                Object body = raw.get("body");
                if (body instanceof Map<?, ?> bodyMap) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> player = (Map<String, Object>) bodyMap;
                    return Optional.of(player);
                }
            }

        } catch (Exception e) {
            log.warn("[Tank01] getMLBPlayerInfo failed for playerID={}: {}", playerId, e.getMessage());
        }

        return Optional.empty();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TEAM ROSTER — bullpen composition for PlatoonAgent P_LHP / P_RHP
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns full team roster for the given team abbreviation (e.g. "LAD").
     *
     * Roster is used by PlatoonAgent to calculate P_LHP_bullpen and P_RHP_bullpen:
     * the probability that a batter will face a left- or right-handed reliever.
     *
     * Returns list of player maps — each includes playerID, name, pos, bats, throws.
     */
    public List<Map<String, Object>> getTeamRoster(String teamAbbr) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl + "/getMLBTeamRoster")
                .queryParam("teamAbv", teamAbbr.toUpperCase())
                .toUriString();

        log.debug("[Tank01] GET getMLBTeamRoster?teamAbv={}", teamAbbr);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<String, Object> raw = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<Map<String, Object>>() {});

                Object body = raw.get("body");
                if (body instanceof Map<?, ?> bodyMap) {
                    Object roster = bodyMap.get("roster");
                    if (roster instanceof List<?> rosterList) {
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> castRoster = (List<Map<String, Object>>) rosterList;
                        log.debug("[Tank01] Roster for {}: {} players", teamAbbr, castRoster.size());
                        return castRoster;
                    }
                }
            }

        } catch (Exception e) {
            log.warn("[Tank01] getMLBTeamRoster failed for {}: {}", teamAbbr, e.getMessage());
        }

        return Collections.emptyList();
    }

    // ── Auth helper ───────────────────────────────────────────────────────────

    private HttpEntity<Void> buildAuthEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(RAPIDAPI_KEY_HEADER, apiKey);
        headers.set(RAPIDAPI_HOST_HEADER, RAPIDAPI_HOST_VALUE);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        return new HttpEntity<>(headers);
    }
}
