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
 * SportsDataService — SportsData.io MLB API v3 client.
 *
 * Endpoints used:
 *   GET /scores/json/GamesByDate/{date}           — today's schedule + game statuses
 *   GET /projections/json/PlayerGameProjectionStatsByDate/{date} — player prop projections
 *   GET /stats/json/PlayerGameStatsByDate/{date}  — completed game box scores (for grading)
 *   GET /scores/json/Standings/{season}           — season standings (for context modifiers)
 *
 * API key passes via header: Ocp-Apim-Subscription-Key
 * Date format: yyyy-MMM-DD  (e.g. 2026-MAR-21)
 *
 * CRITICAL: DataHubTasklet calls getTodayGames() on every tick to build the pre-match gate.
 * If this call fails, ALL scraping is suspended for that tick (fail-safe: no data > bad data).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SportsDataService {

    // ── Config ────────────────────────────────────────────────────────────────
    @Value("${propiq.apis.sportsdata.key}")
    private String apiKey;

    @Value("${propiq.apis.sportsdata.base-url}")
    private String baseUrl;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    // ── Date formatter: SportsData.io uses "yyyy-MMM-DD" (e.g. 2026-MAR-21) ─
    private static final DateTimeFormatter SD_DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MMM-dd").withZone(ZoneId.of("America/Los_Angeles"));

    // ── Auth header name ──────────────────────────────────────────────────────
    private static final String AUTH_HEADER = "Ocp-Apim-Subscription-Key";

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TODAY'S GAMES — Pre-match gate data source
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns today's MLB schedule from SportsData.io.
     *
     * Each game map contains at minimum:
     *   GameID   — unique integer game identifier
     *   Status   — "Scheduled" | "Warmup" | "InProgress" | "Final" | "Postponed"
     *   HomeTeam — 3-letter abbreviation (e.g. "LAD")
     *   AwayTeam — 3-letter abbreviation
     *   DateTime — ISO-8601 game start time
     *   HomeTeamRuns / AwayTeamRuns — current score (null if not started)
     *   Inning   — current inning (null if not started)
     *
     * DataHubTasklet filters this to Status == "Scheduled" | "Warmup" to build the pre-match gate.
     */
    public List<Map<String, Object>> getTodayGames() {
        String dateStr = SD_DATE_FMT.format(LocalDate.now().atStartOfDay(ZoneId.of("America/Los_Angeles")))
                .toUpperCase();
        String url = baseUrl + "/scores/json/GamesByDate/" + dateStr;

        log.debug("[SportsData] GET GamesByDate/{}", dateStr);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Map<String, Object>> games = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<List<Map<String, Object>>>() {});
                log.info("[SportsData] GamesByDate: {} games found for {}", games.size(), dateStr);
                return games;
            }

        } catch (HttpClientErrorException.TooManyRequests e) {
            log.warn("[SportsData] 429 Rate limit hit on GamesByDate. DataHub tick will skip.");
        } catch (HttpClientErrorException.Unauthorized e) {
            log.error("[SportsData] 401 Unauthorized — check SPORTSDATA_API_KEY env var.");
        } catch (Exception e) {
            log.error("[SportsData] GamesByDate failed: {}", e.getMessage());
        }

        return Collections.emptyList();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // PLAYER PROJECTIONS — ML feature enrichment
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns projected player stats for today's games as a map keyed by PlayerID.
     *
     * Key projection fields used by PropEdge enrichment:
     *   PlayerID, Name, Team, Position
     *   ProjectedAtBats, ProjectedHits, ProjectedHomeRuns, ProjectedRBIs
     *   ProjectedStrikeouts (pitchers), ProjectedWalks, ProjectedInningsPitched
     *   DraftKingsSalary (proxy for lineup slot importance)
     *
     * Returns Map<String, Object> where key = String(PlayerID).
     */
    public Map<String, Object> getPlayerProjections() {
        String dateStr = SD_DATE_FMT.format(LocalDate.now().atStartOfDay(ZoneId.of("America/Los_Angeles")))
                .toUpperCase();
        String url = baseUrl + "/projections/json/PlayerGameProjectionStatsByDate/" + dateStr;

        log.debug("[SportsData] GET PlayerGameProjectionStatsByDate/{}", dateStr);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Map<String, Object>> projections = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<List<Map<String, Object>>>() {});

                // Re-key by PlayerID string for O(1) lookup in DataEnricher
                Map<String, Object> byPlayerId = new LinkedHashMap<>();
                for (Map<String, Object> proj : projections) {
                    Object pid = proj.get("PlayerID");
                    if (pid != null) {
                        byPlayerId.put(String.valueOf(pid), proj);
                    }
                }
                log.info("[SportsData] Projections: {} players for {}", byPlayerId.size(), dateStr);
                return byPlayerId;
            }

        } catch (HttpClientErrorException.TooManyRequests e) {
            log.warn("[SportsData] 429 Rate limit hit on PlayerProjections.");
        } catch (Exception e) {
            log.error("[SportsData] PlayerProjections failed: {}", e.getMessage());
        }

        return Collections.emptyMap();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // PLAYER GAME STATS — used by GradingTasklet for result verification
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns actual player game stats for a given date (defaults to yesterday).
     * Called by GradingTasklet at 1:05 AM PT after box scores finalize.
     *
     * Returns Map<String, Object> keyed by String(PlayerID).
     */
    public Map<String, Object> getPlayerGameStats(LocalDate date) {
        String dateStr = SD_DATE_FMT.format(date.atStartOfDay(ZoneId.of("America/Los_Angeles")))
                .toUpperCase();
        String url = baseUrl + "/stats/json/PlayerGameStatsByDate/" + dateStr;

        log.debug("[SportsData] GET PlayerGameStatsByDate/{}", dateStr);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Map<String, Object>> stats = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<List<Map<String, Object>>>() {});

                Map<String, Object> byPlayerId = new LinkedHashMap<>();
                for (Map<String, Object> stat : stats) {
                    Object pid = stat.get("PlayerID");
                    if (pid != null) {
                        byPlayerId.put(String.valueOf(pid), stat);
                    }
                }
                log.info("[SportsData] GameStats: {} player results for {}", byPlayerId.size(), dateStr);
                return byPlayerId;
            }

        } catch (Exception e) {
            log.error("[SportsData] PlayerGameStats failed for {}: {}", dateStr, e.getMessage());
        }

        return Collections.emptyMap();
    }

    /**
     * Convenience overload — defaults to yesterday's stats (for nightly grading).
     */
    public Map<String, Object> getYesterdayGameStats() {
        return getPlayerGameStats(LocalDate.now(ZoneId.of("America/Los_Angeles")).minusDays(1));
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SEASON SCHEDULE — rest days + timezone change tracking
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns the full season schedule for the given year.
     * Used by GetawayAgent to calculate hours_rest, time_zone_change, and
     * previous_game_innings for the GetawayAgent PropEdge fields.
     *
     * Results are cached in Redis by EnrichmentService — this should only
     * be called once per day (or once per season for static data).
     */
    public List<Map<String, Object>> getSeasonSchedule(int season) {
        String url = baseUrl + "/scores/json/Games/" + season;

        log.debug("[SportsData] GET Season Schedule/{}", season);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Map<String, Object>> schedule = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<List<Map<String, Object>>>() {});
                log.info("[SportsData] Season {}: {} games in schedule.", season, schedule.size());
                return schedule;
            }

        } catch (Exception e) {
            log.error("[SportsData] Season schedule fetch failed for {}: {}", season, e.getMessage());
        }

        return Collections.emptyList();
    }

    /**
     * Returns completed game details for a specific GameID.
     * Used to determine previous_game_innings (extra innings detection).
     */
    public Optional<Map<String, Object>> getGameById(String gameId) {
        String url = baseUrl + "/scores/json/BoxScore/" + gameId;

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, buildAuthEntity(), String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<String, Object> box = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<Map<String, Object>>() {});
                return Optional.of(box);
            }

        } catch (Exception e) {
            log.warn("[SportsData] BoxScore fetch failed for gameId={}: {}", gameId, e.getMessage());
        }

        return Optional.empty();
    }

    // ── Auth helper ───────────────────────────────────────────────────────────

    private HttpEntity<Void> buildAuthEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(AUTH_HEADER, apiKey);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        return new HttpEntity<>(headers);
    }
}
