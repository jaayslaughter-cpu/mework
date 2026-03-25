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

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TheOddsApiService — The Odds API v4 client with automatic key rotation.
 *
 * Endpoints used:
 *   GET /sports/baseball_mlb/odds/                        — moneylines, spreads, game totals
 *   GET /sports/baseball_mlb/events/{eventId}/odds        — player props for a specific game
 *   GET /sports/baseball_mlb/scores/                      — game scores (live status check)
 *   GET /remaining-requests                               — quota check (not for polling)
 *
 * KEY ROTATION:
 *   Two API keys are configured: primary and backup.
 *   If the primary key returns 401 (invalid) or 429 (quota exceeded),
 *   the service automatically switches to the backup key for that call.
 *   A sticky flag prevents thrashing between keys.
 *
 * QUOTA PROTECTION (CRITICAL):
 *   DataHubTasklet enforces a 7-minute minimum between calls to this service.
 *   The Odds API allocates ~500 requests/month on the free tier — each call
 *   can consume 1-10 credits depending on the number of bookmakers requested.
 *   We request 4 bookmakers max (draftkings, fanduel, pinnacle, betmgm).
 *
 * BOOKMAKERS:
 *   Sharp reference: pinnacle (Pinnacle Sportsbook) — no vig, sharpest lines
 *   Retail target: draftkings, fanduel — where value gaps appear vs Pinnacle
 *   Supplemental: betmgm, caesars — cross-reference for steam detection
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TheOddsApiService {

    // ── Config ────────────────────────────────────────────────────────────────
    @Value("${propiq.apis.theodds.key-primary}")
    private String primaryKey;

    @Value("${propiq.apis.theodds.key-backup}")
    private String backupKey;

    @Value("${propiq.apis.theodds.base-url}")
    private String baseUrl;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    // Sticky flag: true = use backup key (set on primary 429/401, reset on backup success)
    private final AtomicBoolean useBackupKey = new AtomicBoolean(false);

    // ── Bookmakers (4 max for quota protection) ───────────────────────────────
    private static final String BOOKMAKERS = "pinnacle,draftkings,fanduel,betmgm";

    // ── Player prop markets for MLB ───────────────────────────────────────────
    private static final String PLAYER_PROP_MARKETS =
            "pitcher_strikeouts,batter_hits,batter_total_bases,batter_rbis," +
            "batter_runs_scored,batter_stolen_bases,batter_home_runs," +
            "pitcher_outs,pitcher_walks,pitcher_hits_allowed";

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MLB GAME ODDS — primary enrichment call (every 7 min via DataHubTasklet)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns MLB moneyline, spread, and total odds across 4 bookmakers.
     *
     * Response is a flat map keyed by The Odds API event ID. Each value contains:
     *   id           — event ID (matches to game for prop lookup)
     *   homeTeam     — team name
     *   awayTeam     — team name
     *   commenceTime — ISO-8601 start time
     *   bookmakers[] — array of bookmaker objects with markets → outcomes → price/point
     *
     * LineValueScanner in the Python tier consumes this to calculate:
     *   - Pinnacle true price (no-vig reference)
     *   - DraftKings / FanDuel retail price
     *   - edge_pct = (retail_price - pinnacle_price) / pinnacle_price
     *
     * Returns Map<String, Object> keyed by eventId.
     */
    public Map<String, Object> getMlbOdds() {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl + "/sports/baseball_mlb/odds/")
                .queryParam("apiKey", activeKey())
                .queryParam("regions", "us")
                .queryParam("markets", "h2h,totals")
                .queryParam("bookmakers", BOOKMAKERS)
                .queryParam("oddsFormat", "american")
                .toUriString();

        log.debug("[TheOddsApi] GET /sports/baseball_mlb/odds/ (key={})",
                  useBackupKey.get() ? "backup" : "primary");

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, jsonAcceptEntity(), String.class);

            logRemainingRequests(response.getHeaders());

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Map<String, Object>> events = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<List<Map<String, Object>>>() {});

                Map<String, Object> byEventId = new LinkedHashMap<>();
                for (Map<String, Object> event : events) {
                    Object eid = event.get("id");
                    if (eid != null) byEventId.put(String.valueOf(eid), event);
                }
                log.info("[TheOddsApi] MLB odds: {} events across {} bookmakers.",
                         byEventId.size(), BOOKMAKERS);
                useBackupKey.set(false); // success — revert to primary
                return byEventId;
            }

        } catch (HttpClientErrorException e) {
            handleApiError("getMlbOdds", e);
        } catch (Exception e) {
            log.error("[TheOddsApi] getMlbOdds failed: {}", e.getMessage());
        }

        return Collections.emptyMap();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // PLAYER PROPS — per-game prop odds (SteamAgent + LineValueAgent)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns player prop odds for a specific game event ID.
     *
     * The Odds API event ID is retrieved from getMlbOdds() results.
     * Each bookmaker provides Over/Under lines for the listed prop markets.
     *
     * These are the raw odds_over / odds_under values stored in PropEdge
     * and used by odds_math.calculate_true_probability() to strip the vig.
     *
     * NOTE: Player prop calls consume MORE quota credits than game odds calls.
     * Only call for games where the ML pipeline has active edge candidates.
     *
     * Returns Map keyed by market name → list of player outcomes.
     */
    public Map<String, Object> getMlbPlayerProps(String eventId) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl + "/sports/baseball_mlb/events/" + eventId + "/odds")
                .queryParam("apiKey", activeKey())
                .queryParam("regions", "us")
                .queryParam("markets", PLAYER_PROP_MARKETS)
                .queryParam("bookmakers", BOOKMAKERS)
                .queryParam("oddsFormat", "american")
                .toUriString();

        log.debug("[TheOddsApi] GET /events/{}/odds (player props)", eventId);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, jsonAcceptEntity(), String.class);

            logRemainingRequests(response.getHeaders());

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<String, Object> eventOdds = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<Map<String, Object>>() {});
                log.info("[TheOddsApi] Player props fetched for eventId={}.", eventId);
                useBackupKey.set(false);
                return eventOdds;
            }

        } catch (HttpClientErrorException e) {
            handleApiError("getMlbPlayerProps[" + eventId + "]", e);
        } catch (Exception e) {
            log.error("[TheOddsApi] getMlbPlayerProps failed for {}: {}", eventId, e.getMessage());
        }

        return Collections.emptyMap();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SCORES / GAME STATUS — secondary pre-match confirmation
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Returns recent MLB game scores including completed and in-progress games.
     * Used as a secondary pre-match gate alongside SportsData.io.
     *
     * Parameters:
     *   daysFrom — how many days back to include (default 1 = just today + yesterday)
     *
     * Returns list of game score maps with: id, homeTeam, awayTeam, scores[],
     * completed (boolean), commenceTime.
     */
    public List<Map<String, Object>> getMlbScores(int daysFrom) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl + "/sports/baseball_mlb/scores/")
                .queryParam("apiKey", activeKey())
                .queryParam("daysFrom", daysFrom)
                .toUriString();

        log.debug("[TheOddsApi] GET /sports/baseball_mlb/scores/?daysFrom={}", daysFrom);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.GET, jsonAcceptEntity(), String.class);

            logRemainingRequests(response.getHeaders());

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Map<String, Object>> scores = objectMapper.readValue(
                        response.getBody(),
                        new TypeReference<List<Map<String, Object>>>() {});
                log.debug("[TheOddsApi] MLB scores: {} games", scores.size());
                useBackupKey.set(false);
                return scores;
            }

        } catch (HttpClientErrorException e) {
            handleApiError("getMlbScores", e);
        } catch (Exception e) {
            log.error("[TheOddsApi] getMlbScores failed: {}", e.getMessage());
        }

        return Collections.emptyList();
    }

    // ── Key rotation logic ────────────────────────────────────────────────────

    private String activeKey() {
        return useBackupKey.get() ? backupKey : primaryKey;
    }

    private void handleApiError(String methodName, HttpClientErrorException e) {
        if (e.getStatusCode().value() == 401) {
            if (!useBackupKey.get()) {
                log.warn("[TheOddsApi] {} — Primary key 401. Switching to backup key.", methodName);
                useBackupKey.set(true);
            } else {
                log.error("[TheOddsApi] {} — Both keys returned 401. Check THEODDS_API_KEY env vars.", methodName);
            }
        } else if (e.getStatusCode().value() == 429) {
            if (!useBackupKey.get()) {
                log.warn("[TheOddsApi] {} — Primary key quota exceeded (429). Switching to backup.", methodName);
                useBackupKey.set(true);
            } else {
                log.warn("[TheOddsApi] {} — Both keys quota exceeded. Skipping Odds API this tick.", methodName);
            }
        } else {
            log.error("[TheOddsApi] {} HTTP {}: {}", methodName, e.getStatusCode().value(), e.getMessage());
        }
    }

    // ── Quota monitoring ──────────────────────────────────────────────────────

    private void logRemainingRequests(HttpHeaders headers) {
        String remaining = headers.getFirst("x-requests-remaining");
        String used      = headers.getFirst("x-requests-used");
        if (remaining != null || used != null) {
            log.info("[TheOddsApi] Quota — remaining: {}, used: {}", remaining, used);
            // Warn proactively when getting low
            if (remaining != null && Integer.parseInt(remaining) < 50) {
                log.warn("[TheOddsApi] ⚠️  Only {} requests remaining on active key!", remaining);
            }
        }
    }

    // ── Generic auth entity ───────────────────────────────────────────────────

    private HttpEntity<Void> jsonAcceptEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        return new HttpEntity<>(headers);
    }
}
