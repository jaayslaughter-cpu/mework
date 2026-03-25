package com.propiq.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Master state object stored in Redis as "mlb_hub" with 15s TTL.
 * All 10 agents and the BetAnalyzer read exclusively from this object —
 * they never call external APIs directly.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MlbHubState {

    private Instant timestamp;

    // ── API Data ─────────────────────────────────────────────────────────────
    /** Active prop matchups (from SportsData.io + The Odds API) */
    private List<PropMatchup> activeProps;

    /** Raw live boxscore data keyed by gameId (Tank01, every 15s) */
    private Map<String, GameBoxscore> liveBoxscores;

    // ── Scraper Data (Apify, every 5 min) ────────────────────────────────────
    /** Umpire statistics: calledStrikePct, kPct, accuracy, homeBias */
    private Map<String, UmpireStats> umpireStats;

    /** Public betting percentages keyed by teamOrPropKey */
    private Map<String, PublicBettingData> publicBettingData;

    /** Weather data keyed by gameId */
    private Map<String, WeatherData> weatherData;

    /** Player injury status keyed by playerId */
    private Map<String, InjuryStatus> injuryStatuses;

    /** Pitcher advanced stats: FIP, SwStr%, CSW%, SIERA, keyed by pitcherId */
    private Map<String, PitcherStats> pitcherStats;

    /** Baseball Savant pitch arsenal: whiffPct, zonePct, chasePct, xwOBA */
    private Map<String, SavantData> savantData;

    /** Bullpen fatigue scores 0–4 keyed by teamId */
    private Map<String, Integer> bullpenFatigueScores;

    /** Batter vs Pitcher xwOBA by handedness, keyed by "batterId_pitcherId" */
    private Map<String, Double> bvpXwoba;

    /** Confirmed lineup positions keyed by playerId */
    private Map<String, Integer> lineupPositions;

    /** Projected starters: daysRest, pitchCountYesterday keyed by pitcherId */
    private Map<String, StarterProjection> starterProjections;

    // ── Opening Line Registry ─────────────────────────────────────────────────
    /** First-seen odds for CLV tracking keyed by "gameId_playerId_propType" */
    private Map<String, LineSnapshot> openingLines;

    // ── Convenience lookup methods ────────────────────────────────────────────

    public double getPitcherFip(String pitcherId) {
        PitcherStats ps = pitcherStats != null ? pitcherStats.get(pitcherId) : null;
        return ps != null ? ps.getFip() : 4.50;
    }

    public double getPitcherSwStr(String pitcherId) {
        PitcherStats ps = pitcherStats != null ? pitcherStats.get(pitcherId) : null;
        return ps != null ? ps.getSwStrPct() : 10.0;
    }

    public double getPublicBetPct(String key) {
        PublicBettingData p = publicBettingData != null ? publicBettingData.get(key) : null;
        return p != null ? p.getPublicBetPct() : 50.0;
    }

    public double getLineMovement(String propId) {
        if (openingLines == null || !openingLines.containsKey(propId)) return 0.0;
        LineSnapshot opening = openingLines.get(propId);
        PropMatchup current = activeProps != null
                ? activeProps.stream().filter(p -> p.getId().equals(propId)).findFirst().orElse(null)
                : null;
        if (current == null) return 0.0;
        return Math.abs(current.getNoVigProbOver() - opening.getNoVigProbOver()) * 100.0;
    }

    public boolean isPlayerScratched(String playerName) {
        if (injuryStatuses == null) return false;
        return injuryStatuses.values().stream()
                .anyMatch(i -> i.getPlayerName().equalsIgnoreCase(playerName)
                        && ("OUT".equalsIgnoreCase(i.getStatus()) || "SCRATCHED".equalsIgnoreCase(i.getStatus())));
    }

    public boolean didStadiumRoofStateChange(String gameId) {
        // DataHubTasklet sets a flag if roof state changed vs previous cycle
        WeatherData w = weatherData != null ? weatherData.get(gameId) : null;
        return w != null && w.isRoofStateChanged();
    }

    public boolean isGameLive(String gameId) {
        GameBoxscore bs = liveBoxscores != null ? liveBoxscores.get(gameId) : null;
        return bs != null && "IN_PROGRESS".equalsIgnoreCase(bs.getStatus());
    }

    public int getPitcherPitchCount(String pitcherId) {
        GameBoxscore bs = liveBoxscores != null
                ? liveBoxscores.values().stream()
                        .filter(g -> g.getHomePitcherId().equals(pitcherId) || g.getAwayPitcherId().equals(pitcherId))
                        .findFirst().orElse(null)
                : null;
        if (bs == null) return 0;
        return bs.getHomePitcherId().equals(pitcherId) ? bs.getHomePitcherPitchCount() : bs.getAwayPitcherPitchCount();
    }

    public String getMatchupContextString(PropMatchup prop) {
        StringBuilder sb = new StringBuilder();
        PitcherStats ps = pitcherStats != null ? pitcherStats.get(prop.getOpposingPitcherId()) : null;
        UmpireStats us = umpireStats != null ? umpireStats.get(prop.getUmpireId()) : null;
        WeatherData w = weatherData != null ? weatherData.get(prop.getGameId()) : null;
        if (ps != null) sb.append(String.format("FIP %.2f | SwStr%% %.1f%% | ", ps.getFip(), ps.getSwStrPct()));
        if (us != null) sb.append(String.format("Ump K%% %.1f%% | ", us.getCalledStrikePct()));
        if (w != null)  sb.append(String.format("Wind %d mph %s | ", w.getWindSpeed(), w.getWindDirection()));
        sb.append(String.format("Public %.0f%%", getPublicBetPct(prop.getTeam())));
        return sb.toString();
    }

    // ── Inner model classes (kept co-located for clarity) ─────────────────────

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class UmpireStats {
        private String umpireId;
        private String umpieName;
        private double calledStrikePct;   // Threshold: < 66% = tight zone
        private double kPct;
        private double accuracy;
        private double homeBias;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PublicBettingData {
        private double publicBetPct;
        private double moneyPct;
        private double betVolume;
        private boolean reverseLm;       // Reverse line movement flag
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class WeatherData {
        private int windSpeed;
        private String windDirection;     // "out_lf", "out_rf", "in", "calm"
        private int tempF;
        private int humidity;
        private boolean roofClosed;
        private boolean roofStateChanged; // True if roof state changed vs last cycle
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class InjuryStatus {
        private String playerId;
        private String playerName;
        private String status;            // ACTIVE, DTD, OUT, SCRATCHED
        private int daysSinceInjury;
        private double scratchProb;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PitcherStats {
        private String pitcherId;
        private double fip;
        private double swStrPct;
        private double cswPct;
        private double siera;
        private double era;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class SavantData {
        private String playerId;
        private double xwoba;
        private double hardHitPct;
        private double barrelPct;
        private double exitVelo;
        private double sliderWhiffPct;
        private double fastballZonePct;
        private double chasePct;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class StarterProjection {
        private String pitcherId;
        private int daysRest;
        private int pitchCountYesterday;
        private boolean confirmed;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class LineSnapshot {
        private double noVigProbOver;
        private double noVigProbUnder;
        private long recordedAt;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class GameBoxscore {
        private String gameId;
        private String status;
        private String homePitcherId;
        private String awayPitcherId;
        private int homePitcherPitchCount;
        private int awayPitcherPitchCount;
        private Map<String, PlayerGameStats> playerStats;
        private boolean isFinal;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PlayerGameStats {
        private String playerId;
        private String playerName;
        private double hits;
        private double strikeouts;
        private double totalBases;
        private double homeRuns;
        private double rbis;
        private double walks;
    }
}
