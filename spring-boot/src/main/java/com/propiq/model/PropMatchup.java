package com.propiq.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * A single prop betting opportunity — combines odds, player context,
 * and pre-calculated probabilities for consumption by all 10 agents.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PropMatchup {

    private String id;               // gameId_playerId_propType e.g. "MLB_20260326_NYY-BOS_judge_hits"
    private String gameId;
    private String player;           // Display name: "Aaron Judge"
    private String playerId;
    private String team;             // "NYY"
    private String opposingPitcher;  // Display name
    private String opposingPitcherId;
    private String umpireId;
    private String propType;         // "Hits", "Strikeouts", "TotalBases", "HomeRuns"
    private double line;             // Over/Under target, e.g. 1.5

    // Best odds across DK/FD/BetMGM/bet365
    private String bestOdds;         // American format: "-115", "+105"
    private String bestOddsBook;     // Which book offers best odds
    private Map<String, Integer> marketOdds; // book → American odds for over

    // Pre-calculated by DataHubTasklet
    private double noVigProbOver;    // True probability of OVER (no juice)
    private double noVigProbUnder;
    private double liveOdds;         // Current live decimal odds (in-game)

    // Context flags
    private boolean isUnder;         // True if agents should bet UNDER
    private boolean isLive;          // True if game is in progress
    private boolean lineupConfirmed; // True if player is in confirmed lineup (top 4)

    // DFS platform mapping
    private String dfsPlatform;      // "PrizePicks" or "Underdog"
    private String dfsPickType;      // "OVER" or "UNDER"

    public String getOppositeDirection() {
        return isUnder ? "OVER" : "UNDER";
    }
}
