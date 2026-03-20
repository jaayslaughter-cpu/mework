package com.propiq.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Tracks a live DFS multi-leg slip (PrizePicks/Underdog).
 * AgentTasklet uses this to evaluate live hedge opportunities when
 * 2-of-3 (or 4-of-5) legs have already won.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ActiveSlip {

    private String slipId;
    private String dfsPlatform;      // "PrizePicks" or "Underdog"
    private int totalLegs;
    private int wonLegs;
    private int lostLegs;
    private int pendingLegs;
    private double potentialPayout;  // In units
    private double entryUnits;

    /** The one remaining leg we haven't settled yet */
    private PropMatchup remainingLeg;
}
