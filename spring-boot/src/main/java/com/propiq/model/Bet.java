package com.propiq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable bet record sent to the Kafka bet_queue.
 * Uses a manual Builder pattern to avoid Lombok @Builder conflicts with Jackson.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Bet {

    private String betId;
    private String agentName;
    private String player;
    private String propType;
    private double targetLine;
    private String direction;      // "OVER" or "UNDER"
    private String odds;           // American format
    private double calculatedEv;
    private double xgboostProb;
    private double kellyFraction;
    private double unitSizing;
    private int requiredLegs;
    private String dfsPlatform;   // PrizePicks or Underdog Fantasy
    private Instant placedAt;

    // ── Builder ───────────────────────────────────────────────────────────────
    public static class Builder {
        private final Bet bet = new Bet();

        public Builder() {
            bet.betId = UUID.randomUUID().toString();
            bet.placedAt = Instant.now();
        }

        public Builder withAgent(String agent)         { bet.agentName = agent;       return this; }
        public Builder withPlayer(String player)       { bet.player = player;          return this; }
        public Builder withPropType(String pt)         { bet.propType = pt;            return this; }
        public Builder withTargetLine(double line)     { bet.targetLine = line;        return this; }
        public Builder withDirection(String dir)       { bet.direction = dir;          return this; }
        public Builder withOdds(String odds)           { bet.odds = odds;              return this; }
        public Builder withCalculatedEv(double ev)     { bet.calculatedEv = ev;        return this; }
        public Builder withXgboostProb(double p)       { bet.xgboostProb = p;          return this; }
        public Builder withKellyFraction(double k)     { bet.kellyFraction = k;        return this; }
        public Builder withUnitSizing(double units)    { bet.unitSizing = units;       return this; }
        public Builder withRequiredLegs(int legs)      { bet.requiredLegs = legs;      return this; }
        public Builder withDfsPlatform(String plat)    { bet.dfsPlatform = plat;       return this; }

        public Bet build() { return bet; }
    }
}
