package com.propiq.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * JPA entity mapped to the bet_ledger table.
 * Used by GradingTasklet for settlement and LeaderboardTasklet for ROI.
 */
@Entity
@Table(name = "bet_ledger")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BetRecord {

    @Id
    @Column(name = "bet_id")
    private String id;

    @Column(name = "market_id")
    private Integer marketId;

    @Column(name = "agent_name", nullable = false)
    private String agentName;

    @Column(name = "direction", nullable = false)
    private String direction;       // OVER, UNDER

    @Column(name = "player_name")
    private String playerName;

    @Column(name = "prop_type")
    private String propType;        // Hits, Strikeouts, TotalBases

    @Column(name = "game_id")
    private String gameId;

    @Column(name = "target_line")
    private BigDecimal targetLine;

    @Column(name = "units_risked")
    private BigDecimal unitsRisked;

    @Column(name = "kelly_fraction")
    private BigDecimal kellyFraction;

    @Column(name = "placed_odds")
    private BigDecimal placedOdds;

    @Column(name = "placed_no_vig_prob")
    private BigDecimal placedNoVigProb;

    @Column(name = "xgboost_prob")
    private BigDecimal xgboostProb;

    @Column(name = "ev_pct")
    private BigDecimal evPct;

    @Column(name = "status")
    private String status;          // PENDING, WIN, LOSS, PUSH, ABORTED

    @Column(name = "profit_loss")
    private BigDecimal profitLoss;

    @Column(name = "closing_no_vig_prob")
    private BigDecimal closingNoVigProb;

    @Column(name = "clv_pct")
    private BigDecimal clvPct;

    @Column(name = "placed_at")
    private Instant placedAt;

    @Column(name = "settled_at")
    private Instant settledAt;

    public double getUnitsRiskedDouble() {
        return unitsRisked != null ? unitsRisked.doubleValue() : 0.0;
    }

    public double getTargetLineDouble() {
        return targetLine != null ? targetLine.doubleValue() : 0.0;
    }

    public double getPlacedOddsDouble() {
        return placedOdds != null ? placedOdds.doubleValue() : 0.0;
    }
}
