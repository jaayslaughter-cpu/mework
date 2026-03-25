package com.propiq.repository;

import com.propiq.model.BetRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

/**
 * Spring Data JPA repository for the bet_ledger Postgres table.
 * CLV tracking: opening_line, placed_line, closing_line all stored here.
 */
@Repository
public interface BetLedgerRepository extends JpaRepository<BetRecord, Long> {

    @Query("SELECT b FROM BetRecord b WHERE b.settlementDate IS NULL AND DATE(b.createdAt) = :date")
    List<BetRecord> findUnsettledByDate(@Param("date") LocalDate date);

    @Query("""
        SELECT b.agentName as agent,
               SUM(b.profitLoss) / NULLIF(SUM(b.unitsRisked), 0) * 100 as roi
        FROM BetRecord b
        WHERE b.settlementDate >= :since AND b.settlementDate IS NOT NULL
        GROUP BY b.agentName
        """)
    List<AgentRoiProjection> findAgentRoisSince(@Param("since") LocalDate since);

    interface AgentRoiProjection {
        String getAgent();
        Double getRoi();
    }
}
