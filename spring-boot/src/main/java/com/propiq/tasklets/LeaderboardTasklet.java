package com.propiq.tasklets;

import com.propiq.service.PostgresService;
import com.propiq.service.RedisCacheManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * LeaderboardTasklet — 14-day trailing ROI capital allocator.
 *
 * Runs every 60 seconds. Reads Postgres settlement ledger, ranks
 * the 10 agents by ROI, and pushes capital multipliers to Redis:
 *
 *   Top 3 profitable agents  → 2.0× (max bet size)
 *   Middle agents (ROI > 0)  → 1.0× (base bet size)
 *   Cold agents (ROI < -5%)  → 0.5× (protect bankroll)
 *
 * AgentTasklet reads these multipliers before every dispatch.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LeaderboardTasklet implements Tasklet {

    @Value("${propiq.agents.base-unit:1.0}")
    private double baseUnit;

    @Value("${propiq.agents.top-tier-multiplier:2.0}")
    private double topTierMultiplier;

    @Value("${propiq.agents.cold-tier-multiplier:0.5}")
    private double coldTierMultiplier;

    private final PostgresService postgresService;
    private final RedisCacheManager redisCache;

    @Scheduled(fixedRate = 60000)
    public void executeScheduled() {
        try {
            execute(null, null);
        } catch (Exception e) {
            log.error("LeaderboardTasklet failed: {}", e.getMessage());
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.debug("🏆 LeaderboardTasklet — ranking 10 agents by 14-day ROI");

        Map<String, Double> agentRois = postgresService.getAgentRoisTrailing14Days();

        if (agentRois.isEmpty()) {
            log.info("No settled data yet (Spring Training). Default 1.0× allocation.");
            applyDefaultWeights();
            return RepeatStatus.FINISHED;
        }

        // Sort descending by ROI
        List<Map.Entry<String, Double>> sorted = agentRois.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .collect(Collectors.toList());

        log.info("═══════════════════════════════════════");
        log.info("  PropIQ 10-Agent Leaderboard (14-day) ");
        log.info("═══════════════════════════════════════");

        int rank = 1;
        for (Map.Entry<String, Double> entry : sorted) {
            String agentName = entry.getKey();
            double roi       = entry.getValue();
            double multiplier;
            String tier;

            if (rank <= 3 && roi > 0.0) {
                multiplier = topTierMultiplier;
                tier = "🥇 TOP TIER";
            } else if (roi < -5.0) {
                multiplier = coldTierMultiplier;
                tier = "❄️  COLD TIER";
            } else {
                multiplier = baseUnit;
                tier = "⚖️  MID TIER ";
            }

            redisCache.updateAgentCapitalWeight(agentName, multiplier);
            log.info("  {} | {:<25} | ROI: {:+.1f}% | Capital: {}×",
                    tier, agentName, roi, multiplier);
            rank++;
        }

        // Ensure any agents not yet in Postgres get default weight
        List<String> allAgents = List.of(
            "EV_Hunter", "Under_Machine", "Three_Leg_Correlated", "Standard_Parlay",
            "Live_Agent", "Arb_Agent", "Fade_Agent", "Umpire_Agent", "F5_Agent", "Live_Micro_Agent"
        );
        for (String agent : allAgents) {
            if (!agentRois.containsKey(agent)) {
                redisCache.updateAgentCapitalWeight(agent, baseUnit);
            }
        }

        log.info("═══════════════════════════════════════");
        return RepeatStatus.FINISHED;
    }

    private void applyDefaultWeights() {
        List<String> allAgents = List.of(
            "EV_Hunter", "Under_Machine", "Three_Leg_Correlated", "Standard_Parlay",
            "Live_Agent", "Arb_Agent", "Fade_Agent", "Umpire_Agent", "F5_Agent", "Live_Micro_Agent"
        );
        allAgents.forEach(a -> redisCache.updateAgentCapitalWeight(a, baseUnit));
    }
}
