package com.propiq.tasklets;

import com.propiq.model.*;
import com.propiq.service.NoVigCalculator;
import com.propiq.service.RedisCacheManager;
import com.propiq.service.XGBoostModelService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * AgentTasklet — The 10-Agent XGBoost Army.
 *
 * Executes every 30s. For each active prop:
 *   1. Strips the vig → true no-vig probability
 *   2. Queries Python XGBoost microservice → model probability
 *   3. Routes to the correct agent(s) based on strategy thresholds
 *   4. Sizes each bet using Quarter-Kelly × Leaderboard multiplier
 *   5. Sends to Kafka bet_queue (or abort_queue for late scratches)
 *
 * 10 Agents:
 *   1. EV_Hunter             — EV > 5%
 *   2. Under_Machine         — FIP < 3.50 pitcher duels, 58% win rate
 *   3. Three_Leg_Correlated  — Exactly 3 correlated legs, 8-12% edge
 *   4. Standard_Parlay       — Game outcomes, 2-3% ROI
 *   5. Live_Agent            — In-play line movement > 5%
 *   6. Arb_Agent             — Cross-book arb > 1%
 *   7. Fade_Agent            — Public > 70% → opposite (RLM detection)
 *   8. Umpire_Agent          — Ump called K% > 22% + FIP < 3.80
 *   9. F5_Agent              — First 5 innings unders, FIP < 3.50
 *  10. Live_Micro_Agent      — Pitcher > 95 pitches → batter hits over
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AgentTasklet implements Tasklet {

    private static final double KELLY_FRACTION = 0.25;

    @Value("${propiq.agents.ev-threshold:5.0}")
    private double evThreshold;

    @Value("${propiq.thresholds.min-fip:3.80}")
    private double minFip;

    @Value("${propiq.thresholds.min-ump-k-pct:22.0}")
    private double minUmpKPct;

    @Value("${propiq.thresholds.max-public-pct:70.0}")
    private double maxPublicPct;

    private final RedisCacheManager redisCache;
    private final XGBoostModelService xgboostService;
    private final NoVigCalculator noVigCalc;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedRate = 30000)
    public void executeScheduled() {
        try {
            execute(null, null);
        } catch (Exception e) {
            log.error("AgentTasklet execution failure: {}", e.getMessage());
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.debug("⚔️ AgentTasklet cycle — 10 agents active");

        MlbHubState hub = redisCache.get("mlb_hub", MlbHubState.class);
        if (hub == null || hub.getActiveProps() == null) {
            log.warn("mlb_hub is empty or stale. Waiting for DataHubTasklet...");
            return RepeatStatus.FINISHED;
        }

        // ── PROTOCOL 1: Emergency Abort (late scratches + roof changes) ───────
        for (Bet pendingBet : redisCache.getPendingBets()) {
            if (hub.isPlayerScratched(pendingBet.getPlayer())) {
                log.warn("🚨 ABORT: {} scratched post-placement. Routing to abort_queue.", pendingBet.getPlayer());
                kafkaTemplate.send("abort_queue", pendingBet);
                redisCache.removePendingBet(pendingBet.getBetId());
            } else if (hub.didStadiumRoofStateChange(pendingBet.getPlayer())) {
                log.warn("🚨 ABORT: Roof state changed — liquidating exposure for {}", pendingBet.getPlayer());
                kafkaTemplate.send("abort_queue", pendingBet);
                redisCache.removePendingBet(pendingBet.getBetId());
            }
        }

        // ── PROTOCOL 2: Live Hedge (protect 2/3 leg slips in-play) ───────────
        for (ActiveSlip slip : redisCache.getActiveDfsSlips()) {
            if (slip.getWonLegs() == slip.getTotalLegs() - 1 && slip.getPendingLegs() == 1) {
                PropMatchup finalLeg = slip.getRemainingLeg();
                if (finalLeg == null) continue;

                double liveProb = xgboostService.predictLiveProbability(finalLeg, hub.getLiveBoxscores());
                if (liveProb < 40.0) {
                    double hedgeAmount = slip.getPotentialPayout() / noVigCalc.parseAmerican(
                            String.valueOf(finalLeg.getLiveOdds() > 0
                                    ? (int) finalLeg.getLiveOdds() : -110));
                    Bet hedgeBet = new Bet.Builder()
                            .withAgent("Live_Hedge_Agent")
                            .withPlayer(finalLeg.getPlayer())
                            .withPropType(finalLeg.getPropType())
                            .withTargetLine(finalLeg.getLine())
                            .withDirection(finalLeg.getOppositeDirection())
                            .withUnitSizing(hedgeAmount)
                            .build();
                    kafkaTemplate.send("bet_queue", hedgeBet);
                    log.info("🛡️ HEDGE: {} prob degraded to {}%. Hedging {} units.",
                            finalLeg.getPlayer(),
                            String.format("%.1f", liveProb),
                            String.format("%.2f", hedgeAmount));
                }
            }
        }

        // ── PROTOCOL 3: Evaluate all active props through 10 agents ──────────
        for (PropMatchup prop : hub.getActiveProps()) {

            // Skip scratched players
            if (hub.isPlayerScratched(prop.getPlayer())) continue;

            // Calculate core metrics once (all 10 agents share them)
            double xgbProb    = xgboostService.predictProbability(prop, hub);
            double noVigProb  = noVigCalc.calculateTrueProb(prop.getMarketOdds());
            double decimalOdds = noVigCalc.parseAmerican(prop.getBestOdds());
            double trueEv     = noVigCalc.calculateEv(xgbProb, decimalOdds);
            double publicPct  = hub.getPublicBetPct(prop.getTeam());
            double pitcherFip = hub.getPitcherFip(prop.getOpposingPitcherId());
            double umpireKPct = getUmpireKPct(hub, prop.getUmpireId());
            int    bullpen    = hub.getBullpenFatigueScores() != null
                    ? hub.getBullpenFatigueScores().getOrDefault(prop.getTeam(), 0) : 0;

            // ── AGENT 1: EV Hunter (EV > 5%) ─────────────────────────────────
            if (trueEv > evThreshold) {
                dispatchBet("EV_Hunter", prop, xgbProb, noVigProb, decimalOdds, trueEv, 1,
                        pitcherFip, umpireKPct, publicPct, bullpen);
            }

            // ── AGENT 2: Under Machine (FIP < 3.50, xgbProb > 58%, under) ───
            if (prop.isUnder() && xgbProb > 58.0 && pitcherFip < 3.50 && bullpen < 3) {
                dispatchBet("Under_Machine", prop, xgbProb, noVigProb, decimalOdds, trueEv, 1,
                        pitcherFip, umpireKPct, publicPct, bullpen);
            }

            // ── AGENT 3: Three-Leg Correlated Parlay ─────────────────────────
            double correlation = xgboostService.checkCorrelation(prop, hub);
            if (correlation > 0.72 && trueEv > 8.0) {
                dispatchBet("Three_Leg_Correlated", prop, xgbProb, noVigProb, decimalOdds, trueEv, 3,
                        pitcherFip, umpireKPct, publicPct, bullpen);
            }

            // ── AGENT 4: Standard Parlay (game outcome + props, 2-4 legs) ────
            double gameProb = xgboostService.getGameOutcomeProb(prop.getGameId());
            if (trueEv > 3.5 && gameProb > 60.0) {
                dispatchBet("Standard_Parlay", prop, xgbProb, noVigProb, decimalOdds, trueEv, 2,
                        pitcherFip, umpireKPct, publicPct, bullpen);
            }

            // ── AGENT 5: Live Agent (line movement > 5%, XGBoost disagrees) ─
            double lineMove = hub.getLineMovement(prop.getId());
            if (lineMove > 5.0 && xgbProb > 65.0 && prop.isLive()) {
                dispatchBet("Live_Agent", prop, xgbProb, noVigProb, decimalOdds, trueEv, 1,
                        pitcherFip, umpireKPct, publicPct, bullpen);
            }

            // ── AGENT 6: Arb Agent (guaranteed > 1% cross-book) ─────────────
            double arbEdge = noVigCalc.calculateArbEdge(prop.getMarketOdds());
            if (arbEdge > 1.0) {
                dispatchBet("Arb_Agent", prop, 100.0 - (arbEdge / 2), noVigProb, decimalOdds, arbEdge, 2,
                        pitcherFip, umpireKPct, publicPct, bullpen);
            }

            // ── AGENT 7: Fade Agent (public > 70% → opposite + RLM) ──────────
            boolean isRlm = isReverseLm(hub, prop);
            if (publicPct > maxPublicPct && xgbProb > 55.0 && isRlm) {
                dispatchBet("Fade_Agent", prop, xgbProb, noVigProb, decimalOdds, trueEv, 1,
                        pitcherFip, umpireKPct, publicPct, bullpen);
                log.info("🔄 FADE: {} public={}% with RLM detected",
                        prop.getPlayer(), String.format("%.0f", publicPct));
            }

            // ── AGENT 8: Umpire Agent (called K% > 22% + FIP < 3.80) ─────────
            if (umpireKPct > minUmpKPct && pitcherFip < minFip
                    && "Strikeouts".equals(prop.getPropType()) && xgbProb > 58.0) {
                dispatchBet("Umpire_Agent", prop, xgbProb, noVigProb, decimalOdds, trueEv, 1,
                        pitcherFip, umpireKPct, publicPct, bullpen);
                log.info("🎯 UMPIRE: Ump K%={}% | FIP={} | Firing K over for {}",
                        String.format("%.1f", umpireKPct),
                        String.format("%.2f", pitcherFip),
                        prop.getPlayer());
            }

            // ── AGENT 9: F5 Agent (first 5 innings unders, FIP < 3.50) ───────
            if (prop.isUnder() && pitcherFip < 3.50 && isF5Prop(prop) && xgbProb > 55.0) {
                dispatchBet("F5_Agent", prop, xgbProb, noVigProb, decimalOdds, trueEv, 1,
                        pitcherFip, umpireKPct, publicPct, bullpen);
            }

            // ── AGENT 10: Live Micro Agent (pitcher > 95 pitches → batter hit over)
            if (prop.isLive() && "Hits".equals(prop.getPropType()) && xgbProb > 55.0) {
                int pitchCount = hub.getPitcherPitchCount(prop.getOpposingPitcherId());
                if (pitchCount > 95) {
                    dispatchBet("Live_Micro_Agent", prop, xgbProb, noVigProb, decimalOdds, trueEv, 1,
                            pitcherFip, umpireKPct, publicPct, bullpen);
                    log.info("🔥 FATIGUE EXPLOIT: {} at {} pitches. Live over on {} Hits.",
                            prop.getOpposingPitcher(), pitchCount, prop.getPlayer());
                }
            }
        }

        log.debug("✅ AgentTasklet cycle complete.");
        return RepeatStatus.FINISHED;
    }

    // ── Dispatch ─────────────────────────────────────────────────────────────

    private void dispatchBet(String agentName, PropMatchup prop,
                              double xgbProb, double noVigProb, double decimalOdds,
                              double ev, int legs,
                              double pitcherFip, double umpireKPct,
                              double publicPct, int bullpen) {

        double kellyFraction   = noVigCalc.calculateQuarterKelly(xgbProb, decimalOdds);
        double agentMultiplier = redisCache.getAgentCapitalWeight(agentName);
        double finalUnits      = kellyFraction * agentMultiplier * 100.0;

        if (finalUnits < 0.1) return; // Ignore sub-threshold plays

        String propKey = prop.getPlayer() + "_" + prop.getPropType()
                + "_" + (prop.isUnder() ? "UNDER" : "OVER")
                + "_" + prop.getLine();

        Bet bet = new Bet.Builder()
                .withAgent(agentName)
                .withPlayer(prop.getPlayer())
                .withPropKey(propKey)
                .withPropType(prop.getPropType())
                .withTargetLine(prop.getLine())
                .withDirection(prop.isUnder() ? "UNDER" : "OVER")
                .withOdds(prop.getBestOdds())
                .withBestOddsBook(prop.getBestOddsBook())
                .withMarketOdds(prop.getMarketOdds())
                .withXgboostProb(xgbProb)
                .withNoVigProb(noVigProb * 100.0)
                .withEdgePct(ev)
                .withKellyFraction(kellyFraction)
                .withKellySizePct(kellyFraction * 100.0)
                .withUnitSizing(finalUnits)
                .withRequiredLegs(legs)
                .withDfsPlatform(prop.getDfsPlatform() != null ? prop.getDfsPlatform() : "PrizePicks")
                .withPitcherFip(pitcherFip)
                .withUmpireKPct(umpireKPct)
                .withPublicBetPct(publicPct)
                .withBullpenFatigue(bullpen)
                // 7-point checklist
                .withChecklistFlag("pitcher", pitcherFip < 3.80)
                .withChecklistFlag("umpire",  umpireKPct > 22.0)
                .withChecklistFlag("public",  publicPct < 70.0 || isReverseLm(redisCache.get("mlb_hub", MlbHubState.class), prop))
                .withChecklistFlag("lineup",  prop.isLineupConfirmed())
                .withChecklistFlag("bullpen", bullpen < 3)
                .build();

        kafkaTemplate.send("bet_queue", bet);
        redisCache.addPendingBet(bet);

        log.info("🎯 {} | {} {} {} | xgb={}% | EV={}% | {}u × {}x | {}",
                agentName, prop.getPlayer(), prop.getPropType(), prop.getLine(),
                String.format("%.1f", xgbProb),
                String.format("%.1f", ev),
                String.format("%.2f", kellyFraction),
                agentMultiplier,
                prop.getDfsPlatform());
    }

    // ── Helper methods ────────────────────────────────────────────────────────

    private double getUmpireKPct(MlbHubState hub, String umpireId) {
        if (hub.getUmpireStats() == null || umpireId == null) return 68.0;
        MlbHubState.UmpireStats us = hub.getUmpireStats().get(umpireId);
        return us != null ? us.getCalledStrikePct() : 68.0;
    }

    private boolean isReverseLm(MlbHubState hub, PropMatchup prop) {
        if (hub == null || hub.getPublicBettingData() == null) return false;
        MlbHubState.PublicBettingData pd = hub.getPublicBettingData().get(prop.getTeam());
        if (pd == null) return false;
        // Reverse Line Movement: public > 70% but money < 50% = sharp on opposite
        return pd.getPublicBetPct() > 70.0 && pd.getMoneyPct() < 50.0;
    }

    private boolean isF5Prop(PropMatchup prop) {
        return prop.getPropType() != null && prop.getPropType().toLowerCase().contains("f5");
    }
}
