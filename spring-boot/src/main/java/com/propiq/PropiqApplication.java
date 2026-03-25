package com.propiq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * PropIQ Analytics Engine — 10-Agent MLB Prop Betting System
 *
 * Architecture:
 *   DataHubTasklet    (15s)  — Staggered API polling → Redis mlb_hub
 *   AgentTasklet      (30s)  — 10 XGBoost agents → Kafka bet_queue
 *   BetAnalyzerTasklet (5s)  — Pre-compute EV → bet_analyzer_cache
 *   LeaderboardTasklet (60s) — Capital allocation 0.5×–2.0×
 *   GradingTasklet    (1:05AM)— CLV settlement + anomaly detection
 *   BacktestTasklet   (12:01AM)— Feature audit, accuracy gate
 *   XGBoostTasklet    (Sun 2AM)— Weekly model retrain on winning picks
 *
 * DFS Target (California): PrizePicks / Underdog Fantasy
 * Books tracked: DraftKings, FanDuel, BetMGM, bet365
 */
@SpringBootApplication
@EnableScheduling
public class PropiqApplication {

    public static void main(String[] args) {
        SpringApplication.run(PropiqApplication.class, args);
    }
}
