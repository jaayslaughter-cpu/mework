# PropIQ Analytics ⚾📈

PropIQ Analytics is a high-frequency, algorithmic MLB player prop betting engine. It operates as a headless, multi-agent quantitative system designed to identify +EV (Expected Value) edges in the sports betting market and execute them against California-legal DFS applications (PrizePicks, Underdog Fantasy, Sleeper).

The system utilizes an automated data ingestion pipeline, an XGBoost machine learning evaluation model, and a 7-agent Spring Batch execution framework to hunt mispriced lines, manage bankroll via Quarter-Kelly sizing, and push real-time execution alerts via Telegram.

## 🏗️ System Architecture

PropIQ is built on a containerized microservices architecture:
* **Core Engine:** Java Spring Boot / Spring Batch
* **Machine Learning:** Python (XGBoost, Scikit-learn, SHAP)
* **Message Broker:** Apache Kafka (Asynchronous bet queuing)
* **State & Caching:** Redis (In-memory line caching and pre-computed EV)
* **Ledger & Settlement:** PostgreSQL (Bet tracking, CLV, and 14-day ROI agent leaderboards)
* **Deployment:** Dockerized and hosted on Railway

## 📊 The Data Pipeline (DataHubTasklet)

The system polls lightweight JSON APIs (Tank01, The Odds API) and utilizes Apify to scrape heavy HTML pages on a staggered 5-15 minute pre-match loop. It extracts 85 distinct features across three core categories:

1.  **Physics & Arsenal:** Pitcher slider whiff percentage, FIP, SwStr%, barrel rates, and xwOBA splits (via Baseball Savant & RotoWire).
2.  **Context & Environment:** Umpire strike zone tendencies, wind direction, and injury/rest fatigue logic.
3.  **Market & Sharp Money:** Public betting percentages, sharp money indicators, and reverse line movement (RLM) triggers (via Action Network).

## 🤖 The 7-Agent Execution Model

PropIQ does not rely on a single strategy. Every 30 seconds, the `AgentTasklet` evaluates the Redis cache and deploys 7 specialized virtual quants:

* **The +EV Hunter:** Fires when the XGBoost probability exceeds the No-Vig sportsbook line by >5%.
* **The Under Machine:** Hunts pitcher-friendly umpires, wind blowing in, and low-FIP starters to hammer batter Unders.
* **The Steam Chaser:** Tracks opening vs. current lines to ride sharp money movements (>6% probability shifts).
* **The RLM Agent:** Fades the public when >75% of bets are on one side but the sportsbooks move the odds the opposite way.
* **The Fade Agent:** Actively bets against heavy public money when the ML model identifies the public is mathematically incorrect.
* **The Correlated Parlay Agent:** Identifies mathematically linked events (e.g., Pitcher Over Ks + Opposing Leadoff Under Hits) for DFS optimization.
* **The Standard Parlay Agent:** Combines standalone high-probability (>60%) plays to meet DFS minimum leg requirements.

## 📱 Telegram Integration & Chatbot

PropIQ acts as a two-way quantitative assistant via Telegram.
* **Push Alerts:** The Kafka consumer pushes +EV slips directly to the user's phone, explicitly tagging which DFS app (Underdog/Sleeper) holds the best line.
* **2-Way Querying:** Users can text a player's name (e.g., "Ohtani") to the bot. It instantly checks the `bet_analyzer_cache` and returns a formatted Intelligence Report, including contextual analysis, a 1-10 Confidence Score, and agent consensus.
* **Daily Settlement:** At 1:05 AM PT, the `GradingTasklet` settles the day's boxscores, calculates true Closing Line Value (CLV), and pushes a daily profit/loss receipt.

## 🔒 Security & Bankroll Management

* **Fractional Kelly Criterion:** Bankroll allocation is strictly governed by Half-Kelly/Quarter-Kelly math with a hard 5% maximum cap per play to protect against variance.
* **Emergency Abort Protocol:** Listens for late scratches and injury news, pushing abort signals to Kafka to cancel pending evaluations.
* **OOS Auditing:** The `BacktestTasklet` runs weekly Out-of-Sample SHAP audits, automatically dropping variables that push accuracy below 77.7%.

---
*Disclaimer: This repository contains the architecture for a sports analytics engine. It is strictly for educational and mathematical modeling purposes.*
