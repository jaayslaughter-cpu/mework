# PropIQ Production Ops Runbook

> On-call procedures, startup sequences, incident response, and health check protocols.  
> Keep this document updated after every incident post-mortem.

---

## 1. System Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                     Railway.app                          │
│                                                          │
│  web:    uvicorn api_server:app --port $PORT             │
│  worker: python ml_pipeline.py                           │
│                                                          │
│  ┌──────────────┐   RabbitMQ Topic Exchange              │
│  │  Tier 1: ML  │─► propiq_events                        │
│  │  Pipeline    │       mlb.projections.*                 │
│  └──────────────┘       alerts.market_edges              │
│  ┌──────────────┐       alerts.discord.slips             │
│  │  Tier 2:     │                                         │
│  │  Market Scan │◄──── The Odds API + SBR XML            │
│  └──────────────┘                                         │
│  ┌──────────────┐                                         │
│  │  Tier 3:     │                                         │
│  │  Context Mod │◄──── Apify + Weather APIs               │
│  └──────────────┘                                         │
│  ┌──────────────┐                                         │
│  │  Tier 4: 16  │                                         │
│  │  Exec Agents │──► alerts.discord.slips                 │
│  └──────────────┘                                         │
│  ┌──────────────┐                                         │
│  │  Tier 5:     │──► Discord Webhook                      │
│  │  Discord     │                                         │
│  └──────────────┘                                         │
└─────────────────────────────────────────────────────────┘
         │
         │ HTTP (FastAPI)
         ▼
  Spring Boot Backend (Java 21)
  7 Tasklets: DataHub → Agent → BetAnalyzer →
              Leaderboard → Grading → Backtest → XGBoost
```

---

## 2. Required Environment Variables

| Variable | Description | Where to Set |
|----------|-------------|-------------|
| `REDIS_URL` | Redis connection string (`redis://...`) | Railway → Variables |
| `DATABASE_URL` | Postgres connection string | Railway → Variables |
| `RABBITMQ_URL` | RabbitMQ AMQP URL | Railway → Variables |
| `APIFY_API_KEY` | Apify scraper API key | Railway → Variables |
| `DISCORD_WEBHOOK_URL` | Discord webhook URL | Railway → Variables |
| `ODDS_API_KEY_1` | The Odds API primary key | Railway → Variables |
| `ODDS_API_KEY_2` | The Odds API rotation key | Railway → Variables |
| `SPORTSDATA_API_KEY` | SportsData.io key | Railway → Variables |
| `TANK01_API_KEY` | Tank01 RapidAPI key | Railway → Variables |

---

## 3. Startup Sequence

### Normal Startup (Railway auto-deploys from `main`)
1. Railway detects push to `main` → triggers build
2. Procfile launches: `web` (api_server) + `worker` (ml_pipeline)
3. `ml_pipeline.py` startup sequence:
   - Connect to RabbitMQ, declare `propiq_events` topic exchange
   - Seed Redis sorted-sets from Postgres (`_seed_from_redis()` in all 3 market scanners)
   - Fire Discord startup ping: "✅ PropIQ Engine Online: Webhook Connected!"
   - Begin consuming `*.player_props.*` from The Odds API + SBR

### Manual Restart
```bash
# Via Railway CLI
railway run python ml_pipeline.py

# Health check
curl https://<your-railway-domain>/api/ml/health
```

Expected health response:
```json
{
  "status": "ok",
  "tiers": {"ml": true, "market": true, "context": true, "execution": true, "discord": true},
  "rabbitmq": "connected",
  "redis": "connected"
}
```

---

## 4. Daily Operations Timeline

| Time (PT) | Event | Owner Tier |
|-----------|-------|----------|
| 6:00 AM | Odds API daily reset — key rotation resets | Market (Tier 2) |
| 8:00 AM | SportsData.io + Tank01 lineup pull | Spring Boot DataHubTasklet |
| 9:00 AM | Apify scraper run — lineups, public betting data | Tier 3 Context |
| 10:00 AM | Context modifiers compute daily DataFrame | Tier 3 |
| 10:30 AM | ML inference run — batch predictions for day's slate | Tier 1 |
| 11:00 AM | Market scanners begin real-time monitoring | Tier 2 |
| 12:00 PM | First Discord alerts fire (if props available) | Tier 5 |
| 3:00 PM | Odds API second key check (429 monitoring) | Tier 2 |
| 7:00 PM | Last game starts — monitoring continues | All tiers |
| 11:30 PM | Spring Boot GradingTasklet runs — grades slips | Spring Boot |
| 11:30 PM | Discord daily recap fires | Tier 5 |

---

## 5. Incident Response Playbook

### INC-001: Discord Alerts Not Firing

**Symptoms:** No Discord messages for 2+ hours during active slate

**Triage steps:**
1. Check health endpoint: `GET /api/ml/health`
2. Check Railway logs for Tier 5 errors
3. Verify Discord webhook URL is not rate-limited
4. Check RabbitMQ `alerts.discord.slips` queue depth

**Resolution:**
```bash
# Check webhook manually
curl -X POST $DISCORD_WEBHOOK_URL \
  -H "Content-Type: application/json" \
  -d '{"content": "manual test"}'

# If 429: webhook is rate-limited — wait retry_after seconds
# If 404: webhook URL rotated — update DISCORD_WEBHOOK_URL env var in Railway
```

---

### INC-002: The Odds API 429 Rate Limit

**Symptoms:** `WARNING: Odds API key 1 exhausted, rotating to key 2`

**Expected behavior:** Auto-rotates to key 2 via `tasklets_discord.py` dual-key logic  
**Action required:** None — system self-heals

**If key 2 also 429:**
1. Check usage dashboard at the-odds-api.com
2. If approaching monthly quota: reduce polling interval in `market_scanners.py`
3. Set `ODDS_POLL_INTERVAL_SECONDS=120` env var (default: 60)

---

### INC-003: SportsData.io 403 Forbidden

**Symptoms:** `WARNING: SportsData.io returned 403, falling back to Tank01`

**Expected behavior:** Auto-falls to Tank01 via PR #97 fallback logic  
**Action required:** None during game day

**If Tank01 also fails:**
1. Check Tank01 RapidAPI quota at rapidapi.com/dashboard
2. PropIQ degrades gracefully — ML inference uses cached features from last successful pull

---

### INC-004: RabbitMQ Connection Lost

**Symptoms:** Tier 1/2/3 silent — no messages in queues

**Triage:**
```bash
# Check connection
python3 -c "import pika; pika.BlockingConnection(pika.URLParameters('$RABBITMQ_URL'))"

# If timeout: check Railway RabbitMQ service status
```

**Resolution:**
- ml_pipeline.py has exponential backoff reconnect (max 5 retries, 2^n seconds)
- If persistent: redeploy Railway service (triggers fresh connection)
- Queue durability: all queues declared `durable=True` — no messages lost on restart

---

### INC-005: ML Model Returns All ~0.5 Probabilities

**Symptoms:** All predictions clustered near 0.5, no slips fire above 3% EV gate

**Causes:**
- Feature drift (stats columns changed from data source)
- Missing Statcast data (returns all NaN → imputed to mean → degenerates)
- Calibration layer overfitting

**Triage:**
```bash
# Run SHAP audit endpoint
curl -X POST /api/ml/backtest-audit \
  -d '{"feature_names": [...], "feature_values": [...]}'
```

**Resolution:**
1. Check anomaly detection: `GET /api/ml/anomaly-detect`
2. If feature drift confirmed: retrain with current data
3. Temporarily lower EV gate to 2% to resume alerts while investigating

---

### INC-006: Redis State Lost (Restart)

**Symptoms:** Steam detection doesn't fire for first hour after restart (no tick history)

**Expected behavior:** `_seed_from_redis()` restores sorted-set from Postgres snapshots  
**Resolution:** Automatic after 30-60 min as ticks repopulate

**Prevention:** Ensure Postgres snapshot job runs every 15 min (BacktestTasklet secondary function)

---

## 6. Monitoring & Alerting

### Key Metrics to Watch
| Metric | Normal | Alert Threshold |
|--------|--------|----------------|
| Slips/day fired | 5–25 | <2 or >50 |
| EV% avg on fired slips | 4–12% | <3% |
| Discord delivery success | >98% | <95% |
| Odds API requests/day | <500 | >900 (approaching limit) |
| RabbitMQ queue depth | <100 | >1000 |
| Redis memory | <200MB | >800MB |
| Inference latency (p99) | <500ms | >2000ms |

### Log Locations (Railway)
- `propiq.ml_pipeline` — Tier 1 ML inference logs
- `propiq.market_scanners` — Line movement, steam events
- `propiq.execution_agents` — Slip building, EV calculations
- `propiq.discord_dispatcher` — Webhook delivery status

---

## 7. Deployment Checklist

Before merging any PR to `main`:

- [ ] `python3 -m pytest tests/ -v` passes (or equivalent sandbox verification)
- [ ] No new env vars without Railway variables doc update
- [ ] No new API endpoints without FastAPI schema update in `api_server.py`
- [ ] No new agents without `ExecutionSquad` registration in `execution_agents.py`
- [ ] Discord startup ping wording updated if system name changes
- [ ] `requirements.txt` updated if new Python packages added
- [ ] `TECHNICAL_DESIGN.md` updated for architecture changes
- [ ] `model_zoo.md` updated if new models added

---

## 8. Rollback Procedure

Railway maintains deployment history. To rollback:
1. Railway dashboard → Deployments
2. Select previous successful deployment
3. Click "Redeploy"
4. Monitor `/api/ml/health` — should return `"status": "ok"` within 60s

**Data safety:** RabbitMQ messages are durable (survive rollback). Redis state persists. Postgres is never written by rollback.
