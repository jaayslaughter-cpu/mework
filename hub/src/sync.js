// hub/src/sync.js
// Unified 60-second polling loop for real-time data sync.

const { Pool } = require('pg');
const oddsapi = require('./fetchers/oddsapi');
const espn = require('./fetchers/espn');

const pool = new Pool({
  user: process.env.POSTGRES_USER,
  host: process.env.POSTGRES_HOST || 'postgres',
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: 5432,
});

async function syncLoop() {
  try {
    // 1. Fetch ESPN Live Scores (Keeps cache hot for REST API)
    await espn.getLiveScores().catch(err => console.warn('[Sync] ESPN fetch warning:', err.message));

    // 2. Fetch and UPSERT Odds API lines
    const events = await oddsapi.getMLBEvents().catch(() => []);
    if (!events || events.length === 0) {
      console.log('[Sync] No MLB events found for today.');
      return;
    }

    for (const event of events) {
      try {
        const props = await oddsapi.getPlayerProps(event.id);
        if (!props || !props.bookmakers) continue;

        for (const book of props.bookmakers) {
          for (const market of book.markets) {
            // Aggregate Over/Under outcomes by description
            const outcomeMap = {};

            for (const outcome of market.outcomes) {
              const desc = outcome.description || 'base';
              if (!outcomeMap[desc]) {
                outcomeMap[desc] = { over_odds: null, under_odds: null, point: outcome.point ?? 0.5 };
              }
              const nameLower = (outcome.name || '').toLowerCase();
              if (nameLower === 'over') {
                outcomeMap[desc].over_odds = outcome.price;
              } else if (nameLower === 'under') {
                outcomeMap[desc].under_odds = outcome.price;
              }
            }

            // Now insert each aggregated market row
            for (const [desc, data] of Object.entries(outcomeMap)) {
              if (data.over_odds === null && data.under_odds === null) continue;

              const marketId = `${event.id}_${book.key}_${market.key}_${desc}_${data.point}`
                .replace(/\s+/g, '_')
                .toLowerCase();

              const query = `
                INSERT INTO betting_markets (
                  market_id, game_id, pitcher_id, sportsbook, prop_category,
                  line, over_odds, under_odds, updated_at
                ) VALUES (
                  $1, $2, NULL, $3, $4, $5, $6, $7, NOW()
                )
                ON CONFLICT (market_id) DO UPDATE SET
                  line = EXCLUDED.line,
                  over_odds = EXCLUDED.over_odds,
                  under_odds = EXCLUDED.under_odds,
                  updated_at = NOW();
              `;

              await pool.query(query, [
                marketId,
                event.id,
                book.key,
                market.key,
                data.point,
                data.over_odds,
                data.under_odds,
              ]);
            }
          }
        }
      } catch (err) {
        console.warn(`[Sync] Failed to sync props for event ${event.id}:`, err.message);
      }
    }

    console.log(`[Sync] Cycle complete — ${events.length} events processed at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('[Sync] Critical error in unified loop:', error.message);
  }
}

async function startSyncWorker() {
  console.log('🚀 Starting Unified 60-Second Polling Loop...');

  async function runLoop() {
    const start = Date.now();
    await syncLoop();
    const elapsed = Date.now() - start;
    setTimeout(runLoop, Math.max(0, 60000 - elapsed));
  }

  runLoop(); // Start immediately
}

module.exports = { startSyncWorker };
