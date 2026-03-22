// hub/src/sync.js
// Unified 15-second polling loop for real-time data sync.

const { Pool } = require('pg');
const oddsapi = require('./fetchers/oddsapi');
const espn = require('./fetchers/espn');

const pool = new Pool({
  user: process.env.POSTGRES_USER,
  host: 'postgres',
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: 5432,
});

async function syncLoop() {
  try {
    // 1. Fetch ESPN Live Scores (Keeps cache piping hot for the REST API)
    await espn.getLiveScores().catch(err => console.warn('[Sync] ESPN fetch warning:', err.message));

    // 2. Fetch and UPSERT Odds API lines
    const events = await oddsapi.getMLBEvents().catch(() => []);
    if (!events || events.length === 0) return;

    for (const event of events) {
      try {
        const props = await oddsapi.getPlayerProps(event.id);
        if (!props || !props.bookmakers) continue;

        for (const book of props.bookmakers) {
          for (const market of book.markets) {
            // 1. Aggregate Over/Under outcomes in JavaScript memory first

            for (const outcome of market.outcomes) {
              const point = outcome.point ?? 0.5;
              const marketId = `${event.id}_${book.key}_${market.key}_${outcome.description || 'base'}_${point}`.replace(/\s+/g, '_').toLowerCase();
              
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
                marketId, event.id, book.key, market.key, point, 
                outcome.overOdds, outcome.underOdds
              ]);
            }
          }
        }
      } catch (err) {
        console.warn(`[Sync] Failed to sync props for event ${event.id}:`, err.message);
      }
    }
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
  
  runLoop(); // Start first run immediately
}

module.exports = { startSyncWorker };
