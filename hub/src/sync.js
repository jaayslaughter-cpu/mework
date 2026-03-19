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
            const outcomesByDesc = {};
            
            for (const outcome of market.outcomes) {
              const desc = outcome.description || 'base';
              if (!outcomesByDesc[desc]) {
                outcomesByDesc[desc] = { overOdds: null, underOdds: null, point: outcome.point || 0.5 };
              }
              
              if (outcome.name === 'Over') outcomesByDesc[desc].overOdds = outcome.price;
              if (outcome.name === 'Under') outcomesByDesc[desc].underOdds = outcome.price;
              if (outcome.point) outcomesByDesc[desc].point = outcome.point;
            }

            // 2. Execute a single UPSERT per market description
            for (const [desc, data] of Object.entries(outcomesByDesc)) {
              const marketId = `${event.id}_${book.key}_${market.key}_${desc}`.replace(/\s+/g, '_').toLowerCase();
              
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
                marketId, event.id, book.key, market.key, data.point, 
                data.overOdds, data.underOdds
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

function startSyncWorker() {
  console.log('🚀 Starting Unified 15-Second Polling Loop...');
  syncLoop(); // Run immediately on boot
  setInterval(syncLoop, 15000); // Run every 15 seconds
}

module.exports = { startSyncWorker };
