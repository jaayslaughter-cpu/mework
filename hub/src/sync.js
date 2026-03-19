// hub/src/sync.js
// Background worker that syncs Odds API data to PostgreSQL every 15 minutes.

const { Pool } = require('pg');
const oddsapi = require('./fetchers/oddsapi');
const cron = require('node-cron');

const pool = new Pool({
  user: process.env.POSTGRES_USER,
  host: 'postgres', // Docker service name
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: 5432,
});

async function syncMarkets() {
  console.log('[Sync] Waking up to check for new sportsbook lines...');

  try {
    const events = await oddsapi.getMLBEvents();

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
            for (const outcome of market.outcomes) {
              // Stable ID generation for tracking line movement (Over/Under share same row)
              const marketId = `${event.id}_${book.key}_${market.key}_${outcome.description || 'base'}`.replace(/\s+/g, '_').toLowerCase();
              
              const query = `
                INSERT INTO betting_markets (
                  market_id, game_id, pitcher_id, sportsbook, prop_category, 
                  line, over_odds, under_odds, updated_at
                ) VALUES (
                  $1, $2, NULL, $3, $4, $5, $6, $7, NOW()
                )
                ON CONFLICT (market_id) DO UPDATE SET 
                  line = EXCLUDED.line,
                  over_odds = COALESCE(EXCLUDED.over_odds, betting_markets.over_odds),
                  under_odds = COALESCE(EXCLUDED.under_odds, betting_markets.under_odds),
                  updated_at = NOW();
              `;
              
              // Note: We assign odds to over/under based on outcome.name
              const odds = outcome.price; 
              const point = outcome.point || 0.5;
              
              await pool.query(query, [
                marketId, event.id, book.key, market.key, point, 
                outcome.name === 'Over' ? odds : null, 
                outcome.name === 'Under' ? odds : null
              ]);
            }
          }
        }
      } catch (err) {
        console.error(`[Sync] Failed to sync props for event ${event.id}:`, err.message);
      }
    }

    console.log('[Sync] Successfully synced live player props to PostgreSQL.');
  } catch (error) {
    console.error('[Sync] Critical error in sync loop:', error.message);
  }
}

// Run every 15 minutes
cron.schedule('*/15 * * * *', syncMarkets);

module.exports = { syncMarkets };
