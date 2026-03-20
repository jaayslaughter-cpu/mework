// hub/src/routes/slates.js
// REST API endpoint for daily slate data - serves from DB/cache for speed.

const express = require('express');
const router = express.Router();
const { Pool } = require('pg');

const sportsdata = require('../fetchers/sportsdata');
const espn = require('../fetchers/espn');

const pool = new Pool({
  user: process.env.POSTGRES_USER,
  host: 'postgres',
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: 5432,
});

router.get('/today', async (req, res) => {
  try {
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });

    // 1. Fetch Context (Served instantly from Redis Cache via our fetchers)
    const [games, lineups, injuries, scores] = await Promise.all([
      sportsdata.getTodaysGames(today).catch(() => []),
      sportsdata.getStartingLineups(today).catch(() => []),
      sportsdata.getInjuredPlayers().catch(() => []),
      espn.getLiveScores().catch(() => null)
    ]);

    // 2. Fetch Live Markets (Served instantly from PostgreSQL)
    // We pull markets updated in the last 24 hours to ensure we don't serve stale, days-old lines.
    const dbRes = await pool.query(`
      SELECT market_id, game_id, sportsbook, prop_category, line, over_odds, under_odds, updated_at 
      FROM betting_markets 
      WHERE updated_at >= NOW() - INTERVAL '24 HOURS'
    `);

    // 3. Normalize and Return
    res.json({
      status: 'success',
      date: today,
      slate: {
        games: games || [],
        lineups: lineups || [],
        injuries: injuries || [],
        live_scores: scores || {}
      },
      markets: dbRes.rows
    });

  } catch (error) {
    console.error('[API] Error building daily slate:', error);
    res.status(500).json({ error: 'Failed to aggregate daily slate' });
  }
});

module.exports = router;
