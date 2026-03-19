// hub/src/routes/slates.js
// Aggregator endpoint for daily slate data

const express = require('express');
const router = express.Router();

const sportsdata = require('../fetchers/sportsdata');
const oddsapi = require('../fetchers/oddsapi');
const espn = require('../fetchers/espn');

router.get('/today', async (req, res) => {
  try {
    // Ensure timezone consistency (defaulting to current YYYY-MM-DD for West Coast)
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });

    // Fetch core daily context concurrently
    const results = await Promise.allSettled([
      sportsdata.getTodaysGames(today),
      sportsdata.getStartingLineups(today),
      sportsdata.getInjuredPlayers(),
      oddsapi.getMLBEvents(),
      espn.getLiveScores()
    ]);

    const response = {
      date: today,
      games: results[0].status === 'fulfilled' ? results[0].value : [],
      lineups: results[1].status === 'fulfilled' ? results[1].value : [],
      injuries: results[2].status === 'fulfilled' ? results[2].value : [],
      odds_events: results[3].status === 'fulfilled' ? results[3].value : [],
      live_scores: results[4].status === 'fulfilled' ? results[4].value : [],
      fetch_errors: results
        .filter(r => r.status === 'rejected')
        .map(r => r.reason?.message || 'Unknown fetch error')
    };

    // Return a 206 Partial Content if there are errors, otherwise 200 OK
    const statusCode = response.fetch_errors.length > 0 ? 206 : 200;
    res.status(statusCode).json(response);

  } catch (error) {
    console.error('[Slates API] Critical error building daily slate:', error);
    res.status(500).json({ error: 'Failed to aggregate daily slate' });
  }
});

module.exports = router;
