// hub/src/fetchers/espn.js
// ESPN public scoreboard fetcher for live in-game data.

const axios = require('axios');
const { getOrFetch } = require('../cache');
const { checkAndIncrement } = require('../rateLimiter');
const { withBackoff } = require('../backoff');

const ESPN_URL = 'https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard';

async function getLiveScores() {
  // Cache for 15 seconds since this is highly volatile live data
  return getOrFetch('propiq:espn:scoreboard', 15, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('espn');
      return axios.get(ESPN_URL, { timeout: 5000 }).then(r => r.data);
    });
  });
}

module.exports = { getLiveScores };
