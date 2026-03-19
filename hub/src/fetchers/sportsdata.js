// hub/src/fetchers/sportsdata.js
// SportsData.io fetcher for games, lineups, and injuries.

const axios = require('axios');
const { getOrFetch } = require('../cache');
const { checkAndIncrement } = require('../rateLimiter');
const { withBackoff } = require('../backoff');

const BASE = 'https://api.sportsdata.io/v3/mlb';
const KEY  = process.env.SPORTSDATA_API_KEY;

if (!KEY) console.warn('⚠️ SPORTSDATA_API_KEY is missing. Fetchers will return errors.');

async function getTodaysGames(date) {
  if (!KEY) throw new Error('SPORTSDATA_API_KEY is missing.');
  return getOrFetch(`propiq:sportsdata:games:${date}`, 60, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('sportsdata');
      return axios.get(`${BASE}/scores/json/GamesByDate/${date}?key=${KEY}`).then(r => r.data);
    });
  });
}

async function getStartingLineups(date) {
  if (!KEY) throw new Error('SPORTSDATA_API_KEY is missing.');
  return getOrFetch(`propiq:sportsdata:lineups:${date}`, 300, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('sportsdata');
      return axios.get(`${BASE}/projections/json/StartingLineupsByDate/${date}?key=${KEY}`).then(r => r.data);
    });
  });
}

async function getInjuredPlayers() {
  if (!KEY) throw new Error('SPORTSDATA_API_KEY is missing.');
  return getOrFetch(`propiq:sportsdata:injuries:today`, 600, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('sportsdata');
      return axios.get(`${BASE}/projections/json/InjuredPlayers?key=${KEY}`).then(r => r.data);
    });
  });
}

module.exports = { getTodaysGames, getStartingLineups, getInjuredPlayers };
