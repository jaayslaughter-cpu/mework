// hub/src/fetchers/sportsdata.js
// SportsData.io fetcher for player props, pre-game odds, and line movement data.
// SportsData.io fetcher for games, lineups, and injuries.

const axios = require('axios');
const { getOrFetch } = require('../cache');
const { checkAndIncrement } = require('../rateLimiter');
const { withBackoff } = require('../backoff');

const BASE = 'https://api.sportsdata.io/v3/mlb';
const KEY  = process.env.SPORTSDATA_API_KEY;

// Fail-fast if API key is missing
if (!KEY) {
  throw new Error('SPORTSDATA_API_KEY is missing from environment variables.');
}

async function getTodaysGames(date) {
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

async function getPlayerPropsByGame(gameId) {
  return getOrFetch(`propiq:sportsdata:props:${gameId}`, 12, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('sportsdata');
      return axios.get(`${BASE}/odds/json/BettingPlayerPropsByGameID/${gameId}?key=${KEY}`).then(r => r.data);
    });
  });
}

async function getGameOdds(date) {
  return getOrFetch(`propiq:sportsdata:gamelines:${date}`, 12, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('sportsdata');
      return axios.get(`${BASE}/odds/json/GameOddsByDate/${date}?key=${KEY}`).then(r => r.data);
    });
  });
}

const getStartingLineups = async (date) => {
  if (!KEY) throw new Error('SPORTSDATA_API_KEY is missing.');
  return getOrFetch(`propiq:sportsdata:lineups:${date}`, 300, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('sportsdata');
      return axios.get(`${BASE}/projections/json/StartingLineupsByDate/${date}?key=${KEY}`).then(r => r.data);
    });
  });
}

const getInjuredPlayers = async () => {
  if (!KEY) throw new Error('SPORTSDATA_API_KEY is missing.');
  return getOrFetch('propiq:sportsdata:injuries:today', 600, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('sportsdata');
      return axios.get(`${BASE}/projections/json/InjuredPlayers?key=${KEY}`).then(r => r.data);
    });
  });
}

module.exports = { getTodaysGames, getPlayerPropsByGame, getGameOdds, getStartingLineups, getInjuredPlayers };
module.exports = { getTodaysGames, getStartingLineups, getInjuredPlayers };
