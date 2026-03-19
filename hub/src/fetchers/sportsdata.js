// hub/src/fetchers/sportsdata.js
// SportsData.io fetcher for player props, pre-game odds, and line movement data.

const axios = require('axios');
const { getOrFetch } = require('../cache');
const { checkAndIncrement } = require('../rateLimiter');
const { withBackoff } = require('../backoff');

const BASE = 'https://api.sportsdata.io/v3/mlb';
const KEY  = process.env.SPORTSDATA_API_KEY;

async function getTodaysGames(date) {
  return getOrFetch(`propiq:sportsdata:games:${date}`, 60, async () => {
    await checkAndIncrement('sportsdata');
    return withBackoff(() =>
      axios.get(`${BASE}/scores/json/GamesByDate/${date}?key=${KEY}`).then(r => r.data)
    );
  });
}

async function getPlayerPropsByGame(gameId) {
  return getOrFetch(`propiq:sportsdata:props:${gameId}`, 12, async () => {
    await checkAndIncrement('sportsdata');
    return withBackoff(() =>
      axios.get(`${BASE}/odds/json/BettingPlayerPropsByGameID/${gameId}?key=${KEY}`).then(r => r.data)
    );
  });
}

async function getGameOdds(date) {
  return getOrFetch(`propiq:sportsdata:gamelines:${date}`, 12, async () => {
    await checkAndIncrement('sportsdata');
    return withBackoff(() =>
      axios.get(`${BASE}/odds/json/GameOddsByDate/${date}?key=${KEY}`).then(r => r.data)
    );
  });
}

async function getStartingLineups(date) {
  return getOrFetch(`propiq:sportsdata:lineups:${date}`, 300, async () => {
    await checkAndIncrement('sportsdata');
    return withBackoff(() =>
      axios.get(`${BASE}/projections/json/StartingLineupsByDate/${date}?key=${KEY}`).then(r => r.data)
    );
  });
}

async function getInjuredPlayers() {
  return getOrFetch(`propiq:sportsdata:injuries:today`, 600, async () => {
    await checkAndIncrement('sportsdata');
    return withBackoff(() =>
      axios.get(`${BASE}/projections/json/InjuredPlayers?key=${KEY}`).then(r => r.data)
    );
  });
}

module.exports = { getTodaysGames, getPlayerPropsByGame, getGameOdds, getStartingLineups, getInjuredPlayers };
