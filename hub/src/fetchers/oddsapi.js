// hub/src/fetchers/oddsapi.js
// The Odds API fetcher for real-time MLB lines and player props.

const axios = require('axios');
const { getOrFetch } = require('../cache');
const { checkAndIncrement } = require('../rateLimiter');
const { withBackoff } = require('../backoff');

const BASE = 'https://api.the-odds-api.com/v4/sports/baseball_mlb';
const KEY = process.env.ODDS_API_KEY;

if (!KEY) {
  console.warn('⚠️ ODDS_API_KEY is missing from environment variables.');
}

async function getMLBEvents() {
  if (!KEY) throw new Error('ODDS_API_KEY is missing.');

  // Cache for 60 seconds
  return getOrFetch(`propiq:oddsapi:events:today`, 60, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('oddsapi');
      const today = new Date().toISOString().slice(0, 10);
      return axios.get(`${BASE}/events?apiKey=${KEY}`).then(r => 
        r.data.filter(e => e.commence_time.startsWith(today))
      );
    });
  });
}

async function getPlayerProps(eventId, markets = 'pitcher_strikeouts,batter_total_bases,batter_home_runs,batter_hits_runs_rbis') {
  if (!KEY) throw new Error('ODDS_API_KEY is missing.');

  // Cache for 15 seconds (Hot Data)
  return getOrFetch(`propiq:oddsapi:props:${eventId}:${markets}`, 15, async () => {
    return withBackoff(async () => {
      await checkAndIncrement('oddsapi');
      return axios.get(`${BASE}/events/${eventId}/odds?apiKey=${KEY}&regions=us&markets=${markets}&bookmakers=draftkings,fanduel,underdog&oddsFormat=american`).then(r => r.data);
    });
  });
}

module.exports = { getMLBEvents, getPlayerProps };
