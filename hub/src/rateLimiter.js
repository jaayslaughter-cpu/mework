// hub/src/rateLimiter.js
// Tracks rolling request counts per API source using Redis INCR.
// Hard limits enforced BEFORE any HTTP call is made.

const { client } = require('./cache');

const RATE_LIMITS = {
  sportsdata: 30,   // max requests per 60-second window
  oddsapi:    25,   // max requests per 60-second window
  tank01:     20,   // max requests per 60-second window
  espn:       30    // ESPN public API
};

async function checkAndIncrement(source) {
  const key = `propiq:hub:rate_limit:${source}`;
  const count = await client.incr(key);

  if (count === 1) {
    await client.expire(key, 60);
  }

  const limit = RATE_LIMITS[source];
  if (count > limit) {
    throw new Error(`[RateLimit] ${source} limit reached (${count}/${limit}/min). Skipping request.`);
  }
}

module.exports = { checkAndIncrement };
