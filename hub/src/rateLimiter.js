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
  // Validate source parameter
  if (!RATE_LIMITS[source]) {
    throw new Error(`[RateLimit] Unknown rate limit source: ${source}`);
  }

  const key = `propiq:hub:rate_limit:${source}`;
  const limit = RATE_LIMITS[source];

  // Use Redis transaction to ensure INCR and EXPIRE are atomic
  const results = await client.multi()
    .incr(key)
    .expire(key, 60)
    .exec();

  const count = results[0];

  if (count > limit) {
    throw new Error(`[RateLimit] ${source} limit reached (${count}/${limit}/min). Skipping request.`);
  }
}

module.exports = { checkAndIncrement };
