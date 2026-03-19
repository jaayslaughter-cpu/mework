// hub/src/rateLimiter.js
// Tracks rolling request counts per API source using Redis ZSET.
// Implements true rolling 60-second window rate limiting.

const { client } = require('./cache');

const RATE_LIMITS = {
  sportsdata: 30,   // max requests per 60-second window
  oddsapi:    25,   // max requests per 60-second window
  tank01:     20,   // max requests per 60-second window
  espn:       30    // ESPN public API
};

async function checkAndIncrement(source) {
  if (!RATE_LIMITS[source]) {
    throw new Error(`[RateLimit] Unknown rate limit source: ${source}`);
  }

  const limit = RATE_LIMITS[source];
  const now = Date.now();
  const windowStart = now - 60000;
  const key = `propiq:hub:rate_limit:rolling:${source}`;
  const reqId = `${now}-${Math.random().toString(36).substr(2, 5)}`;

  // Redis Transaction: Remove old requests, add new request, count current window, set TTL
  const results = await client.multi()
    .zRemRangeByScore(key, 0, windowStart) // 1. Remove requests older than 60s
    .zAdd(key, { score: now, value: reqId }) // 2. Add current request
    .zCard(key) // 3. Get total count in current window
    .expire(key, 60) // 4. Set TTL so idle keys clean themselves up
    .exec();

  const count = results[2]; // Result of zCard

  if (count > limit) {
    // Revert the addition if we are over the limit
    await client.zRem(key, reqId);
    throw new Error(`[RateLimit] ${source} rolling limit reached (${count}/${limit}/min). Skipping request.`);
  }
}

module.exports = { checkAndIncrement };
