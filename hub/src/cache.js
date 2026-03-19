// hub/src/cache.js
// Redis cache-aside pattern for all Hub API calls.
// Key Schema: propiq:{source}:{resource}:{date_or_id}

const redis = require('redis');

const client = redis.createClient({
  url: `redis://:${process.env.REDIS_PASSWORD}@redis:6379`
});

client.on('error', (err) => console.error('[Redis] Connection error:', err));
client.on('connect', () => console.log('[Redis] Connected successfully'));

// Connect on module load
client.connect().catch(err => console.error('[Redis] Initial connection failed:', err));

/**
 * Fetches data using cache-aside pattern.
 * @param {string} key     - Redis cache key (see key schema below)
 * @param {number} ttl     - Cache TTL in seconds
 * @param {Function} fetchFn - Async function that fetches fresh data if cache miss
 * @returns {Promise<any>} - Cached or freshly fetched data
 * 
 * Key Schema & TTL Strategy:
 * propiq:sportsdata:props:{game_id}          TTL: 12s
 * propiq:sportsdata:games:{date}             TTL: 60s
 * propiq:sportsdata:lineups:{date}           TTL: 300s
 * propiq:sportsdata:injuries:{date}          TTL: 600s
 * propiq:oddsapi:odds:{date}                 TTL: 12s
 * propiq:tank01:scores:{date}                TTL: 12s
 * propiq:espn:scoreboard:{date}              TTL: 20s
 * propiq:hub:rate_limit:{source}             TTL: 60s
 * propiq:rotowire:trends                     TTL: 3600s
 * propiq:rotowire:expert_sentiment           TTL: 21600s
 * propiq:bettingpros:consensus               TTL: 3600s
 */
async function getOrFetch(key, ttl, fetchFn) {
  // Try to get from cache first
  try {
    const cached = await client.get(key);
    if (cached) {
      if (process.env.DEBUG) {
        console.log(`[Cache HIT] key: ${key}`);
      }
      return JSON.parse(cached);
    }
  } catch (cacheReadErr) {
    console.error(`[Cache] Read error for key ${key}:`, cacheReadErr.message);
  }

  // Cache miss or read error - fetch fresh data (only once!)
  if (process.env.DEBUG) {
    console.log(`[Cache MISS] key: ${key}`);
  }

  const fresh = await fetchFn();

  // Try to cache the result, but don't fail if Redis is down
  if (fresh !== null && fresh !== undefined) {
    try {
      await client.setEx(key, ttl, JSON.stringify(fresh));
    } catch (cacheWriteErr) {
      console.error(`[Cache] Write error for key ${key}:`, cacheWriteErr.message);
    }
  }

  return fresh;
}

module.exports = { client, getOrFetch };
