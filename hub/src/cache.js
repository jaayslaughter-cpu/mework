// hub/src/cache.js
// Redis cache-aside pattern for all Hub API calls.
// Key Schema: propiq:{source}:{resource}:{date_or_id}

const redis = require('redis');

// Conditionally format Redis URL to avoid passing "undefined" as password
const auth = process.env.REDIS_PASSWORD ? `:${encodeURIComponent(process.env.REDIS_PASSWORD)}@` : '';
const redisUrl = `redis://${auth}redis:6379`;

const client = redis.createClient({
  url: redisUrl,
  socket: {
    connectTimeout: 5000,
    reconnectStrategy: false
  }
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
