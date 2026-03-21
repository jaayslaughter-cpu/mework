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
    connectTimeout: 1000,
    reconnectStrategy: () => false
  }
});

client.on('error', (err) => {
  console.error('[Redis] Connection error:', err);
  // Crash the process on connection failures to trigger container restarts
  process.exit(1);
});
client.on('connect', () => console.log('[Redis] Connected successfully'));

// Connect on module load
client.connect().catch(err => console.error('[Redis] Initial connection failed:', err));

// In-memory lock map to prevent concurrent fetchFn calls for the same key
const pendingFetches = new Map();

/**
 * Fetches data using cache-aside pattern with lock to prevent thundering herd.
 * @param {string} key     - Redis cache key (see key schema below)
 * @param {number} ttl     - Cache TTL in seconds
 * @param {Function} fetchFn - Async function that fetches fresh data if cache miss
 */
const getOrFetch = async (key, ttl, fetchFn) => {
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

  // Cache miss - check if another request is already fetching this key
  if (pendingFetches.has(key)) {
    if (process.env.DEBUG) {
      console.log(`[Cache LOCK] Waiting for pending fetch: ${key}`);
    }
    return pendingFetches.get(key);
  }

  if (process.env.DEBUG) {
    console.log(`[Cache MISS] key: ${key}`);
  }

  // Create a promise for this fetch and store it in the lock map
  const fetchPromise = (async () => {
    try {
      // Double-check cache in case it was populated while waiting
      try {
        const cached = await client.get(key);
        if (cached) {
          return JSON.parse(cached);
        }
      } catch (e) {
        // Ignore cache read error, proceed with fetch
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
    } finally {
      // Remove from pending fetches when done
      pendingFetches.delete(key);
    }
  })();

  pendingFetches.set(key, fetchPromise);
  return fetchPromise;
}

module.exports = { client, getOrFetch };
