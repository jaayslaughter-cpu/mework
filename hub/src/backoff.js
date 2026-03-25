// hub/src/backoff.js
// Exponential backoff with jitter for 429 and 5xx responses.

export async function withBackoff(fn, maxRetries = 4, baseDelay = 1000) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      const isRateLimit = err?.response?.status === 429;
      const isServerErr = err?.response?.status >= 500;

      if ((isRateLimit || isServerErr) && attempt < maxRetries) {
        const delay = baseDelay * Math.pow(2, attempt) + Math.random() * 500;
        console.warn(`[Backoff] Attempt ${attempt + 1} failed (${err?.response?.status}). Retrying in ${Math.round(delay)}ms...`);
        await new Promise(r => setTimeout(r, delay));
      } else {
        throw err;
      }
    }
  }
}

module.exports = { withBackoff };
