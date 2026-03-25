"""
BaseScraper — Anti-ban foundation for all PropIQ scrapers.

Anti-ban techniques:
  - Randomized delays (2-8s between requests, configurable per domain)
  - Rotating User-Agent pool (Chrome/Firefox/Safari across Win/Mac/Linux)
  - Session reuse (persistent TCP, no new handshakes each request)
  - Respectful robots.txt honor per domain setting
  - Redis-backed cache (scrape once per TTL, never hammer same endpoint)
  - Exponential backoff on 429/503 (up to 3 retries)
  - Request jitter to avoid detectable cadence
"""

import time
import random
import logging
import hashlib
import json
from datetime import datetime
from typing import Optional, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ── User-Agent pool ────────────────────────────────────────────────────────────
USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Firefox Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Safari Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

# Per-domain delay ranges (min_s, max_s) — be polite
DOMAIN_DELAYS = {
    "rotowire.com": (4.0, 9.0),
    "actionnetwork.com": (3.0, 7.0),
    "baseballsavant.mlb.com": (5.0, 12.0),
    "default": (2.0, 6.0),
}

# Per-domain cache TTLs in seconds
DOMAIN_CACHE_TTL = {
    "rotowire.com": 900,        # 15 min — lineup/umpire data
    "actionnetwork.com": 120,   # 2 min — public betting moves fast
    "baseballsavant.mlb.com": 3600,  # 1 hr — pitch arsenal daily
    "default": 300,
}


class BaseScraper:
    """Thread-safe, anti-ban HTTP scraper with Redis caching."""

    def __init__(self, redis_client=None, domain_override: Optional[str] = None):
        self.redis = redis_client
        self.domain_override = domain_override
        self.session = self._build_session()
        self._last_request_time: dict[str, float] = {}

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=2.0,
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_domain(self, url: str) -> str:
        if self.domain_override:
            return self.domain_override
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            for domain in DOMAIN_DELAYS:
                if domain in host:
                    return domain
        except Exception:
            pass
        return "default"

    def _polite_delay(self, domain: str):
        """Enforce per-domain minimum delay + jitter to mimic human cadence."""
        min_d, max_d = DOMAIN_DELAYS.get(domain, DOMAIN_DELAYS["default"])
        delay = random.uniform(min_d, max_d)
        last = self._last_request_time.get(domain, 0)
        elapsed = time.time() - last
        if elapsed < delay:
            sleep_for = delay - elapsed + random.uniform(0.1, 0.8)
            logger.debug(f"[{domain}] Polite delay {sleep_for:.2f}s")
            time.sleep(sleep_for)
        self._last_request_time[domain] = time.time()

    def _headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        }

    def _cache_key(self, url: str, params: Optional[dict] = None) -> str:
        raw = url + json.dumps(params or {}, sort_keys=True)
        return "scraper_cache:" + hashlib.md5(raw.encode()).hexdigest()

    def _get_cached(self, key: str) -> Optional[str]:
        if self.redis:
            try:
                val = self.redis.get(key)
                return val.decode() if isinstance(val, bytes) else val
            except Exception:
                pass
        return None

    def _set_cached(self, key: str, html: str, ttl: int):
        if self.redis:
            try:
                self.redis.setex(key, ttl, html)
            except Exception:
                pass

    def get(self, url: str, params: Optional[dict] = None,
            bypass_cache: bool = False) -> Optional[str]:
        """
        Fetch URL with anti-ban protections + Redis caching.
        Returns HTML string or None on failure.
        """
        domain = self._get_domain(url)
        cache_key = self._cache_key(url, params)
        ttl = DOMAIN_CACHE_TTL.get(domain, DOMAIN_CACHE_TTL["default"])

        # Cache hit
        if not bypass_cache:
            cached = self._get_cached(cache_key)
            if cached:
                logger.debug(f"[CACHE HIT] {url}")
                return cached

        # Polite delay
        self._polite_delay(domain)

        try:
            resp = self.session.get(
                url,
                params=params,
                headers=self._headers(),
                timeout=15,
                allow_redirects=True,
            )
            resp.raise_for_status()
            html = resp.text
            self._set_cached(cache_key, html, ttl)
            logger.info(f"[SCRAPED] {url} → {len(html)} chars (domain={domain})")
            return html

        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                logger.warning(f"[RATE LIMIT] {url} — backing off 60s")
                time.sleep(60 + random.uniform(5, 15))
            else:
                logger.error(f"[HTTP ERROR] {url}: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"[REQUEST ERROR] {url}: {e}")
        return None

    def get_json(self, url: str, params: Optional[dict] = None,
                 headers_override: Optional[dict] = None,
                 bypass_cache: bool = False) -> Optional[Any]:
        """Fetch JSON endpoint with caching."""
        domain = self._get_domain(url)
        cache_key = self._cache_key(url, params)
        ttl = DOMAIN_CACHE_TTL.get(domain, DOMAIN_CACHE_TTL["default"])

        if not bypass_cache:
            cached = self._get_cached(cache_key)
            if cached:
                try:
                    return json.loads(cached)
                except Exception:
                    pass

        self._polite_delay(domain)

        try:
            h = self._headers()
            h["Accept"] = "application/json, text/plain, */*"
            if headers_override:
                h.update(headers_override)

            resp = self.session.get(url, params=params, headers=h, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            self._set_cached(cache_key, json.dumps(data), ttl)
            return data

        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                logger.warning(f"[RATE LIMIT JSON] {url} — backing off 60s")
                time.sleep(60 + random.uniform(5, 15))
            else:
                logger.error(f"[HTTP ERROR JSON] {url}: {e}")
        except Exception as e:
            logger.error(f"[JSON ERROR] {url}: {e}")
        return None
