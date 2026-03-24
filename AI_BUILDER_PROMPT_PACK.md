# PropIQ AI Builder Prompt Pack
## Universal prompts — works with Factory AI, Cursor, Claude, Copilot, Gemini, Bolt.new, Replit

---

## TICKET-7.1 — Frontend Next.js Scaffold ✅ (complete in /frontend)

---

## TICKET-7.2 — Connect Frontend to Live API

> **Paste to: Factory AI / Cursor / Copilot**

The PropIQ Next.js frontend is scaffolded at `/frontend`. Now wire it to the live backend APIs.

**Objective:** Replace mock data in `frontend/src/app/page.tsx` with real API calls.

**Required changes to `frontend/src/app/page.tsx`:**
1. Add a `useEffect` that calls `GET ${process.env.NEXT_PUBLIC_HUB_URL}/api/slates/today` on mount
2. For each market in the response, call `POST ${process.env.NEXT_PUBLIC_ENGINE_URL}/api/predict/edge`
3. Map results to `PropCard` objects matching the existing interface
4. Show a loading skeleton while data loads (use `animate-pulse` Tailwind classes)
5. Show an error state if both APIs are unreachable

**Error handling:**
- If Hub is down, show "Live data unavailable — showing last cached data"
- If Engine is down, show edge as "N/A" but still display the prop card

---

## TICKET-7.3 — Agent Army Dashboard Page

> **Paste to: Factory AI / Cursor**

Add a new page at `/frontend/src/app/agents/page.tsx` that displays today's Agent Army tickets.

**Requirements:**
- 4 cards: Agent_2Leg, Agent_3Leg, Agent_Best, Agent_5Leg
- Each card shows: agent name, legs (player, prop, line, odds, edge%), joint probability, estimated parlay odds
- Color code: green border = ticket generated, zinc border = no qualifying ticket today
- Data source: `GET ${process.env.NEXT_PUBLIC_ENGINE_URL}/api/agents/today`
- Add this page to the header nav in `layout.tsx`

---

## TICKET-8.1 — Train XGBoost Models

> **Paste to: Cursor / Claude / Copilot (in terminal)**

The training script is at `scripts/train_model.py`. Run it to train the 3 models.

**Pre-requisites:**
```bash
pip install -r api/requirements.txt
export SPORTSBLAZE_API_KEY=your_key_here  # Optional — skips if missing
```

**Run training:**
```bash
python scripts/train_model.py
```

This will:
1. Fetch Statcast pitch data (April 2023 — ~2 weeks for speed)
2. Enrich with Baseball Savant leaderboard data (barrels/PA, xwOBA, hard hit%)
3. Optionally enrich with SportsBlaze stats if API key is set
4. Train 3 XGBoost models and save to `api/models/`
5. Print accuracy metrics for each model

**After training:** Restart the engine container: `docker-compose restart engine`

---

## TICKET-8.2 — Add FanGraphs Data Enrichment

> **Paste to: Factory AI**

Enhance the training pipeline by adding FanGraphs advanced metrics as additional features.

**Add to `scripts/train_model.py`:**

```python
def fetch_fangraphs_batting(year: int) -> pd.DataFrame:
    """
    Fetch FanGraphs batting leaderboard via pybaseball.
    Includes: wRC+, wOBA, BB%, K%, Hard%, Contact%
    """
    import pybaseball as pyb
    try:
        df = pyb.batting_stats(year, qual=50)
        cols = ['IDfg', 'Name', 'Team', 'wRC+', 'wOBA', 'BB%', 'K%', 'Hard%', 'Contact%', 'O-Swing%', 'Z-Swing%']
        available = [c for c in cols if c in df.columns]
        return df[available].copy()
    except Exception as e:
        print(f"FanGraphs batting fetch failed: {e}")
        return pd.DataFrame()
```

Merge this into `engineer_features()` on the `batter` column matching FanGraphs `IDfg`.

---

## TICKET-8.3 — Implement Secure Secrets Management

> **Paste to: Factory AI / Copilot**

Replace all `os.environ.get()` calls with a centralized `config.py` module that validates required keys on startup.

**Create `api/config.py`:**
```python
import os
from dataclasses import dataclass

@dataclass
class Config:
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    sportsblaze_api_key: str
    redis_url: str

    @classmethod
    def from_env(cls) -> "Config":
        required = ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]
        missing = [k for k in required if not os.environ.get(k)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
        return cls(
            postgres_user=os.environ["POSTGRES_USER"],
            postgres_password=os.environ["POSTGRES_PASSWORD"],
            postgres_db=os.environ["POSTGRES_DB"],
            postgres_host=os.environ.get("POSTGRES_HOST", "postgres"),
            sportsblaze_api_key=os.environ.get("SPORTSBLAZE_API_KEY", ""),
            redis_url=os.environ.get("REDIS_URL", "redis://redis:6379"),
        )

config = Config.from_env()
```

Import and use `from config import config` in all API services.
