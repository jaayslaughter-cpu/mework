# Context7 MCP Integration — PropIQ Analytics

Context7 gives your AI assistant (Cursor, Claude Desktop, VS Code) **live, version-accurate docs**
for every library in this stack — eliminating hallucinated deprecated syntax.

## Your API Keys
```
ctx7sk-8e246594-e35e-44f1-b8df-bd2416b8831e   (primary)
ctx7sk-5c73433a-6430-4a2f-8df7-fe3f2c768d28   (backup)
```

---

## Setup by IDE

### Cursor
Create `.cursor/mcp.json` in your project root:
```json
{
  "mcpServers": {
    "context7": {
      "command": "npx",
      "args": ["-y", "@upstash/context7-mcp@latest"],
      "env": {
        "CONTEXT7_API_KEY": "ctx7sk-8e246594-e35e-44f1-b8df-bd2416b8831e"
      }
    }
  }
}
```

### Claude Desktop
Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "context7": {
      "command": "npx",
      "args": ["-y", "@upstash/context7-mcp@latest"],
      "env": {
        "CONTEXT7_API_KEY": "ctx7sk-8e246594-e35e-44f1-b8df-bd2416b8831e"
      }
    }
  }
}
```

### VS Code (with Copilot)
Add to `.vscode/settings.json`:
```json
{
  "mcp.servers": {
    "context7": {
      "command": "npx",
      "args": ["-y", "@upstash/context7-mcp@latest"],
      "env": {
        "CONTEXT7_API_KEY": "ctx7sk-8e246594-e35e-44f1-b8df-bd2416b8831e"
      }
    }
  }
}
```

---

## Add a Workspace Rule (Cursor / Windsurf)

Create `.cursor/rules` or `.windsurfrules`:
```
Always use the Context7 MCP tools to fetch the latest documentation
before generating code for any external library in this project.

Key libraries to always look up:
- next.js (App Router, not Pages Router)
- xgboost (Python API)
- fastapi
- sqlalchemy
- pybaseball
- requests
- pandas
```

---

## How It Helps PropIQ Specifically

| What you're building | What Context7 ensures |
|---|---|
| Next.js 14 App Router pages | Uses `app/` directory syntax, not deprecated `pages/` |
| XGBoost model loading | Correct `.load_model()` vs `.load_weights()` syntax |
| FastAPI routes | Current `Annotated` dependency injection pattern |
| pybaseball Statcast calls | Latest function signatures (they change) |
| SQLAlchemy 2.0 | Async session syntax, not legacy 1.x patterns |

---

## REST API (for agent-level integration)

```bash
curl -X GET "https://context7.com/api/v2/libs/search?libraryName=next.js&query=app+router+ssr" \
  -H "Authorization: Bearer ctx7sk-8e246594-e35e-44f1-b8df-bd2416b8831e"
```

```python
# In your Tasklet agent or ETL pipeline
import requests

def get_docs(library: str, query: str) -> str:
    resp = requests.get(
        "https://context7.com/api/v2/libs/search",
        params={"libraryName": library, "query": query},
        headers={"Authorization": "Bearer ctx7sk-8e246594-e35e-44f1-b8df-bd2416b8831e"},
        timeout=10,
    )
    if resp.status_code == 200:
        return resp.json()
    return {}
```

---

## Connecting to PropIQ GitHub Repos

Context7 can index your own repo so your AI can reference `mework` code:
1. Go to [context7.com](https://context7.com)
2. Log in → **Add Library**
3. Paste: `https://github.com/jaayslaughter-cpu/mework`
4. Your AI can now say: `use context7 mework` to pull live code context

---

## Does NOT replace your sports APIs
Context7 sits **alongside** your data pipeline:

```
┌─────────────────────────────────────────────┐
│  AI Assistant (Cursor / Claude)             │
│    ↓ uses Context7 for accurate code gen    │
├─────────────────────────────────────────────┤
│  PropIQ Pipeline                           │
│    ├── The Odds API → live prop lines       │
│    ├── MLB Stats API → player stats         │
│    ├── ESPN API → injuries / scores         │
│    └── XGBoost Model → predictions         │
└─────────────────────────────────────────────┘
```
