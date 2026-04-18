# How to Export Your Discord Pick History

## Step 1 — Get DiscordChatExporter

The easiest free tool. Download from:
https://github.com/Tyrrrz/DiscordChatExporter/releases

**Mac/Linux:** Download `DiscordChatExporter.Cli.zip`
**Windows:** Download `DiscordChatExporter.zip` (GUI) or `.Cli.zip`

---

## Step 2 — Get your Discord token

1. Open Discord in your browser (discord.com — not the app)
2. Press F12 → Network tab
3. Click any channel
4. Find any request to `discord.com/api`
5. Look in Request Headers for `Authorization:` — that's your token

⚠️ Keep this private — it's your account token.

---

## Step 3 — Get your PropIQ channel ID

1. In Discord, right-click your picks channel
2. Click "Copy Channel ID"
   (If you don't see this, go to User Settings → Advanced → Enable Developer Mode)

---

## Step 4 — Export the channel

```bash
# Export as JSON (required for the backfill script)
./DiscordChatExporter.Cli export \
  -t YOUR_TOKEN_HERE \
  -c YOUR_CHANNEL_ID_HERE \
  -f Json \
  -o picks_export.json \
  --after 2026-03-26    # PropIQ launch date
```

This creates `picks_export.json` in your current folder.

---

## Step 5 — Run the backfill script

```bash
# Install dependencies
pip install requests psycopg2-binary python-dotenv

# Set your Railway DATABASE_URL
export DATABASE_URL="postgresql://user:password@host:port/dbname"
# (Find this in Railway → your Postgres service → Connect tab)

# Preview first — no writes
python3 discord_backfill.py --input picks_export.json --dry-run

# If preview looks right, write to DB
python3 discord_backfill.py --input picks_export.json --write
```

---

## What the script does

For each PropIQ embed in your Discord history, it:
1. Parses player name, prop type, side, line, agent, platform, date, stake
2. Looks up the actual game result from ESPN (for past dates)
3. Inserts one row per leg into `bet_ledger` with WIN/LOSS/OPEN status
4. Skips any legs already in the database (duplicate protection)

Tonight's GradingTasklet (2AM PT) will automatically grade any OPEN rows
from past games that ESPN couldn't match.

---

## Verify it worked

Run in Railway's Postgres console (or any SQL client):

```sql
-- See what dates were backfilled
SELECT bet_date, COUNT(*) as picks, 
       COUNT(CASE WHEN actual_outcome IS NOT NULL THEN 1 END) as graded
FROM bet_ledger 
GROUP BY bet_date 
ORDER BY bet_date DESC;

-- Check total graded rows (XGBoost needs 200)
SELECT COUNT(*) FROM bet_ledger 
WHERE actual_outcome IS NOT NULL AND discord_sent = TRUE;
```
