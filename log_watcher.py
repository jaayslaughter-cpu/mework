"""
PropIQ Log Watcher — runs at 8:10 AM PT via Railway cron or local cron
Hits Railway's log stream API, extracts key dispatch lines,
and sends a summary via email (Gmail SMTP) or SMS (Twilio).

Deploy options:
  A) Add as a Railway cron job (easiest — same project)
  B) Run locally via cron: 10 8 * * * /usr/bin/python3 /path/to/log_watcher.py
  C) GitHub Actions scheduled workflow

Required env vars:
  RAILWAY_API_TOKEN   — Railway API token (Settings → Tokens)
  RAILWAY_SERVICE_ID  — Service ID for the propiq service (Railway dashboard URL)
  RAILWAY_PROJECT_ID  — Project ID (Railway dashboard URL)

One of:
  NOTIFY_EMAIL        — Gmail address to send TO (and FROM via App Password)
  GMAIL_APP_PASSWORD  — Gmail App Password (not your login password)
  TWILIO_SID / TWILIO_TOKEN / TWILIO_FROM / TWILIO_TO  — for SMS
"""

import os
import re
import smtplib
import datetime
import urllib.request
import urllib.parse
import json
from email.mime.text import MIMEText

# ── Config ────────────────────────────────────────────────────────────────
RAILWAY_TOKEN      = os.getenv("RAILWAY_API_TOKEN", "")
SERVICE_ID         = os.getenv("RAILWAY_SERVICE_ID", "")
PROJECT_ID         = os.getenv("RAILWAY_PROJECT_ID", "")
NOTIFY_EMAIL       = os.getenv("NOTIFY_EMAIL", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
TWILIO_SID         = os.getenv("TWILIO_SID", "")
TWILIO_TOKEN       = os.getenv("TWILIO_TOKEN", "")
TWILIO_FROM        = os.getenv("TWILIO_FROM", "")
TWILIO_TO          = os.getenv("TWILIO_TO", "")

# Lines to extract from logs (regex patterns)
WATCH_PATTERNS = [
    r"\[PP\] Fetched \d+ MLB props",
    r"\[UD\] Fetched \d+ MLB lines",
    r"\[PP\] HTTP \d+",
    r"\[UD\] HTTP \d+",
    r"\[Apify\] Proxy .+ -> HTTP \d+",
    r"No props fetched from either platform",
    r"No MLB games found",
    r"Leg pool: \d+ evaluated legs",
    r"Dedup pass: \d+/\d+ parlays queued",
    r"Dispatch complete -- \d+ parlays",
    r"\[PP-SB\] Fetched \d+",
    r"\[UD-Apify\] Proxy fetched \d+",
    r"No legs passed EV/prob gates",
    r"below 6\.0 gate",
    r"No qualifying parlay",
    r"SEND$",
]


def fetch_railway_logs(minutes_back: int = 15) -> list[str]:
    """
    Fetch recent logs from Railway GraphQL API.
    Returns list of log line strings.
    """
    if not RAILWAY_TOKEN or not SERVICE_ID:
        return ["ERROR: RAILWAY_API_TOKEN or RAILWAY_SERVICE_ID not set"]

    # Railway GraphQL endpoint
    url = "https://backboard.railway.app/graphql/v2"
    
    # Compute time window
    now = datetime.datetime.now(datetime.timezone.utc)
    since = (now - datetime.timedelta(minutes=minutes_back)).isoformat()

    query = """
    query ServiceLogs($serviceId: String!, $filter: String) {
      serviceLogs(serviceId: $serviceId, filter: $filter, limit: 500) {
        message
        timestamp
        severity
      }
    }
    """
    payload = json.dumps({
        "query": query,
        "variables": {
            "serviceId": SERVICE_ID,
            "filter": since,
        }
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {RAILWAY_TOKEN}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        logs = data.get("data", {}).get("serviceLogs", [])
        return [entry.get("message", "") for entry in logs]
    except Exception as e:
        return [f"ERROR fetching logs: {e}"]


def extract_key_lines(log_lines: list[str]) -> list[str]:
    """Pull only the lines that matter for dispatch monitoring."""
    hits = []
    for line in log_lines:
        for pattern in WATCH_PATTERNS:
            if re.search(pattern, line):
                # Strip Railway timestamp prefix if present
                clean = re.sub(r"^\d{4}-\d{2}-\d{2}T[\d:.Z+\-]+\s*", "", line).strip()
                hits.append(clean)
                break
    return hits


def build_summary(key_lines: list[str]) -> str:
    """Build a human-readable dispatch summary."""
    now_pt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=7)
    time_str = now_pt.strftime("%I:%M %p PT")

    if not key_lines:
        return f"PropIQ @ {time_str}: No matching log lines found. Check Railway manually."

    # Detect outcome
    dispatch_line = next((l for l in key_lines if "Dispatch complete" in l), None)
    no_props      = any("No props fetched" in l for l in key_lines)
    no_legs       = any("No legs passed" in l for l in key_lines)
    no_games      = any("No MLB games" in l for l in key_lines)

    if no_games:
        status = "⚾ NO GAMES TODAY"
    elif no_props:
        status = "❌ NO PROPS FETCHED — check PP/UD/Apify"
    elif no_legs:
        status = "⚠️  PROPS FETCHED BUT NO LEGS PASSED GATES"
    elif dispatch_line:
        n = re.search(r"(\d+) parlays", dispatch_line)
        count = n.group(1) if n else "?"
        status = f"✅ {count} PARLAY(S) SENT TO DISCORD"
    else:
        status = "⏳ DISPATCH MAY STILL BE RUNNING"

    lines_str = "\n".join(f"  {l}" for l in key_lines)
    return (
        f"PropIQ Dispatch @ {time_str}\n"
        f"Status: {status}\n\n"
        f"Key log lines:\n{lines_str}"
    )


def send_email(subject: str, body: str) -> None:
    if not NOTIFY_EMAIL or not GMAIL_APP_PASSWORD:
        print("[Notify] Email not configured — printing to stdout:")
        print(body)
        return
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = NOTIFY_EMAIL
    msg["To"]      = NOTIFY_EMAIL
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(NOTIFY_EMAIL, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        print(f"[Notify] Email sent to {NOTIFY_EMAIL}")
    except Exception as e:
        print(f"[Notify] Email failed: {e}")
        print(body)


def send_sms(body: str) -> None:
    if not TWILIO_SID or not TWILIO_TOKEN or not TWILIO_FROM or not TWILIO_TO:
        return  # SMS not configured — email is primary
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    data = urllib.parse.urlencode({
        "From": TWILIO_FROM,
        "To":   TWILIO_TO,
        "Body": body[:1600],
    }).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    # Basic auth
    import base64
    creds = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
    req.add_header("Authorization", f"Basic {creds}")
    try:
        with urllib.request.urlopen(req, timeout=10):
            print(f"[Notify] SMS sent to {TWILIO_TO}")
    except Exception as e:
        print(f"[Notify] SMS failed: {e}")


def main():
    print("[LogWatcher] Fetching Railway logs...")
    log_lines  = fetch_railway_logs(minutes_back=15)
    key_lines  = extract_key_lines(log_lines)
    summary    = build_summary(key_lines)

    print(summary)

    subject = "PropIQ Dispatch " + (
        "✅ SENT" if "SENT TO DISCORD" in summary else
        "❌ NO PROPS"  if "NO PROPS" in summary else
        "⚠️  CHECK"
    )
    send_email(subject, summary)
    send_sms(summary)


if __name__ == "__main__":
    main()
