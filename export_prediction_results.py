"""
export_prediction_results.py — PropIQ prediction_results.csv exporter
======================================================================
Queries bet_ledger from Railway Postgres and writes prediction_results.csv.
Also emails the file to ALERT_EMAIL.

Run manually:
    python export_prediction_results.py

Or trigger via APScheduler one-off:
    from export_prediction_results import export_and_email
    export_and_email()

Output columns match what calibrate_model.py / drift_monitor.py expect:
    model_prob, outcome, agent_name, player_name, prop_type, direction,
    line, confidence, ev_pct, clv, bet_date, graded_at, platform, status
"""
from __future__ import annotations

import csv
import io
import logging
import os
import smtplib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

EXPORT_PATH = "/tmp/prediction_results.csv"
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "jayjayslaughter2014@gmail.com")
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")


def _pg_conn():
    import psycopg2  # noqa: PLC0415
    return psycopg2.connect(os.environ["DATABASE_URL"])


def export_prediction_results(output_path: str = EXPORT_PATH) -> int:
    """
    Pull all graded rows from bet_ledger and write prediction_results.csv.
    Returns the number of rows written.
    """
    conn = _pg_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            model_prob,
            actual_outcome          AS outcome,
            agent_name,
            player_name,
            prop_type,
            direction,
            line,
            confidence,
            ev_pct,
            clv,
            bet_date,
            graded_at,
            platform,
            status,
            features_json,
            lookahead_safe,
            discord_sent
        FROM bet_ledger
        WHERE actual_outcome IS NOT NULL
          AND discord_sent   = TRUE
        ORDER BY graded_at DESC
    """)

    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    logger.info("[Export] Wrote %d rows to %s", len(rows), output_path)
    return len(rows)


def email_csv(path: str, row_count: int) -> bool:
    """Email the CSV to ALERT_EMAIL via Gmail SMTP."""
    if not SMTP_USER or not SMTP_PASS:
        logger.warning("[Export] SMTP_USER/SMTP_PASS not set — skipping email")
        return False

    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = ALERT_EMAIL
    msg["Subject"] = (
        f"PropIQ prediction_results.csv — {row_count} graded rows "
        f"({datetime.now().strftime('%Y-%m-%d')})"
    )

    body = (
        f"Attached: prediction_results.csv\n\n"
        f"Rows: {row_count} graded bets (discord_sent=TRUE, actual_outcome IS NOT NULL)\n\n"
        f"Columns: model_prob, outcome, agent_name, player_name, prop_type, direction, "
        f"line, confidence, ev_pct, clv, bet_date, graded_at, platform, status, "
        f"features_json, lookahead_safe, discord_sent\n\n"
        f"Use model_prob + outcome to retrain / evaluate calibration.\n"
        f"— PropIQ"
    )
    msg.attach(MIMEText(body, "plain"))

    with open(path, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f'attachment; filename="prediction_results.csv"',
    )
    msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())
        logger.info("[Export] Email sent to %s", ALERT_EMAIL)
        return True
    except Exception as exc:
        logger.error("[Export] Email failed: %s", exc)
        return False


def export_and_email() -> None:
    """Export prediction_results.csv and email it. Entry point for APScheduler."""
    try:
        row_count = export_prediction_results(EXPORT_PATH)
        if row_count == 0:
            logger.info("[Export] No graded rows yet — skipping email")
            return
        email_csv(EXPORT_PATH, row_count)
    except Exception as exc:
        logger.error("[Export] export_and_email failed: %s", exc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    export_and_email()
