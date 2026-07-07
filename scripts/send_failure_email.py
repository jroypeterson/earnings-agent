"""Out-of-band failure/alert email via Gmail SMTP (app password).

Slack is the primary alert channel — but every Slack alert (including the
if:failure() ones) rides Slack, so if Slack itself is down a red run would only
be visible in the GitHub UI. The non-Slack backup is an email sent from each
workflow's if:failure() block.

NOTE: the workflows INLINE this logic (runner python3 + stdlib) rather than
calling this file, so the backup keeps working even if `actions/checkout`
itself is what failed. This module is the canonical, unit-tested reference for
that logic (keep them in sync) and is usable for manual/local sends.

By design it fires ONLY on failures (wired into if:failure()), not on normal
earnings posts — keeping it signal, not noise.

Reuses the existing Gmail app-password creds (GMAIL_ADDRESS / GMAIL_APP_PASSWORD,
same as 13F Analyzer; see AUTHENTICATIONS.md) rather than a new SMTP secret.
Sends to ALERT_EMAIL_TO (default jroypeterson+alerts@gmail.com).

Best-effort: no-ops with a notice when creds are unset (opt-in); raises only on
an actual send error, so a misconfigured backup shows as a red step.

Usage:
  python scripts/send_failure_email.py "<subject>" "<body>"
"""
import os
import sys
import smtplib
from email.mime.text import MIMEText

# Fleet-wide subject grammar (root CONVENTIONS.md §5):
#   [ClaudeFin] earnings_agent — <what>
# so one inbox filter catches every Claude-project alert email.
SUBJECT_PREFIX = "[ClaudeFin] earnings_agent — "
DEFAULT_SUBJECT = SUBJECT_PREFIX + "alert"


def send(subject: str, body: str) -> bool:
    """Send the alert email. Returns True if sent, False if skipped (creds
    unset). Raises only on an actual SMTP error (so a misconfigured backup is
    visible as a red step). Env is read at call time for testability."""
    addr = os.environ.get("GMAIL_ADDRESS", "").strip()
    pw = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    to = os.environ.get("ALERT_EMAIL_TO", "jroypeterson+alerts@gmail.com").strip()
    if not addr or not pw:
        print("send_failure_email: GMAIL_ADDRESS/GMAIL_APP_PASSWORD unset — "
              "skipping email backup (opt-in)")
        return False
    msg = MIMEText(body or subject, "plain")
    msg["Subject"] = subject
    msg["From"] = addr
    msg["To"] = to
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
        server.login(addr, pw)
        server.send_message(msg)
    print(f"send_failure_email: sent to {to}: {subject!r}")
    return True


def main() -> None:
    subject = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
        "ALERT_SUBJECT", DEFAULT_SUBJECT)
    body = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("ALERT_BODY", "")
    send(subject, body)


if __name__ == "__main__":
    main()
