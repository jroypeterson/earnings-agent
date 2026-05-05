"""Gmail API client — read-only access for IR-alert scanning.

Loads OAuth credentials from `gmail_token.json` (refresh token bound to
the authorized account, produced by `scripts/authorize_gmail.py`). The
access token auto-refreshes when expired and the updated token is
written back to disk so subsequent runs reuse it.

Reuses the announcement-detection regex from `rss_client.py` so the
"is this a real earnings-date pre-announcement?" judgment stays
consistent across email and RSS sources.
"""

from __future__ import annotations

import base64
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from rss_client import (
    _ANNOUNCEMENT_RE,
    _NOISE_RE,
    _QUARTER_FROM_MONTH,
    _extract_date_from_title,
    _title_matches_quarter,
)

logger = logging.getLogger("earnings_agent")

# Read-only scope — minimum required. Don't broaden without good reason;
# the audit trail is cleaner if the token can't ever send/modify mail.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

GMAIL_TOKEN_PATH = Path("gmail_token.json")
GMAIL_CLIENT_PATH = Path("gmail_client_credentials.json")


class GmailError(Exception):
    """Raised when Gmail auth or API calls fail in a non-recoverable way."""


@dataclass
class GmailMessage:
    id: str
    thread_id: str
    sender: str         # raw "From:" header, e.g. "Notified <noreply@notified.com>"
    subject: str
    body: str           # plaintext, may be truncated by `body_max_chars`
    received_date: date # local-date interpretation of internalDate


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def get_gmail_service(
    token_path: Path = GMAIL_TOKEN_PATH,
    client_path: Path = GMAIL_CLIENT_PATH,
):
    """Authenticate against Gmail API. Refreshes the access token if expired
    and persists the refreshed credentials.

    Raises GmailError when token_path is missing — the caller is expected
    to handle that as "Gmail integration not configured" rather than a
    hard failure (so cron runs without secrets configured no-op cleanly).
    """
    if not token_path.exists():
        raise GmailError(
            f"{token_path} not found — run scripts/authorize_gmail.py "
            "or set GMAIL_TOKEN_JSON in CI."
        )
    try:
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    except Exception as exc:
        raise GmailError(f"Failed to load credentials from {token_path}: {exc}") from exc

    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as exc:
            raise GmailError(f"Token refresh failed: {exc}") from exc
        try:
            token_path.write_text(creds.to_json(), encoding="utf-8")
        except OSError as exc:
            logger.warning(f"Could not persist refreshed token to {token_path}: {exc}")

    return build("gmail", "v1", credentials=creds)


# ---------------------------------------------------------------------------
# Message queries
# ---------------------------------------------------------------------------


def list_message_ids(service, query: str, max_results: int = 100) -> list[str]:
    """Return message IDs matching a Gmail search query.

    Same query syntax as the Gmail UI search bar:
      * `from:notified.com newer_than:30d`
      * `to:floridabusinessman+ir@gmail.com`
      * `label:ir-earnings is:unread`
    """
    out: list[str] = []
    page_token: str | None = None
    try:
        while len(out) < max_results:
            r = service.users().messages().list(
                userId="me", q=query,
                maxResults=min(max_results - len(out), 100),
                pageToken=page_token,
            ).execute()
            for m in r.get("messages", []):
                out.append(m["id"])
            page_token = r.get("nextPageToken")
            if not page_token:
                break
    except HttpError as exc:
        raise GmailError(f"Gmail list failed: {exc}") from exc
    return out[:max_results]


def get_message(
    service, message_id: str, *, body_max_chars: int = 2000,
) -> GmailMessage:
    """Fetch a full message and extract sender, subject, plaintext body, and
    received date.

    body_max_chars caps body length — IR alerts have the relevant
    "we will release Q1 results on May 8" sentence in the first few
    hundred chars; truncating saves processing time and keeps regex
    backtracking bounded.
    """
    try:
        msg = service.users().messages().get(
            userId="me", id=message_id, format="full",
        ).execute()
    except HttpError as exc:
        raise GmailError(f"Gmail fetch failed for {message_id}: {exc}") from exc

    headers = {
        h["name"].lower(): h["value"]
        for h in msg.get("payload", {}).get("headers", [])
    }
    sender = headers.get("from", "")
    subject = headers.get("subject", "")

    # Gmail internalDate is ms since epoch; treat as the message receipt
    # instant in UTC. We need a calendar date, not a datetime, so the
    # America/New_York day boundary at midnight isn't worth fussing over —
    # IR alerts are dated by the day the company sent them.
    try:
        ts = int(msg.get("internalDate", "0")) / 1000
        received = datetime.fromtimestamp(ts, tz=timezone.utc).date()
    except (ValueError, OSError):
        received = date.today()

    body = _extract_plaintext_body(msg.get("payload", {}))[:body_max_chars]

    return GmailMessage(
        id=msg["id"],
        thread_id=msg.get("threadId", ""),
        sender=sender,
        subject=subject,
        body=body,
        received_date=received,
    )


# ---------------------------------------------------------------------------
# Body extraction (text/plain preferred, text/html stripped as fallback)
# ---------------------------------------------------------------------------


def _walk_parts(payload: dict):
    """Depth-first traversal of MIME parts."""
    yield payload
    for part in payload.get("parts", []) or []:
        yield from _walk_parts(part)


def _extract_plaintext_body(payload: dict) -> str:
    plain = ""
    html = ""
    for part in _walk_parts(payload):
        mime = part.get("mimeType", "")
        data = (part.get("body") or {}).get("data")
        if not data:
            continue
        try:
            decoded = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        except Exception:
            continue
        if mime == "text/plain" and not plain:
            plain = decoded
        elif mime == "text/html" and not html:
            html = decoded
    if plain:
        return plain
    if html:
        return _strip_html(html)
    return ""


_HTML_BLOCK_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def _strip_html(html: str) -> str:
    """Quick-and-dirty HTML-to-text. Sufficient for IR alerts; not a parser."""
    html = _HTML_BLOCK_RE.sub(" ", html)
    html = _HTML_TAG_RE.sub(" ", html)
    return _WHITESPACE_RE.sub(" ", html).strip()


# ---------------------------------------------------------------------------
# Announcement detection (delegates regex to rss_client for consistency)
# ---------------------------------------------------------------------------


def detect_earnings_announcement(
    msg: GmailMessage, expected_release_date: date,
) -> tuple[date | None, bool]:
    """Return (announced_date, matched).

    `matched=True` when the message's subject+body looks like a real
    earnings-date pre-announcement for the same reporting quarter as
    `expected_release_date`. announced_date may still be None when the
    regex matched but no parseable Month-DD date is in the text — in
    that case the caller can choose to confirm the existing date
    without overriding it.

    Reuses the same regex hygiene as rss_client.detect_announcement so
    "earnings preview / what to expect / transcript" noise is rejected.
    """
    text = f"{msg.subject}\n{msg.body}"
    if _NOISE_RE.search(text):
        return None, False
    if not _ANNOUNCEMENT_RE.search(text):
        return None, False
    target_q = _QUARTER_FROM_MONTH[expected_release_date.month]
    if not _title_matches_quarter(text, target_q):
        return None, False
    announced = _extract_date_from_title(text, expected_release_date.year)
    return announced, True


# ---------------------------------------------------------------------------
# Sender pattern helpers (for filtering and provenance)
# ---------------------------------------------------------------------------


def extract_sender_email(raw_from: str) -> str:
    """Extract the bare email address from a 'Display Name <a@b.c>' header."""
    m = re.search(r"<([^>]+)>", raw_from)
    return (m.group(1) if m else raw_from).strip().lower()


def is_known_ir_sender(sender_email: str) -> bool:
    """Quick check for the major IR alert platform domains.

    Conservative — used for prioritization and provenance only, not as
    a filter (other senders may still match the announcement regex).
    """
    sender_email = sender_email.lower()
    domains = (
        "notified.com", "q4inc.com", "globenewswire.com",
        "businesswire.com", "prnewswire.com", "investorroom.com",
    )
    return any(d in sender_email for d in domains)
