"""
Slack Web API client (bot-token).

Used by paths that need to post a parent message and later read replies
in its thread (cross-check disagreements, unseen-ticker alerts, urgent
date-move alerts). Webhook posting in `notifications.post_slack` remains
the path for one-shot messages that don't need replies (heartbeat,
digest).

Required env: SLACK_BOT_TOKEN (xoxb-...), SLACK_CHANNEL_ID (Cxxx).
Required scopes: chat:write, channels:history (or groups:history for
private channels).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import requests

logger = logging.getLogger("earnings_agent")

_API_BASE = "https://slack.com/api"
_HTTP_TIMEOUT = 10


class SlackAPIError(RuntimeError):
    pass


@dataclass
class SlackReply:
    user: str
    text: str
    ts: str
    is_bot: bool


def _post(token: str, method: str, payload: dict) -> dict:
    resp = requests.post(
        f"{_API_BASE}/{method}",
        json=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("ok"):
        raise SlackAPIError(f"{method} failed: {body.get('error', 'unknown')}")
    return body


def _get(token: str, method: str, params: dict) -> dict:
    resp = requests.get(
        f"{_API_BASE}/{method}",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("ok"):
        raise SlackAPIError(f"{method} failed: {body.get('error', 'unknown')}")
    return body


def post_message(
    token: str,
    channel: str,
    *,
    blocks: list[dict] | None = None,
    text: str = "",
    thread_ts: str | None = None,
) -> str:
    """
    Post a message via chat.postMessage. Returns the message `ts`, which
    is also the thread root `ts` when replying to threaded messages.
    """
    payload: dict = {"channel": channel, "text": text}
    if blocks:
        payload["blocks"] = blocks
    if thread_ts:
        payload["thread_ts"] = thread_ts
    body = _post(token, "chat.postMessage", payload)
    return body["ts"]


def fetch_thread_replies(
    token: str,
    channel: str,
    thread_ts: str,
    *,
    oldest: str | None = None,
) -> list[SlackReply]:
    """
    Return replies in the thread rooted at thread_ts. When `oldest` is
    set (a Slack ts string), only replies strictly after that ts are
    returned. The parent message itself is excluded from the result.
    """
    params: dict = {"channel": channel, "ts": thread_ts, "limit": 200}
    if oldest:
        params["oldest"] = oldest
    body = _get(token, "conversations.replies", params)
    out: list[SlackReply] = []
    for msg in body.get("messages", []):
        ts = msg.get("ts")
        if not ts or ts == thread_ts:
            continue  # skip parent
        if oldest and ts <= oldest:
            continue  # Slack's `oldest` is inclusive; we want strict
        out.append(SlackReply(
            user=msg.get("user", ""),
            text=msg.get("text", ""),
            ts=ts,
            is_bot=bool(msg.get("bot_id")),
        ))
    return out
