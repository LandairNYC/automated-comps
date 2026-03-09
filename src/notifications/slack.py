"""
CompScope Slack Notifications
src/notifications/slack.py

Sends pipeline run summaries to a Slack webhook.
Set SLACK_WEBHOOK_URL in your .env file.
"""

import os
import json
import requests
from datetime import datetime, timezone
from typing import Optional


SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def _post(payload: dict) -> bool:
    if not SLACK_WEBHOOK_URL:
        print("[slack] SLACK_WEBHOOK_URL not set — skipping notification")
        return False
    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[slack] Failed to send notification: {e}")
        return False


def notify_success(
    new_records: int,
    updated_records: int,
    total_records: int,
    duration_seconds: int,
    cutoff_date: str,
    dry_run: bool = False,
):
    mode = "🧪 DRY RUN" if dry_run else "✅ Success"
    mins = duration_seconds // 60
    secs = duration_seconds % 60
    duration_str = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"CompScope Sync {mode}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*New Records:*\n{new_records:,}"},
                    {"type": "mrkdwn", "text": f"*Updated Records:*\n{updated_records:,}"},
                    {"type": "mrkdwn", "text": f"*Total in DB:*\n{total_records:,}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{duration_str}"},
                    {"type": "mrkdwn", "text": f"*Pulled Sales Since:*\n{cutoff_date}"},
                    {"type": "mrkdwn", "text": f"*Run Time:*\n{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
                ]
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": "CompScope Beta — Development Site Comps"}
                ]
            }
        ]
    }
    _post(payload)


def notify_failure(
    stage: str,
    error_message: str,
    duration_seconds: int,
):
    mins = duration_seconds // 60
    secs = duration_seconds % 60
    duration_str = f"{mins}m {secs}s" if mins > 0 else f"{secs}s"

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "❌ CompScope Sync Failed",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Failed At:*\n{stage}"},
                    {"type": "mrkdwn", "text": f"*Duration Before Failure:*\n{duration_str}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{error_message[:500]}```"
                }
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": "Run manually: `python pipeline.py --skip-extract` to retry transform + sync"}
                ]
            }
        ]
    }
    _post(payload)
