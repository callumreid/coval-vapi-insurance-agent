"""Monitoring cron — Bronstate Auto Insurance demo.

Reads `pool/transcripts.json`, picks one at random, submits it to Coval as a
monitoring conversation via `POST /v1/conversations:submit`.

Triggered by a Fly machine cron schedule (`fly machines run --schedule=hourly`).
"""

import json
import logging
import os
import random
import sys
import uuid
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

POOL_PATH = Path(__file__).resolve().parent / "pool" / "transcripts.json"
COVAL_API_URL = "https://api.coval.dev/v1/conversations:submit"

# Bronstate Auto Insurance demo: 5 monitoring metrics scored on every submitted
# conversation. Mirrors the dashboard's primary failure-mode beats (dispatch
# roadside 12s timeout) plus FNOL flow + fraud + holistic success.
MONITORING_METRIC_IDS = [
    "XBt7PXBetrV6dT9Cur98Ka",  # Conversation Success
    "TKpxSZg4W8mvjMEjXEfLNS",  # Pause Anomalies
    "2K25iAgH2mHHhXZAtdx9Zq",  # Patient Communication During Tool Delay
    "3EeuoESDinYctA4f7Kecir",  # FNOL Completeness
    "AHJ4nfC2NuNJNYuCRnkJYi",  # Fraud Detection
]


def main() -> int:
    api_key = os.environ.get("COVAL_API_KEY")
    agent_id = os.environ.get("COVAL_AGENT_ID")
    if not api_key or not agent_id:
        logger.error("missing COVAL_API_KEY or COVAL_AGENT_ID env vars")
        return 1

    if not POOL_PATH.exists():
        logger.error(f"transcript pool not found at {POOL_PATH}")
        return 1

    try:
        pool = json.loads(POOL_PATH.read_text())
    except json.JSONDecodeError as exc:
        logger.error(f"transcript pool malformed JSON: {exc}")
        return 1

    if not pool:
        logger.warning("transcript pool empty; skipping cron run")
        return 0

    transcript = random.choice(pool)
    tier = random.choice(["smb", "enterprise", "trial"])
    body = {
        "transcript": transcript,
        "agent_id": agent_id,
        "metrics": MONITORING_METRIC_IDS,
        "metadata": {"source": "demo-cron", "tier": tier},
        "external_conversation_id": f"demo-{uuid.uuid4()}",
    }

    logger.info(
        f"submitting monitoring conversation: tier={tier} pool_size={len(pool)} "
        f"metrics={len(MONITORING_METRIC_IDS)}"
    )
    try:
        resp = httpx.post(
            COVAL_API_URL,
            json=body,
            headers={"x-api-key": api_key, "content-type": "application/json"},
            timeout=30.0,
        )
    except httpx.RequestError as exc:
        logger.error(f"request error: {exc}")
        return 1

    if resp.status_code >= 300:
        logger.error(f"submit failed: status={resp.status_code} body={resp.text[:300]}")
        return 1

    logger.info(f"submit ok: status={resp.status_code}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
