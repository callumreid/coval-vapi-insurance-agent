"""Vapi voice agent webhook server — Bronstate Auto Insurance (Morgan).

Vapi setup
──────────
Two entry-points in Vapi need to point at <deployed-url>/webhook:
  1. The phone number's serverUrl   → assistant-request
  2. The assistant's serverUrl      → tool-calls, end-of-call-report

Distinct failure mode for this org:
  `dispatch_roadside` sleeps 12 seconds before returning (simulated slow
  third-party dispatch API). The agent should explain the delay and offer a
  callback rather than stalling silently — Pause Anomalies metric catches
  dead-air, and the Patient Communication During Tool Delay LLM judge catches
  the failure to acknowledge the wait.

Run locally
───────────
  pip install -r requirements.txt
  uvicorn server:app --port 8000 --reload

Deploy to Fly.io
────────────────
  fly deploy
"""

import json
import logging
import os
import time

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Bronstate Auto Insurance — Morgan (Vapi)")

# ── Config ───────────────────────────────────────────────────────────────────
# Populated post-create via Fly secret VAPI_ASSISTANT_ID.
MORGAN_ASSISTANT_ID = os.environ.get("VAPI_ASSISTANT_ID", "")


# ── Mock tool handlers ────────────────────────────────────────────────────────

_MOCK_TOOLS: dict[str, callable] = {}


def _tool(name: str):
    def decorator(fn):
        _MOCK_TOOLS[name] = fn
        return fn
    return decorator


def _tool_succeeded(result: str) -> bool:
    try:
        parsed = json.loads(result)
    except (json.JSONDecodeError, TypeError):
        return True
    return not (isinstance(parsed, dict) and parsed.get("error"))


@_tool("lookup_policy")
def _lookup_policy(args: dict) -> str:
    policy_number = args.get("policy_number", "BSA-0000000")
    return json.dumps({
        "policy_number": policy_number,
        "holder_name": "Alicia Romero",
        "status": "ACTIVE",
        "policy_type": "Auto — Full Coverage",
        "effective_date": "2024-08-15",
        "renewal_date": "2026-08-15",
        "vehicle": "2022 Toyota Camry SE, VIN ...HJ4892",
        "deductible": 500,
        "liability_limit": "100/300/100",
        "comprehensive": True,
        "collision": True,
        "roadside_assistance": True,
        "rental_reimbursement": True,
    })


@_tool("file_fnol")
def _file_fnol(args: dict) -> str:
    policy_number = args.get("policy_number", "BSA-0000000")
    incident_date = args.get("incident_date", "unspecified")
    location = args.get("location", "unspecified")
    parties_involved = args.get("parties_involved", "unspecified")
    damage_description = args.get("damage_description", "unspecified")
    return json.dumps({
        "success": True,
        "claim_id": "CLM-2026-051203",
        "policy_number": policy_number,
        "incident_date": incident_date,
        "location": location,
        "parties_involved": parties_involved,
        "damage_description": damage_description,
        "next_steps": "An adjuster will contact you within 24 hours. Please upload photos to the claim via the BSA app.",
        "adjuster_phone": "1-800-555-0142",
    })


@_tool("dispatch_roadside")
def _dispatch_roadside(args: dict) -> str:
    # DISTINCT FAILURE MODE — simulated slow third-party dispatch API.
    # Sleeps 12 seconds. The agent should explain the delay + offer a callback
    # rather than stall silently. Pause Anomalies metric trips on dead air; the
    # Patient Communication During Tool Delay LLM judge catches non-acknowledgment.
    logger.warning("dispatch_roadside: simulating slow upstream (12s)")
    time.sleep(12)
    policy_number = args.get("policy_number", "BSA-0000000")
    location = args.get("location", "unspecified")
    issue_type = args.get("issue_type", "general")
    return json.dumps({
        "success": True,
        "dispatch_id": "RSA-2026-988412",
        "policy_number": policy_number,
        "location": location,
        "issue_type": issue_type,
        "eta_minutes": 45,
        "provider": "Bronstate Partner Network — Bay Area Towing",
        "note": "Apologies for the delay confirming dispatch. Service vehicle is on the way.",
    })


@_tool("check_coverage")
def _check_coverage(args: dict) -> str:
    policy_number = args.get("policy_number", "BSA-0000000")
    coverage_type = args.get("coverage_type", "liability")
    coverage_details = {
        "liability": {
            "bodily_injury_per_person": 100000,
            "bodily_injury_per_accident": 300000,
            "property_damage": 100000,
        },
        "collision": {"deductible": 500, "covered": True},
        "comprehensive": {"deductible": 250, "covered": True},
        "rental": {"daily_limit": 50, "max_days": 30, "covered": True},
        "uninsured_motorist": {"per_person": 100000, "per_accident": 300000},
        "roadside": {"included": True, "annual_uses": 4, "uses_remaining": 3},
        "glass": {"deductible_waived": True, "covered": True},
    }
    detail = coverage_details.get(coverage_type, {"covered": True, "note": "Standard coverage applies."})
    return json.dumps({
        "policy_number": policy_number,
        "coverage_type": coverage_type,
        **detail,
    })


@_tool("glass_claim")
def _glass_claim(args: dict) -> str:
    policy_number = args.get("policy_number", "BSA-0000000")
    damage_description = args.get("damage_description", "unspecified")
    return json.dumps({
        "success": True,
        "claim_id": "GLS-2026-447721",
        "policy_number": policy_number,
        "damage_description": damage_description,
        "deductible_waived": True,
        "repair_options": [
            {"shop": "Safelite — Downtown", "earliest": "Tomorrow 9:00 AM", "mobile": True},
            {"shop": "Bay Auto Glass — Oakland", "earliest": "Today 4:00 PM", "mobile": False},
            {"shop": "ClearView Repair — San Jose", "earliest": "Thursday 11:00 AM", "mobile": True},
        ],
        "next_steps": "Choose a shop and we will schedule. Mobile service can come to your home or office.",
    })


@_tool("policy_modification")
def _policy_modification(args: dict) -> str:
    policy_number = args.get("policy_number", "BSA-0000000")
    change_type = args.get("change_type", "address")
    new_value = args.get("new_value", "unspecified")
    return json.dumps({
        "success": True,
        "policy_number": policy_number,
        "change_type": change_type,
        "new_value": new_value,
        "effective_date": "2026-05-21",
        "confirmation_number": "MOD-2026-771204",
        "premium_impact": "No change to premium.",
        "message": f"Your {change_type} has been updated to {new_value}. A confirmation email is on the way.",
    })


# ── Webhook endpoint ──────────────────────────────────────────────────────────

@app.post("/webhook")
async def vapi_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    message = body.get("message", {})
    msg_type = message.get("type", "")
    call = message.get("call", {})
    call_id = call.get("id", "")

    logger.info(f"Vapi webhook: type={msg_type} call={call_id}")

    # ── assistant-request ─────────────────────────────────────────────────────
    if msg_type == "assistant-request":
        return JSONResponse({"assistantId": MORGAN_ASSISTANT_ID})

    # ── tool-calls ────────────────────────────────────────────────────────────
    elif msg_type == "tool-calls":
        results = []
        tool_list = message.get("toolCallList", [])
        logger.info(f"  tool-calls payload keys: {list(message.keys())} toolCallList count: {len(tool_list)}")
        for tc in tool_list:
            fn = tc.get("function", {})
            name = fn.get("name", "")
            raw_args = fn.get("arguments", "{}")
            try:
                args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
            except (json.JSONDecodeError, TypeError):
                args = {}

            handler = _MOCK_TOOLS.get(name)
            if handler:
                result = handler(args)
                logger.info(f"  Tool call: {name} succeeded={_tool_succeeded(result)}")
            else:
                result = json.dumps({"error": f"Unknown tool: {name}"})
                logger.warning(f"  Unknown tool: {name}")

            results.append({"toolCallId": tc.get("id", ""), "result": result})

        return JSONResponse({"results": results})

    # ── end-of-call-report ────────────────────────────────────────────────────
    elif msg_type == "end-of-call-report":
        logger.info(f"  Call ended: call={call_id} reason={call.get('endedReason', '')}")

    # ── all other events ──────────────────────────────────────────────────────
    return JSONResponse({})


@app.get("/health")
async def health():
    return {"status": "ok"}
