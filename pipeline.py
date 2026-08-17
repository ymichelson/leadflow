"""The pipeline: one inquiry in, one CRM action out.

Flow: normalize -> classify (AI reads) -> route by confidence (code decides)
-> dedupe -> write to CRM. Every branch ends in the CRM; nothing is dropped.
"""

import logging
import os
from datetime import datetime, UTC

from classify import classify_inquiry
from crm import write_lead
from utils import clean_text, normalize_phone

log = logging.getLogger("leadflow")

CONFIDENCE_THRESHOLD = int(os.environ.get("CONFIDENCE_THRESHOLD", "70"))

# Last-seen intake timestamp, used by the silence alert in sla.py
last_intake_at: dict = {"ts": datetime.now(UTC)}

# Small in-memory activity log so the demo page can show what happened.
recent_events: list[dict] = []


async def handle_inquiry(source: str, text: str, phone: str | None = None,
                         email: str | None = None,
                         name: str | None = None,
                         submission_id: str | None = None) -> dict:
    last_intake_at["ts"] = datetime.now(UTC)

    inquiry = {
        "source": source,
        "text": clean_text(text),
        "phone": normalize_phone(phone),
        "email": (email or "").strip().lower() or None,
        # A form field is more trustworthy than asking the model to re-extract
        # the same name from free text. WhatsApp has no explicit name here, so
        # its value remains None and the classifier may still extract one.
        "name": clean_text(name, limit=200) or None,
        "submission_id": submission_id,
    }

    verdict = classify_inquiry(inquiry["text"])

    # The code decides: low confidence means the AI does not get to decide.
    needs_review = verdict["confidence"] < CONFIDENCE_THRESHOLD

    result = await write_lead(inquiry, verdict, needs_review)

    event = {
        "at": datetime.now(UTC).strftime("%H:%M:%S"),
        "source": source,
        "category": verdict["category"],
        "urgency": verdict["urgency"],
        "confidence": verdict["confidence"],
        "needs_review": needs_review,
        "crm": "נכתב ל-CRM" if result.get("ok") else "בתור לניסיון חוזר",
        "created": result.get("created", False),
        "submission_id": submission_id,
    }
    recent_events.insert(0, event)
    del recent_events[30:]

    log.info("inquiry handled: %s", event)
    return {"verdict": verdict, "needs_review": needs_review, "crm": result}
