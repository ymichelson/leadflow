"""AI brain: classify + extract structured fields from a raw inquiry.

Design rule: the AI reads, the code decides. This module never touches the
CRM. It returns a structured verdict + a confidence score, and the pipeline
decides what to do with it.

Fails open: any error here (API down, bad JSON, missing key) returns
confidence=0, which routes the inquiry to the human review queue. An AI
failure may delay a lead; it must never lose one.
"""

import json
import logging
import os

import anthropic

log = logging.getLogger("leadflow")

MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

SYSTEM_PROMPT = """אתה מסווג פניות נכנסות לעסק ישראלי. תקבל טקסט גולמי של פנייה (מוואטסאפ, מייל או טופס באתר).

החזר אך ורק JSON תקין, בלי שום טקסט נוסף, במבנה הבא:
{
  "category": "new_lead" | "existing_customer" | "support" | "spam" | "other",
  "urgency": "high" | "normal" | "low",
  "name": "שם הפונה אם צוין, אחרת null",
  "summary": "משפט אחד בעברית שמסכם מה הפונה רוצה",
  "confidence": מספר בין 0 ל-100, כמה אתה בטוח בסיווג
}

כללים:
- אם הפונה מזכיר שכבר דיבר עם העסק, קיבל הצעת מחיר או מחכה לתשובה - זה existing_customer והדחיפות לפחות high אם הוא מחכה זמן רב.
- אם הטקסט לא ברור, קצר מדי או דו-משמעי - תן confidence נמוך. עדיף להודות בחוסר ודאות מאשר לנחש.
- ספאם זה רק פרסומות והודעות אוטומטיות ברורות. אם יש ספק - זה לא ספאם."""


def classify_inquiry(text: str) -> dict:
    """Return a classification dict. Never raises."""
    fallback = {
        "category": "other",
        "urgency": "normal",
        "name": None,
        "summary": "",
        "confidence": 0,
        "error": None,
    }
    try:
        client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env
        response = client.messages.create(
            model=MODEL,
            max_tokens=500,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": text}],
        )
        raw = response.content[0].text.strip()
        # Defensive: strip accidental markdown fences before parsing
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        data = json.loads(raw)
        return {
            "category": str(data.get("category", "other")),
            "urgency": str(data.get("urgency", "normal")),
            "name": data.get("name"),
            "summary": str(data.get("summary", ""))[:300],
            "confidence": max(0, min(100, int(data.get("confidence", 0)))),
            "error": None,
        }
    except Exception as e:  # noqa: BLE001 - deliberate: fail open, never crash intake
        log.warning("classify failed, routing to human review: %s", e)
        fallback["error"] = str(e)
        return fallback
