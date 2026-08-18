"""AI classification adapters and output validation.

This module returns a provider-independent verdict. It does not write to the
CRM; routing remains deterministic application code.

Fails open: any error here (API down, bad JSON, missing key) returns
confidence=0, which routes the inquiry to the human review queue. An AI
failure may delay a lead; it must never lose one.
"""

import json
import logging
import os

import anthropic
import httpx

log = logging.getLogger("leadflow")

MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash")

VALID_CATEGORIES = {"new_lead", "existing_customer", "support", "spam", "other"}
VALID_URGENCIES = {"high", "normal", "low"}

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": sorted(VALID_CATEGORIES)},
        "urgency": {"type": "string", "enum": sorted(VALID_URGENCIES)},
        "name": {"type": ["string", "null"]},
        "summary": {"type": "string"},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
    },
    "required": ["category", "urgency", "name", "summary", "confidence"],
    "additionalProperties": False,
}

# Gemini's generateContent endpoint accepts its own subset of JSON Schema.
# It expresses a nullable value with `nullable` and does not accept
# `additionalProperties`. The returned data still goes through _parse_verdict,
# so the application's provider-independent allowlists remain authoritative.
GEMINI_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        **OUTPUT_SCHEMA["properties"],
        "name": {"type": "string", "nullable": True},
    },
    "required": OUTPUT_SCHEMA["required"],
}

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
- `support` קודם ל-`existing_customer`: תקלה, חשבונית, ביטול או שינוי תור,
  ובקשה תפעולית לגבי שירות קיים הם support גם כשהפונה הוא לקוח קיים.
- `existing_customer` מיועד להמשך של תהליך מכירה קיים: הפונה כבר דיבר עם
  העסק, מחכה להצעת מחיר או שואל על הצעה שקיבל. הדחיפות לפחות high אם הוא
  מחכה זמן רב.
- אם הטקסט לא ברור, קצר מדי או דו-משמעי - תן confidence נמוך. עדיף להודות בחוסר ודאות מאשר לנחש.
- ספאם זה רק פרסומות והודעות אוטומטיות ברורות. אם יש ספק - זה לא ספאם."""


def _fallback(error: str | None = None) -> dict:
    return {
        "category": "other",
        "urgency": "normal",
        "name": None,
        "summary": "",
        "confidence": 0,
        "error": error,
    }


def _parse_verdict(raw: str) -> dict:
    """Parse and validate one provider's JSON response."""
    raw = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    data = json.loads(raw)
    category = str(data.get("category", "other"))
    urgency = str(data.get("urgency", "normal"))
    if category not in VALID_CATEGORIES or urgency not in VALID_URGENCIES:
        raise ValueError(
            f"invalid model output: category={category!r}, urgency={urgency!r}"
        )
    return {
        "category": category,
        "urgency": urgency,
        "name": data.get("name"),
        "summary": str(data.get("summary", ""))[:300],
        "confidence": max(0, min(100, int(data.get("confidence", 0)))),
        "error": None,
    }


def _classify_with_anthropic(text: str) -> str:
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env
    response = client.messages.create(
        model=MODEL,
        max_tokens=500,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": text}],
    )
    return response.content[0].text


def _classify_with_gemini(text: str) -> str:
    """Call Gemini over REST; httpx is already a project dependency."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured")
    response = httpx.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
        headers={"x-goog-api-key": api_key, "Content-Type": "application/json"},
        json={
            "contents": [{"parts": [{"text": f"{SYSTEM_PROMPT}\n\nהפנייה:\n{text}"}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": GEMINI_OUTPUT_SCHEMA,
            },
        },
        timeout=30,
    )
    if response.is_error:
        # Google's validation body explains schema mistakes and never contains
        # the API key because the key is sent in a header, not in the URL.
        raise RuntimeError(
            f"Gemini API {response.status_code}: {response.text[:800]}"
        )
    response.raise_for_status()
    return response.json()["candidates"][0]["content"]["parts"][0]["text"]


def classify_inquiry(text: str) -> dict:
    """Return a classification dict from the configured provider. Never raises."""
    fallback = {
        **_fallback(),
    }
    try:
        provider = os.environ.get("AI_PROVIDER", "anthropic").strip().lower()
        if provider == "gemini":
            raw = _classify_with_gemini(text)
        elif provider == "anthropic":
            raw = _classify_with_anthropic(text)
        else:
            raise ValueError(f"unsupported AI_PROVIDER: {provider!r}")
        return _parse_verdict(raw)
    except Exception as e:  # noqa: BLE001 - deliberate: fail open, never crash intake
        log.warning("classify failed, routing to human review: %s", e)
        fallback["error"] = str(e)
        return fallback
