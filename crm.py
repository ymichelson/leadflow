"""HubSpot integration. The CRM is the single source of truth:
every inquiry ends up here as a contact + a note, nothing is stored
anywhere else.

Failure policy: a failed write goes into a retry queue with exponential
backoff. Delay is acceptable; loss is not.
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, UTC

import httpx

import business_hours
import reps

log = logging.getLogger("leadflow")

BASE = "https://api.hubapi.com"
NOTE_TO_CONTACT_ASSOCIATION = 202  # HubSpot-defined association type id

# Custom contact properties used by the slice. They make the AI decision and
# the review state queryable inside HubSpot instead of burying them only in a
# note. The rep field demonstrates the assignment policy; it is not HubSpot's
# native owner field (see README).
REP_PROPERTY = "leadflow_assigned_rep"
CATEGORY_PROPERTY = "leadflow_category"
URGENCY_PROPERTY = "leadflow_urgency"
REVIEW_PROPERTY = "leadflow_needs_review"
UNASSIGNED_LABEL = "ללא נציג"

PROPERTY_DEFINITIONS = {
    REP_PROPERTY: {
        "label": "נציג מטפל (LeadFlow)",
        "type": "string",
        "fieldType": "text",
        "description": "הנציג שאליו שויך הליד אוטומטית בקליטה",
    },
    CATEGORY_PROPERTY: {
        "label": "סיווג אחרון (LeadFlow)",
        "type": "string",
        "fieldType": "text",
        "description": "סיווג הפנייה האחרונה על ידי LeadFlow",
    },
    URGENCY_PROPERTY: {
        "label": "דחיפות אחרונה (LeadFlow)",
        "type": "string",
        "fieldType": "text",
        "description": "דחיפות הפנייה האחרונה על ידי LeadFlow",
    },
    REVIEW_PROPERTY: {
        "label": "דורש בדיקת אדם (LeadFlow)",
        "type": "bool",
        "fieldType": "booleancheckbox",
        "description": "האם הפנייה האחרונה דורשת בדיקה אנושית",
        # HubSpot's property API requires both boolean options explicitly even
        # though the values themselves are fixed.
        "options": [
            {"label": "כן", "value": "true", "displayOrder": 0, "hidden": False},
            {"label": "לא", "value": "false", "displayOrder": 1, "hidden": False},
        ],
    },
}

# A HubSpot create/update is rejected when it references a property that does
# not exist. Readiness is tracked per property because schema creation can
# partially succeed during an outage.
_property_ready: dict[str, bool] = {name: False for name in PROPERTY_DEFINITIONS}


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {os.environ.get('HUBSPOT_TOKEN', '')}",
        "Content-Type": "application/json",
    }


# ------------------------------------------------------------------ startup
# Both helpers below are called once at boot and never raise: if HubSpot is
# unreachable at startup the service must still come up and keep accepting
# inquiries. A degraded feature is acceptable; a refused intake is not.

async def ensure_leadflow_properties(names: list[str] | None = None) -> bool:
    """Create the custom contact properties used by the slice.

    This is safe to call repeatedly. HubSpot returns 409 when a property already
    exists, which is the expected result after the first successful startup.
    """
    wanted = names or list(PROPERTY_DEFINITIONS)
    for name in wanted:
        if _property_ready.get(name):
            continue
        definition = PROPERTY_DEFINITIONS[name]
        payload = {"name": name, "groupName": "contactinformation", **definition}
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    f"{BASE}/crm/v3/properties/contacts",
                    headers=_headers(), json=payload,
                )
            if r.status_code != 409:
                r.raise_for_status()
            _property_ready[name] = True
            log.info("contact property '%s' is ready", name)
        except Exception as e:  # noqa: BLE001 - never block startup on the CRM
            log.warning("could not ensure contact property '%s': %s", name, e)
    return all(_property_ready.get(name, False) for name in wanted)


async def ensure_rep_property() -> bool:
    """Backward-compatible helper used by older callers and tests."""
    return await ensure_leadflow_properties([REP_PROPERTY])


async def seed_rotation_from_crm() -> None:
    """Resume round-robin where the previous process left off.

    Asks the CRM how many contacts already carry a rep and hands that count to
    reps.set_rotation_start. No local counter file, no second database - the
    CRM is still the only place that knows who owns what.
    """
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{BASE}/crm/v3/objects/contacts/search",
                headers=_headers(),
                json={
                    "filterGroups": [{"filters": [
                        {"propertyName": REP_PROPERTY, "operator": "HAS_PROPERTY"},
                    ]}],
                    "properties": [REP_PROPERTY],
                    "limit": 1,
                },
            )
            r.raise_for_status()
            reps.set_rotation_start(int(r.json().get("total", 0)))
    except Exception as e:  # noqa: BLE001
        # Worst case we restart the cycle at the first rep: a fairness rounding
        # error of at most len(REPS)-1 leads, never a lost or unassigned lead.
        log.warning("rotation seed failed, starting at first rep: %s", e)


# ------------------------------------------------------------------- writes

async def find_contact(phone: str | None, email: str | None) -> str | None:
    """Dedupe: search by normalized phone OR email before creating anything.

    Exact identifiers only. Names are fuzzy (two different customers can share
    a name), so we never match on names automatically.
    """
    filters = []
    if phone:
        filters.append({"filters": [{"propertyName": "phone", "operator": "EQ", "value": phone}]})
    if email:
        filters.append({"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]})
    if not filters:
        return None
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{BASE}/crm/v3/objects/contacts/search",
            headers=_headers(),
            json={"filterGroups": filters, "properties": ["phone", "email"], "limit": 1},
        )
        r.raise_for_status()
        results = r.json().get("results", [])
        return results[0]["id"] if results else None


def _leadflow_metadata(verdict: dict, needs_review: bool,
                       rep: str | None = None) -> dict:
    """Properties that make the latest routing decision visible in HubSpot."""
    values = {
        CATEGORY_PROPERTY: verdict.get("category", "other"),
        URGENCY_PROPERTY: verdict.get("urgency", "normal"),
        REVIEW_PROPERTY: "true" if needs_review else "false",
    }
    if rep:
        values[REP_PROPERTY] = rep
    return {name: value for name, value in values.items()
            if _property_ready.get(name)}


def _contact_properties(phone: str | None, email: str | None, name: str | None,
                        verdict: dict, needs_review: bool,
                        rep: str | None = None) -> dict:
    """Build a contact payload without doing I/O, so the business rules test cleanly."""
    category = verdict.get("category", "other")
    props = {
        "phone": phone or "",
        "email": email or "",
        "lifecyclestage": "lead",
        # Human review is not an attempted customer contact. Confident spam is
        # retained, as promised in Part A, but excluded from the active sales SLA.
        "hs_lead_status": "UNQUALIFIED" if category == "spam" and not needs_review else "NEW",
    }
    props.update(_leadflow_metadata(verdict, needs_review, rep))
    if name:
        parts = name.split(" ", 1)
        props["firstname"] = parts[0]
        if len(parts) > 1:
            props["lastname"] = parts[1]
    return props


async def create_contact(phone: str | None, email: str | None, name: str | None,
                         verdict: dict, needs_review: bool,
                         rep: str | None = None) -> str:
    props = _contact_properties(phone, email, name, verdict, needs_review, rep)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(f"{BASE}/crm/v3/objects/contacts",
                              headers=_headers(), json={"properties": props})
        r.raise_for_status()
        return r.json()["id"]


async def update_contact_metadata(contact_id: str, verdict: dict,
                                  needs_review: bool) -> None:
    """Expose the latest classification/review state on an existing contact."""
    props = _leadflow_metadata(verdict, needs_review)
    if not props:
        return
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.patch(
            f"{BASE}/crm/v3/objects/contacts/{contact_id}",
            headers=_headers(), json={"properties": props},
        )
        r.raise_for_status()


async def add_note(contact_id: str, body: str) -> None:
    payload = {
        "properties": {
            "hs_note_body": body[:9000],
            "hs_timestamp": str(int(time.time() * 1000)),
        },
        "associations": [{
            "to": {"id": contact_id},
            "types": [{"associationCategory": "HUBSPOT_DEFINED",
                       "associationTypeId": NOTE_TO_CONTACT_ASSOCIATION}],
        }],
    }
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(f"{BASE}/crm/v3/objects/notes",
                              headers=_headers(), json=payload)
        r.raise_for_status()


def _business_hours_since(created_raw: str) -> float | None:
    """Working hours since the lead arrived. Bad/absent value -> None.

    Working hours, not wall-clock hours: a lead that came in Thursday evening
    has not been ignored all weekend, and saying so would make the first
    report the owner reads indefensible.
    """
    if not created_raw:
        return None
    try:
        created = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
        return business_hours.business_hours_between(created, datetime.now(UTC))
    except Exception:  # noqa: BLE001 - an unparseable date must not kill the report
        return None


def _lead_display_name(contact: dict, properties: dict) -> str:
    """Return a safe label for the public demo status payload.

    Real names can be enabled deliberately for a private local demo, but the
    default must not publish customer PII from an unauthenticated JSON endpoint.
    """
    if os.environ.get("EXPOSE_DEMO_PII", "").lower() in {"1", "true", "yes"}:
        return (
            f"{properties.get('firstname') or ''} {properties.get('lastname') or ''}"
            .strip() or "ללא שם"
        )
    suffix = str(contact.get("id", ""))[-4:] or "----"
    return f"ליד …{suffix}"


async def find_overdue_leads(sla_hours: float) -> list[dict]:
    """SLA check, grouped by the salesperson who owns the lead.

    Deliberately conservative: we only look at hs_lead_status == NEW, i.e.
    leads nobody moved. Any status change or logged call stops the clock.

    The clock counts WORKING hours (see business_hours.py), so the weekend does
    not make a rep look like they ignored somebody.

    The HubSpot filter below still uses wall-clock time, and that is correct:
    working hours elapsed can never exceed wall-clock hours elapsed, so this
    filter is a guaranteed superset. It narrows the fetch cheaply, and the real
    decision is made locally against the business-hours clock.

    Returns one entry per rep, worst offender first:
        [{"rep": "דנה", "count": 2,
          "leads": [{"name": ..., "hours_overdue": 6.4, "id": ..., "phone": ...}]}]
    """
    cutoff_ms = int((time.time() - sla_hours * 3600) * 1000)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{BASE}/crm/v3/objects/contacts/search",
            headers=_headers(),
            json={
                "filterGroups": [{
                    "filters": [
                        {"propertyName": "hs_lead_status", "operator": "EQ", "value": "NEW"},
                        {"propertyName": "createdate", "operator": "LT", "value": str(cutoff_ms)},
                    ]
                }],
                "properties": ["firstname", "lastname", "phone", "email",
                               "createdate", REP_PROPERTY],
                # Oldest first: the wall-clock filter over-fetches now that the
                # real test is business hours, so if we ever hit the page limit
                # we want to have kept the leads waiting longest, not a random 50.
                "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
                "limit": 100,
            },
        )
        r.raise_for_status()

    by_rep: dict[str, list[dict]] = {}
    for c in r.json().get("results", []):
        p = c.get("properties", {})
        age = _business_hours_since(p.get("createdate") or "")
        # age is None only when the date is unparseable. Keep the lead in the
        # report rather than dropping it - we'd rather show a lead we can't
        # time than hide one.
        if age is not None and age < sla_hours:
            continue   # inside the window once the weekend is discounted
        # Leads created before this feature existed still show up, under a
        # visible "no rep" bucket. An unowned overdue lead is the worst kind -
        # not the kind to hide.
        rep = (p.get(REP_PROPERTY) or "").strip() or UNASSIGNED_LABEL
        by_rep.setdefault(rep, []).append({
            # /status is intentionally PII-safe by default. The CRM remains the
            # place to inspect the actual contact and phone number.
            "name": _lead_display_name(c, p),
            "hours_overdue": round(age - sla_hours, 1) if age is not None else None,
        })

    groups = [
        {"rep": rep,
         "count": len(leads),
         "leads": sorted(leads, key=lambda x: x["hours_overdue"] or 0, reverse=True)}
        for rep, leads in by_rep.items()
    ]
    groups.sort(key=lambda g: g["count"], reverse=True)
    return groups


def breach_total(groups: list[dict]) -> int:
    """Total overdue leads across every rep."""
    return sum(g["count"] for g in groups)


# --------------------------------------------------------------------------
# Retry queue: failed CRM writes wait here and are retried with backoff.
#
# This is a SQLite table so a restart does not lose parked leads. It is queue
# plumbing, NOT a second copy of the CRM: a row exists only while a write is
# owed, it is deleted the moment the write succeeds, and nothing in the app
# ever reads it to answer a question about a lead. The CRM stays the only
# place you look up who a customer is.
#
# sqlite3 is in the standard library, so this adds no dependency.
# --------------------------------------------------------------------------

MAX_ATTEMPTS = 8
BACKOFF_CAP_SECONDS = 300


def _db() -> sqlite3.Connection:
    """Open the queue DB, creating the schema if needed.

    Path is read per call (not cached at import) so tests can point it at a
    temp file. CREATE TABLE IF NOT EXISTS is cheap and makes every entry point
    self-initialising - no ordering bug where a write beats the setup call.
    """
    path = os.environ.get("STATE_DB") or os.environ.get(
        "RETRY_DB", "leadflow_queue.db"
    )
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS retry_queue (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            inquiry         TEXT NOT NULL,
            verdict         TEXT NOT NULL,
            needs_review    INTEGER NOT NULL,
            attempts        INTEGER NOT NULL DEFAULT 0,
            queued_at       TEXT NOT NULL,
            next_attempt_at REAL NOT NULL,
            status          TEXT NOT NULL DEFAULT 'pending',
            last_error      TEXT
        )
    """)
    conn.commit()
    return conn


def enqueue(inquiry: dict, verdict: dict, needs_review: bool, error: str = "") -> None:
    """Park a failed write, or raise so the durable inbox keeps its copy."""
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO retry_queue (inquiry, verdict, needs_review, queued_at,"
                " next_attempt_at, last_error) VALUES (?, ?, ?, ?, ?, ?)",
                (json.dumps(inquiry, ensure_ascii=False),
                 json.dumps(verdict, ensure_ascii=False),
                 int(needs_review), datetime.now(UTC).isoformat(), time.time(), error),
            )
    except Exception:  # noqa: BLE001
        # Do not log the inquiry itself: it contains customer PII. Propagating
        # keeps the original durable-inbox row intact for another attempt.
        log.exception("retry queue persistence failed; durable intake retained")
        raise


def queue_size() -> int:
    """Leads still waiting to be written."""
    try:
        with _db() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM retry_queue WHERE status = 'pending'"
            ).fetchone()[0]
    except Exception as e:  # noqa: BLE001 - /status must never 500
        log.warning("queue_size failed: %s", e)
        return 0


def dead_letter_size() -> int:
    """Leads that exhausted every retry and need a human. Kept, never deleted."""
    try:
        with _db() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM retry_queue WHERE status = 'dead'"
            ).fetchone()[0]
    except Exception as e:  # noqa: BLE001
        log.warning("dead_letter_size failed: %s", e)
        return 0


def _due_items(limit: int = 20) -> list[dict]:
    """Rows whose backoff has elapsed. Capped so one tick can't stampede HubSpot."""
    with _db() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM retry_queue WHERE status = 'pending' AND next_attempt_at <= ?"
            " ORDER BY next_attempt_at LIMIT ?",
            (time.time(), limit),
        ).fetchall()
    return [dict(r) for r in rows]


def _mark_done(row_id: int) -> None:
    """Write succeeded - the CRM now owns it, so the queue row goes away."""
    with _db() as conn:
        conn.execute("DELETE FROM retry_queue WHERE id = ?", (row_id,))


def _reschedule(row_id: int, attempts: int, error: str) -> None:
    """Same backoff as before: 2**attempts seconds, capped at 300."""
    delay = min(2 ** attempts, BACKOFF_CAP_SECONDS)
    with _db() as conn:
        conn.execute(
            "UPDATE retry_queue SET attempts = ?, next_attempt_at = ?, last_error = ?"
            " WHERE id = ?",
            (attempts, time.time() + delay, error, row_id),
        )
    log.info("retry #%d rescheduled in %ds (attempt %d/%d)",
             row_id, delay, attempts, MAX_ATTEMPTS)


def _mark_dead(row_id: int, error: str) -> None:
    """Out of retries. The row STAYS in the table - a human has to deal with it.

    Deleting here would be the one place in this system where an inquiry
    silently disappears, which is exactly what the design forbids.
    """
    with _db() as conn:
        conn.execute(
            "UPDATE retry_queue SET status = 'dead', last_error = ? WHERE id = ?",
            (error, row_id),
        )
    log.error("ALERT A HUMAN: inquiry #%d failed %d times and is parked in the "
              "dead-letter queue: %s", row_id, MAX_ATTEMPTS, error)


def _build_note(inquiry: dict, verdict: dict, needs_review: bool,
                created: bool, rep: str | None, delayed: bool = False) -> str:
    """One note format for immediate and retried writes.

    Previously the retry path discarded classification, urgency, summary and
    review state after it finally succeeded. A delayed write must carry exactly
    the same business information as a first-attempt write.
    """
    note_lines = []
    if delayed:
        note_lines.append("(נכתב באיחור אחרי תקלת CRM)")
    note_lines.extend([
        f"פנייה חדשה ({inquiry['source']})" if created
        else f"פנייה נוספת מאותו לקוח ({inquiry['source']})",
        f"סיווג: {verdict['category']} | דחיפות: {verdict['urgency']} | "
        f"ביטחון: {verdict['confidence']}",
    ])
    if inquiry.get("submission_id"):
        note_lines.append(f"מזהה קליטה: {inquiry['submission_id']}")
    if rep:
        note_lines.append(f"שויך לנציג: {rep}")
    if needs_review:
        note_lines.append("*** דורש בדיקת אדם - המערכת לא בטוחה בסיווג ***")
    if verdict.get("summary"):
        note_lines.append(f"תקציר: {verdict['summary']}")
    note_lines.extend(["---", inquiry["text"]])
    return "\n".join(note_lines)


async def _write_once(inquiry: dict, verdict: dict, needs_review: bool,
                      delayed: bool = False) -> dict:
    """Perform one CRM write attempt. Never enqueues by itself."""
    if not all(_property_ready.values()):
        await ensure_leadflow_properties()

    contact_id = await find_contact(inquiry.get("phone"), inquiry.get("email"))
    created = False
    rep = None
    if not contact_id:
        # A rep is picked only when a genuinely new contact is created. A
        # returning customer keeps the assignment already stored in the CRM.
        rep = reps.next_rep()
        contact_id = await create_contact(
            inquiry.get("phone"), inquiry.get("email"),
            inquiry.get("name") or verdict.get("name"), verdict,
            needs_review, rep,
        )
        created = True
    else:
        await update_contact_metadata(contact_id, verdict, needs_review)

    await add_note(
        contact_id,
        _build_note(inquiry, verdict, needs_review, created, rep, delayed),
    )
    return {"ok": True, "contact_id": contact_id, "created": created, "rep": rep}


async def write_lead(inquiry: dict, verdict: dict, needs_review: bool) -> dict:
    """Create/update the lead + note. On failure, enqueue for retry."""
    try:
        return await _write_once(inquiry, verdict, needs_review)
    except Exception as e:  # noqa: BLE001 - fail open: park it, never drop it
        log.warning("CRM write failed, queued for retry: %s", e)
        enqueue(inquiry, verdict, needs_review, str(e))
        return {"ok": False, "queued": True, "error": str(e)}


async def retry_one(row: dict) -> None:
    """Attempt one parked write. Success deletes the row, failure reschedules."""
    inquiry = json.loads(row["inquiry"])
    verdict = json.loads(row["verdict"])
    try:
        await _write_once(inquiry, verdict, bool(row["needs_review"]), delayed=True)
        _mark_done(row["id"])
        log.info("retry succeeded for queued inquiry #%d", row["id"])
    except Exception as e:  # noqa: BLE001
        attempts = row["attempts"] + 1
        if attempts < MAX_ATTEMPTS:
            _reschedule(row["id"], attempts, str(e))
        else:
            _mark_dead(row["id"], str(e))


async def retry_loop() -> None:
    """Background: retry parked writes with exponential backoff.

    The wait is now stored per row as next_attempt_at instead of sleeping the
    whole loop. Same delay per item (2**attempts, capped at 300s, 8 tries) -
    but one stuck lead no longer holds up every lead behind it, and the wait
    survives a restart, which was the entire point of moving to a table.
    """
    while True:
        await asyncio.sleep(30)
        try:
            due = _due_items()
        except Exception as e:  # noqa: BLE001 - the retrier itself must never die
            log.warning("could not read retry queue: %s", e)
            continue
        for row in due:
            await retry_one(row)
