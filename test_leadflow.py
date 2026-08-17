"""Tests for the logic that fails silently if it's wrong.

Run: pytest test_leadflow.py
"""

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import crm
import intake
import reps
import eval as evalmod   # aliased so it doesn't shadow the builtin in this file
from utils import normalize_phone, clean_text, valid_email
from classify import VALID_CATEGORIES, classify_inquiry


# ---- phone normalization: the dedupe foundation ----

def test_normalize_phone_local_format():
    assert normalize_phone("050-1234567") == "+972501234567"

def test_normalize_phone_plain_digits():
    assert normalize_phone("0501234567") == "+972501234567"

def test_normalize_phone_international():
    assert normalize_phone("+972501234567") == "+972501234567"

def test_normalize_phone_whatsapp_format():
    # WhatsApp sends numbers without the plus
    assert normalize_phone("972501234567") == "+972501234567"

def test_normalize_phone_same_person_all_formats():
    forms = ["050-1234567", "0501234567", "+972501234567", "972501234567", "050 123 4567"]
    assert len({normalize_phone(f) for f in forms}) == 1

def test_normalize_phone_garbage_returns_none():
    assert normalize_phone("abc") is None
    assert normalize_phone("") is None
    assert normalize_phone(None) is None
    assert normalize_phone("123") is None


def test_email_sanity_check():
    assert valid_email("person@example.com") is True
    assert valid_email(" person+demo@example.co.il ") is True
    assert valid_email("missing-at.example.com") is False
    assert valid_email("person@localhost") is False


# ---- classification fails open, never crashes intake ----

def test_classify_fails_open_without_api_key():
    with patch.dict("os.environ", {"AI_PROVIDER": "anthropic", "ANTHROPIC_API_KEY": ""}):
        verdict = classify_inquiry("היי, אשמח להצעת מחיר")
    assert verdict["confidence"] == 0          # -> routed to human review
    assert verdict["category"] == "other"      # no guessing
    assert verdict["error"] is not None        # the failure is visible, not silent


def test_classify_rejects_an_unknown_high_confidence_category():
    """The model can suggest values; only code may approve known routing values."""
    response = SimpleNamespace(content=[SimpleNamespace(text=json.dumps({
        "category": "vip_emergency",
        "urgency": "high",
        "name": "שרה",
        "summary": "מבקשת טיפול",
        "confidence": 99,
    }))])
    client = MagicMock()
    client.messages.create.return_value = response
    with patch.dict("os.environ", {"AI_PROVIDER": "anthropic"}), \
         patch("classify.anthropic.Anthropic", return_value=client):
        verdict = classify_inquiry("צריכה עזרה")
    assert verdict["category"] == "other"
    assert verdict["confidence"] == 0
    assert "invalid model output" in verdict["error"]


def test_gemini_provider_returns_the_same_validated_contract():
    api_response = MagicMock()
    api_response.is_error = False
    api_response.json.return_value = {
        "candidates": [{"content": {"parts": [{"text": json.dumps({
            "category": "new_lead", "urgency": "normal", "name": "שרה",
            "summary": "בקשת הצעה", "confidence": 93,
        })}]}}]
    }
    with patch.dict("os.environ", {"AI_PROVIDER": "gemini", "GEMINI_API_KEY": "secret"}), \
         patch("classify.httpx.post", return_value=api_response) as post:
        verdict = classify_inquiry("אשמח להצעה")
    assert verdict == {"category": "new_lead", "urgency": "normal", "name": "שרה",
                       "summary": "בקשת הצעה", "confidence": 93, "error": None}
    api_response.raise_for_status.assert_called_once()
    assert post.call_args.kwargs["headers"]["x-goog-api-key"] == "secret"
    schema = post.call_args.kwargs["json"]["generationConfig"]["responseSchema"]
    assert schema["properties"]["category"]["enum"] == sorted(VALID_CATEGORIES)
    assert schema["properties"]["name"] == {"type": "string", "nullable": True}
    assert "additionalProperties" not in schema


def test_unknown_provider_fails_open():
    with patch.dict("os.environ", {"AI_PROVIDER": "mystery"}):
        verdict = classify_inquiry("אשמח להצעה")
    assert verdict["confidence"] == 0
    assert "unsupported AI_PROVIDER" in verdict["error"]


# ---- pipeline orchestration: preserve explicit form data ----

def test_pipeline_prefers_the_form_name_and_normalizes_before_crm():
    verdict = {"category": "new_lead", "urgency": "normal", "name": "AI guess",
               "summary": "quote", "confidence": 92, "error": None}
    crm_result = {"ok": True, "contact_id": "123", "created": True}
    with patch("pipeline.classify_inquiry", return_value=verdict), \
         patch("pipeline.write_lead", new=AsyncMock(return_value=crm_result)) as write:
        result = asyncio.run(__import__("pipeline").handle_inquiry(
            "טופס אתר", "אשמח להצעה", "050-1234567", "SARAH@EXAMPLE.COM", "שרה לוי"))
    inquiry = write.await_args.args[0]
    assert inquiry["name"] == "שרה לוי"
    assert inquiry["phone"] == "+972501234567"
    assert inquiry["email"] == "sarah@example.com"
    assert result["needs_review"] is False


# ---- durable intake: acknowledge only after saving ----

def _fresh_state(tmp_path):
    return patch.dict(os.environ, {"STATE_DB": str(tmp_path / "state.db")})


def test_intake_is_persisted_before_processing_and_is_idempotent(tmp_path):
    with _fresh_state(tmp_path):
        first = intake.accept_inquiry(
            "טופס אתר", "אשמח להצעה", "050-1234567", "x@example.com", "שרה",
            submission_id="web-test-1",
        )
        second = intake.accept_inquiry(
            "טופס אתר", "אשמח להצעה", "050-1234567", "x@example.com", "שרה",
            submission_id="web-test-1",
        )
        assert first == {"submission_id": "web-test-1", "status": "received",
                         "duplicate": False}
        assert second == {"submission_id": "web-test-1", "status": "received",
                          "duplicate": True}
        assert intake.pending_size() == 1


def test_idempotency_key_cannot_hide_different_content(tmp_path):
    with _fresh_state(tmp_path):
        intake.accept_inquiry("טופס אתר", "פנייה אחת", submission_id="same-id")
        try:
            intake.accept_inquiry("טופס אתר", "פנייה אחרת", submission_id="same-id")
            assert False, "expected an idempotency conflict"
        except intake.IdempotencyConflict:
            pass


def test_worker_processes_the_saved_payload_and_marks_it_completed(tmp_path):
    pipeline_result = {"needs_review": False, "crm": {"ok": True}}
    with _fresh_state(tmp_path), \
         patch("intake.handle_inquiry", new=AsyncMock(return_value=pipeline_result)) as handle:
        intake.accept_inquiry(
            "טופס אתר", "אשמח להצעה", "050-1234567", "x@example.com", "שרה",
            submission_id="saved-1",
        )
        claimed = intake._claim_one()
        asyncio.run(intake.process_claimed(claimed))
        status = intake.get_status("saved-1")
        duplicate = intake.accept_inquiry(
            "טופס אתר", "אשמח להצעה", "050-1234567", "x@example.com", "שרה",
            submission_id="saved-1",
        )
        with intake._db() as conn:
            stored_payload = conn.execute(
                "SELECT payload FROM inquiry_inbox WHERE submission_id = 'saved-1'"
            ).fetchone()[0]
    assert status["status"] == "completed"
    assert status["attempts"] == 1
    assert duplicate["duplicate"] is True
    assert duplicate["status"] == "completed"
    assert stored_payload == "{}"  # completed inbox rows do not retain customer PII
    assert handle.await_args.kwargs["submission_id"] == "saved-1"
    assert handle.await_args.kwargs["text"] == "אשמח להצעה"


def test_interrupted_processing_is_recovered_after_restart(tmp_path):
    with _fresh_state(tmp_path):
        intake.accept_inquiry("טופס אתר", "לא לאבד אותי", submission_id="restart-1")
        first_claim = intake._claim_one()
        assert first_claim["status"] == "processing"
        assert intake.recover_in_progress() == 1
        second_claim = intake._claim_one()
    assert second_claim["submission_id"] == "restart-1"
    assert second_claim["attempts"] == 2


def test_worker_failure_is_kept_for_retry(tmp_path):
    with _fresh_state(tmp_path), \
         patch("intake.handle_inquiry", new=AsyncMock(side_effect=RuntimeError("boom"))):
        intake.accept_inquiry("טופס אתר", "חשוב", submission_id="retry-1")
        asyncio.run(intake.process_claimed(intake._claim_one()))
        status = intake.get_status("retry-1")
    assert status["status"] == "retry"
    assert status["attempts"] == 1
    assert "boom" in status["last_error"]


def test_http_intake_returns_202_after_durable_save(tmp_path):
    class RequestStub:
        async def json(self):
            return {"text": "אשמח להצעה", "email": "x@example.com",
                    "submission_id": "http-1"}

    with _fresh_state(tmp_path):
        response = asyncio.run(main.api_inquiry(RequestStub()))
        body = json.loads(response.body)
        saved = intake.get_status("http-1")
    assert response.status_code == 202
    assert body == {"submission_id": "http-1", "status": "received",
                    "duplicate": False}
    assert saved["status"] == "received"


def test_http_intake_requires_a_callback_channel(tmp_path):
    class RequestStub:
        async def json(self):
            return {"text": "אשמח להצעה", "submission_id": "no-contact"}

    with _fresh_state(tmp_path):
        response = asyncio.run(main.api_inquiry(RequestStub()))
        assert intake.get_status("no-contact") is None
    assert response.status_code == 400
    assert json.loads(response.body) == {
        "error": "a valid phone number or email is required"
    }


# ---- round-robin assignment: silently uneven if it's wrong ----

def test_round_robin_cycles_through_every_rep():
    reps.set_rotation_start(0)
    picked = [reps.next_rep() for _ in range(len(reps.REPS) * 2)]
    assert picked == reps.REPS + reps.REPS

def test_rotation_seed_resumes_mid_cycle():
    # 4 leads already assigned across 3 reps -> next one is the 2nd rep
    reps.set_rotation_start(4)
    assert reps.next_rep() == reps.REPS[1]

def test_rotation_seed_ignores_nonsense():
    reps.set_rotation_start(0)
    reps.set_rotation_start(-5)   # must not move the position
    assert reps.next_rep() == reps.REPS[0]


# ---- the breach report has to name a rep for every lead ----

def test_breach_total_counts_leads_not_reps():
    groups = [{"rep": "דנה", "count": 2, "leads": []},
              {"rep": "יוסי", "count": 1, "leads": []}]
    assert crm.breach_total(groups) == 3

def test_breach_total_empty():
    assert crm.breach_total([]) == 0


def test_status_lead_label_hides_customer_name_by_default():
    contact = {"id": "12345678"}
    props = {"firstname": "שרה", "lastname": "לוי"}
    with patch.dict("os.environ", {"EXPOSE_DEMO_PII": ""}):
        assert crm._lead_display_name(contact, props) == "ליד …5678"


def test_private_demo_can_show_customer_name_explicitly():
    contact = {"id": "12345678"}
    props = {"firstname": "שרה", "lastname": "לוי"}
    with patch.dict("os.environ", {"EXPOSE_DEMO_PII": "true"}):
        assert crm._lead_display_name(contact, props) == "שרה לוי"


# ---- CRM routing semantics ----

def test_human_review_is_not_misreported_as_attempted_contact():
    verdict = {"category": "other", "urgency": "normal"}
    with patch.dict(crm._property_ready,
                    {name: True for name in crm.PROPERTY_DEFINITIONS}):
        props = crm._contact_properties(None, "x@example.com", "X", verdict, True, "דנה")
    assert props["hs_lead_status"] == "NEW"
    assert props[crm.REVIEW_PROPERTY] == "true"
    assert props[crm.CATEGORY_PROPERTY] == "other"


def test_review_property_has_hubspots_required_boolean_options():
    options = crm.PROPERTY_DEFINITIONS[crm.REVIEW_PROPERTY]["options"]
    assert {option["value"] for option in options} == {"true", "false"}


def test_confident_spam_is_retained_but_not_an_active_sales_lead():
    verdict = {"category": "spam", "urgency": "low"}
    with patch.dict(crm._property_ready,
                    {name: True for name in crm.PROPERTY_DEFINITIONS}):
        props = crm._contact_properties(None, "spam@example.com", None,
                                        verdict, False, "יוסי")
    assert props["hs_lead_status"] == "UNQUALIFIED"
    assert props[crm.CATEGORY_PROPERTY] == "spam"
    assert props[crm.REVIEW_PROPERTY] == "false"


def test_delayed_note_preserves_the_original_decision():
    inquiry = {"source": "טופס אתר", "text": "אשמח להצעה"}
    verdict = {"category": "new_lead", "urgency": "high",
               "confidence": 86, "summary": "בקשת הצעה"}
    note = crm._build_note(inquiry, verdict, True, True, "מאיה", delayed=True)
    for expected in ("תקלת CRM", "new_lead", "high", "86", "בקשת הצעה",
                     "דורש בדיקת אדם", "אשמח להצעה"):
        assert expected in note


def test_existing_contact_is_updated_instead_of_created_again():
    inquiry = {"source": "טופס אתר", "text": "פנייה נוספת",
               "phone": "+972501234567", "email": "x@example.com", "name": "שרה"}
    verdict = {"category": "existing_customer", "urgency": "high",
               "confidence": 91, "summary": "מחכה לתשובה"}
    ready = {name: True for name in crm.PROPERTY_DEFINITIONS}
    with patch.dict(crm._property_ready, ready), \
         patch("crm.find_contact", new=AsyncMock(return_value="contact-1")), \
         patch("crm.update_contact_metadata", new=AsyncMock()) as update, \
         patch("crm.create_contact", new=AsyncMock()) as create, \
         patch("crm.add_note", new=AsyncMock()) as note:
        result = asyncio.run(crm._write_once(inquiry, verdict, False))
    assert result["created"] is False
    create.assert_not_awaited()
    update.assert_awaited_once_with("contact-1", verdict, False)
    note.assert_awaited_once()


def test_new_contact_is_created_once_and_gets_a_note():
    inquiry = {"source": "טופס אתר", "text": "אשמח להצעה",
               "phone": "+972501234567", "email": "x@example.com", "name": "שרה"}
    verdict = {"category": "new_lead", "urgency": "normal",
               "confidence": 90, "summary": "בקשת הצעה"}
    ready = {name: True for name in crm.PROPERTY_DEFINITIONS}
    with patch.dict(crm._property_ready, ready), \
         patch("crm.find_contact", new=AsyncMock(return_value=None)), \
         patch("crm.reps.next_rep", return_value="דנה"), \
         patch("crm.create_contact", new=AsyncMock(return_value="contact-2")) as create, \
         patch("crm.add_note", new=AsyncMock()) as note:
        result = asyncio.run(crm._write_once(inquiry, verdict, False))
    assert result == {"ok": True, "contact_id": "contact-2",
                      "created": True, "rep": "דנה"}
    create.assert_awaited_once()
    note.assert_awaited_once()


def test_failed_first_write_is_queued_with_the_full_payload():
    inquiry = {"source": "טופס אתר", "text": "חשוב", "phone": None,
               "email": "x@example.com", "name": "שרה"}
    verdict = {"category": "new_lead", "urgency": "high",
               "confidence": 88, "summary": "בקשה חשובה"}
    with patch("crm._write_once", new=AsyncMock(side_effect=RuntimeError("down"))), \
         patch("crm.enqueue") as enqueue_mock:
        result = asyncio.run(crm.write_lead(inquiry, verdict, False))
    assert result["queued"] is True
    enqueue_mock.assert_called_once_with(inquiry, verdict, False, "down")


def test_retry_reuses_the_same_write_path_in_delayed_mode():
    inquiry = {"source": "טופס אתר", "text": "חשוב"}
    verdict = {"category": "new_lead", "urgency": "high", "confidence": 88}
    row = {"id": 17, "inquiry": json.dumps(inquiry),
           "verdict": json.dumps(verdict), "needs_review": 1, "attempts": 2}
    with patch("crm._write_once", new=AsyncMock(return_value={"ok": True})) as write, \
         patch("crm._mark_done") as done:
        asyncio.run(crm.retry_one(row))
    write.assert_awaited_once_with(inquiry, verdict, True, delayed=True)
    done.assert_called_once_with(17)


# ---- the eval test set must stay honest ----
# These guard against the failure mode where someone "fixes" a bad score by
# quietly deleting the hard cases.

def test_eval_labels_are_real_categories():
    # A typo in an expected label would score as a permanent miss forever.
    for case in evalmod.CASES:
        assert case["expected"] in evalmod.VALID_CATEGORIES, case["text"][:30]

def test_eval_keeps_the_hard_cases():
    ambiguous = [c for c in evalmod.CASES if c.get("ambiguous")]
    assert len(ambiguous) >= 3, "the ambiguous cases are the point, don't remove them"

def test_eval_covers_every_category():
    covered = {c["expected"] for c in evalmod.CASES}
    for required in ("new_lead", "existing_customer", "support", "spam"):
        assert required in covered

def test_eval_has_no_duplicate_cases():
    texts = [c["text"] for c in evalmod.CASES]
    assert len(texts) == len(set(texts))


# ---- the SLA clock must pause for nights and the weekend ----
# If this is wrong the first report the owner reads blames a rep for Shabbat,
# and the tool loses its credibility in week one.

from datetime import datetime
from zoneinfo import ZoneInfo

import business_hours as bh

IL = ZoneInfo("Asia/Jerusalem")


def _il(*args):
    return datetime(*args, tzinfo=IL)


def test_weekend_does_not_count_against_a_rep():
    # Lead arrives Thursday 17:00. Office shuts at 18:00, Fri+Sat are closed.
    # By Sunday 09:00 exactly one working hour has passed - not 64.
    thursday_evening = _il(2026, 8, 13, 17, 0)
    sunday_morning = _il(2026, 8, 16, 9, 0)
    assert bh.business_hours_between(thursday_evening, sunday_morning) == 1.0
    wall_clock = (sunday_morning - thursday_evening).total_seconds() / 3600
    assert wall_clock == 64.0        # what the old code would have reported

def test_a_thursday_lead_does_breach_by_sunday_midday():
    # Same lead, four working hours later: now it is genuinely overdue.
    assert bh.business_hours_between(_il(2026, 8, 13, 17), _il(2026, 8, 16, 12)) == 4.0

def test_a_full_weekend_counts_as_zero():
    assert bh.business_hours_between(_il(2026, 8, 14, 10), _il(2026, 8, 15, 23)) == 0.0

def test_overnight_gap_is_not_counted():
    # Tue 17:00 -> Wed 10:00 is 17 wall-clock hours but 2 working hours.
    assert bh.business_hours_between(_il(2026, 8, 18, 17), _il(2026, 8, 19, 10)) == 2.0

def test_same_day_is_plain_arithmetic():
    assert bh.business_hours_between(_il(2026, 8, 18, 10), _il(2026, 8, 18, 14)) == 4.0

def test_clock_never_runs_backwards():
    assert bh.business_hours_between(_il(2026, 8, 18, 14), _il(2026, 8, 18, 10)) == 0.0

def test_working_time_knows_the_israeli_weekend():
    assert bh.is_working_time(_il(2026, 8, 16, 12)) is True    # Sunday midday
    assert bh.is_working_time(_il(2026, 8, 14, 12)) is False   # Friday
    assert bh.is_working_time(_il(2026, 8, 15, 12)) is False   # Saturday
    assert bh.is_working_time(_il(2026, 8, 16, 22)) is False   # Sunday night

def test_dst_is_handled_not_hardcoded():
    # The old code hardcoded UTC+3, which is wrong every winter.
    assert _il(2026, 8, 16, 12).utcoffset().total_seconds() == 3 * 3600
    assert _il(2026, 1, 16, 12).utcoffset().total_seconds() == 2 * 3600


# ---- webhook signature: the door to the whole system ----

import hashlib
import hmac
import os

import main

SECRET = "test-app-secret"
BODY = b'{"entry":[{"changes":[]}]}'


def _sign(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_signature_accepts_a_correctly_signed_body():
    with patch.dict(os.environ, {"APP_SECRET": SECRET}):
        assert main.verify_meta_signature(BODY, _sign(BODY, SECRET)) is True

def test_signature_rejects_a_wrong_secret():
    with patch.dict(os.environ, {"APP_SECRET": SECRET}):
        assert main.verify_meta_signature(BODY, _sign(BODY, "attacker")) is False

def test_signature_rejects_a_tampered_body():
    # Signature was valid for BODY; the body then changed by one byte.
    sig = _sign(BODY, SECRET)
    with patch.dict(os.environ, {"APP_SECRET": SECRET}):
        assert main.verify_meta_signature(BODY + b" ", sig) is False

def test_signature_rejects_missing_or_malformed_header():
    with patch.dict(os.environ, {"APP_SECRET": SECRET}):
        assert main.verify_meta_signature(BODY, None) is False
        assert main.verify_meta_signature(BODY, "") is False
        assert main.verify_meta_signature(BODY, "md5=abc") is False

def test_signature_accepts_when_no_app_secret_configured():
    # Documented escape hatch so local testing works. Logs a warning.
    with patch.dict(os.environ, {"APP_SECRET": ""}):
        assert main.verify_meta_signature(BODY, None) is True


# ---- retry queue survives a restart ----

def _fresh_queue(tmp_path):
    return patch.dict(os.environ, {"RETRY_DB": str(tmp_path / "q.db")})


def test_queued_lead_survives_a_restart(tmp_path):
    inquiry = {"source": "טופס אתר", "text": "אשמח להצעת מחיר", "phone": "+972501234567"}
    with _fresh_queue(tmp_path):
        crm.enqueue(inquiry, {"category": "new_lead", "confidence": 90}, False, "boom")
        assert crm.queue_size() == 1
        # A restart is just a new connection to the same file - nothing cached.
        due = crm._due_items()
    assert len(due) == 1
    assert json.loads(due[0]["inquiry"])["text"] == "אשמח להצעת מחיר"

def test_successful_retry_removes_the_row(tmp_path):
    with _fresh_queue(tmp_path):
        crm.enqueue({"text": "x"}, {}, False)
        row_id = crm._due_items()[0]["id"]
        crm._mark_done(row_id)
        assert crm.queue_size() == 0

def test_backoff_pushes_the_next_attempt_into_the_future(tmp_path):
    with _fresh_queue(tmp_path):
        crm.enqueue({"text": "x"}, {}, False)
        row_id = crm._due_items()[0]["id"]
        crm._reschedule(row_id, 3, "still failing")   # 2**3 = 8 seconds
        assert crm._due_items() == []                 # not due yet
        assert crm.queue_size() == 1                  # but still queued

def test_backoff_is_capped(tmp_path):
    with _fresh_queue(tmp_path):
        crm.enqueue({"text": "x"}, {}, False)
        row_id = crm._due_items()[0]["id"]
        crm._reschedule(row_id, 20, "err")   # 2**20 would be 12 days
        with crm._db() as conn:
            nxt = conn.execute(
                "SELECT next_attempt_at FROM retry_queue WHERE id = ?", (row_id,)
            ).fetchone()[0]
        assert nxt - time.time() <= crm.BACKOFF_CAP_SECONDS + 1

def test_exhausted_lead_is_parked_not_deleted(tmp_path):
    # The invariant: there is no code path where an inquiry disappears.
    with _fresh_queue(tmp_path):
        crm.enqueue({"text": "לקוח אמיתי"}, {}, False)
        row_id = crm._due_items()[0]["id"]
        crm._mark_dead(row_id, "hubspot gone")
        assert crm.queue_size() == 0        # no longer retried
        assert crm.dead_letter_size() == 1  # but still there for a human
        with crm._db() as conn:
            kept = conn.execute(
                "SELECT inquiry FROM retry_queue WHERE id = ?", (row_id,)
            ).fetchone()[0]
        assert "לקוח אמיתי" in kept


# ---- text hygiene ----

def test_clean_text_caps_length():
    assert len(clean_text("א" * 10000)) == 4000

def test_clean_text_handles_none():
    assert clean_text(None) == ""
