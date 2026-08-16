"""Tests for the logic that fails silently if it's wrong.

Run: pytest test_leadflow.py
"""

import json
import time
from unittest.mock import patch

import crm
import reps
import eval as evalmod   # aliased so it doesn't shadow the builtin in this file
from utils import normalize_phone, clean_text
from classify import classify_inquiry


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


# ---- classification fails open, never crashes intake ----

def test_classify_fails_open_without_api_key():
    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}):
        verdict = classify_inquiry("היי, אשמח להצעת מחיר")
    assert verdict["confidence"] == 0          # -> routed to human review
    assert verdict["category"] == "other"      # no guessing
    assert verdict["error"] is not None        # the failure is visible, not silent


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
