"""Durable, idempotent intake before AI or CRM work begins.

The HTTP request only promises that the inquiry was safely received. A small
worker claims saved rows and runs the existing pipeline afterwards. If the
process dies, unfinished rows remain in SQLite and are recovered on restart.

This table is a delivery ledger, not a CRM. HubSpot remains the system of
record; the ledger exists only to prove that accepted work cannot disappear.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from datetime import UTC, datetime

from pipeline import handle_inquiry
from utils import clean_text

log = logging.getLogger("leadflow")

MAX_ATTEMPTS = 8
BACKOFF_CAP_SECONDS = 300
POLL_SECONDS = 0.5
_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class IdempotencyConflict(ValueError):
    """The same submission ID was reused for different content."""


def _db() -> sqlite3.Connection:
    path = os.environ.get("STATE_DB") or os.environ.get(
        "RETRY_DB", "leadflow_queue.db"
    )
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS inquiry_inbox (
            submission_id  TEXT PRIMARY KEY,
            payload        TEXT NOT NULL,
            payload_hash   TEXT NOT NULL,
            status         TEXT NOT NULL DEFAULT 'received',
            attempts       INTEGER NOT NULL DEFAULT 0,
            received_at    TEXT NOT NULL,
            updated_at     REAL NOT NULL,
            next_attempt_at REAL NOT NULL,
            last_error     TEXT,
            result         TEXT
        )
    """)
    conn.commit()
    return conn


def _canonical_payload(source: str, text: str, phone: str | None,
                       email: str | None, name: str | None) -> str:
    payload = {
        "source": clean_text(source, limit=100),
        "text": clean_text(text),
        "phone": clean_text(phone, limit=100) or None,
        "email": clean_text(email, limit=320).lower() or None,
        "name": clean_text(name, limit=200) or None,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def accept_inquiry(source: str, text: str, phone: str | None = None,
                   email: str | None = None, name: str | None = None,
                   submission_id: str | None = None) -> dict:
    """Persist an inquiry before acknowledging it.

    Reusing the same ID with the same payload is safe and does not create new
    work. Reusing it for different content is rejected loudly.
    """
    sid = (submission_id or str(uuid.uuid4())).strip()
    if not _ID_PATTERN.fullmatch(sid):
        raise ValueError("invalid submission_id")
    payload = _canonical_payload(source, text, phone, email, name)
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    if not json.loads(payload)["text"]:
        raise ValueError("empty inquiry")

    now = time.time()
    received_at = datetime.now(UTC).isoformat()
    with _db() as conn:
        try:
            conn.execute(
                """INSERT INTO inquiry_inbox
                   (submission_id, payload, status, attempts, received_at,
                    updated_at, next_attempt_at, payload_hash)
                   VALUES (?, ?, 'received', 0, ?, ?, ?, ?)""",
                (sid, payload, received_at, now, now, payload_hash),
            )
            return {"submission_id": sid, "status": "received", "duplicate": False}
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT payload_hash, status FROM inquiry_inbox WHERE submission_id = ?",
                (sid,),
            ).fetchone()
            if row is None or row["payload_hash"] != payload_hash:
                raise IdempotencyConflict(
                    "submission_id already belongs to another inquiry"
                )
            return {"submission_id": sid, "status": row["status"], "duplicate": True}


def recover_in_progress() -> int:
    """Return work interrupted by a process restart to the claimable queue."""
    now = time.time()
    with _db() as conn:
        cursor = conn.execute(
            """UPDATE inquiry_inbox
               SET status = 'received', updated_at = ?, next_attempt_at = ?,
                   last_error = 'worker restarted while processing'
               WHERE status = 'processing'""",
            (now, now),
        )
        return cursor.rowcount


def _claim_one() -> dict | None:
    """Atomically claim one due row so two workers cannot process it."""
    now = time.time()
    with _db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """SELECT * FROM inquiry_inbox
               WHERE status IN ('received', 'retry') AND next_attempt_at <= ?
               ORDER BY received_at ASC LIMIT 1""",
            (now,),
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            """UPDATE inquiry_inbox
               SET status = 'processing', attempts = attempts + 1, updated_at = ?
               WHERE submission_id = ?""",
            (now, row["submission_id"]),
        )
        claimed = dict(row)
        claimed["attempts"] += 1
        claimed["status"] = "processing"
        return claimed


def _mark_completed(submission_id: str, result: dict) -> None:
    safe_result = {
        "needs_review": bool(result.get("needs_review")),
        "crm_ok": bool(result.get("crm", {}).get("ok")),
        "crm_queued": bool(result.get("crm", {}).get("queued")),
    }
    with _db() as conn:
        conn.execute(
            """UPDATE inquiry_inbox
               SET status = 'completed', updated_at = ?, payload = '{}',
                   result = ?, last_error = NULL
               WHERE submission_id = ?""",
            (time.time(), json.dumps(safe_result), submission_id),
        )


def _mark_failed(submission_id: str, attempts: int, error: str) -> None:
    now = time.time()
    if attempts >= MAX_ATTEMPTS:
        status, next_attempt = "dead", now
    else:
        status = "retry"
        next_attempt = now + min(2 ** attempts, BACKOFF_CAP_SECONDS)
    with _db() as conn:
        conn.execute(
            """UPDATE inquiry_inbox
               SET status = ?, updated_at = ?, next_attempt_at = ?, last_error = ?
               WHERE submission_id = ?""",
            (status, now, next_attempt, error[:1000], submission_id),
        )


async def process_claimed(row: dict) -> None:
    payload = json.loads(row["payload"])
    try:
        result = await handle_inquiry(
            source=payload["source"],
            text=payload["text"],
            phone=payload.get("phone"),
            email=payload.get("email"),
            name=payload.get("name"),
            submission_id=row["submission_id"],
        )
        _mark_completed(row["submission_id"], result)
        log.info("durable inquiry completed: %s", row["submission_id"])
    except Exception as exc:  # noqa: BLE001 - the ledger must retain every failure
        _mark_failed(row["submission_id"], row["attempts"], str(exc))
        log.warning("durable inquiry rescheduled: %s | %s",
                    row["submission_id"], exc)


async def processing_loop() -> None:
    while True:
        try:
            row = _claim_one()
            if row:
                await process_claimed(row)
                continue
        except Exception as exc:  # noqa: BLE001 - one DB error must not kill the worker
            log.warning("intake worker error: %s", exc)
        await asyncio.sleep(POLL_SECONDS)


def get_status(submission_id: str) -> dict | None:
    with _db() as conn:
        row = conn.execute(
            "SELECT submission_id, status, attempts, received_at, last_error "
            "FROM inquiry_inbox WHERE submission_id = ?",
            (submission_id,),
        ).fetchone()
        return dict(row) if row else None


def pending_size() -> int:
    try:
        with _db() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM inquiry_inbox "
                "WHERE status IN ('received', 'processing', 'retry')"
            ).fetchone()[0]
    except Exception as exc:  # noqa: BLE001 - status page must remain available
        log.warning("intake pending_size failed: %s", exc)
        return 0


def dead_size() -> int:
    try:
        with _db() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM inquiry_inbox WHERE status = 'dead'"
            ).fetchone()[0]
    except Exception as exc:  # noqa: BLE001
        log.warning("intake dead_size failed: %s", exc)
        return 0
