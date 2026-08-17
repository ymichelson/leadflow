"""Background controllers.

1. SLA check: every 15 minutes, ask the CRM one question - which open leads
   crossed the response window with nobody touching them. This is why the
   service runs on an always-on server: nobody calls this code, it wakes up
   on its own.

2. Silence alert: the deadliest intake failure is not a loud error but a
   webhook that died quietly. If nothing came in for N working hours,
   something is probably broken upstream.
"""

import asyncio
import logging
import os
from datetime import datetime, UTC

import business_hours
from crm import breach_total, find_overdue_leads
from pipeline import last_intake_at

log = logging.getLogger("leadflow")

SLA_HOURS = float(os.environ.get("SLA_HOURS", "4"))
SILENCE_HOURS = float(os.environ.get("SILENCE_HOURS", "4"))
CHECK_EVERY_SECONDS = int(os.environ.get("CHECK_EVERY_SECONDS", "900"))  # 15 min

# Exposed to the demo page. One entry per rep: {"rep", "count", "leads"}.
current_breaches: list[dict] = []
sla_check: dict = {"ok": None, "checked_at": None}
silence_alert: dict = {"active": False, "since": None}


async def sla_loop() -> None:
    while True:
        try:
            breaches = await find_overdue_leads(SLA_HOURS)
            current_breaches.clear()
            current_breaches.extend(breaches)
            sla_check["ok"] = True
            sla_check["checked_at"] = datetime.now(UTC).isoformat()
            if breaches:
                # Name the rep in the log line - that is the whole point of
                # the report: which salesperson is not getting back to people.
                log.warning("SLA ALERT: %d leads with no response past %.1f hours (%s)",
                            breach_total(breaches), SLA_HOURS,
                            ", ".join(f"{g['rep']}: {g['count']}" for g in breaches))
        except Exception as e:  # noqa: BLE001 - the checker itself must never die
            # CRM down? Fine. We simply catch the breaches on the next cycle.
            # A late SLA alert is acceptable; a dead checker is not.
            log.warning("SLA check skipped (CRM unreachable?): %s", e)
            sla_check["ok"] = False
        await asyncio.sleep(CHECK_EVERY_SECONDS)


async def silence_loop() -> None:
    while True:
        await asyncio.sleep(300)
        try:
            now = datetime.now(UTC)
            # Working hours here too, for the same reason as the SLA clock:
            # silence on a Friday is a closed office, not a dead webhook. The
            # old "(now.hour + 3) % 24" hardcoded UTC+3, which is wrong for
            # half the year and never knew about the weekend at all.
            hours_quiet = business_hours.business_hours_between(
                last_intake_at["ts"], now)
            if hours_quiet >= SILENCE_HOURS and business_hours.is_working_time(now):
                if not silence_alert["active"]:
                    log.error("SILENCE ALERT: no inquiries for %.1f hours - "
                              "check webhooks/channels", hours_quiet)
                silence_alert["active"] = True
                silence_alert["since"] = last_intake_at["ts"].isoformat()
            else:
                silence_alert["active"] = False
                silence_alert["since"] = None
        except Exception as e:  # noqa: BLE001
            log.warning("silence check error: %s", e)
