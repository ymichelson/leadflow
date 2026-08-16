"""The response clock only runs when the office is open.

Why this file exists: the plan said "first response within 4 WORKING hours"
and the code measured 4 wall-clock hours. Those are the same number on a
Tuesday morning and wildly different on a Thursday evening. A lead that comes
in Thursday at 17:00 would show up Sunday morning as 64 hours overdue, and the
rep it names did nothing wrong - it was Shabbat.

The first report the owner ever sees has to be defensible, or the whole tool
becomes an argument instead of a management aid. So the SLA clock pauses
outside working hours and over the weekend.

Israeli defaults: Sunday to Thursday, 09:00-18:00, Asia/Jerusalem (which also
handles the DST switch - a hardcoded UTC+3 is wrong for half the year).
"""

import logging
import os
from datetime import datetime, time, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 only
    ZoneInfo = None

log = logging.getLogger("leadflow")

_DAY_NAMES = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

TZ_NAME = os.environ.get("BUSINESS_TZ", "Asia/Jerusalem")


def _load_tz():
    try:
        return ZoneInfo(TZ_NAME)
    except Exception as e:  # noqa: BLE001 - a missing tz database must not kill intake
        log.warning("timezone %s unavailable (%s), falling back to UTC", TZ_NAME, e)
        from datetime import timezone
        return timezone.utc


TZ = _load_tz()


def _load_work_days() -> set[int]:
    raw = os.environ.get("WORK_DAYS", "sun,mon,tue,wed,thu")
    days = {_DAY_NAMES[d.strip().lower()] for d in raw.split(",")
            if d.strip().lower() in _DAY_NAMES}
    return days


WORK_DAYS: set[int] = _load_work_days()
WORK_START = int(os.environ.get("WORK_START", "9"))
WORK_END = int(os.environ.get("WORK_END", "18"))

# A broken config must not quietly make every lead look on-time. Falling back
# to a 24/7 clock over-reports breaches, which a human notices and corrects.
# Under-reporting is the dangerous direction: it hides exactly what we measure.
_CONFIG_OK = bool(WORK_DAYS) and 0 <= WORK_START < WORK_END <= 24
if not _CONFIG_OK:
    log.error("bad working-hours config (days=%s %s-%s) - falling back to a 24/7 "
              "clock so breaches are over-reported, never hidden",
              sorted(WORK_DAYS), WORK_START, WORK_END)


def is_working_time(moment: datetime) -> bool:
    """Is the office open at this instant?"""
    if not _CONFIG_OK:
        return True
    local = moment.astimezone(TZ)
    return local.weekday() in WORK_DAYS and WORK_START <= local.hour < WORK_END


def business_hours_between(start: datetime, end: datetime) -> float:
    """Working hours elapsed between two instants, ignoring nights and weekends.

    Walks day by day rather than doing modular arithmetic: slower, but it reads
    the way the rule is spoken, and the SLA loop runs once every 15 minutes so
    the cost is irrelevant. Clearer code wins here.
    """
    if not _CONFIG_OK:
        return max(0.0, (end - start).total_seconds() / 3600)
    if end <= start:
        return 0.0

    start, end = start.astimezone(TZ), end.astimezone(TZ)
    total = 0.0
    day = start.date()
    guard = 0
    while day <= end.date() and guard < 400:   # guard: never spin on a bad date
        guard += 1
        if day.weekday() in WORK_DAYS:
            open_at = datetime.combine(day, time(hour=WORK_START), tzinfo=TZ)
            close_at = datetime.combine(day, time(hour=WORK_END - 1, minute=59,
                                                 second=59), tzinfo=TZ) + timedelta(seconds=1)
            lo, hi = max(start, open_at), min(end, close_at)
            if hi > lo:
                total += (hi - lo).total_seconds() / 3600
        day += timedelta(days=1)
    return total


def describe() -> str:
    """One line for /status and the logs, so the rule is visible not implied."""
    if not _CONFIG_OK:
        return "24/7 (working-hours config invalid)"
    # Display order starts on Sunday, because that is how an Israeli week reads.
    week = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
    names = [n for n in week if _DAY_NAMES[n] in WORK_DAYS]
    return f"{','.join(names)} {WORK_START:02d}:00-{WORK_END:02d}:00 {TZ_NAME}"
