"""Small shared helpers. Kept separate so they are easy to test."""

import re


def normalize_phone(raw: str | None) -> str | None:
    """Normalize Israeli phone numbers to E.164 (+972...).

    The same customer can appear as 050-1234567, 0501234567 or +972501234567.
    Normalization lets the CRM deduplicate equivalent phone formats.
    """
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    if digits.startswith("972"):
        digits = digits[3:]
    digits = digits.lstrip("0")
    # Israeli mobile/landline numbers are 8-9 digits after the leading zero
    if len(digits) < 8 or len(digits) > 9:
        return None
    return f"+972{digits}"


def valid_email(raw: str | None) -> bool:
    """Apply a deliberately small email sanity check, not full RFC validation."""
    if not raw:
        return False
    value = raw.strip()
    return bool(re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value))


def clean_text(raw: str | None, limit: int = 4000) -> str:
    """Trim and cap free text before sending it anywhere."""
    if not raw:
        return ""
    return raw.strip()[:limit]
