"""Round-robin assignment policy for new contacts.

The in-memory position is seeded from HubSpot on startup. HubSpot remains the
source of truth for existing assignments.
"""

import logging
import os

log = logging.getLogger("leadflow")

DEFAULT_REPS = "דנה,יוסי,מאיה"


def _load_reps() -> list[str]:
    raw = os.environ.get("REPS") or DEFAULT_REPS
    names = [n.strip() for n in raw.split(",") if n.strip()]
    return names or [n.strip() for n in DEFAULT_REPS.split(",")]


REPS: list[str] = _load_reps()

# Rotation position. Dict (not a bare int) so it can be mutated from functions
# without `global`. Single-process asyncio: read-modify-write below contains no
# `await`, so two concurrent inquiries can never receive the same rep.
_rotation: dict = {"i": 0}


def next_rep() -> str:
    """Return the next rep in the rotation and advance the position."""
    rep = REPS[_rotation["i"] % len(REPS)]
    _rotation["i"] = (_rotation["i"] + 1) % len(REPS)
    return rep


def set_rotation_start(assigned_so_far: int) -> None:
    """Resume the rotation after a restart.

    `assigned_so_far` is how many leads the CRM says already carry a rep.
    Starting at that count modulo the number of reps continues the cycle
    instead of restarting it at the first name every deploy.
    """
    if assigned_so_far < 0:
        return
    _rotation["i"] = assigned_so_far % len(REPS)
    log.info("rep rotation seeded from CRM: %d assigned leads -> next is %s",
             assigned_so_far, REPS[_rotation["i"]])


def current_position() -> int:
    """Exposed for /status so the rotation is observable, not magic."""
    return _rotation["i"]
