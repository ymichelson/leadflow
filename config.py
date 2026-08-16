"""Reads .env into the environment.

Why this exists: several modules read os.environ at import time (reps.py picks
up REPS, business_hours.py picks up the working week). So the file has to be
loaded before those imports run, which is why main.py imports this first.

Deliberately hand-rolled instead of adding python-dotenv. Fifteen lines of
standard library beats a dependency, and a take-home should not add a package
to do something this small.
"""

import os
from pathlib import Path


def load_env(filename: str = ".env") -> None:
    """Load KEY=value lines from .env. Missing file is fine, not an error."""
    path = Path(__file__).resolve().parent / filename
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        # A real environment variable always wins over the file, so you can
        # override one setting for a single run without editing anything:
        #     SLA_HOURS=1 uvicorn main:app
        if key and key not in os.environ:
            os.environ[key] = value


load_env()
