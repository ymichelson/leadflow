"""Load local .env values before modules read configuration at import time."""

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
        # Process environment variables take precedence over the local file.
        if key and key not in os.environ:
            os.environ[key] = value


load_env()
