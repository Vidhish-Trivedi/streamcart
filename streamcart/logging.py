from __future__ import annotations

import sys
from datetime import datetime
from typing import Any

import orjson

from streamcart.compat import UTC


def log(level: str, message: str, **fields: Any) -> None:
    record = {
        "ts": datetime.now(UTC).isoformat(),
        "level": level,
        "message": message,
        **fields,
    }
    sys.stdout.write(orjson.dumps(record).decode() + "\n")
    sys.stdout.flush()
