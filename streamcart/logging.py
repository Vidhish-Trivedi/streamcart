from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any

import orjson


def log(level: str, message: str, **fields: Any) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        **fields,
    }
    sys.stdout.write(orjson.dumps(record).decode() + "\n")
    sys.stdout.flush()
