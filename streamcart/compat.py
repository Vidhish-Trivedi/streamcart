"""Python version shims shared by generator, Spark jobs, and Airflow."""

from __future__ import annotations

from datetime import datetime, timezone

try:
    from datetime import UTC
except ImportError:  # Python < 3.11 (Spark image fallback)
    UTC = timezone.utc

__all__ = ["UTC", "datetime", "timezone"]
