"""SQL-style quality expectations for gold tables (Great Expectations-like, no extra runtime)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    actual: Any
    expected: str
    detail: str = ""


def expect_non_empty(count: int) -> CheckResult:
    return CheckResult("gold_not_empty", count > 0, count, "> 0")


def expect_gmv_non_negative(min_gmv: int | None) -> CheckResult:
    ok = min_gmv is None or min_gmv >= 0
    return CheckResult("gmv_non_negative", ok, min_gmv, ">= 0")


def expect_failure_rate_bound(max_rate: float | None, ceiling: float = 0.8) -> CheckResult:
    ok = max_rate is None or max_rate <= ceiling
    return CheckResult("payment_failure_rate_bounded", ok, max_rate, f"<= {ceiling}")


def expect_unique_ratio(distinct_ids: int, row_count: int) -> CheckResult:
    if row_count == 0:
        return CheckResult("fact_orders_unique_event_id", True, 1.0, "== 1.0")
    ratio = distinct_ids / row_count
    return CheckResult("fact_orders_unique_event_id", ratio == 1.0, ratio, "== 1.0")


def expect_watermark_fresh(age_seconds: float | None, max_age: float = 900) -> CheckResult:
    ok = age_seconds is None or age_seconds <= max_age
    return CheckResult("gold_watermark_fresh", ok, age_seconds, f"<= {max_age}s")


def summarize(results: list[CheckResult]) -> dict[str, Any]:
    failed = [r.name for r in results if not r.passed]
    return {
        "passed": not failed,
        "failed": failed,
        "results": [r.__dict__ for r in results],
    }
