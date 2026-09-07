from __future__ import annotations

from streamcart.quality import (
    expect_failure_rate_bound,
    expect_gmv_non_negative,
    expect_non_empty,
    expect_unique_ratio,
    expect_watermark_fresh,
    summarize,
)


def test_quality_passes_healthy_gold() -> None:
    results = [
        expect_non_empty(10),
        expect_unique_ratio(10, 10),
        expect_gmv_non_negative(0),
        expect_failure_rate_bound(0.2),
        expect_watermark_fresh(30),
    ]
    summary = summarize(results)
    assert summary["passed"] is True
    assert summary["failed"] == []


def test_quality_fails_duplicates_and_negative_gmv() -> None:
    results = [
        expect_unique_ratio(9, 10),
        expect_gmv_non_negative(-5),
    ]
    summary = summarize(results)
    assert summary["passed"] is False
    assert "fact_orders_unique_event_id" in summary["failed"]
    assert "gmv_non_negative" in summary["failed"]
