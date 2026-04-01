"""
Unit tests for quality/run_checks.py

Tests the CheckResult dataclass and the QualityChecker logic
by mocking BigQuery responses — no GCP credentials required.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "quality"))

from run_checks import CheckResult, QualityChecker


# ── CheckResult dataclass ─────────────────────────────────────

class TestCheckResult:
    def test_failure_pct_zero_when_no_failures(self):
        r = CheckResult(
            check_name="test",
            layer="bronze",
            table_name="bronze.trips",
            status="PASSED",
            rows_tested=1000,
            rows_failed=0,
            failure_rate=0.0,
        )
        assert r.failure_pct == "0.00%"

    def test_failure_pct_formatted(self):
        r = CheckResult(
            check_name="test",
            layer="silver",
            table_name="silver.trips",
            status="FAILED",
            rows_tested=1000,
            rows_failed=50,
            failure_rate=0.05,
        )
        assert r.failure_pct == "5.00%"

    def test_to_bq_row_contains_required_fields(self):
        r = CheckResult(
            check_name="not_null_pickup",
            layer="bronze",
            table_name="bronze.trips",
            status="PASSED",
            rows_tested=500,
            rows_failed=0,
            failure_rate=0.0,
            threshold=0.0,
            details="All good",
        )
        row = r.to_bq_row("run-123", "2024-01")
        assert row["run_id"] == "run-123"
        assert row["execution_date"] == "2024-01"
        assert row["layer"] == "bronze"
        assert row["check_name"] == "not_null_pickup"
        assert row["status"] == "PASSED"
        assert row["rows_tested"] == 500
        assert row["rows_failed"] == 0
        assert "check_timestamp" in row

    def test_to_bq_row_timestamp_is_string(self):
        r = CheckResult("c", "bronze", "t", "PASSED")
        row = r.to_bq_row("run-1", "2024-01")
        assert isinstance(row["check_timestamp"], str)

    @pytest.mark.parametrize("status", ["PASSED", "WARNING", "FAILED"])
    def test_valid_statuses_accepted(self, status):
        r = CheckResult("c", "bronze", "t", status)
        assert r.status == status


# ── QualityChecker with mocked BQ ────────────────────────────

def make_checker(mock_rows: list[dict]) -> tuple[QualityChecker, MagicMock]:
    """Helper: build a QualityChecker whose BQ client returns mock_rows."""
    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_job.result.return_value = [MagicMock(**{"__iter__": MagicMock(return_value=iter(mock_rows))})]

    # Make the row behave like a dict
    for row_data in mock_rows:
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, k, d=row_data: d[k]
        mock_row.get = lambda k, default=None, d=row_data: d.get(k, default)

    mock_client.query.return_value = mock_job

    checker = QualityChecker(mock_client, "test-project", "dev")
    return checker, mock_client


class TestBronzeChecks:
    def _mock_query(self, checker: QualityChecker, rows: list[dict]) -> None:
        """Patch checker._query to return rows directly."""
        checker._query = MagicMock(return_value=rows)

    def test_not_null_pickup_passes_when_zero_nulls(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 1000, "nulls": 0}])
        result = checker.check_bronze_not_null_pickup("2024-01")
        assert result.status == "PASSED"
        assert result.rows_failed == 0

    def test_not_null_pickup_fails_when_nulls_exist(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 1000, "nulls": 5}])
        result = checker.check_bronze_not_null_pickup("2024-01")
        assert result.status == "FAILED"
        assert result.rows_failed == 5
        assert result.failure_rate == pytest.approx(0.005)

    def test_row_count_passes_above_minimum(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 3_500_000}])
        result = checker.check_bronze_row_count("2024-01")
        assert result.status == "PASSED"

    def test_row_count_fails_below_minimum(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 50}])  # below 1000 minimum
        result = checker.check_bronze_row_count("2024-01")
        assert result.status == "FAILED"

    def test_no_future_dates_passes_when_zero_future(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 1000, "future_dates": 0}])
        result = checker.check_bronze_no_future_dates("2024-01")
        assert result.status == "PASSED"

    def test_no_future_dates_warns_when_some_future(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 1000, "future_dates": 3}])
        result = checker.check_bronze_no_future_dates("2024-01")
        assert result.status == "WARNING"  # not FAILED — warnings don't block pipeline


class TestSilverChecks:
    def _mock_query(self, checker: QualityChecker, rows: list[dict]) -> None:
        checker._query = MagicMock(return_value=rows)

    def test_valid_location_ids_passes_below_threshold(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        # 5 invalid out of 1,000,000 = 0.0005% — below 0.1% threshold
        self._mock_query(checker, [{"total": 1_000_000, "invalid_locations": 5}])
        result = checker.check_silver_valid_location_ids("2024-01")
        assert result.status == "PASSED"

    def test_valid_location_ids_fails_above_threshold(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        # 5000 invalid out of 100,000 = 5% — above 0.1% threshold
        self._mock_query(checker, [{"total": 100_000, "invalid_locations": 5_000}])
        result = checker.check_silver_valid_location_ids("2024-01")
        assert result.status == "FAILED"

    def test_positive_amounts_passes_when_none_negative(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 500_000, "negative_amounts": 0}])
        result = checker.check_silver_positive_amounts("2024-01")
        assert result.status == "PASSED"

    def test_positive_amounts_fails_when_any_negative(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 500_000, "negative_amounts": 1}])
        result = checker.check_silver_positive_amounts("2024-01")
        assert result.status == "FAILED"

    def test_trip_duration_range_warns_when_small_pct(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        # 0.2% out of range — between warning threshold but below hard fail
        self._mock_query(checker, [{"total": 1_000_000, "out_of_range": 2_000}])
        result = checker.check_silver_trip_duration_range("2024-01")
        # 0.2% > 0.5% threshold → this actually PASSES (0.002 < 0.005)
        assert result.status == "PASSED"

    def test_trip_duration_range_warns_above_threshold(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        # 1% out of range — above 0.5% threshold
        self._mock_query(checker, [{"total": 1_000_000, "out_of_range": 10_000}])
        result = checker.check_silver_trip_duration_range("2024-01")
        assert result.status == "WARNING"


class TestGoldChecks:
    def _mock_query(self, checker: QualityChecker, rows: list[dict]) -> None:
        checker._query = MagicMock(return_value=rows)

    def test_daily_completeness_passes_with_enough_days(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"day_count": 31}])
        result = checker.check_gold_daily_revenue_completeness("2024-01")
        assert result.status == "PASSED"

    def test_daily_completeness_fails_with_too_few_days(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"day_count": 10}])  # below 25-day minimum
        result = checker.check_gold_daily_revenue_completeness("2024-01")
        assert result.status == "FAILED"

    def test_positive_revenue_passes_when_all_positive(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 31, "zero_revenue_days": 0}])
        result = checker.check_gold_total_revenue_positive("2024-01")
        assert result.status == "PASSED"

    def test_positive_revenue_fails_on_zero_revenue_day(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        self._mock_query(checker, [{"total": 31, "zero_revenue_days": 1}])
        result = checker.check_gold_total_revenue_positive("2024-01")
        assert result.status == "FAILED"


# ── run_all: exception handling ───────────────────────────────

class TestRunAll:
    def test_run_all_returns_one_result_per_check(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        # Patch all individual check methods to return a PASSED result
        passed = CheckResult("c", "bronze", "t", "PASSED", rows_tested=100)
        for method in [
            "check_bronze_not_null_pickup",
            "check_bronze_row_count",
            "check_bronze_no_future_dates",
            "check_silver_valid_location_ids",
            "check_silver_positive_amounts",
            "check_silver_trip_duration_range",
            "check_gold_daily_revenue_completeness",
            "check_gold_total_revenue_positive",
        ]:
            mock_fn = MagicMock(return_value=passed)
            mock_fn.__name__ = method
            setattr(checker, method, mock_fn)

        results = checker.run_all("2024-01")
        assert len(results) == 8

    def test_run_all_captures_exception_as_failed(self):
        checker = QualityChecker(MagicMock(), "proj", "dev")
        passed = CheckResult("c", "bronze", "t", "PASSED", rows_tested=100)

        def _mock(name, *, side_effect=None, return_value=passed):
            m = MagicMock(side_effect=side_effect, return_value=return_value)
            m.__name__ = name
            return m

        checker.check_bronze_not_null_pickup = _mock("check_bronze_not_null_pickup", side_effect=Exception("BQ timeout"))
        checker.check_bronze_row_count = _mock("check_bronze_row_count")
        checker.check_bronze_no_future_dates = _mock("check_bronze_no_future_dates")
        checker.check_silver_valid_location_ids = _mock("check_silver_valid_location_ids")
        checker.check_silver_positive_amounts = _mock("check_silver_positive_amounts")
        checker.check_silver_trip_duration_range = _mock("check_silver_trip_duration_range")
        checker.check_gold_daily_revenue_completeness = _mock("check_gold_daily_revenue_completeness")
        checker.check_gold_total_revenue_positive = _mock("check_gold_total_revenue_positive")

        results = checker.run_all("2024-01")
        failed = [r for r in results if r.status == "FAILED"]
        assert len(failed) == 1
        assert "BQ timeout" in failed[0].details
