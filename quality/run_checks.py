"""
Data Quality Runner
===================
Executes a suite of data quality checks across bronze, silver, and gold layers.
Each check returns PASSED / WARNING / FAILED and writes results to the
metadata.quality_checks table.

Exits with code 1 if any check is FAILED (blocking — pipeline should halt).
Exits with code 0 if all checks pass or warnings only.

Usage:
    python quality/run_checks.py \
        --project-id my-project \
        --execution-date 2024-01 \
        --env dev
"""

from __future__ import annotations

import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import click
import structlog
from google.cloud import bigquery
from tabulate import tabulate

logger = structlog.get_logger(__name__)


# ── Check result ─────────────────────────────────────────────

@dataclass
class CheckResult:
    check_name: str
    layer: str
    table_name: str
    status: str          # PASSED | WARNING | FAILED
    rows_tested: int = 0
    rows_failed: int = 0
    failure_rate: float = 0.0
    threshold: float = 0.0
    details: str = ""

    @property
    def failure_pct(self) -> str:
        return f"{self.failure_rate:.2%}"

    def to_bq_row(self, run_id: str, execution_date: str) -> dict:
        return {
            "run_id": run_id,
            "execution_date": execution_date,
            "layer": self.layer,
            "check_name": self.check_name,
            "table_name": self.table_name,
            "status": self.status,
            "rows_tested": self.rows_tested,
            "rows_failed": self.rows_failed,
            "failure_rate": self.failure_rate,
            "threshold": self.threshold,
            "check_timestamp": datetime.now(timezone.utc).isoformat(),
            "details": self.details,
        }


# ── Check implementations ─────────────────────────────────────

class QualityChecker:
    def __init__(self, client: bigquery.Client, project_id: str, env: str) -> None:
        self._client = client
        self._project = project_id
        self._env = env

    def _ds(self, layer: str) -> str:
        return f"{self._project}.nyc_taxi_{self._env}_{layer}"

    def _query(self, sql: str, params: list[bigquery.ScalarQueryParameter]) -> list[dict]:
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        job = self._client.query(sql, job_config=job_config)
        return [dict(row) for row in job.result()]

    # ── Bronze checks ────────────────────────────────────────

    def check_bronze_not_null_pickup(self, execution_date: str) -> CheckResult:
        table = f"{self._ds('bronze')}.trips"
        rows = self._query(
            f"""
            SELECT
              COUNT(*) AS total,
              COUNTIF(tpep_pickup_datetime IS NULL) AS nulls
            FROM `{table}`
            WHERE _execution_date = @ed
            """,
            [bigquery.ScalarQueryParameter("ed", "STRING", execution_date)],
        )
        total = rows[0]["total"]
        nulls = rows[0]["nulls"]
        rate = nulls / total if total > 0 else 0
        return CheckResult(
            check_name="not_null_pickup_datetime",
            layer="bronze",
            table_name=table,
            status="PASSED" if nulls == 0 else "FAILED",
            rows_tested=total,
            rows_failed=nulls,
            failure_rate=rate,
            threshold=0.0,
            details=f"{nulls} NULL pickup datetimes out of {total} rows",
        )

    def check_bronze_row_count(self, execution_date: str) -> CheckResult:
        table = f"{self._ds('bronze')}.trips"
        rows = self._query(
            f"SELECT COUNT(*) AS total FROM `{table}` WHERE _execution_date = @ed",
            [bigquery.ScalarQueryParameter("ed", "STRING", execution_date)],
        )
        total = rows[0]["total"]
        # Expect at least 1000 rows for any given month (NYC always has millions)
        min_expected = 1_000
        status = "PASSED" if total >= min_expected else "FAILED"
        return CheckResult(
            check_name="minimum_row_count",
            layer="bronze",
            table_name=table,
            status=status,
            rows_tested=total,
            rows_failed=0 if status == "PASSED" else 1,
            details=f"Row count: {total:,}. Minimum expected: {min_expected:,}",
        )

    def check_bronze_no_future_dates(self, execution_date: str) -> CheckResult:
        table = f"{self._ds('bronze')}.trips"
        rows = self._query(
            f"""
            SELECT
              COUNT(*) AS total,
              COUNTIF(tpep_pickup_datetime > CURRENT_TIMESTAMP()) AS future_dates
            FROM `{table}`
            WHERE _execution_date = @ed
            """,
            [bigquery.ScalarQueryParameter("ed", "STRING", execution_date)],
        )
        total = rows[0]["total"]
        future = rows[0]["future_dates"]
        rate = future / total if total > 0 else 0
        return CheckResult(
            check_name="no_future_pickup_dates",
            layer="bronze",
            table_name=table,
            status="PASSED" if future == 0 else "WARNING",
            rows_tested=total,
            rows_failed=future,
            failure_rate=rate,
            threshold=0.0,
            details=f"{future} rows with future pickup datetime",
        )

    # ── Silver checks ────────────────────────────────────────

    def check_silver_valid_location_ids(self, execution_date: str) -> CheckResult:
        table = f"{self._ds('silver')}.trips"
        rows = self._query(
            f"""
            SELECT
              COUNT(*) AS total,
              COUNTIF(
                pickup_location_id NOT BETWEEN 1 AND 265
                OR dropoff_location_id NOT BETWEEN 1 AND 265
              ) AS invalid_locations
            FROM `{table}`
            WHERE _execution_date = @ed
            """,
            [bigquery.ScalarQueryParameter("ed", "STRING", execution_date)],
        )
        total = rows[0]["total"]
        invalid = rows[0]["invalid_locations"]
        rate = invalid / total if total > 0 else 0
        threshold = 0.001  # <0.1% tolerance
        return CheckResult(
            check_name="valid_location_ids",
            layer="silver",
            table_name=table,
            status="PASSED" if rate <= threshold else "FAILED",
            rows_tested=total,
            rows_failed=invalid,
            failure_rate=rate,
            threshold=threshold,
            details=f"{invalid} rows with invalid TLC zone IDs (allowed range: 1-265)",
        )

    def check_silver_positive_amounts(self, execution_date: str) -> CheckResult:
        table = f"{self._ds('silver')}.trips"
        rows = self._query(
            f"""
            SELECT
              COUNT(*) AS total,
              COUNTIF(total_amount < 0 OR fare_amount < 0) AS negative_amounts
            FROM `{table}`
            WHERE _execution_date = @ed
            """,
            [bigquery.ScalarQueryParameter("ed", "STRING", execution_date)],
        )
        total = rows[0]["total"]
        negative = rows[0]["negative_amounts"]
        return CheckResult(
            check_name="non_negative_amounts",
            layer="silver",
            table_name=table,
            status="PASSED" if negative == 0 else "FAILED",
            rows_tested=total,
            rows_failed=negative,
            failure_rate=negative / total if total > 0 else 0,
            threshold=0.0,
            details=f"{negative} rows with negative total_amount or fare_amount",
        )

    def check_silver_trip_duration_range(self, execution_date: str) -> CheckResult:
        table = f"{self._ds('silver')}.trips"
        rows = self._query(
            f"""
            SELECT
              COUNT(*) AS total,
              COUNTIF(trip_duration_min NOT BETWEEN 1 AND 300) AS out_of_range
            FROM `{table}`
            WHERE _execution_date = @ed
            """,
            [bigquery.ScalarQueryParameter("ed", "STRING", execution_date)],
        )
        total = rows[0]["total"]
        bad = rows[0]["out_of_range"]
        rate = bad / total if total > 0 else 0
        threshold = 0.005  # <0.5% tolerance
        return CheckResult(
            check_name="trip_duration_range_1_to_300_min",
            layer="silver",
            table_name=table,
            status="PASSED" if rate <= threshold else "WARNING",
            rows_tested=total,
            rows_failed=bad,
            failure_rate=rate,
            threshold=threshold,
            details=f"{bad} trips with duration outside [1, 300] minutes ({rate:.2%})",
        )

    # ── Gold checks ──────────────────────────────────────────

    def check_gold_daily_revenue_completeness(self, execution_date: str) -> CheckResult:
        table = f"{self._ds('gold')}.kpi_daily_revenue"
        rows = self._query(
            f"""
            SELECT COUNT(DISTINCT pickup_date) AS day_count
            FROM `{table}`
            WHERE FORMAT_DATE('%Y-%m', pickup_date) = @ed
            """,
            [bigquery.ScalarQueryParameter("ed", "STRING", execution_date)],
        )
        day_count = rows[0]["day_count"]
        # Most months have 28-31 days; require at least 25 to account for edge months
        min_days = 25
        status = "PASSED" if day_count >= min_days else "FAILED"
        return CheckResult(
            check_name="daily_revenue_completeness",
            layer="gold",
            table_name=table,
            status=status,
            rows_tested=day_count,
            rows_failed=0 if status == "PASSED" else 1,
            details=f"{day_count} days found in kpi_daily_revenue for {execution_date}. Minimum: {min_days}",
        )

    def check_gold_total_revenue_positive(self, execution_date: str) -> CheckResult:
        table = f"{self._ds('gold')}.kpi_daily_revenue"
        rows = self._query(
            f"""
            SELECT
              COUNT(*) AS total,
              COUNTIF(total_revenue <= 0) AS zero_revenue_days
            FROM `{table}`
            WHERE FORMAT_DATE('%Y-%m', pickup_date) = @ed
            """,
            [bigquery.ScalarQueryParameter("ed", "STRING", execution_date)],
        )
        total = rows[0]["total"]
        bad = rows[0]["zero_revenue_days"]
        return CheckResult(
            check_name="positive_daily_revenue",
            layer="gold",
            table_name=table,
            status="PASSED" if bad == 0 else "FAILED",
            rows_tested=total,
            rows_failed=bad,
            failure_rate=bad / total if total > 0 else 0,
            details=f"{bad} days with zero or negative total_revenue",
        )

    def run_all(self, execution_date: str) -> list[CheckResult]:
        checks = [
            # Bronze
            self.check_bronze_not_null_pickup,
            self.check_bronze_row_count,
            self.check_bronze_no_future_dates,
            # Silver
            self.check_silver_valid_location_ids,
            self.check_silver_positive_amounts,
            self.check_silver_trip_duration_range,
            # Gold
            self.check_gold_daily_revenue_completeness,
            self.check_gold_total_revenue_positive,
        ]
        results = []
        for check_fn in checks:
            log = logger.bind(check=check_fn.__name__, execution_date=execution_date)
            try:
                result = check_fn(execution_date)
                log.info("check_done", status=result.status, details=result.details)
                results.append(result)
            except Exception as exc:
                log.error("check_error", error=str(exc))
                results.append(
                    CheckResult(
                        check_name=check_fn.__name__,
                        layer="unknown",
                        table_name="unknown",
                        status="FAILED",
                        details=f"Check raised exception: {exc}",
                    )
                )
        return results


# ── CLI entry point ───────────────────────────────────────────

@click.command()
@click.option("--project-id", required=True)
@click.option("--execution-date", required=True, help="YYYY-MM")
@click.option("--env", default="dev", type=click.Choice(["dev", "staging", "prod"]))
@click.option("--write-results/--no-write-results", default=True,
              help="Write check results to metadata.quality_checks table")
def main(project_id: str, execution_date: str, env: str, write_results: bool) -> None:
    run_id = str(uuid.uuid4())
    client = bigquery.Client(project=project_id)
    checker = QualityChecker(client, project_id, env)

    logger.info("quality_checks_started", run_id=run_id, execution_date=execution_date)

    results = checker.run_all(execution_date)

    # ── Print table ──────────────────────────────────────────
    table_data = [
        [r.layer, r.check_name, r.status, r.rows_tested, r.rows_failed, r.failure_pct]
        for r in results
    ]
    print("\n" + tabulate(
        table_data,
        headers=["Layer", "Check", "Status", "Rows Tested", "Rows Failed", "Failure %"],
        tablefmt="rounded_outline",
    ))

    # ── Write to BQ ──────────────────────────────────────────
    if write_results:
        metadata_dataset = f"{project_id}.nyc_taxi_{env}_metadata"
        table_ref = f"{metadata_dataset}.quality_checks"
        rows = [r.to_bq_row(run_id, execution_date) for r in results]
        errors = client.insert_rows_json(client.get_table(table_ref), rows)
        if errors:
            logger.warning("bq_insert_warning", errors=errors)

    # ── Exit code ────────────────────────────────────────────
    failed = [r for r in results if r.status == "FAILED"]
    if failed:
        logger.error("quality_gate_failed", failed_checks=[r.check_name for r in failed])
        print(f"\n[FAIL] {len(failed)} check(s) failed. Pipeline should not proceed.")
        sys.exit(1)
    else:
        warnings = [r for r in results if r.status == "WARNING"]
        logger.info("quality_checks_passed", warnings=len(warnings))
        print(f"\n[PASS] All checks passed ({len(warnings)} warning(s)).")
        sys.exit(0)


if __name__ == "__main__":
    main()
