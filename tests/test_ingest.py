"""
Unit tests for ingestion/ingest.py

All tests run without GCP credentials.
GCS and BQ clients are mocked where needed.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))

from ingest import build_tlc_url, enrich_dataframe, validate_raw_dataframe


# ── build_tlc_url ─────────────────────────────────────────────

class TestBuildTlcUrl:
    def test_standard_month(self):
        url = build_tlc_url("2024-01")
        assert url == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    def test_url_contains_execution_date(self):
        url = build_tlc_url("2023-12")
        assert "2023-12" in url

    def test_url_is_parquet(self):
        url = build_tlc_url("2024-06")
        assert url.endswith(".parquet")

    def test_url_is_yellow_taxi(self):
        url = build_tlc_url("2024-06")
        assert "yellow_tripdata" in url


# ── validate_raw_dataframe ────────────────────────────────────

class TestValidateRawDataframe:
    def test_valid_dataframe_passes(self, sample_trips_df, execution_date):
        result = validate_raw_dataframe(sample_trips_df, execution_date)
        assert result["valid"] is True
        assert result["issues"] == []
        assert result["row_count"] == 5

    def test_empty_dataframe_fails(self, empty_df, execution_date):
        result = validate_raw_dataframe(empty_df, execution_date)
        assert result["valid"] is False
        assert any("empty" in issue.lower() for issue in result["issues"])

    def test_missing_pickup_column_fails(self, sample_trips_df, execution_date):
        df = sample_trips_df.drop(columns=["tpep_pickup_datetime"])
        result = validate_raw_dataframe(df, execution_date)
        assert result["valid"] is False
        assert any("tpep_pickup_datetime" in issue for issue in result["issues"])

    def test_missing_total_amount_column_fails(self, sample_trips_df, execution_date):
        df = sample_trips_df.drop(columns=["total_amount"])
        result = validate_raw_dataframe(df, execution_date)
        assert result["valid"] is False
        assert any("total_amount" in issue for issue in result["issues"])

    def test_wrong_month_data_warns(self, execution_date):
        """If >20% of rows are from the wrong month, validation should warn."""
        df = pd.DataFrame(
            {
                "tpep_pickup_datetime": pd.to_datetime(
                    ["2023-06-01"] * 9 + ["2024-01-15"]  # 90% wrong month
                ),
                "total_amount": [10.0] * 10,
            }
        )
        result = validate_raw_dataframe(df, execution_date)
        assert result["valid"] is False
        assert any("month" in issue.lower() for issue in result["issues"])

    def test_row_count_reported(self, sample_trips_df, execution_date):
        result = validate_raw_dataframe(sample_trips_df, execution_date)
        assert result["row_count"] == len(sample_trips_df)

    def test_column_count_reported(self, sample_trips_df, execution_date):
        result = validate_raw_dataframe(sample_trips_df, execution_date)
        assert result["column_count"] == len(sample_trips_df.columns)


# ── enrich_dataframe ──────────────────────────────────────────

class TestEnrichDataframe:
    def test_adds_ingested_at_column(self, sample_trips_df, execution_date):
        df = enrich_dataframe(sample_trips_df, execution_date, "test_file.parquet")
        assert "_ingested_at" in df.columns

    def test_adds_source_file_column(self, sample_trips_df, execution_date):
        df = enrich_dataframe(sample_trips_df, execution_date, "yellow/2024-01/file.parquet")
        assert "_source_file" in df.columns
        assert (df["_source_file"] == "yellow/2024-01/file.parquet").all()

    def test_adds_execution_date_column(self, sample_trips_df, execution_date):
        df = enrich_dataframe(sample_trips_df, execution_date, "file.parquet")
        assert "_execution_date" in df.columns
        assert (df["_execution_date"] == execution_date).all()

    def test_does_not_modify_original(self, sample_trips_df, execution_date):
        original_cols = set(sample_trips_df.columns)
        enrich_dataframe(sample_trips_df, execution_date, "file.parquet")
        assert set(sample_trips_df.columns) == original_cols

    def test_row_count_unchanged(self, sample_trips_df, execution_date):
        original_len = len(sample_trips_df)
        df = enrich_dataframe(sample_trips_df, execution_date, "file.parquet")
        assert len(df) == original_len

    def test_all_rows_get_same_execution_date(self, sample_trips_df, execution_date):
        df = enrich_dataframe(sample_trips_df, "2024-03", "file.parquet")
        assert df["_execution_date"].nunique() == 1
        assert df["_execution_date"].iloc[0] == "2024-03"

    def test_ingested_at_is_iso_string(self, sample_trips_df, execution_date):
        df = enrich_dataframe(sample_trips_df, execution_date, "file.parquet")
        # Should be parseable as a datetime
        pd.to_datetime(df["_ingested_at"].iloc[0])  # raises if invalid


# ── Integration-style: validate then enrich pipeline ─────────

class TestValidateThenEnrich:
    def test_valid_df_can_be_enriched(self, sample_trips_df, execution_date):
        validation = validate_raw_dataframe(sample_trips_df, execution_date)
        assert validation["valid"]
        enriched = enrich_dataframe(sample_trips_df, execution_date, "src.parquet")
        assert "_ingested_at" in enriched.columns
        assert len(enriched) == len(sample_trips_df)
