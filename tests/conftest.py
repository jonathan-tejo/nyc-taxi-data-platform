"""Shared fixtures for all test modules."""

import pandas as pd
import pytest


@pytest.fixture
def sample_trips_df() -> pd.DataFrame:
    """Minimal valid TLC-like DataFrame for testing."""
    return pd.DataFrame(
        {
            "VendorID": [1, 2, 1, 2, 1],
            "tpep_pickup_datetime": pd.to_datetime(
                [
                    "2024-01-15 08:00:00",
                    "2024-01-15 09:30:00",
                    "2024-01-16 14:00:00",
                    "2024-01-20 22:00:00",
                    "2024-01-31 23:59:00",
                ]
            ),
            "tpep_dropoff_datetime": pd.to_datetime(
                [
                    "2024-01-15 08:25:00",
                    "2024-01-15 09:55:00",
                    "2024-01-16 14:40:00",
                    "2024-01-20 22:30:00",
                    "2024-02-01 00:20:00",  # crosses month boundary
                ]
            ),
            "passenger_count": [1.0, 2.0, 1.0, 3.0, 1.0],
            "trip_distance": [2.5, 5.1, 8.3, 3.0, 1.1],
            "RatecodeID": [1.0, 1.0, 2.0, 1.0, 1.0],
            "store_and_fwd_flag": ["N", "N", "Y", "N", "N"],
            "PULocationID": [161, 237, 132, 79, 43],
            "DOLocationID": [236, 142, 138, 107, 90],
            "payment_type": [1, 2, 1, 1, 2],
            "fare_amount": [12.0, 18.5, 52.0, 14.0, 8.0],
            "extra": [0.5, 0.0, 0.5, 0.5, 0.5],
            "mta_tax": [0.5, 0.5, 0.5, 0.5, 0.5],
            "tip_amount": [3.0, 0.0, 10.0, 4.0, 0.0],
            "tolls_amount": [0.0, 0.0, 6.55, 0.0, 0.0],
            "improvement_surcharge": [1.0, 1.0, 1.0, 1.0, 1.0],
            "total_amount": [17.0, 20.0, 70.55, 20.0, 10.0],
            "congestion_surcharge": [2.5, 0.0, 0.0, 2.5, 0.0],
            "airport_fee": [0.0, 0.0, 1.25, 0.0, 0.0],
        }
    )


@pytest.fixture
def empty_df() -> pd.DataFrame:
    return pd.DataFrame()


@pytest.fixture
def execution_date() -> str:
    return "2024-01"
