"""Shared pytest fixtures."""
from __future__ import annotations

import pytest

from src.silver.entity_resolution import ProfileIndex


@pytest.fixture
def sample_profiles() -> list[dict]:
    return [
        {
            "internal_uuid": "u1", "emirates_id": "784-1990-1234567-1",
            "first_name": "Ahmed", "last_name": "Al Mansoori",
            "full_name": "Ahmed Al Mansoori", "dob": "1990-01-15",
            "phone": "+971501234567", "email": "ahmed@example.ae",
            "emirate": "Dubai", "kyc_verified_at": "2025-01-01T00:00:00+00:00",
            "ingest_run_id": "run_test",
            "is_new_user": False, "had_overdue_before": False,
        },
        {
            "internal_uuid": "u2", "emirates_id": "784-1988-7654321-2",
            "first_name": "Fatima", "last_name": "Al Nahyan",
            "full_name": "Fatima Al Nahyan", "dob": "1988-05-20",
            "phone": "+971509876543", "email": "fatima@example.ae",
            "emirate": "Abu Dhabi", "kyc_verified_at": "2025-02-01T00:00:00+00:00",
            "ingest_run_id": "run_test",
            "is_new_user": True, "had_overdue_before": False,
        },
    ]


@pytest.fixture
def profile_index(sample_profiles) -> ProfileIndex:
    return ProfileIndex(sample_profiles)
