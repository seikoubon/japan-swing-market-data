import pytest

from japan_swing_market.runner import public_evidence


def test_public_evidence_only_emits_approved_fields():
    run = {
        "status": "probed", "end": "2026-11-24", "datasets_checked": 13, "pending_files": 0,
        "preflight_checks": {
            "quality": True, "data_signature_matches": True, "quality_sha_matches": True,
            "derived_stale": False, "incremental_pending": False,
        },
        "planned_files": [{"Key": "deliberately private test key"}],
        "error": "PRIVATE_ERROR_TEXT_SENTINEL",
    }
    assert public_evidence("incremental-probe", run) == {
        "status": "probed", "authenticated": True, "target_date_jst": "2026-11-24",
        "datasets_checked": 13, "previous_snapshot_verified": True,
        "catalog_changes_detected": False, "published_snapshot_verified": False,
    }
    run["preflight_checks"]["quality_sha_matches"] = False
    with pytest.raises(ValueError, match="Snapshot verification"):
        public_evidence("incremental-probe", run)


def test_publication_requires_r2_readback():
    run = {
        "status": "succeeded", "end": "2026-11-24", "datasets_checked": 13, "pending_files": 1,
        "preflight_checks": {"quality": False},
    }
    with pytest.raises(ValueError, match="Snapshot verification"):
        public_evidence("daily", run)
    run["published_snapshot"] = "passed"
    assert public_evidence("daily", run)["published_snapshot_verified"]
