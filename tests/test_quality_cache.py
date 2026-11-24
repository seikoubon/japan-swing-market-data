from datetime import date
from importlib import import_module

from japan_swing_market.lake import Lake, normalize, table, write_parquet
from japan_swing_market.quality_cache import CACHE_PATH, summarize_clean_rows

cache = import_module("japan_swing_market.quality_cache")


def put(lake, name, session, **data):
    rows = normalize(name, [{"Date": session, "Code": "12340", **data}], f"{session}T12:00:00Z")
    path = lake.root / "clean" / name / f"{session[:7]}.parquet"
    write_parquet(path, table(name, rows))


def scan(lake, day, use_cache=True):
    stats = {}
    results = summarize_clean_rows(lake, date.fromisoformat(day), stats, use_cache=use_cache)
    return results, stats["files_reused"], stats["files_scanned"]


def test_reuse_warnings_and_recheck_changed_and_new_months(tmp_path):
    lake = Lake(tmp_path)
    put(lake, "prices", "2026-09-01", O=100, C=100, H=110, L=90, Vo=100, Va=10000, AdjFactor=2)
    put(lake, "master", "2026-10-01")
    cold, reused, scanned = scan(lake, "2026-10-01")
    assert (reused, scanned, cold["warnings"]) == (0, 2, 1)

    put(lake, "master", "2026-10-01", CoName="changed")
    put(lake, "master", "2026-11-01")
    full, _, _ = scan(lake, "2026-11-01", use_cache=False)
    warm, reused, scanned = scan(lake, "2026-11-01")
    assert (reused, scanned) == (1, 2)
    assert warm == full
    assert warm["issues"][0]["check"] == "unknown_corporate_action_type"


def test_date_rollback_and_future_rows_are_checked_again(tmp_path):
    lake = Lake(tmp_path)
    put(lake, "master", "2026-11-01")
    scan(lake, "2026-11-02")
    previous_day, reused, scanned = scan(lake, "2026-10-31")
    assert (reused, scanned, previous_day["errors"]) == (0, 1, 1)
    assert previous_day["issues"][0]["check"] == "future_effective_date"
    current, reused, scanned = scan(lake, "2026-11-01")
    assert (reused, scanned, current["errors"]) == (0, 1, 0)


def test_wrong_rule_version_or_damaged_cache_falls_back(tmp_path, monkeypatch):
    lake = Lake(tmp_path)
    put(lake, "master", "2026-11-01")
    scan(lake, "2026-11-01")
    monkeypatch.setattr(cache, "rules_signature", lambda: "new-rules")
    assert scan(lake, "2026-11-01")[1:] == (0, 1)
    assert scan(lake, "2026-11-01")[1:] == (1, 0)
    (tmp_path / CACHE_PATH).write_text('{"broken": true}')
    assert scan(lake, "2026-11-01")[1:] == (0, 1)
