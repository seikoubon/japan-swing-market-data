import csv
import gzip
import io
import json
from datetime import date

import boto3
import pyarrow as pa
import pytest
from moto import mock_aws

from japan_swing_market.catalog import DATASETS, DEFAULT_DATASETS, OPTIONS_225, OPTIONS_225_COLUMNS
from japan_swing_market.daily_update import DailyUpdate, compare_clean, data_signature
from japan_swing_market.lake import Lake, digest, normalize, table, write_parquet
from japan_swing_market.pipeline import Pipeline, days
from japan_swing_market.quality_cache import CACHE_PATH
from japan_swing_market.report import report
from japan_swing_market.research import build
from japan_swing_market.storage import Storage


DAY = "2026-11-13"


def price(code="86970", close=100):
    return {"Date": DAY, "Code": code, "O": 100, "H": 110, "L": 90, "C": close,
            "Vo": 1000, "Va": close * 1000, "AdjFactor": 1}


class Feed:
    def __init__(self):
        self.rows = {
            "prices": [price(), price("72030")],
            "master": [{"Date": DAY, "Code": c, "Mkt": "0111", "ProdCat": "011", "CoName": "fixture"}
                       for c in ("86970", "72030")],
            "indices": [{"Date": DAY, "Code": "0000", "C": 100}],
            "valuation": [{"Date": DAY, "Code": "86970", "MktCap": 100}],
            "short_ratio": [{"Date": DAY, "S33": "0001"}],
            OPTIONS_225: [{**dict.fromkeys(sorted(OPTIONS_225_COLUMNS), ""), "Date": DAY,
                           "Code": "130060018", "EmMrgnTrgDiv": "002", "IV": "20"}],
        }
        self.versions = {name: "v1" for name in self.rows}
        self.downloads = []
        self.api_calls = []

    def payload(self, name):
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=list(self.rows[name][0]))
        writer.writeheader()
        writer.writerows(self.rows[name])
        return gzip.compress(out.getvalue().encode(), mtime=0)

    def pages(self, endpoint, params):
        self.api_calls.append((endpoint, params))
        if endpoint == "/markets/calendar":
            yield {"data": [{"Date": str(d), "HolDiv": "1" if str(d) == DAY else "0"}
                            for d in days(params["from"], params["to"])]}
        elif endpoint == "/bulk/list":
            name = next(n for n, spec in DATASETS.items() if spec.endpoint == params["endpoint"])
            yield {"data": [{"Key": f"{name}_20261113.csv.gz", "LastModified": self.versions[name],
                             "Size": len(self.payload(name))}] if name in self.rows else []}
        elif endpoint == "/equities/bars/daily" and "code" in params:
            yield {"data": [{**r, "AdjC": r["C"]} for r in self.rows["prices"]
                            if r["Code"] == params["code"]]}
        else:
            raise AssertionError(f"Unexpected broad API acquisition {endpoint}: {params}")

    def download(self, key):
        self.downloads.append(key)
        name = key.rsplit("_", 1)[0]
        return self.payload(name)


def seed(tmp_path, s3, feed):
    storage = Storage(tmp_path / "seed", client=s3, bucket="market-test")
    storage.pull(allow_empty=True)
    lake = Lake(storage.root)
    pipe = Pipeline(feed, lake)
    pipe.bulk((*DEFAULT_DATASETS, OPTIONS_225), DAY, DAY, max_files=100)
    lake.state.update({"smoke_passed": "fixture", "planned_scope": {
        "mode": "daily", "start": DAY, "end": DAY, "datasets": list(DEFAULT_DATASETS)}})
    build(lake)
    refs = [{"Code": r["Code"], "Date": date.fromisoformat(DAY), "C": r["C"], "AdjC": r["C"]}
            for r in feed.rows["prices"]]
    write_parquet(storage.root / "references/prices-api.parquet", pa.Table.from_pylist(refs))
    result = report(lake)
    assert result["errors"] == 0 and result["coverage_complete"]
    files = {p.relative_to(storage.root).as_posix(): digest(p.read_bytes())
             for p in storage.root.rglob("*") if p.is_file()}
    lake.state["incremental_update"] = {
        "version": 1,
        "data_signature": data_signature(files),
        "quality_sha": digest((storage.root / "reports/latest.json").read_bytes()),
        "full_integrity_at": "2026-11-13T23:00:00+00:00"}
    lake.save()
    storage.push()
    feed.downloads.clear()
    feed.api_calls.clear()
    return storage


@pytest.fixture
def remote(tmp_path):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="market-test")
        feed = Feed()
        seeded = seed(tmp_path, s3, feed)
        worker = Storage(tmp_path / "worker", client=s3, bucket="market-test")
        yield seeded, worker, feed


def test_no_change_reads_only_metadata_and_never_republishes(remote):
    seeded, worker, feed = remote
    before = worker.s3.get_object(Bucket=worker.bucket, Key=worker.key("latest.json"))["Body"].read()
    result = DailyUpdate(worker, feed, "2026-11-14").run()
    after = worker.s3.get_object(Bucket=worker.bucket, Key=worker.key("latest.json"))["Body"].read()
    assert result["status"] == "unchanged" and before == after
    assert feed.downloads == []
    assert worker.metrics["files"] == 2
    assert not (worker.root / "raw").exists()
    assert not (worker.root / "clean").exists()
    assert len(feed.api_calls) == 13  # One calendar query, twelve small catalogs.


def test_metadata_regeneration_does_not_overwrite_raw_or_observation_times(remote):
    seeded, worker, feed = remote
    before = json.loads(seeded.marker.read_text())["files"]
    feed.versions["prices"] = "v2"
    result = DailyUpdate(worker, feed, "2026-11-14").run()
    assert result["status"] == "unchanged"
    assert feed.downloads == ["prices_20261113.csv.gz"]
    after = json.loads(worker.marker.read_text())["files"]
    assert all(after[p] == h for p, h in before.items() if p.startswith("raw/"))
    assert after["clean/prices/2026-11.parquet"] == before["clean/prices/2026-11.parquet"]
    assert result["content_probe"]["unchanged"]
    assert worker.metrics["files"] == 2
    assert not (worker.root / "clean").exists()
    assert result["validation"] == "reused_verified_snapshot"
    second = Storage(worker.root.parent / "second", client=worker.s3, bucket=worker.bucket)
    assert DailyUpdate(second, feed, "2026-11-14").run()["status"] == "unchanged"


def test_correction_changes_only_its_query_scope_and_retains_old_raw(remote):
    seeded, worker, feed = remote
    before = json.loads(seeded.marker.read_text())["files"]
    feed.rows["prices"][0] = price(close=101)
    feed.versions["prices"] = "v2"
    result = DailyUpdate(worker, feed, "2026-11-14").run()
    assert result["status"] == "succeeded" and result["quality_errors"] == 0
    assert feed.downloads == ["prices_20261113.csv.gz"]
    after = json.loads(worker.marker.read_text())["files"]
    assert after["clean/prices/2026-11.parquet"] != before["clean/prices/2026-11.parquet"]
    assert all(after[p] == h for p, h in before.items() if p.startswith("raw/"))
    restored = Storage(worker.root.parent / "restored", client=worker.s3, bucket=worker.bucket)
    restored.pull()
    assert report(Lake(restored.root))["errors"] == 0


def test_next_daily_run_reuses_published_row_checks(remote):
    _, worker, feed = remote
    feed.rows["prices"][0] = price(close=101)
    feed.versions["prices"] = "v2"
    cold = DailyUpdate(worker, feed, "2026-11-14").run()
    assert cold["quality_profile"]["files_reused"] == 0
    assert CACHE_PATH in json.loads(worker.marker.read_text())["files"]

    next_worker = Storage(worker.root.parent / "next-worker", client=worker.s3, bucket=worker.bucket)
    feed.rows["prices"][0] = price(close=102)
    feed.versions["prices"] = "v3"
    warm = DailyUpdate(next_worker, feed, "2026-11-14").run()
    assert warm["quality_profile"]["files_reused"] > 0
    assert warm["quality_profile"]["files_scanned"] > 0
    saved = json.loads((next_worker.root / "reports/latest.json").read_text())
    checked = report(Lake(next_worker.root))
    del saved["generated_at"], checked["generated_at"]
    assert saved == checked


def test_repacked_identical_csv_reads_only_one_verified_raw_without_publication(remote):
    seeded, worker, feed = remote
    original = worker.s3.get_object(Bucket=worker.bucket, Key=worker.key("latest.json"))["Body"].read()
    payload = feed.payload
    feed.payload = lambda name: (gzip.compress(gzip.decompress(payload(name)), mtime=1)
                                 if name == "prices" else payload(name))
    feed.versions["prices"] = "repacked"
    result = DailyUpdate(worker, feed, "2026-11-14").run()
    assert result["status"] == "unchanged" and result["content_probe"]["unchanged"]
    assert worker.metrics["files"] == 3  # State, quality and one SHA-verified Raw blob.
    assert not (worker.root / "clean").exists()
    assert worker.s3.get_object(Bucket=worker.bucket, Key=worker.key("latest.json"))["Body"].read() == original


def test_options_corrected_bulk_preserves_string_keys_and_reports_iv(remote):
    seeded, worker, feed = remote
    key = f"{OPTIONS_225}_20261113.csv.gz"
    feed.rows[OPTIONS_225].append({**feed.rows[OPTIONS_225][0], "EmMrgnTrgDiv": "001", "IV": ""})
    feed.versions[OPTIONS_225] = "options-correction"
    result = DailyUpdate(worker, feed, "2026-11-14").run()
    assert result["status"] == "succeeded"
    assert feed.downloads == [key]
    quality = result[OPTIONS_225]
    assert quality["rows"] == 2 and quality["sessions"] == quality["expected_sessions"] == 1
    assert not quality["missing_sessions"] and quality["duplicate_keys"] == 0 and quality["iv_missing"] == 1
    path = "clean/options_225/2026-11.parquet"
    assert result["published_parquet_rows"][path] == 2
    assert seeded.marker.exists()
    feed.downloads.clear()
    retry = Storage(worker.root.parent / "options-retry", client=worker.s3, bucket=worker.bucket)
    assert DailyUpdate(retry, feed, "2026-11-14").run()["status"] == "unchanged"
    assert feed.downloads == []


def test_options_pending_after_publication_time_allow_retry_on_next_run(remote):
    _, worker, feed = remote
    pages = feed.pages

    def next_session(endpoint, params):
        for page in pages(endpoint, params):
            if endpoint == "/markets/calendar":
                for row in page["data"]:
                    if row["Date"] == "2026-11-16":
                        row["HolDiv"] = "2"
            yield page

    feed.pages = next_session
    result = DailyUpdate(worker, feed, "2026-11-17").run()
    assert result["status"] == "unchanged"
    assert result[OPTIONS_225]["publication_due_through"] == "2026-11-16"
    assert result[OPTIONS_225]["pending_sessions"] == ["2026-11-16"]
    assert feed.downloads == []


def test_options_reject_identical_duplicate_keys(remote):
    _, worker, feed = remote
    feed.rows[OPTIONS_225].append(dict(feed.rows[OPTIONS_225][0]))
    feed.versions[OPTIONS_225] = "duplicate"
    with pytest.raises(ValueError, match="options_225: 1 quality errors"):
        DailyUpdate(worker, feed, "2026-11-14").run()


def test_shadow_accepts_source_scoped_corrections_but_rejects_changes_elsewhere(tmp_path):
    before, after = tmp_path / "before", tmp_path / "after"
    original = [price(), {**price(), "Date": "2026-11-12"}]
    corrected = [{**price(), "C": 101}, original[1]]
    for root, rows in ((before, original), (after, corrected)):
        write_parquet(root / "prices/2026-11.parquet", table("prices", normalize("prices", rows, "t")))
    with pytest.raises(ValueError, match="Shadow business rows differ: prices"):
        compare_clean(before, after)
    result = compare_clean(before, after, {"prices": [DAY]})
    assert not result["prices"]["equal"] and result["prices"]["untouched_equal"]
    corrected[1] = {**corrected[1], "C": 102}
    write_parquet(after / "prices/2026-11.parquet", table("prices", normalize("prices", corrected, "t")))
    with pytest.raises(ValueError, match="Shadow business rows differ: prices"):
        compare_clean(before, after, {"prices": [DAY]})


def test_initial_publication_compares_untouched_rows_before_writing(remote):
    seeded, worker, feed = remote
    lake = Lake(seeded.root)
    lake.state.pop("incremental_update")
    lake.save()
    seeded.push()
    feed.rows["prices"][0] = price(close=101)
    feed.versions["prices"] = "corrected"
    result = DailyUpdate(worker, feed, "2026-11-14").run()
    assert result["semantic_comparison"]["prices"]["untouched_equal"]
    assert result["published_snapshot"] == "passed"


def test_failed_initial_comparison_never_publishes_a_checkpoint(remote, monkeypatch):
    seeded, worker, feed = remote
    lake = Lake(seeded.root)
    lake.state.pop("incremental_update")
    lake.save()
    seeded.push()
    old = worker.s3.get_object(Bucket=worker.bucket, Key=worker.key("latest.json"))["Body"].read()

    def unexpected_change(lake):
        build(lake)
        rows = [{"Date": DAY, "Code": c, "Mkt": "0111", "ProdCat": "011", "CoName": "wrong"}
                for c in ("86970", "72030")]
        lake.replace("master", normalize("master", rows, "t"), dates={date.fromisoformat(DAY)})

    monkeypatch.setattr("japan_swing_market.daily_update.build", unexpected_change)
    with pytest.raises(ValueError, match="Shadow business rows differ: master"):
        DailyUpdate(worker, feed, "2026-11-14").run()
    assert worker.s3.get_object(Bucket=worker.bucket, Key=worker.key("latest.json"))["Body"].read() == old


def test_failed_source_is_checkpointed_without_marking_success(remote):
    seeded, worker, feed = remote
    original = json.loads(seeded.marker.read_text())["files"]
    feed.rows["prices"][0] = price(close=200)
    feed.versions["prices"] = "bad-correction"
    with pytest.raises(ValueError, match="quality errors"):
        DailyUpdate(worker, feed, "2026-11-14").run()
    after = json.loads(worker.marker.read_text())["files"]
    assert after["clean/prices/2026-11.parquet"] == original["clean/prices/2026-11.parquet"]
    assert json.loads((worker.root / "state.json").read_text())["incremental_pending"] is True
    assert len([p for p in after if p.startswith("raw/")]) > len(
        [p for p in original if p.startswith("raw/")])
    other = Storage(worker.root.parent / "retry", client=worker.s3, bucket=worker.bucket)
    feed.downloads.clear()
    with pytest.raises(ValueError, match="quality errors"):
        DailyUpdate(other, feed, "2026-11-14").run()
    assert feed.downloads == []  # Invalid original was retained and reused.


def test_partial_publication_refuses_non_raw_loss_and_raw_changes(remote):
    seeded, worker, _ = remote
    marker = worker.open_snapshot()
    worker.restore(p for p in marker["files"] if not p.startswith("raw/"))
    path = worker.root / "clean/prices/2026-11.parquet"
    content = path.read_bytes()
    path.unlink()
    with pytest.raises(ValueError, match="missing non-Raw"):
        worker.push()
    path.write_bytes(content)
    raw = next(p for p in marker["files"] if p.startswith("raw/") and p.endswith(".gz"))
    worker.restore_file(raw).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="Immutable Raw"):
        worker.push()


def test_probe_and_shadow_do_not_write_r2(remote):
    seeded, worker, feed = remote
    original = worker.s3.get_object(Bucket=worker.bucket, Key=worker.key("latest.json"))["Body"].read()
    feed.versions["prices"] = "v2"
    assert DailyUpdate(worker, feed, "2026-11-14", mode="probe").run()["status"] == "probed"
    shadow = Storage(worker.root.parent / "shadow", client=worker.s3, bucket=worker.bucket)
    assert DailyUpdate(shadow, feed, "2026-11-14", mode="shadow").run()["status"] == "shadow_passed"
    assert worker.s3.get_object(Bucket=worker.bucket, Key=worker.key("latest.json"))["Body"].read() == original


def test_raw_audit_catches_up_after_missing_its_due_day(remote):
    seeded, worker, feed = remote
    lake = Lake(seeded.root)
    lake.state["incremental_update"]["full_integrity_at"] = "2026-11-01T00:00:00+00:00"
    lake.save()
    seeded.push()
    result = DailyUpdate(worker, feed, "2026-11-14").run()
    assert result["integrity_audit_due"] and result["status"] == "succeeded"
    assert (worker.root / "raw").exists()
    assert feed.downloads == []


@pytest.mark.parametrize("today,status", [("2026-11-14", "unchanged"), ("2026-11-15", "succeeded")])
def test_integrity_interval_uses_japanese_calendar_day(remote, today, status):
    seeded, worker, feed = remote
    lake = Lake(seeded.root)
    # 06:17 JST on Nov 8 is still Nov 7 in UTC; do not audit a day early.
    lake.state["incremental_update"]["full_integrity_at"] = "2026-11-07T21:17:00+00:00"
    lake.save()
    seeded.push()
    assert DailyUpdate(worker, feed, today).run()["status"] == status


def test_unavailable_calendar_is_not_assumed_to_be_a_holiday(remote):
    _, worker, feed = remote
    pages = feed.pages

    def broken(endpoint, params):
        if endpoint == "/markets/calendar":
            yield {"data": []}
        else:
            yield from pages(endpoint, params)

    feed.pages = broken
    with pytest.raises(ValueError, match="Calendar range is incomplete"):
        DailyUpdate(worker, feed, "2026-11-14").run()
