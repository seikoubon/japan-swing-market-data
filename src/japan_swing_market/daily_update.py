"""Metadata-first market updates; keep full Clean validation and defer historical Raw.

All R2 writes use Storage's pinned manifest and optimistic lock. The probe and
shadow modes never publish. No task dispatch or scheduler is created here.
"""

import gzip
import json
import os
import shutil
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import DATASETS, DEFAULT_DATASETS, INCREMENTAL_DATASETS, OPTIONS_225
from .lake import Lake, atomic, canonical, digest, iso, now, write_parquet
from .pipeline import Pipeline, bulk_dates, days
from .report import report
from .research import build, connect, inventory, reconcile_prices


def data_signature(files):
    return digest(canonical({p: h for p, h in files.items()
                             if p.startswith(("clean/", "research/", "quarantine/", "references/"))}))


def calendar_probe(client, start, end):
    rows = []
    for page in client.pages("/markets/calendar", {"from": iso(start), "to": iso(end)}):
        rows.extend(page["data"])
    if {iso(r["Date"]) for r in rows} != {str(d) for d in days(start, end)}:
        raise ValueError("Calendar range is incomplete; do not infer holidays")
    return {iso(r["Date"]): str(r["HolDiv"]) for r in rows}


def refresh_references(client, lake, start, end):
    """Fresh, uniform API adjustment basis; don't mix references from different runs."""
    con = connect(lake)
    actions = con.execute("""SELECT DISTINCT Code FROM prices
        WHERE AdjFactor<>1 AND Date BETWEEN ? AND ? ORDER BY Code""", [start, end]).fetchall()
    codes = sorted({"86970", "72030"} | {r[0] for r in actions})
    con.close()
    if len(codes) > 100:
        raise ValueError("More than 100 corporate-action codes require bounded manual review")
    rows = []
    for security in codes:
        for page in client.pages("/equities/bars/daily", {"code": security, "from": start, "to": end}):
            lake.raw("validation_prices", canonical(page), page["data"], now())
            for row in page["data"]:
                if row.get("AdjC") is not None and row.get("C") is not None:
                    rows.append({"Code": str(row["Code"]), "Date": date.fromisoformat(iso(row["Date"])),
                                 "C": float(row["C"]), "AdjC": float(row["AdjC"])})
    if not rows:
        raise ValueError("API adjustment references empty")
    missing = set(codes) - {r["Code"] for r in rows}
    if missing:
        raise ValueError("Corporate-action API reference unavailable")
    path = lake.root / "references/prices-api.parquet"
    write_parquet(path, pa.Table.from_pylist(rows))
    return path


class DailyUpdate:
    def __init__(self, storage, client, today, *, end=None, maximum=500, mode="publish", jst_hour=4,
                 require_established=False):
        if mode not in {"publish", "shadow", "probe"}:
            raise ValueError("Invalid incremental update mode")
        self.storage, self.client = storage, client
        self.root, self.today = storage.root, date.fromisoformat(iso(today))
        self.end = iso(end) if end is not None else str(self.today - timedelta(days=1))
        self.maximum, self.mode = maximum, mode
        self.require_established = require_established
        self.options_due = str(self.today - timedelta(days=1 if jst_hour >= 3 else 2))
        self.result = {"started_at": now(), "mode": mode, "status": "running", "timings": {}}
        self.materialized = False
        self.baseline = None
        self.publishing = False
        self.first_migration = False

    def record(self):
        atomic(self.root / "reports/incremental-update.json", canonical(self.result))
        atomic(self.root / "last-run.json", canonical({"at": now(), "mode": "incremental",
               "status": self.result["status"], "exit_code": 0 if self.result["status"] in
               {"succeeded", "unchanged", "probed", "shadow_passed"} else 1}))
        print(json.dumps(self.result, ensure_ascii=False), flush=True)

    def run(self):
        started = time.monotonic()
        try:
            self.baseline = self.storage.open_snapshot()
            required = {"state.json", "reports/latest.json"}
            if not required <= set(self.baseline["files"]):
                raise ValueError("Existing verified market snapshot required")
            self.storage.restore(sorted(required))
            lake = Lake(self.root)
            if not lake.state.get("smoke_passed"):
                raise ValueError("Existing smoke acceptance required")
            previous_report = json.loads((self.root / "reports/latest.json").read_text())
            if self.require_established and not all(
                previous_report["inventory"].get(name, {}).get("rows", 0) > 0
                for name in INCREMENTAL_DATASETS
            ):
                raise ValueError("Existing verified market inventory required")
            end = self.end
            start = str(self.today.replace(year=self.today.year - 10, day=min(self.today.day, 28)))
            previous = lake.state.get("incremental_update", {})
            self.first_migration = not previous.get("version")
            latest = previous_report.get("inventory", {}).get("prices", {}).get("end")
            if not latest or latest == "None" or latest > end:
                raise ValueError("Previous price inventory unavailable or ahead of target")
            options_latest = previous_report["inventory"].get(OPTIONS_225, {}).get("end")
            calendar_start = min(latest, options_latest) if options_latest not in (None, "None") else latest
            recent_calendar = calendar_probe(self.client, calendar_start, end)
            business = sorted(d for d, div in recent_calendar.items() if d >= latest and div == "1")
            expected = business[-1] if business else latest
            pipe = Pipeline(self.client, lake)
            catalog = pipe.bulk_catalog(DEFAULT_DATASETS, start, end)
            options_catalog = pipe.bulk_catalog((OPTIONS_225,), None, None)[OPTIONS_225]
            options_files = []
            for item in options_catalog:
                scope = bulk_dates(item["Key"])
                if scope is None:
                    raise ValueError("Unknown options bulk date format")
                first, last = str(min(scope)), str(max(scope))
                if first <= end:
                    options_files.append((first, min(last, end), item))
            if not options_files:
                raise ValueError("No options bulk files for target")
            options_start = min(first for first, _, _ in options_files)
            listed_through = max(last for _, last, _ in options_files)
            catalog[OPTIONS_225] = [item for _, _, item in options_files]
            starts = {OPTIONS_225: options_start}
            pending = pipe.bulk_pending(catalog, start, end, starts=starts)
            self.result.update({"start": start, "end": end, "expected_session": expected,
                                "catalog_files": sum(map(len, catalog.values())),
                                "datasets_checked": len(catalog) + 1,
                                "pending_files": len(pending), "planned_files": pending,
                                OPTIONS_225: {
                                    "catalog_start": options_start, "listed_through": listed_through,
                                    "publication_due_through": min(self.options_due, end),
                                    "pending_sessions": sorted(d for d, div in recent_calendar.items()
                                        if div in {"1", "2"} and listed_through < d <= end),
                                }})
            self.result["timings"]["preflight"] = time.monotonic() - started
            fresh = all(previous_report.get("inventory", {}).get(n, {}).get("end", "") >= expected
                        for n in ("master", "prices", "indices", "valuation", "short_ratio"))
            quality = (previous_report.get("errors") == 0
                       and previous_report.get("coverage_complete") is True)
            verified = (previous.get("data_signature") == data_signature(self.baseline["files"])
                        and previous.get("quality_sha") == digest((self.root / "reports/latest.json").read_bytes())
                        and not lake.state.get("derived_stale") and not lake.state.get("incremental_pending"))
            audit_date = previous.get("full_integrity_at")
            audit_due = (not audit_date or
                         (self.today - datetime.fromisoformat(audit_date).astimezone(
                             ZoneInfo("Asia/Tokyo")).date()).days >= 7)
            self.result["integrity_audit_due"] = audit_due
            self.result["preflight_checks"] = {
                "fresh": fresh, "quality": quality,
                "data_signature_matches": previous.get("data_signature") == data_signature(
                    self.baseline["files"]),
                "quality_sha_matches": previous.get("quality_sha") == digest(
                    (self.root / "reports/latest.json").read_bytes()),
                "derived_stale": bool(lake.state.get("derived_stale")),
                "incremental_pending": bool(lake.state.get("incremental_pending")),
            }
            print(json.dumps({"event": "incremental_preflight", "pending_files": pending,
                              "audit_due": audit_due, **self.result["preflight_checks"]}), flush=True)
            prefetched, content_unchanged = {}, False
            # A regenerated archive can have a new timestamp but identical source bytes.
            # Check small, already-applied candidates before restoring gigabytes of Clean.
            if (pending and len(pending) <= self.maximum and fresh and quality and verified
                    and not audit_due and self.mode != "shadow"
                    and sum(p["Size"] for p in pending) <= 10_000_000):
                candidates = []
                for info in pending:
                    state_key = digest(canonical({"dataset": info["dataset"], "key": info["Key"]}))
                    prior = lake.state["bulk"].get(state_key)
                    scope = bulk_dates(info["Key"])
                    selected = sorted(str(d) for d in (scope or ()) if starts.get(info["dataset"], start) <= str(d) <= end)
                    if (not prior or prior["raw"] not in self.baseline["files"] or not selected
                            or prior["start"] > selected[0] or prior["end"] < selected[-1]):
                        break
                    candidates.append((info, prior))
                if len(candidates) == len(pending):
                    stage = time.monotonic()
                    content_unchanged = True
                    for info, prior in candidates:
                        payload = self.client.download(info["Key"])
                        prefetched[(info["dataset"], info["Key"])] = payload
                        same = (len(payload) == info["Size"] and digest(payload)
                                == (self.root / prior["raw"]).name.split(".")[0])
                        if same:
                            same = lake.raw(info["dataset"], payload, [], prior["observed_at"],
                                            kind="csv-gzip") == prior["raw"]
                        elif len(payload) == info["Size"]:
                            try:
                                csv_bytes = gzip.decompress(payload)
                                saved = self.storage.restore_file(prior["raw"]).read_bytes()
                                same = digest(csv_bytes) == digest(gzip.decompress(gzip.decompress(saved)))
                            except (OSError, EOFError):
                                # Let normal acquisition retain and reject invalid source bytes.
                                same = False
                        content_unchanged = content_unchanged and same
                    self.result["content_probe"] = {"files": len(prefetched),
                        "bytes": sum(map(len, prefetched.values())), "unchanged": content_unchanged}
                    self.result["timings"]["content_probe"] = time.monotonic() - stage
                    print(json.dumps({"event": "incremental_content_probe",
                                      **self.result["content_probe"]}), flush=True)
            if self.mode == "probe":
                self.result["status"] = "probed"
                return self.result
            if (not pending or content_unchanged) and fresh and quality and verified and not audit_due:
                if saved_options := previous_report.get(OPTIONS_225):
                    summary = {k: v for k, v in saved_options.items() if k != "pending_sessions"}
                    self.result[OPTIONS_225].update(summary)
                self.result.update({"status": "unchanged", "validation": "reused_verified_snapshot",
                                    "last_full_quality_at": previous_report["generated_at"]})
                return self.result

            stage = time.monotonic()
            self.storage.restore(p for p in self.baseline["files"]
                                 if audit_due or not p.startswith("raw/"))
            self.materialized = True
            self.result["timings"]["restore"] = time.monotonic() - stage
            stage = time.monotonic()
            before = inventory(lake)
            self.result["timings"]["before_inventory"] = time.monotonic() - stage
            baseline_clean = self.root.parent / (self.root.name + "-shadow-baseline")
            compare_before_publication = (self.mode == "shadow" or self.first_migration
                                          or options_latest in (None, "None"))
            if compare_before_publication:
                for path in (self.root / "clean").rglob("*.parquet"):
                    target = baseline_clean / path.relative_to(self.root / "clean")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.unlink(missing_ok=True)
                    try:
                        os.link(path, target)
                    except OSError:
                        shutil.copyfile(path, target)
            atomic(self.root / ".daily-baseline.json", canonical(self.baseline))
            # The preflight lake was read-only; construct it again after full working restore.
            lake = Lake(self.root)
            pipe = Pipeline(self.client, lake)
            lake.state["planned_scope"] = {"mode": "incremental", "start": start, "end": end,
                                            "datasets": list(INCREMENTAL_DATASETS),
                                            OPTIONS_225: {"start": options_start, "end": end,
                                                          "listed_through": listed_through}}
            lake.save()
            stage = time.monotonic()
            for name, files in catalog.items():
                atomic(self.root / f"reports/catalog-{name}.json", canonical(files))
            outcome = pipe.bulk(INCREMENTAL_DATASETS, start, end, max_files=self.maximum,
                                catalog=catalog, restore_raw=self.storage.restore_file,
                                payloads=prefetched, starts=starts)
            self.result.update(outcome)
            # Monthly bulk data can start before the rolling API entitlement date.
            # Use the official full calendar CSV for just that older edge.
            first_calendar_day = before["calendar"]["start"]
            if options_start < start and first_calendar_day in ("None", None):
                first_calendar_day = start
            if options_start < start and options_start < first_calendar_day:
                calendar_catalog = pipe.bulk_catalog(("calendar",), None, None)
                previous_day = str(date.fromisoformat(start) - timedelta(days=1))
                pipe.bulk(("calendar",), options_start, previous_day, max_files=1,
                          catalog=calendar_catalog, restore_raw=self.storage.restore_file)
            if outcome["more_pending"]:
                raise ValueError("Bulk update incomplete; pending work retained for the next run")
            # CSV arrival isn't proof of daily completeness. Patch only missing sessions.
            con = connect(lake)
            price_days = []
            applied_scopes = outcome["applied_scopes"]
            try:
                for name in ("master", "prices", "indices", "valuation", "short_ratio"):
                    found = {str(r[0]) for r in con.execute(
                        f'SELECT DISTINCT Date FROM "{name}" WHERE Date BETWEEN ? AND ?',
                        [latest, end]).fetchall()}
                    for day in sorted(set(business) - found):
                        pipe.fetch(name, {"date": day}, refresh=True, dates={day})
                        applied_scopes[name] = sorted(set(applied_scopes.get(name, [])) | {day})
                        if name == "prices":
                            price_days.append(date.fromisoformat(day))
            finally:
                con.close()
            self.result["timings"]["acquire"] = time.monotonic() - stage
            price_work = [p for p in pending if p["dataset"] == "prices"]
            if price_work or price_days or not (self.root / "references/prices-api.parquet").exists():
                scopes = [bulk_dates(p["Key"]) for p in price_work]
                dates = price_days + [d for scope in scopes if scope for d in scope if start <= str(d) <= end]
                reference_start = str(min(dates) - timedelta(days=7)) if dates else str(
                    date.fromisoformat(latest) - timedelta(days=35))
                refresh_references(self.client, lake, max(start, reference_start), end)
            stage = time.monotonic()
            build(lake)
            self.result["timings"]["build"] = time.monotonic() - stage
            adjustment_started = time.monotonic()
            reconciliation = reconcile_prices(lake, self.root / "references/prices-api.parquet")
            if reconciliation.get("checked", 0) == 0 or reconciliation.get("mismatches"):
                raise ValueError("Adjustment reconciliation must have checked references")
            self.result["timings"]["reconcile_prices"] = time.monotonic() - adjustment_started
            profile = self.result["quality_profile"] = {}
            quality_result = report(lake, use_row_cache=True, timings=profile)
            if quality_result["errors"] or not quality_result.get("coverage_complete") or (
                quality_result.get("freshness", {}).get("delayed")
            ):
                raise ValueError("Market quality, coverage or freshness validation failed")
            self.result[OPTIONS_225].update(quality_result[OPTIONS_225])
            self.result.update({"reconciliation": reconciliation, "before_inventory": before,
                                "inventory": quality_result["inventory"],
                                "quality_errors": quality_result["errors"]})
            self.result["timings"]["build_validate"] = time.monotonic() - stage
            if compare_before_publication:
                self.result["semantic_comparison"] = compare_clean(
                    baseline_clean, self.root / "clean", applied_scopes)
            files = {p.relative_to(self.root).as_posix(): digest(p.read_bytes())
                     for base in ("clean", "research", "quarantine", "references")
                     for p in (self.root / base).rglob("*") if p.is_file()
                     and not p.name.startswith(".") and not p.name.endswith((".tmp", ".lock"))}
            lake.state["incremental_update"] = {
                "version": 1, "completed_at": now(), "end": end,
                "data_signature": data_signature(files),
                "quality_sha": digest((self.root / "reports/latest.json").read_bytes()),
                "full_integrity_at": now() if audit_due else audit_date,
            }
            lake.state.pop("incremental_pending", None)
            lake.save()
            if self.mode == "shadow":
                self.result["status"] = "shadow_passed"
            else:
                stage = time.monotonic()
                self.result["status"] = "succeeded"
                self.record()
                self.publishing = True
                self.storage.push()
                self.result["push"] = dict(self.storage.metrics)
                self.verify_publication()
                self.result["timings"]["publish"] = time.monotonic() - stage
            return self.result
        except Exception as exc:
            self.result.update({"status": "failed", "error_type": type(exc).__name__,
                                "error": str(exc) if isinstance(exc, (ValueError, RuntimeError)) else
                                "See sanitized acquisition logs"})
            # Retain validated per-file progress and failed Raw, like the legacy workflow.
            # Rebuild first so an old Research table can never masquerade as current.
            if (self.materialized and self.mode == "publish" and not self.publishing
                    and not self.first_migration):
                try:
                    checkpoint_lake = Lake(self.root)
                    build(checkpoint_lake)
                    checkpoint_report = report(checkpoint_lake)
                    if checkpoint_report["errors"]:
                        raise ValueError("Checkpoint has clean quality errors")
                    checkpoint_lake.state["incremental_pending"] = True
                    checkpoint_lake.save()
                    self.record()
                    self.storage.push()
                    self.result["failed_checkpoint"] = "persisted"
                except Exception as checkpoint_error:
                    self.result["failed_checkpoint"] = type(checkpoint_error).__name__
            raise
        finally:
            self.result["total_seconds"] = time.monotonic() - started
            self.result["download"] = getattr(self.storage, "download_metrics", None) or (
                self.storage.metrics if self.storage.metrics.get("operation") == "pull" else
                self.result.get("download", {}))
            self.result["manifest_bytes"] = getattr(self.storage, "manifest_bytes", 0)
            self.record()

    def verify_publication(self):
        response = self.storage.s3.get_object(Bucket=self.storage.bucket, Key=self.storage.key("latest.json"))
        remote = json.loads(response["Body"].read())
        marker = json.loads(self.storage.marker.read_text())
        if remote["files"] != marker["files"] or response["ETag"] != marker["etag"]:
            raise ValueError("Published snapshot changed")
        if any(remote["files"].get(p) != sha for p, sha in self.baseline["files"].items()
               if p.startswith("raw/")):
            raise ValueError("Previous Raw snapshot changed")
        prices = max(p for p in remote["files"] if p.startswith("clean/prices/") and p.endswith(".parquet"))
        options = sorted(p for p in remote["files"] if p.startswith(f"clean/{OPTIONS_225}/")
                         and p.endswith(".parquet"))
        if not options:
            raise ValueError("Options Clean files missing from published manifest")
        checked, transferred = {}, 0
        for path in dict.fromkeys((prices, options[0], options[-1])):
            body = self.storage.s3.get_object(
                Bucket=self.storage.bucket, Key=self.storage.key("blobs/" + remote["files"][path])
            )["Body"].read()
            if digest(body) != remote["files"][path]:
                raise ValueError(f"Published checksum mismatch: {path}")
            rows = pq.read_table(pa.BufferReader(body)).num_rows
            if not rows:
                raise ValueError(f"Published Parquet is empty: {path}")
            checked[path] = rows
            transferred += len(body)
        self.result["published_parquet_rows"] = checked
        self.result["publication_verification_bytes"] = transferred
        self.result["published_snapshot"] = "passed"


def compare_clean(before, after, applied_scopes=None):
    """Require unchanged business rows outside the source scopes actually replaced."""
    import duckdb

    output = {}
    con = duckdb.connect(config={"threads": 4, "memory_limit": "4GB"})
    try:
        for name in INCREMENTAL_DATASETS:
            paths = list((before / name).glob("*.parquet")) + list((after / name).glob("*.parquet"))
            if not paths:
                continue
            con.execute("CREATE OR REPLACE VIEW compared AS SELECT * FROM read_parquet("
                        + "[" + ",".join("'" + str(p).replace("'", "''") + "'" for p in paths)
                        + "],union_by_name=true,filename=true)")
            omit = {"_row_hash", "_observed_at", "filename"}
            if name == "prices":
                omit.update({"AdjO", "AdjH", "AdjL", "AdjC", "AdjVo"})
            fields = sorted(r[0] for r in con.execute("DESCRIBE compared").fetchall() if r[0] not in omit)
            quoted = ",".join('"' + f.replace('"', '""') + '"' for f in fields)
            rows = con.execute("SELECT starts_with(filename,?),count(*),bit_xor(hash(" + quoted
                               + ")) FROM compared GROUP BY 1", [str(before)]).fetchall()
            versions = {r[0]: (r[1], r[2]) for r in rows}
            match = versions.get(True, (0, 0)) == versions.get(False, (0, 0))
            scopes = sorted((applied_scopes or {}).get(name, []))
            untouched_match = match
            if scopes:
                placeholders = ",".join("?" for _ in scopes)
                rows = con.execute("SELECT starts_with(filename,?),count(*),bit_xor(hash(" + quoted
                                   + f')) FROM compared WHERE "{DATASETS[name].date}" NOT IN ('
                                   + placeholders + ") GROUP BY 1", [str(before), *scopes]).fetchall()
                untouched = {r[0]: (r[1], r[2]) for r in rows}
                untouched_match = untouched.get(True, (0, 0)) == untouched.get(False, (0, 0))
            output[name] = {"equal": match, "untouched_equal": untouched_match,
                            "applied_dates": scopes, "before": versions.get(True),
                            "after": versions.get(False)}
        differences = [n for n, item in output.items() if not item["untouched_equal"] and n != "calendar"]
        if differences:
            raise ValueError("Shadow business rows differ: " + ",".join(differences))
    finally:
        con.close()
    return output

