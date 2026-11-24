import calendar
import csv
import gzip
import io
import re
import time
from datetime import date, timedelta

from .catalog import DATASETS, OPTIONS_225, OPTIONS_225_COLUMNS
from .lake import canonical, code, digest, iso, normalize, now
from .quality import assert_valid, quarantine_financial_periods


def days(start, end):
    current = date.fromisoformat(iso(start))
    end = date.fromisoformat(iso(end))
    while current <= end:
        yield current
        current += timedelta(days=1)


def bulk_dates(key):
    match = re.search(r"_(\d{8}|\d{6})\.csv\.gz$", key)
    if not match:
        return None
    text = match[1]
    if len(text) == 8:
        return {date.fromisoformat(iso(text))}
    year, month = int(text[:4]), int(text[4:])
    return set(
        days(
            date(year, month, 1).isoformat(),
            date(year, month, calendar.monthrange(year, month)[1]).isoformat(),
        )
    )


class Pipeline:
    def __init__(self, client, lake):
        self.client, self.lake = client, lake

    def fetch(self, name, params, *, refresh=False, dates=None, codes=None):
        try:
            return self._fetch(name, params, refresh=refresh, dates=dates, codes=codes)
        except Exception as exc:
            self.lake.log(
                {
                    "event": "fetch_failed",
                    "dataset": name,
                    "params": params,
                    "error_type": type(exc).__name__,
                    "status": "failed",
                }
            )
            raise

    def _fetch(self, name, params, *, refresh=False, dates=None, codes=None):
        spec = DATASETS[name]
        key = digest(canonical({"dataset": name, "params": params}))
        previous = self.lake.state["requests"].get(key)
        if previous and not refresh:
            return previous["count"]
        observed, raw_paths, records = now(), [], []
        # Commit/checkpoint only after the final page and quality validation.
        for page in self.client.pages(spec.endpoint, params):
            raw_paths.append(self.lake.raw(name, canonical(page), page["data"], observed))
            records.extend(page["data"])
        normalized = normalize(name, records, observed)
        if name == "calendar":
            requested = set(days(params["from"], params["to"]))
            found = {r["Date"] for r in normalized}
            if found != requested:
                raise ValueError("Calendar range is incomplete; do not infer holidays")
        if spec.required_daily and not records:
            raise ValueError(f"{name}: required dataset empty; update may be delayed")
        if dates is not None and any(r[spec.date] not in set(dates) for r in normalized):
            raise ValueError(f"{name}: response outside requested scope")
        if codes is not None and any(r.get("Code") not in codes for r in normalized):
            raise ValueError(f"{name}: response outside requested security scope")
        normalized = quarantine_financial_periods(name, normalized, self.lake, key)
        assert_valid(name, normalized, self.lake)
        self.lake.state["derived_stale"] = True
        self.lake.replace(name, normalized, dates=dates, codes=codes)
        entry = {
            "dataset": name,
            "params": params,
            "count": len(records),
            "observed_at": observed,
            "raw": raw_paths,
            "status": "complete",
        }
        self.lake.state["requests"][key] = entry
        self.lake.save()
        self.lake.log({"event": "fetch_complete", **entry})
        return len(records)

    def calendar(self, start, end, refresh=False):
        self.fetch("calendar", {"from": iso(start), "to": iso(end)}, refresh=refresh)
        import pyarrow.parquet as pq

        selected = []
        for path in self.lake.files("calendar"):
            selected.extend(
                r for r in pq.read_table(path).to_pylist() if iso(start) <= str(r["Date"]) <= iso(end)
            )
        return sorted(r["Date"] for r in selected if r["HolDiv"] == "1")

    def api_range(self, names, start, end, refresh=False):
        business_days = self.calendar(start, end, refresh=refresh)
        for name in names:
            if name == "calendar":
                continue
            # Financial statements / publication datasets can have non-business-day disclosures.
            target_days = (
                business_days
                if name in {"master", "prices", "indices", "valuation", "short_ratio"}
                else list(days(start, end))
            )
            if name == "investor_types":
                self.fetch(
                    name, {"from": iso(start), "to": iso(end)}, refresh=refresh, dates=set(days(start, end))
                )
                continue
            for day in target_days:
                self.fetch(name, {DATASETS[name].query_date: str(day)}, refresh=refresh, dates={day})

    def bulk_catalog(self, names, start, end):
        catalog = {}
        for name in names:
            if name == "calendar" and start is not None:
                continue
            files = []
            query = {"endpoint": DATASETS[name].endpoint}
            if start is not None:
                query.update({"from": iso(start), "to": iso(end)})
            for page in self.client.pages("/bulk/list", query):
                files.extend(page["data"])
            if not files and DATASETS[name].required_daily:
                raise ValueError(f"No bulk files for {name}")
            keys = [f["Key"] for f in files]
            if len(keys) != len(set(keys)):
                raise ValueError(f"Duplicate bulk catalog keys for {name}")
            catalog[name] = files
        return catalog

    def bulk_pending(self, catalog, start, end, *, starts=None):
        starts = starts or {}
        pending = []
        for name, files in catalog.items():
            first_day = starts.get(name, start)
            for info in files:
                key = digest(canonical({"dataset": name, "key": info["Key"]}))
                previous = self.lake.state["bulk"].get(key)
                scope = bulk_dates(info["Key"])
                selected = {d for d in scope if iso(first_day) <= str(d) <= iso(end)} if scope else None
                first = str(min(selected)) if selected else iso(first_day)
                last = str(max(selected)) if selected else iso(end)
                fingerprint = {k: info[k] for k in ("Key", "LastModified", "Size")}
                if not (previous and previous["file"] == fingerprint
                        and previous["start"] <= first and previous["end"] >= last):
                    pending.append({"dataset": name, **fingerprint})
        return pending

    def bulk(self, names, start, end, max_files=10, deadline=None, *, catalog=None,
             restore_raw=None, payloads=None, starts=None):
        """Resume unchanged files using Key + LastModified + Size; refresh changed archives."""
        starts = starts or {}
        count = 0
        applied_scopes = {}
        for name in names:
            if name == "calendar" and (catalog is None or name not in catalog):
                self.calendar(start, end, refresh=True)
                continue
            first_day = starts.get(name, start)
            spec = DATASETS[name]
            touched = set()
            files = (catalog if catalog is not None else self.bulk_catalog([name], start, end))[name]
            if not files and spec.required_daily:
                raise ValueError(f"No bulk files for {name}")
            # Older snapshots first; newer generated daily files win if scopes overlap.
            for info in sorted(files, key=lambda f: (f["LastModified"], f["Key"])):
                key = info["Key"]
                state_key = digest(canonical({"dataset": name, "key": key}))
                fingerprint = {k: info[k] for k in ("Key", "LastModified", "Size")}
                previous = self.lake.state["bulk"].get(state_key)
                raw_previous = self.lake.state.get("bulk_raw", {}).get(state_key) or previous
                file_scope = bulk_dates(key)
                requested_scope = (
                    {d for d in file_scope if iso(first_day) <= str(d) <= iso(end)} if file_scope else None
                )
                scope_start = str(min(requested_scope)) if requested_scope else iso(first_day)
                scope_end = str(max(requested_scope)) if requested_scope else iso(end)
                raw_known = bool(
                    raw_previous
                    and raw_previous["file"] == fingerprint
                )
                cached = bool(
                    previous
                    and previous["file"] == fingerprint
                    and previous["start"] <= scope_start
                    and previous["end"] >= scope_end
                )
                if cached and not (requested_scope and requested_scope & touched):
                    continue
                if not cached and (count >= max_files or (deadline and time.monotonic() >= deadline)):
                    return {"processed_files": count, "more_pending": True,
                            "applied_scopes": applied_scopes}
                if raw_known and restore_raw:
                    restore_raw(raw_previous["raw"])
                raw_cached = bool(raw_known and (self.lake.root / raw_previous["raw"]).exists())
                observed = raw_previous["observed_at"] if raw_cached else now()
                self.lake.log({"event": "bulk_started", "dataset": name, "key": key, "cached": cached})
                payload = (
                    gzip.decompress((self.lake.root / raw_previous["raw"]).read_bytes())
                    if raw_cached
                    else payloads[(name, key)] if payloads and (name, key) in payloads
                    else self.client.download(key)
                )
                # Preserve bytes before validation; malformed CSV remains available for inspection.
                raw_ref = self.lake.raw(name, payload, [], observed, kind="csv-gzip")
                self.lake.state.setdefault("bulk_raw", {})[state_key] = {
                    "file": fingerprint,
                    "raw": raw_ref,
                    "observed_at": observed,
                    "dataset": name,
                }
                self.lake.save()
                if len(payload) != info["Size"]:
                    raise ValueError(f"Bulk size mismatch for {name}")
                if (previous and previous["raw"] == raw_ref
                        and previous["start"] <= scope_start and previous["end"] >= scope_end
                        and not (requested_scope and requested_scope & touched)):
                    # Metadata-only regeneration: keep original row observation times.
                    self.lake.state["bulk"][state_key] = {**previous, "file": fingerprint}
                    self.lake.save()
                    count += 1
                    continue
                try:
                    reader = csv.DictReader(io.StringIO(gzip.decompress(payload).decode("utf-8-sig")))
                    records = list(reader)
                except (OSError, UnicodeError):
                    raise ValueError(f"Invalid gzip CSV for {name}") from None
                # A companion raw parquet retains every source row, as JSON, without type inference.
                self.lake.raw(name, canonical({"data": records, "file_key": key}), records, observed)
                if name == OPTIONS_225:
                    columns = reader.fieldnames or []
                    if not records or not OPTIONS_225_COLUMNS <= set(columns) or len(columns) != len(set(columns)):
                        raise ValueError("Options bulk CSV is empty or has an invalid column set")
                normalized = normalize(name, records, observed)
                normalized = [r for r in normalized if iso(first_day) <= str(r[spec.date]) <= iso(end)]
                scope = bulk_dates(key)
                if scope is not None:
                    scope = {d for d in scope if iso(first_day) <= str(d) <= iso(end)}
                    if any(r[spec.date] not in scope for r in normalized):
                        raise ValueError(f"Bulk filename date disagrees with rows for {name}")
                else:
                    # Unknown filenames must not imply an authoritative deletion scope.
                    scope = {r[spec.date] for r in normalized}
                    self.lake.log({"event": "bulk_scope_inferred_from_rows", "dataset": name, "key": key})
                if name == "calendar" and {r["Date"] for r in normalized} != set(days(first_day, end)):
                    raise ValueError("Bulk calendar range is incomplete; do not infer holidays")
                normalized = quarantine_financial_periods(name, normalized, self.lake, state_key)
                assert_valid(name, normalized, self.lake)
                self.lake.state["derived_stale"] = True
                self.lake.replace(name, normalized, dates=scope)
                applied_scopes[name] = sorted(set(applied_scopes.get(name, [])) | {str(d) for d in scope})
                self.lake.state["bulk"][state_key] = {
                    "file": fingerprint,
                    "raw": raw_ref,
                    "count": len(normalized),
                    "dataset": name,
                    "start": scope_start,
                    "end": scope_end,
                    "observed_at": observed,
                }
                self.lake.save()
                print(
                    canonical(
                        {
                            "event": "bulk_file_complete",
                            "dataset": name,
                            "key": key,
                            "rows": len(normalized),
                            "reused_raw": raw_cached,
                        }
                    ).decode(),
                    flush=True,
                )
                touched.update(scope)
                if not cached:
                    count += 1
        return {"processed_files": count, "more_pending": False, "applied_scopes": applied_scopes}

    def smoke(self, start, end, codes=("86970", "72030")):
        business = self.calendar(start, end, refresh=True)
        if not business:
            raise ValueError("Smoke range has no business days")
        counts = {}
        for security in codes:
            security = code(security)
            for name in ("master", "prices", "financials"):
                if name == "master":
                    params = {"code": security, "date": str(business[-1])}
                    scope = {business[-1]}
                elif name == "prices":
                    params = {"code": security, "from": iso(start), "to": iso(end)}
                    scope = set(business)
                else:
                    # Summary supports code or date, not from/to. Fetch all allowed history for two stocks.
                    params, scope = {"code": security}, None
                n = self.fetch(name, params, refresh=True, dates=scope, codes={security})
                counts[f"{name}:{security}"] = n
                if not n:
                    raise ValueError(f"Smoke expected non-empty {name}:{security}")
        return counts

