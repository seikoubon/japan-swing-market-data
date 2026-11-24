"""Reuse the report's per-file Clean row checks, bound to verified input bytes."""

import hashlib
import json
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import DATASETS
from .lake import atomic, canonical, digest
from .quality import check_rows

CACHE_PATH = "reports/clean-row-checks.json"
ISSUE_LIMIT = 200


def rules_signature():
    # Cover the actual check implementation, key/date definitions, cache format
    # and the runtime which converts Parquet values to Python objects.
    sources = [Path(__file__).with_name(name).read_bytes()
               for name in ("quality.py", "catalog.py", "quality_cache.py")]
    return digest(canonical([pa.__version__, *map(digest, sources)]))


def read_cache(path, rules, as_of):
    try:
        saved = json.loads(path.read_bytes())
        payload = saved["payload"]
        if (saved["sha256"] == digest(canonical(payload)) and payload["rules"] == rules
                and date.fromisoformat(payload["validated_on"]) <= as_of
                and isinstance(payload["files"], dict)):
            return payload["files"]
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return {}


def summarize_clean_rows(lake, as_of, timings, *, use_cache):
    cache_path = lake.root / CACHE_PATH
    started = perf_counter()
    rules = rules_signature() if use_cache else None
    previous = read_cache(cache_path, rules, as_of) if use_cache else {}
    current = {}
    stats = {"row_cache": perf_counter() - started, "parquet_to_rows": 0.0, "row_checks": 0.0,
             "files_scanned": 0, "files_reused": 0}
    result = {"errors": 0, "warnings": 0, "issues": []}
    for name in DATASETS:
        for path in lake.files(name):
            relative = path.relative_to(lake.root).as_posix()
            # Hash and decode the same open file, even if a path is atomically replaced.
            with (path.open("rb") if use_cache else nullcontext(path)) as source:
                if use_cache:
                    started = perf_counter()
                    sha = hashlib.file_digest(source, "sha256").hexdigest()
                    source.seek(0)
                    cached = previous.get(relative)
                    stats["row_cache"] += perf_counter() - started
                    if isinstance(cached, dict) and cached.get("sha256") == sha:
                        current[relative] = cached
                        result["warnings"] += cached["warnings"]
                        result["issues"].extend(cached["issues"][:ISSUE_LIMIT - len(result["issues"])])
                        stats["files_reused"] += 1
                        continue
                started = perf_counter()
                rows = pq.read_table(source).to_pylist()
                stats["parquet_to_rows"] += perf_counter() - started
            started = perf_counter()
            issues = check_rows(name, rows, as_of=as_of if use_cache else None)
            errors = sum(issue["severity"] == "error" for issue in issues)
            warnings = sum(issue["severity"] == "warning" for issue in issues)
            examples = [issue for issue in issues if issue["severity"] != "info"][:ISSUE_LIMIT]
            result["errors"] += errors
            result["warnings"] += warnings
            result["issues"].extend(examples[:ISSUE_LIMIT - len(result["issues"])])
            # Future-effective rows are errors. Only error-free results are
            # reusable and only at the validation date or a later JST date.
            if use_cache and not errors:
                current[relative] = {"sha256": sha, "warnings": warnings, "issues": examples}
            stats["row_checks"] += perf_counter() - started
            stats["files_scanned"] += 1
            del rows, issues
    if use_cache:
        started = perf_counter()
        payload = {"rules": rules, "validated_on": as_of.isoformat(), "files": current}
        atomic(cache_path, canonical({"payload": payload, "sha256": digest(canonical(payload))}))
        stats["row_cache"] += perf_counter() - started
    timings.update(stats)
    return result
