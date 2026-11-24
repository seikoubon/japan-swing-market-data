from datetime import date, datetime
from time import perf_counter
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from .catalog import DATASETS, OPTIONS_225
from .lake import atomic, canonical, now
from .quality_cache import summarize_clean_rows
from .research import connect, inventory


def options_quality(con, inventory, scope):
    start, end, listed = (scope[k] for k in ("start", "end", "listed_through"))
    calendar = con.execute(
        "SELECT Date, HolDiv FROM calendar WHERE Date BETWEEN ? AND ?", [start, end]
    ).fetchall()
    if len({day for day, _ in calendar}) != (date.fromisoformat(end) - date.fromisoformat(start)).days + 1:
        raise ValueError("Options coverage requires a complete JPX calendar")
    # OSE holiday trading is reported under the following business day's session.
    business = {str(day) for day, division in calendar if division in {"1", "2"}}
    found, duplicates, iv_missing = set(), 0, 0
    if inventory["rows"]:
        daily = con.execute(f"""SELECT Date, sum(n - 1), sum(missing_iv) FROM (
                SELECT Date, Code, EmMrgnTrgDiv, count(*) AS n,
                       count(*) FILTER (WHERE IV IS NULL) AS missing_iv
                FROM {OPTIONS_225} WHERE Date BETWEEN ? AND ? GROUP BY 1, 2, 3
            ) GROUP BY Date""", [start, end]).fetchall()
        found = {str(day) for day, _, _ in daily}
        duplicates = sum(n for _, n, _ in daily)
        iv_missing = sum(n for _, _, n in daily)
    return {
        "rows": inventory["rows"], "start": inventory["start"], "end": inventory["end"],
        "expected_sessions": sum(day <= listed for day in business),
        "sessions": len(found), "missing_sessions": sorted(day for day in business - found if day <= listed),
        "pending_sessions": sorted(day for day in business - found if day > listed),
        "unexpected_sessions": sorted(found - business),
        "duplicate_keys": duplicates, "iv_missing": iv_missing,
    }


def report(lake, *, use_row_cache=False, timings=None):
    timings = {} if timings is None else timings
    generated_at = now()
    as_of = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    started = perf_counter()
    counts = inventory(lake)
    timings["inventory"] = perf_counter() - started
    summary = {
        "generated_at": generated_at,
        "inventory": counts,
        "errors": 0,
        "warnings": 0,
        "issues": [],
        "missing_records": 0,
        "quarantined_records": 0,
        "quarantine_examples": [],
        "scope": lake.state.get("planned_scope"),
    }
    summary.update(summarize_clean_rows(lake, as_of, timings, use_cache=use_row_cache))
    started = perf_counter()
    isolated = {p.replace("\\", "/") for paths in lake.state.get("quarantine", {}).values() for p in paths}
    seen_isolated = set()
    for relative in sorted(isolated):
        for row in pq.read_table(lake.root / relative).to_pylist():
            dataset = relative.split("/")[1]
            key = (dataset, row["_row_hash"])
            if key in seen_isolated:
                continue
            seen_isolated.add(key)
            summary["quarantined_records"] += 1
            if len(summary["quarantine_examples"]) < 100:
                summary["quarantine_examples"].append(
                    {
                        "code": row["Code"],
                        "dataset": dataset,
                        "date": str(row[DATASETS[dataset].date]),
                        "disclosure_number": row.get("DiscNo"),
                        "scheduled_date": str(row.get("SchDate")),
                        "reason": row["_quality_reason"],
                        "period_start": str(row.get("CurPerSt")),
                        "period_end": str(row.get("CurPerEn")),
                        "document_type": row.get("DocType"),
                        "path": relative,
                    }
                )
    summary["warnings"] += summary["quarantined_records"]
    timings["quarantine"] = perf_counter() - started
    started = perf_counter()
    con = connect(lake)
    if lake.files("master") and lake.files("prices"):
        # Check only dates with complete, unscoped price/master acquisitions.
        complete = {n: set() for n in ("master", "prices")}
        for entry in lake.state["requests"].values():
            n, params = entry["dataset"], entry["params"]
            if n in complete and "date" in params and "code" not in params:
                complete[n].add(params["date"])
        for entry in lake.state["bulk"].values():
            n = entry["dataset"]
            if n in complete:
                from .pipeline import bulk_dates

                scope = bulk_dates(entry["file"]["Key"])
                complete[n].update(str(d) for d in scope or [] if entry["start"] <= str(d) <= entry["end"])
        comparable = sorted(complete["master"] & complete["prices"])
        if comparable:
            con.execute("CREATE TEMP TABLE comparable(day DATE)")
            con.executemany("INSERT INTO comparable VALUES (?)", [(d,) for d in comparable])
            missing = con.execute("""SELECT m.Date,m.Code,'missing_price_record' AS reason
                FROM master m JOIN comparable d ON m.Date=d.day
                LEFT JOIN prices p ON p.Date=m.Date AND p.Code=m.Code WHERE p.Code IS NULL""").fetchall()
            summary["missing_records"] = len(missing)
            summary["errors"] += len(missing)
            summary["issues"].extend(
                {"dataset": "prices", "date": str(d), "code": c, "check": r, "severity": "error"}
                for d, c, r in missing[:100]
            )
        summary["membership_changes"] = con.execute("""WITH s AS (SELECT Code,Date,Mkt,CoName,
            lag(Mkt) OVER (PARTITION BY Code ORDER BY Date) AS prev_market,
            lag(CoName) OVER (PARTITION BY Code ORDER BY Date) AS prev_name FROM master)
            SELECT count(*) FROM s WHERE (Mkt<>prev_market OR CoName<>prev_name)""").fetchone()[0]
    timings["master_prices"] = perf_counter() - started
    started = perf_counter()
    planned = lake.state.get("planned_scope")
    if planned and lake.files("calendar"):
        business = [
            r[0]
            for r in con.execute(
                "SELECT Date FROM calendar WHERE HolDiv='1' AND Date BETWEEN ? AND ?",
                [planned["start"], planned["end"]],
            ).fetchall()
        ]
        summary["coverage_gaps"] = {}
        summary["coverage_exceptions"] = []
        for name in planned["datasets"]:
            if name not in {"prices", "master", "indices", "valuation", "short_ratio"}:
                continue
            found = (
                {
                    r[0]
                    for r in con.execute(f'SELECT DISTINCT "{DATASETS[name].date}" FROM "{name}"').fetchall()
                }
                if lake.files(name)
                else set()
            )
            gaps = sorted(set(business) - found)
            # Provider explicitly documents this one absent session. Never infer
            # exceptions from an empty response or from arbitrary missing dates.
            halted = date(2020, 10, 1)
            if name == "short_ratio" and halted in gaps:
                summary["coverage_exceptions"].append(
                    {
                        "dataset": name,
                        "date": str(halted),
                        "reason": "documented_exchange_wide_trading_halt",
                        "source": "https://jpx-jquants.com/ja/spec/mkt-short-ratio",
                    }
                )
                gaps.remove(halted)
            summary["coverage_gaps"][name] = [str(d) for d in gaps]
            # Partial backfill is visibly incomplete, without preventing resumable checkpoint publication.
        if options := planned.get(OPTIONS_225):
            details = options_quality(con, counts[OPTIONS_225], options)
            summary[OPTIONS_225] = details
            summary["coverage_gaps"][OPTIONS_225] = details["missing_sessions"]
            summary["errors"] += details["duplicate_keys"] + len(details["unexpected_sessions"])
            summary["warnings"] += details["iv_missing"]
        summary["coverage_complete"] = not any(summary["coverage_gaps"].values())
    if lake.files("prices"):
        latest = con.execute("SELECT max(Date) FROM prices").fetchone()[0]
        if latest and lake.files("calendar"):
            expected = con.execute(
                "SELECT max(Date) FROM calendar WHERE HolDiv='1' AND Date<?",
                [as_of],
            ).fetchone()[0]
            summary["freshness"] = {
                "latest_price_date": str(latest),
                "expected_previous_session": str(expected),
                "delayed": bool(expected and latest < expected),
            }
    con.close()
    timings["coverage_freshness"] = perf_counter() - started
    atomic(lake.root / "reports" / "latest.json", canonical(summary))
    lines = [
        "# Data quality",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        f"Clean errors: {summary['errors']}; warnings: {summary['warnings']}; source records quarantined: {summary['quarantined_records']}",
        "",
        "|Dataset|Rows|From|To|Codes|",
        "|---|---:|---|---|---:|",
    ]
    for n, r in summary["inventory"].items():
        lines.append(f"|{n}|{r['rows']}|{r['start']}|{r['end']}|{r['codes']}|")
    if "coverage_complete" in summary:
        lines += [
            "",
            f"Requested-range coverage complete: {summary['coverage_complete']}",
            "See latest.json coverage_gaps for missing sessions; sparse datasets need publication-aware review.",
        ]
    atomic(lake.root / "reports" / "latest.md", "\n".join(lines).encode())
    return summary

