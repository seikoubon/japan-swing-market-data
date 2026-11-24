import math
from collections import Counter, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from .catalog import DATASETS, OPTIONS_225
from .lake import now, table, write_parquet


def check_rows(name, rows, *, as_of=None):
    issues, seen = [], {}

    def add(row, check, severity="error"):
        issues.append(
            {
                "dataset": name,
                "date": str(row.get(DATASETS[name].date)),
                "code": row.get("Code"),
                "check": check,
                "severity": severity,
            }
        )

    for row in rows:
        key = tuple(row.get(k) for k in DATASETS[name].keys)
        if key in seen:
            add(
                row,
                "duplicate" if seen[key] == row["_row_hash"] else "conflicting_duplicate",
                "warning" if name != OPTIONS_225 and seen[key] == row["_row_hash"] else "error",
            )
        seen[key] = row["_row_hash"]
        if row[DATASETS[name].date] > (as_of or datetime.now(ZoneInfo("Asia/Tokyo")).date()) and name != "calendar":
            add(row, "future_effective_date")
        for k, v in row.items():
            if isinstance(v, float) and not math.isfinite(v):
                add(row, f"non_finite:{k}")
        if name == "prices":
            columns = ("O", "H", "L", "C", "Vo", "Va")
            vals = [row.get(k) for k in columns]
            if all(v is None for v in vals):
                add(row, "no_execution_reason_unknown", "info")
            elif any(v is None for v in vals):
                add(row, "partial_ohlcv_null")
            else:
                o, h, low, c, vol, value = vals
                if min(o, h, low, c, vol, value) <= 0:
                    add(row, "non_positive_price_volume_value")
                if low > min(o, c) or h < max(o, c) or low > h:
                    add(row, "ohlc_order")
                if vol > 0 and not low - 0.11 <= value / vol <= h + 0.11:
                    add(row, "vwap_outside_daily_range")
            factor = row.get("AdjFactor")
            if factor is None or factor <= 0:
                add(row, "invalid_adjustment_factor")
            if factor not in {None, 1.0} and row.get("ExRT") not in {"1", "2", "3"}:
                add(row, "unknown_corporate_action_type", "warning")
        if name == "financials":
            start, end = row.get("CurPerSt"), row.get("CurPerEn")
            if start and end and start > end:
                add(row, "financial_period_order")
            if end and row.get("DocType", "").find("FinancialStatements") >= 0 and end > row["DiscDate"]:
                add(row, "financial_period_after_disclosure")
            if not row.get("DiscTime"):
                add(row, "missing_disclosure_time", "warning")
        if name in {"margin", "margin_alert"}:
            base = row.get("Date") if name == "margin" else row.get("AppDate")
            if base and row.get("PubDate") and row["PubDate"] < base:
                add(row, "published_before_reference_date")
            if name == "margin" and not row.get("PubDate"):
                add(row, "historical_publication_date_unavailable", "info")
        if name == "short_sales" and row.get("CalcDate") and row["CalcDate"] > row["DiscDate"]:
            add(row, "calculation_after_disclosure")
    return issues


def assert_valid(name, rows, lake):
    issues = check_rows(name, rows)
    informational = [i for i in issues if i["severity"] == "info"]
    for issue in issues:
        if issue["severity"] != "info":
            lake.log(issue)
    if informational:
        lake.log(
            {
                "event": "quality_information_summary",
                "dataset": name,
                "counts": dict(Counter(i["check"] for i in informational)),
                "examples": informational[:10],
            }
        )
    errors = sum(i["severity"] == "error" for i in issues)
    if errors:
        raise ValueError(f"{name}: {errors} quality errors; see data/logs (raw retained)")
    return issues


def quarantine_financial_periods(name, rows, lake, source_id):
    """Preserve impossible source periods separately; never make them available to research."""
    if name == "earnings_dates":
        groups = defaultdict(list)
        for row in rows:
            groups[tuple(row.get(k) for k in DATASETS[name].keys)].append(row)
        good, isolated = [], []
        for group in groups.values():
            if len({r["_row_hash"] for r in group}) <= 1:
                good.extend(group)
                continue
            # No intraday ordering or event identifier is provided. Retain all
            # conflicting alternatives; never pick a scheduled date by row order.
            for row in group:
                path = lake.root / "quarantine/earnings_dates" / (row["_row_hash"] + ".parquet")
                reason = "ambiguous_same_publication_schedule"
                if not path.exists():
                    write_parquet(
                        path,
                        table(
                            name,
                            [
                                {
                                    **row,
                                    "_quality_reason": reason,
                                    "_quarantined_at": now(),
                                }
                            ],
                        ),
                    )
                isolated.append(path.relative_to(lake.root).as_posix())
                lake.log(
                    {
                        "event": "source_record_quarantined",
                        "severity": "warning",
                        "dataset": name,
                        "date": str(row["PubDate"]),
                        "code": row["Code"],
                        "scheduled_date": str(row.get("SchDate")),
                        "reason": reason,
                        "path": isolated[-1],
                    }
                )
        lake.state.setdefault("quarantine", {})[source_id] = isolated
        return good
    if name != "financials":
        return rows
    good, isolated = [], []
    for row in rows:
        errors = {i["check"] for i in check_rows(name, [row]) if i["severity"] == "error"}
        if errors == {"financial_period_after_disclosure"}:
            path = lake.root / "quarantine/financials" / (row["_row_hash"] + ".parquet")
            isolated_row = {
                **row,
                "_quality_reason": "financial_period_after_disclosure",
                "_quarantined_at": now(),
            }
            if not path.exists():
                write_parquet(path, table(name, [isolated_row]))
            isolated.append(path.relative_to(lake.root).as_posix())
            lake.log(
                {
                    "event": "source_record_quarantined",
                    "severity": "warning",
                    "dataset": name,
                    "date": str(row["DiscDate"]),
                    "code": row["Code"],
                    "disclosure_number": row["DiscNo"],
                    "period_start": str(row.get("CurPerSt")),
                    "period_end": str(row.get("CurPerEn")),
                    "document_type": row.get("DocType"),
                    "reason": "financial_period_after_disclosure",
                    "path": isolated[-1],
                }
            )
        else:
            good.append(row)
    lake.state.setdefault("quarantine", {})[source_id] = isolated
    return good

