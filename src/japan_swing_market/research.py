from pathlib import Path

import duckdb

from .catalog import DATASETS, MARKETS
from .lake import atomic, canonical, now


def quoted_path(path):
    return "'" + str(Path(path).resolve()).replace("'", "''") + "'"


def connect(lake):
    con = duckdb.connect(config={"threads": 4, "memory_limit": "4GB"})
    for name in DATASETS:
        if lake.files(name):
            pattern = quoted_path(lake.root / "clean" / name / "*.parquet")
            con.execute(f'CREATE VIEW "{name}" AS SELECT * FROM read_parquet({pattern}, union_by_name=true)')
    for name in ("prices", "financials", "margin"):
        path = lake.root / "research" / f"{name}.parquet"
        if path.exists() and not lake.state.get("derived_stale"):
            con.execute(f'CREATE VIEW "research_{name}" AS SELECT * FROM read_parquet({quoted_path(path)})')
    if lake.files("master"):
        # Same code can reappear after delisting, a regional-only listing, or
        # reuse for a different subscription right. This is membership evidence,
        # not an assertion of issuer identity or an official delisting date.
        con.execute("""CREATE VIEW listing_membership AS WITH sessions AS (
          SELECT Date,row_number() OVER(ORDER BY Date) n FROM (SELECT DISTINCT Date FROM master)
        ), sequenced AS (
          SELECT m.Code,m.Date,s.n,lag(s.n) OVER(PARTITION BY m.Code ORDER BY m.Date) previous_n
          FROM master m JOIN sessions s USING(Date)
        ), episodes AS (
          SELECT Code,Date,sum(CASE WHEN previous_n IS NULL OR n>previous_n+1 THEN 1 ELSE 0 END)
            OVER(PARTITION BY Code ORDER BY Date) listing_episode FROM sequenced
        ) SELECT *,min(Date) OVER(PARTITION BY Code,listing_episode) listing_episode_start FROM episodes""")
        if "research_prices" in {r[0] for r in con.execute("SHOW TABLES").fetchall()}:
            con.execute("""CREATE VIEW research_price_sessions AS SELECT p.*,e.listing_episode,
              e.listing_episode_start FROM research_prices p LEFT JOIN listing_membership e USING(Code,Date)""")
        markets = ",".join("'" + m + "'" for m in MARKETS)
        columns = {r[0] for r in con.execute("DESCRIBE master").fetchall()}
        if {"Mkt", "ProdCat"} <= columns:
            con.execute(
                f"CREATE VIEW universe AS SELECT * FROM master WHERE Mkt IN ({markets}) AND ProdCat='011'"
            )
    if lake.files("master") and "universe" in {r[0] for r in con.execute("SHOW TABLES").fetchall()}:
        con.execute(
            "CREATE MACRO universe_at(day) AS TABLE SELECT * FROM universe WHERE Date=cast(day AS DATE)"
        )
    if (lake.root / "research/financials.parquet").exists() and not lake.state.get("derived_stale"):
        con.execute("""CREATE MACRO financial_events_at(decision_time) AS TABLE
          SELECT * FROM research_financials
          WHERE estimated_available_at_jst<=cast(decision_time AS TIMESTAMP)
            AND (disclosed_at_jst IS NULL OR disclosed_at_jst<=cast(decision_time AS TIMESTAMP))""")
    return con


def build(lake, as_of=None):
    con = connect(lake)
    built = []
    if lake.files("prices"):
        # Missing optional columns are explicitly added before building derived data.
        cols = {r[0] for r in con.execute("DESCRIBE prices").fetchall()}
        extra = []
        for field, typ in {
            "ExRT": "VARCHAR",
            "AdjFactor": "DOUBLE",
            "O": "DOUBLE",
            "H": "DOUBLE",
            "L": "DOUBLE",
            "C": "DOUBLE",
            "Vo": "DOUBLE",
            "Va": "DOUBLE",
        }.items():
            if field not in cols:
                extra.append(f'NULL::{typ} AS "{field}"')
        con.execute(
            "CREATE VIEW prices_input AS SELECT *" + ("," + ",".join(extra) if extra else "") + " FROM prices"
        )
        boundary = f"WHERE Date <= DATE {quoted_path_date(as_of)}" if as_of else ""
        # Exclude the current row: factors apply only to earlier sessions.
        # log/product window is evaluated in DuckDB, never by rounding intermediate factors.
        sql = f"""WITH source AS (SELECT * FROM prices_input {boundary}), factors AS (
          SELECT *, coalesce(product(AdjFactor) OVER w, 1.0) AS CumAdj,
            coalesce(product(CASE WHEN ExRT='3' THEN 1.0 ELSE AdjFactor END) OVER w,1.0) AS CumVolAdj,
            max(Date) OVER () AS adjustment_as_of
          FROM source WINDOW w AS (PARTITION BY Code ORDER BY Date
            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING))
          SELECT Date, Code, O, H, L, C, Vo, Va, AdjFactor, ExRT, CumAdj, CumVolAdj,
            round(O*CumAdj,1) AS AdjO, round(H*CumAdj,1) AS AdjH,
            round(L*CumAdj,1) AS AdjL, round(C*CumAdj,1) AS AdjC,
            round(Vo/CumVolAdj,1) AS AdjVo, adjustment_as_of FROM factors"""
        write_query(con, lake.root / "research" / "prices.parquet", sql)
        built.append("prices")
    if lake.files("financials"):
        cols = {r[0] for r in con.execute("DESCRIBE financials").fetchall()}
        time_expr = (
            "try_cast(cast(DiscDate AS VARCHAR)||' '||DiscTime AS TIMESTAMP)"
            if "DiscTime" in cols
            else "NULL::TIMESTAMP"
        )
        # Availability is an estimate, NOT an observed historical API timestamp.
        sql = f"""SELECT *, {time_expr} AS disclosed_at_jst,
          cast(DiscDate AS TIMESTAMP) + INTERVAL 1 DAY + INTERVAL 6 HOUR AS estimated_available_at_jst,
          'standard_next_morning_0600_estimate' AS availability_basis FROM financials"""
        write_query(con, lake.root / "research" / "financials.parquet", sql)
        built.append("financials")
    if lake.files("margin"):
        cols = {r[0] for r in con.execute("DESCRIBE margin").fetchall()}
        if {"LongVol", "ShrtVol"} <= cols:
            write_query(
                con,
                lake.root / "research" / "margin.parquet",
                "SELECT *, LongVol/nullif(ShrtVol,0) AS margin_ratio FROM margin",
            )
            built.append("margin")
    con.close()
    atomic(
        lake.root / "research" / "build.json", canonical({"built_at": now(), "tables": built, "as_of": as_of})
    )
    lake.state["derived_stale"] = False
    lake.save()
    return built


def quoted_path_date(day):
    from .lake import iso

    return "'" + iso(day) + "'"


def write_query(con, path, sql):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp.parquet")
    con.execute(f"COPY ({sql}) TO {quoted_path(temp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    temp.replace(path)


def inventory(lake):
    con, result = connect(lake), {}
    for name, spec in DATASETS.items():
        if not lake.files(name):
            continue
        codes = (
            'count(distinct "Code")' if name not in {"calendar", "investor_types", "short_ratio"} else "NULL"
        )
        count, first, last, securities = con.execute(
            f'SELECT count(*),min("{spec.date}"),max("{spec.date}"),{codes} FROM "{name}"'
        ).fetchone()
        result[name] = {"rows": count, "start": str(first), "end": str(last), "codes": securities}
    con.close()
    return result


def reconcile_prices(lake, reference_path=None):
    """Compare API-adjusted fields with reconstructed relative adjustments.

    The API may include corporate actions later than the downloaded range. Normalize
    to the last common close and allow its 0.1-yen rounding uncertainty.
    """
    con = connect(lake)
    if reference_path is None:
        columns = {r[0] for r in con.execute("DESCRIBE prices").fetchall()}
        if not {"AdjC", "C"} <= columns:
            con.close()
            return {"checked": 0, "status": "no_api_adjusted_reference"}
        source = "prices"
    else:
        con.execute(f"CREATE VIEW api_reference AS SELECT * FROM read_parquet({quoted_path(reference_path)})")
        source = "api_reference"
    references = con.execute(f"""SELECT p.Code,p.Date,p.AdjC,r.AdjC AS calculated
        FROM {source} p JOIN research_prices r USING(Code,Date)
        WHERE p.AdjC IS NOT NULL AND p.C>0 AND r.AdjC>0 ORDER BY p.Code,p.Date""").fetchall()
    if not references:
        con.close()
        return {"checked": 0, "status": "no_api_adjusted_reference"}
    anchors = {security: (api, calc) for security, _, api, calc in references}
    errors = []
    for security, day, api, calc in references:
        api_anchor, calc_anchor = anchors[security]
        scale = api_anchor / calc_anchor
        # Both rounded API values and the local reconstructed value carry at most 0.05 uncertainty.
        tolerance = 0.11 + 0.051 * abs(calc / calc_anchor) + 0.051 * abs(scale)
        if abs(api - calc * scale) > tolerance:
            errors.append(
                {
                    "dataset": "prices",
                    "date": str(day),
                    "code": security,
                    "check": "api_relative_adjustment_mismatch",
                    "severity": "error",
                }
            )
    con.close()
    for error in errors:
        lake.log(error)
    result = {
        "checked": len(references),
        "mismatches": len(errors),
        "status": "passed" if not errors else "failed",
        "basis": "relative_to_last_common_close",
    }
    lake.log({"event": "adjustment_reconciliation", **result})
    if errors:
        raise ValueError(f"API adjustment reconciliation failed for {len(errors)} rows")
    return result

