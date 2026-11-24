import gzip
import json
from datetime import date

import httpx
import pyarrow.parquet as pq
import pytest

from japan_swing_market.client import APIError, Client
from japan_swing_market.lake import Lake, canonical, normalize
from japan_swing_market.pipeline import Pipeline
from japan_swing_market.quality import check_rows
from japan_swing_market.report import report
from japan_swing_market.research import build, connect, inventory

# All records here are SYNTHETIC fixtures, never downloaded market data.


def price(day="2026-11-12", code="167A0", **kwargs):
    return {
        "Date": day,
        "Code": code,
        "O": 100,
        "H": 110,
        "L": 90,
        "C": 100,
        "Vo": 1000,
        "Va": 100000,
        "AdjFactor": 1,
        "ExRT": None,
        **kwargs,
    }


def norm(name, rows):
    return normalize(name, rows, "2026-11-14T00:00:00+00:00")


def test_pagination_and_key_only_sent_to_api():
    seen = []

    def handler(req):
        seen.append(req)
        if req.url.path.endswith("bulk/get"):
            return httpx.Response(200, json={"url": "https://storage.example/file?signature=secret"})
        if req.url.host == "storage.example":
            assert "x-api-key" not in req.headers
            return httpx.Response(200, content=b"payload")
        if "pagination_key" not in req.url.params:
            return httpx.Response(200, json={"data": [{"a": 1}], "pagination_key": "next"})
        return httpx.Response(200, json={"data": [{"a": 2}]})

    c = Client("do-not-log", transport=httpx.MockTransport(handler), interval=0)
    assert len(list(c.pages("/equities/master", {"code": "167A0"}))) == 2
    assert c.download("key") == b"payload"
    c.close()
    assert seen[0].headers["x-api-key"] == "do-not-log"


def test_retry_rate_limit_and_redaction():
    waits, calls = [], []

    def handler(req):
        calls.append(req)
        return httpx.Response(429 if len(calls) == 1 else 200, json={"data": []})

    c = Client("sensitive", transport=httpx.MockTransport(handler), interval=0, sleep=waits.append)
    assert c.get("/equities/master")["data"] == []
    assert 65 in waits
    c.close()
    c = Client(
        "sensitive",
        transport=httpx.MockTransport(lambda req: httpx.Response(403, text="sensitive")),
        interval=0,
    )
    with pytest.raises(APIError) as error:
        c.get("/equities/master")
    assert "sensitive" not in str(error.value)
    c.close()


def test_repeated_pagination_token_fails():
    c = Client(
        "k",
        transport=httpx.MockTransport(
            lambda req: httpx.Response(200, json={"data": [], "pagination_key": "x"})
        ),
        interval=0,
    )
    with pytest.raises(APIError, match="Repeated"):
        list(c.pages("/equities/master"))
    c.close()


def test_numeric_date_and_alphanumeric_normalization():
    row = norm(
        "financials",
        [
            {
                "Code": "167A",
                "DiscDate": "20261112",
                "DiscTime": "15:30:00",
                "DiscNo": "001",
                "Sales": "12345",
                "OP": "",
                "EqAR": "0.6",
            }
        ],
    )[0]
    assert row["Code"] == "167A0"
    assert row["DiscDate"] == date(2026, 11, 12)
    assert row["DiscNo"] == "001"
    assert row["Sales"] == 12345
    assert row["OP"] is None
    assert row["EqAR"] == 0.6


def test_idempotence_and_authoritative_scope_replacement(tmp_path):
    lake = Lake(tmp_path)
    rows = norm("prices", [price(), price(code="72030")])
    lake.replace("prices", rows)
    lake.replace("prices", rows)
    assert inventory(lake)["prices"]["rows"] == 2
    updated = norm("prices", [price(C=105)])
    lake.replace("prices", updated, dates={date(2026, 11, 12)}, codes={"167A0"})
    assert inventory(lake)["prices"]["rows"] == 2
    lake.replace("prices", [], dates={date(2026, 11, 12)}, codes={"167A0"})
    assert inventory(lake)["prices"]["rows"] == 1


def test_raw_is_immutable_and_rebuildable(tmp_path):
    lake = Lake(tmp_path)
    payload = canonical({"data": [price()]})
    p = lake.raw("prices", payload, [price()], "first")
    assert lake.raw("prices", payload, [price()], "second") == p
    assert gzip.decompress((tmp_path / p).read_bytes()) == payload
    raw_parquet = (tmp_path / p).with_suffix(".parquet")
    assert json.loads(pq.read_table(raw_parquet)["payload_json"][0].as_py())["Code"] == "167A0"


def test_quality_null_vs_bad_prices():
    no_trade = price(**{k: None for k in ("O", "H", "L", "C", "Vo", "Va")})
    issues = check_rows("prices", norm("prices", [no_trade]))
    assert issues[0]["check"] == "no_execution_reason_unknown"
    assert not any(i["severity"] == "error" for i in issues)
    bad = check_rows("prices", norm("prices", [price(H=95), price(code="72030", Va=99999999)]))
    assert {i["check"] for i in bad} >= {"ohlc_order", "vwap_outside_daily_range"}
    partial = check_rows("prices", norm("prices", [price(Vo=None)]))
    assert any(i["check"] == "partial_ohlcv_null" for i in partial)


def test_conflicting_duplicates_do_not_overwrite(tmp_path):
    lake = Lake(tmp_path)
    lake.replace("prices", norm("prices", [price()]))
    with pytest.raises(ValueError, match="Conflicting"):
        lake.replace("prices", norm("prices", [price(), price(C=101)]))
    assert pq.read_table(lake.files("prices")[0])["C"][0].as_py() == 100


def test_split_and_rights_adjustment_and_asof(tmp_path):
    lake = Lake(tmp_path)
    lake.replace(
        "prices",
        norm(
            "prices",
            [
                price("2026-11-10", C=100, O=100, H=110, L=90),
                price("2026-11-11", C=50, O=50, H=55, L=45, Va=50000, AdjFactor=0.5, ExRT="1"),
                price("2026-11-12", C=40, O=40, H=44, L=36, Va=40000, AdjFactor=0.8, ExRT="3"),
            ],
        ),
    )
    build(lake)
    rows = pq.read_table(tmp_path / "research/prices.parquet").to_pylist()
    assert [r["AdjC"] for r in rows] == [40, 40, 40]
    assert [r["AdjVo"] for r in rows] == [2000, 1000, 1000]
    build(lake, "2026-11-11")
    rows = pq.read_table(tmp_path / "research/prices.parquet").to_pylist()
    assert [r["AdjC"] for r in rows] == [50, 50]
    assert rows[-1]["CumAdj"] == 1


def test_financial_revisions_not_overwritten_and_availability_labelled(tmp_path):
    lake = Lake(tmp_path)
    lake.replace(
        "financials",
        norm(
            "financials",
            [
                {
                    "Code": "167A0",
                    "DiscDate": "2026-11-11",
                    "DiscTime": "15:31:00",
                    "DiscNo": "1",
                    "DocType": "ForecastRevision",
                    "FSales": "100",
                },
                {
                    "Code": "167A0",
                    "DiscDate": "2026-11-12",
                    "DiscTime": "15:31:00",
                    "DiscNo": "2",
                    "DocType": "ForecastRevision",
                    "FSales": "90",
                },
            ],
        ),
    )
    build(lake)
    data = pq.read_table(tmp_path / "research/financials.parquet").to_pylist()
    assert len(data) == 2
    assert data[0]["estimated_available_at_jst"].hour == 6
    assert "estimate" in data[0]["availability_basis"]
    from japan_swing_market.research import connect

    con = connect(lake)
    assert (
        con.execute("SELECT count(*) FROM financial_events_at(TIMESTAMP '2026-11-12 05:59:59')").fetchone()[0]
        == 0
    )
    assert con.execute(
        "SELECT FSales FROM financial_events_at(TIMESTAMP '2026-11-12 06:00:00')"
    ).fetchall() == [(100.0,)]
    assert (
        con.execute("SELECT count(*) FROM financial_events_at(TIMESTAMP '2026-11-13 06:00:00')").fetchone()[0]
        == 2
    )
    con.close()


class Stub:
    def __init__(self, pages):
        self.responses, self.calls = pages, 0

    def pages(self, endpoint, params):
        self.calls += 1
        yield from self.responses


def test_failed_page_does_not_checkpoint(tmp_path):
    class Fail:
        def pages(self, endpoint, params):
            yield {"data": [price()]}
            raise APIError("second page failed")

    lake = Lake(tmp_path)
    with pytest.raises(APIError):
        Pipeline(Fail(), lake).fetch("prices", {"date": "2026-11-12"})
    assert not lake.state["requests"]
    assert not lake.files("prices")
    assert list((tmp_path / "raw/prices").glob("*.gz"))


def test_resume_skips_completed_request(tmp_path):
    lake, stub = Lake(tmp_path), Stub([{"data": [price()]}])
    pipe = Pipeline(stub, lake)
    for _ in range(2):
        assert pipe.fetch("prices", {"date": "2026-11-12"}) == 1
    assert stub.calls == 1
    pipe.fetch("prices", {"date": "2026-11-12"}, refresh=True)
    assert stub.calls == 2


def test_required_empty_and_calendar_gaps_fail(tmp_path):
    pipe = Pipeline(Stub([{"data": []}]), Lake(tmp_path))
    with pytest.raises(ValueError, match="empty"):
        pipe.fetch("prices", {"date": "2026-11-12"})
    pipe = Pipeline(Stub([{"data": [{"Date": "2026-11-11", "HolDiv": "1"}]}]), Lake(tmp_path))
    with pytest.raises(ValueError, match="incomplete"):
        pipe.calendar("2026-11-11", "2026-11-12")


def test_duckdb_union_schema_and_point_in_time_membership(tmp_path):
    lake = Lake(tmp_path)
    lake.replace(
        "master",
        norm(
            "master",
            [
                {"Date": "2022-03-31", "Code": "167A0", "Mkt": "0101", "CoName": "fixture"},
                {"Date": "2022-04-01", "Code": "167A0", "Mkt": "0111", "CoName": "fixture", "ProdCat": "011"},
            ],
        ),
    )
    con = connect(lake)
    assert con.execute("select Mkt from master where Date='2022-03-31'").fetchone()[0] == "0101"
    assert con.execute("select ProdCat from master where Date='2022-03-31'").fetchone()[0] is None
    con.close()


def test_report_detects_universe_missing_record(tmp_path):
    lake = Lake(tmp_path)
    lake.replace(
        "master",
        norm("master", [{"Date": "2026-11-12", "Code": "167A0", "Mkt": "0111", "CoName": "fixture"}]),
    )
    lake.replace("prices", norm("prices", [price(code="72030")]))
    for name in ("master", "prices"):
        lake.state["requests"][name] = {"dataset": name, "params": {"date": "2026-11-12"}}
    result = report(lake)
    assert result["missing_records"] == 1
    assert result["errors"] == 1


def test_bulk_resume_and_changed_file(tmp_path):
    import csv
    import io

    class Bulk:
        last_modified = "v1"
        calls = 0

        def __init__(self):
            out = io.StringIO()
            writer = csv.DictWriter(out, fieldnames=list(price()))
            writer.writeheader()
            writer.writerow(price())
            self.payload = gzip.compress(out.getvalue().encode())

        def pages(self, endpoint, params):
            yield {
                "data": [
                    {
                        "Key": "equities_bars_daily_202611.csv.gz",
                        "LastModified": self.last_modified,
                        "Size": len(self.payload),
                    }
                ]
            }

        def download(self, key):
            self.calls += 1
            return self.payload

    client, lake = Bulk(), Lake(tmp_path)
    pipe = Pipeline(client, lake)
    assert pipe.bulk(["prices"], "2026-11-01", "2026-11-13")["processed_files"] == 1
    assert pipe.bulk(["prices"], "2026-11-01", "2026-11-13")["processed_files"] == 0
    client.last_modified = "v2"
    assert pipe.bulk(["prices"], "2026-11-01", "2026-11-13")["processed_files"] == 1
    assert client.calls == 2
    assert inventory(lake)["prices"]["rows"] == 1
    # Invalid corrections retain the earlier clean checkpoint and cache the failed original.
    client.last_modified = "v3"
    client.payload = gzip.compress(
        b"Date,Code,O,H,L,C,Vo,Va,AdjFactor\n2026-11-12,72030,100,90,95,100,1000,100000,1\n"
    )
    for _ in range(2):
        with pytest.raises(ValueError, match="quality errors"):
            pipe.bulk(["prices"], "2026-11-01", "2026-11-13")
    assert client.calls == 3
    assert inventory(lake)["prices"]["rows"] == 1


def test_bulk_rolling_window_does_not_redownload_old_month(tmp_path):
    import csv
    import io

    class Bulk:
        calls = 0

        def __init__(self):
            out = io.StringIO()
            writer = csv.DictWriter(out, fieldnames=list(price()))
            writer.writeheader()
            writer.writerow(price("2026-10-12"))
            self.payload = gzip.compress(out.getvalue().encode())

        def pages(self, endpoint, params):
            yield {
                "data": [
                    {
                        "Key": "equities_bars_daily_202610.csv.gz",
                        "LastModified": "v1",
                        "Size": len(self.payload),
                    }
                ]
            }

        def download(self, key):
            self.calls += 1
            return self.payload

    client = Bulk()
    pipe = Pipeline(client, Lake(tmp_path))
    pipe.bulk(["prices"], "2026-10-01", "2026-11-12")
    pipe.bulk(["prices"], "2026-10-02", "2026-11-13")
    assert client.calls == 1
    # Expand an already parsed monthly file without repeating the HTTP download.
    pipe.bulk(["prices"], "2026-10-01", "2026-11-13")
    assert client.calls == 1


def test_unknown_optional_price_columns_are_supported(tmp_path):
    lake = Lake(tmp_path)
    row = price()
    del row["ExRT"]
    lake.replace("prices", norm("prices", [row]))
    build(lake)
    assert pq.read_table(tmp_path / "research/prices.parquet")["AdjC"][0].as_py() == 100


def test_api_adjustment_relative_reconciliation(tmp_path):
    from japan_swing_market.research import reconcile_prices

    lake = Lake(tmp_path)
    lake.replace(
        "prices",
        norm(
            "prices",
            [
                price("2026-11-10", AdjC=25),
                price("2026-11-11", AdjFactor=0.5, ExRT="1", C=50, O=50, H=55, L=45, Va=50000, AdjC=25),
            ],
        ),
    )
    build(lake)
    assert reconcile_prices(lake)["checked"] == 2
    assert reconcile_prices(lake)["mismatches"] == 0
    lake.replace("prices", norm("prices", [price("2026-11-10", AdjC=50)]))
    build(lake)
    with pytest.raises(ValueError, match="reconciliation"):
        reconcile_prices(lake)


def test_universe_excludes_etfs_and_retains_past_market(tmp_path):
    lake = Lake(tmp_path)
    lake.replace(
        "master",
        norm(
            "master",
            [
                {"Date": "2022-03-31", "Code": "167A0", "Mkt": "0101", "ProdCat": "011", "CoName": "fixture"},
                {"Date": "2022-03-31", "Code": "15700", "Mkt": "0109", "ProdCat": "014", "CoName": "ETF"},
            ],
        ),
    )
    con = connect(lake)
    assert con.execute("SELECT Code FROM universe").fetchall() == [("167A0",)]
    con.close()


def test_freshness_uses_jst_previous_session_at_morning_schedule(tmp_path, monkeypatch):
    import importlib
    from datetime import datetime

    report_module = importlib.import_module("japan_swing_market.report")

    class Clock:
        @staticmethod
        def now(zone):
            return datetime(2026, 11, 14, 6, 17, tzinfo=zone)

    monkeypatch.setattr(report_module, "datetime", Clock)
    lake = Lake(tmp_path)
    lake.replace(
        "calendar",
        norm("calendar", [{"Date": "2026-11-12", "HolDiv": "1"}, {"Date": "2026-11-13", "HolDiv": "1"}]),
    )
    lake.replace("prices", norm("prices", [price("2026-11-12")]))
    result = report_module.report(lake)
    assert result["freshness"]["expected_previous_session"] == "2026-11-13"
    assert result["freshness"]["delayed"] is True


def test_impossible_financial_source_is_retained_and_excluded_from_research(tmp_path):
    from japan_swing_market.report import report

    rows = [
        {
            "Code": "95650",
            "DiscDate": "2026-03-12",
            "DiscTime": "15:30:00",
            "DiscNo": "bad",
            "DocType": "1QFinancialStatements_NonConsolidated_JP",
            "CurPerSt": "2025-11-01",
            "CurPerEn": "2026-04-30",
        },
        {
            "Code": "72030",
            "DiscDate": "2026-03-12",
            "DiscTime": "15:30:00",
            "DiscNo": "good",
            "DocType": "3QFinancialStatements_Consolidated_JP",
            "CurPerSt": "2025-04-01",
            "CurPerEn": "2025-12-31",
        },
    ]
    lake = Lake(tmp_path)
    pipe = Pipeline(Stub([{"data": rows}]), lake)
    pipe.fetch("financials", {"date": "2026-03-12"})
    assert inventory(lake)["financials"]["rows"] == 1
    originals = list((tmp_path / "raw/financials").glob("*.parquet"))
    assert pq.read_table(originals[0]).num_rows == 2
    assert len(list((tmp_path / "quarantine/financials").glob("*.parquet"))) == 1
    build(lake)
    from japan_swing_market.research import connect

    con = connect(lake)
    assert con.execute("SELECT Code FROM research_financials").fetchall() == [("72030",)]
    con.close()
    quality = report(lake)
    assert quality["errors"] == 0 and quality["quarantined_records"] == 1 and quality["warnings"] == 1


def test_documented_halt_is_separate_from_unexplained_gap(tmp_path):
    lake = Lake(tmp_path)
    lake.replace(
        "calendar",
        norm(
            "calendar",
            [
                {"Date": "2020-10-01", "HolDiv": "1"},
                {"Date": "2020-10-02", "HolDiv": "1"},
            ],
        ),
    )
    lake.state["planned_scope"] = {"start": "2020-10-01", "end": "2020-10-02", "datasets": ["short_ratio"]}
    result = report(lake)
    assert result["coverage_gaps"]["short_ratio"] == ["2020-10-02"]
    assert result["coverage_exceptions"][0]["date"] == "2020-10-01"
    assert result["coverage_complete"] is False


def test_schedule_conflicting_alternatives_are_both_preserved_not_chosen(tmp_path):
    base = {"PubDate": "2021-12-29", "Code": "30970", "FQName": "1Q", "FYE": "0630"}
    rows = [
        {**base, "SchDate": "2022-11-09"},
        {**base, "SchDate": ""},
        {**base, "Code": "72030", "SchDate": "2022-02-01"},
    ]
    lake = Lake(tmp_path)
    Pipeline(Stub([{"data": rows}]), lake).fetch("earnings_dates", {"date": "2021-12-29"})
    assert inventory(lake)["earnings_dates"]["rows"] == 1
    assert sum(pq.read_table(p).num_rows for p in (tmp_path / "raw/earnings_dates").glob("*.parquet")) == 3
    assert len(list((tmp_path / "quarantine/earnings_dates").glob("*.parquet"))) == 2
    result = report(lake)
    assert result["errors"] == 0 and result["quarantined_records"] == 2
    assert result["warnings"] == 2
    assert {r["reason"] for r in result["quarantine_examples"]} == {"ambiguous_same_publication_schedule"}


def test_reused_code_has_separate_listing_periods(tmp_path):
    lake = Lake(tmp_path)
    lake.replace(
        "master",
        norm(
            "master",
            [
                {"Date": "2026-11-10", "Code": "167A0"},
                {"Date": "2026-11-11", "Code": "72030"},
                {"Date": "2026-11-12", "Code": "167A0"},
                {"Date": "2026-11-13", "Code": "167A0"},
            ],
        ),
    )
    lake.replace("prices", norm("prices", [price("2026-11-10"), price("2026-11-12"), price("2026-11-13")]))
    build(lake)
    con = connect(lake)
    rows = con.execute(
        "SELECT listing_episode,listing_episode_start FROM research_price_sessions ORDER BY Date"
    ).fetchall()
    con.close()
    assert [r[0] for r in rows] == [1, 2, 2]
    assert [str(r[1]) for r in rows] == ["2026-11-10", "2026-11-12", "2026-11-12"]
