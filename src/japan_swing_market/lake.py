import gzip
import hashlib
import json
import os
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import DATASETS, OPTIONS_225, is_numeric


def now():
    return datetime.now(UTC).isoformat()


def incremental_target_date(jst_now):
    """Include the current JST date once the evening releases are available."""
    today = jst_now.date()
    return today if jst_now.hour >= 19 else today - timedelta(days=1)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(data)
    os.replace(temp, path)


def iso(value):
    value = str(value)
    if re.fullmatch(r"\d{8}", value):
        value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return date.fromisoformat(value).isoformat()


def code(value):
    value = str(value).strip().upper()
    if re.fullmatch(r"[0-9A-Z]{4}", value):
        value += "0"
    if not re.fullmatch(r"[0-9A-Z]{5}", value):
        raise ValueError("Invalid security code")
    return value


def normalize(name, rows, observed):
    spec = DATASETS[name]
    out = []
    for original in rows:
        row = {}
        for field, value in original.items():
            if isinstance(value, (dict, list)):
                value = canonical(value).decode()
            if value == "" or value is None:
                row[field] = None
            elif field == "Code" and name not in {"indices", OPTIONS_225}:
                row[field] = code(value)
            elif field == spec.date or field in {
                "PubDate",
                "DiscDate",
                "CalcDate",
                "AppDate",
                "StDate",
                "EnDate",
                "SchDate",
                "CurPerSt",
                "CurPerEn",
                "CurFYSt",
                "CurFYEn",
                "NxtFYSt",
                "NxtFYEn",
                "LTD",
                "SQD",
            }:
                row[field] = date.fromisoformat(iso(value))
            elif is_numeric(name, field):
                if value in {"-", "*"}:
                    row[field] = None
                else:
                    row[field] = float(value)
            else:
                row[field] = str(value)
        row["_row_hash"] = digest(canonical(original))
        row["_observed_at"] = observed
        for key in spec.keys:
            if row.get(key) is None:
                raise ValueError(f"Missing primary key {name}.{key}")
        out.append(row)
    return out


def table(name, rows):
    fields = sorted({k for row in rows for k in row} | set(DATASETS[name].keys))
    schema = pa.schema(
        [
            (
                f,
                pa.date32()
                if f == DATASETS[name].date
                or f
                in {
                    "PubDate",
                    "DiscDate",
                    "CalcDate",
                    "AppDate",
                    "StDate",
                    "EnDate",
                    "SchDate",
                    "CurPerSt",
                    "CurPerEn",
                    "CurFYSt",
                    "CurFYEn",
                    "NxtFYSt",
                    "NxtFYEn",
                    "LTD",
                    "SQD",
                }
                else pa.float64()
                if is_numeric(name, f)
                else pa.string(),
            )
            for f in fields
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def write_parquet(path, arrow):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    pq.write_table(arrow, temp, compression="zstd")
    os.replace(temp, path)


class Lake:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.root / "state.json"
        self.state = (
            json.loads(self.state_path.read_text())
            if self.state_path.exists()
            else {"requests": {}, "bulk": {}}
        )
        marker = self.root / ".remote.json"
        remote = json.loads(marker.read_text()) if marker.exists() else {}
        self.remote_raw = remote.get("files", {}) if remote.get("preserve_raw") else {}

    def save(self):
        atomic(self.state_path, canonical(self.state))

    def raw(self, name, payload, rows, observed, kind="json"):
        sha = digest(payload)
        path = self.root / "raw" / name / f"{sha}.{kind}.gz"
        relative = path.relative_to(self.root).as_posix()
        if relative in self.remote_raw:
            companion = path.with_suffix(".parquet").relative_to(self.root).as_posix()
            if companion not in self.remote_raw:
                raise ValueError("Stored Raw companion missing from manifest")
            compressed = gzip.compress(payload, mtime=0)
            if digest(compressed) != self.remote_raw[relative]:
                # Compression versions can differ; verify the saved bytes and payload.
                from .storage import Storage

                stored = Storage(self.root).restore_file(relative).read_bytes()
                if gzip.decompress(stored) != payload:
                    raise ValueError("Stored Raw payload mismatch")
            return relative
        if not path.exists():
            atomic(path, gzip.compress(payload, mtime=0))
            raw = pa.table(
                {
                    "payload_json": [canonical(r).decode() for r in rows],
                    "_observed_at": [observed] * len(rows),
                },
                schema=pa.schema([("payload_json", pa.string()), ("_observed_at", pa.string())]),
            )
            write_parquet(path.with_suffix(".parquet"), raw)
        return path.relative_to(self.root).as_posix()

    def files(self, name):
        return sorted((self.root / "clean" / name).glob("*.parquet"))

    def replace(self, name, rows, dates=None, codes=None):
        """Replace complete query scopes, rather than append or silently mask conflicting duplicates."""
        spec = DATASETS[name]
        dates = (
            {date.fromisoformat(iso(d)) for d in dates} if dates is not None else {r[spec.date] for r in rows}
        )
        months = {d.strftime("%Y-%m") for d in dates}
        for month in months:
            path = self.root / "clean" / name / f"{month}.parquet"
            old = pq.read_table(path).to_pylist() if path.exists() else []
            retained = [
                r for r in old if not (r[spec.date] in dates and (codes is None or r.get("Code") in codes))
            ]
            new = [r for r in rows if r[spec.date].strftime("%Y-%m") == month]
            combined = retained + new
            dedup = {}
            for r in combined:
                key = tuple(r[k] for k in spec.keys)
                if key in dedup and dedup[key]["_row_hash"] != r["_row_hash"]:
                    raise ValueError(f"Conflicting duplicate in {name}: {key}")
                dedup[key] = r
            ordered = sorted(dedup.values(), key=lambda r: tuple(str(r[k]) for k in spec.keys))
            # Empty parquet still carries keys; known optional columns unified in DuckDB.
            write_parquet(path, table(name, ordered))

    def log(self, event):
        path = self.root / "logs" / f"{datetime.now(UTC).strftime('%Y-%m-%d')}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"at": now(), **event}, ensure_ascii=False, default=str) + "\n")

