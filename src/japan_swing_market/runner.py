"""Run the private lake on a public runner with a fixed, non-sensitive output schema."""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from .catalog import INCREMENTAL_DATASETS
from .client import Client
from .daily_update import DailyUpdate
from .lake import incremental_target_date
from .storage import Storage


class ReadOnlyStorage(Storage):
    def __init__(self, root):
        super().__init__(root)
        self.s3 = SimpleNamespace(get_object=self.s3.get_object)

    def push(self):
        raise RuntimeError("Storage writes are disabled for this run")


def run_engine(mode, root):
    current = datetime.now(ZoneInfo("Asia/Tokyo"))
    storage = (ReadOnlyStorage if mode == "incremental-probe" else Storage)(root)
    client = Client()
    try:
        authenticated = any(page["data"] for page in client.pages("/equities/master", {"code": "86970"}))
        if not authenticated:
            raise RuntimeError("Authentication check failed")
        DailyUpdate(storage, client, current.date(), end=incremental_target_date(current),
                    jst_hour=current.hour, mode="probe" if mode == "incremental-probe" else "publish",
                    require_established=True).run()
    finally:
        client.close()


def public_evidence(mode, run):
    """Select only fixed fields and validate their types. Never forward vendor or SDK text."""
    status = run.get("status")
    allowed = {"probed"} if mode == "incremental-probe" else {"succeeded", "unchanged"}
    if status not in allowed or run.get("datasets_checked") != len(INCREMENTAL_DATASETS):
        raise ValueError("Market update evidence is incomplete")
    preflight = run["preflight_checks"]
    snapshot_verified = all(preflight.get(key) is True for key in (
        "quality", "data_signature_matches", "quality_sha_matches",
    )) and not preflight["derived_stale"] and not preflight["incremental_pending"]
    published = run.get("published_snapshot") == "passed"
    if (status == "succeeded" and not published) or (status in {"probed", "unchanged"} and not snapshot_verified):
        raise ValueError("Snapshot verification did not pass")
    # This date comes from the job clock, not from market records or catalog entries.
    target_day = datetime.strptime(run["end"], "%Y-%m-%d").date().isoformat()
    return {
        "status": status,
        "authenticated": True,
        "target_date_jst": target_day,
        "datasets_checked": len(INCREMENTAL_DATASETS),
        "previous_snapshot_verified": snapshot_verified,
        "catalog_changes_detected": bool(run["pending_files"]),
        "published_snapshot_verified": published,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("daily", "incremental-probe"))
    parser.add_argument("--engine-root", type=Path)
    args = parser.parse_args()
    if args.engine_root is not None:
        run_engine(args.mode, args.engine_root)
        return 0
    try:
        with TemporaryDirectory(prefix="market-lake-", dir=os.environ.get("RUNNER_TEMP")) as work:
            base = Path(work)
            attempts = 3 if args.mode == "daily" else 1
            for attempt in range(attempts):
                result = subprocess.run(
                    [sys.executable, "-m", __spec__.name, args.mode, "--engine-root", str(base)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
                )
                if result.returncode == 0:
                    break
                if attempt + 1 < attempts:
                    time.sleep(45)
            else:
                raise RuntimeError("Market job failed")
            run = json.loads((base / "reports/incremental-update.json").read_bytes())
            print(json.dumps(public_evidence(args.mode, run)), flush=True)
    except Exception:
        print("Market job failed. Check the configured access and private snapshot.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
