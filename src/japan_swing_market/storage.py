"""Private S3/R2 snapshots: immutable blobs + atomic manifest publication with optimistic locking."""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

from .lake import atomic, canonical, digest, now


class Storage:
    def __init__(self, root, client=None, bucket=None, prefix=None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.bucket = bucket or os.environ.get("SWING_S3_BUCKET")
        self.prefix = (prefix or os.environ.get("SWING_S3_PREFIX", "japan-swing/v1")).strip("/")
        if not self.bucket:
            raise ValueError("SWING_S3_BUCKET is not configured")
        self.workers = max(1, min(16, int(os.environ.get("SWING_TRANSFER_WORKERS", "8"))))
        self.metrics = {}
        self.s3 = client or boto3.session.Session().client(
            "s3",
            endpoint_url=os.environ.get("SWING_S3_ENDPOINT") or None,
            region_name=os.environ.get("AWS_DEFAULT_REGION", "auto"),
            config=Config(max_pool_connections=self.workers, retries={"max_attempts": 5, "mode": "standard"}),
        )
        self.marker = self.root / ".remote.json"

    def progress(self, operation, total):
        lock, state, started = Lock(), {"files": 0, "bytes": 0}, time.monotonic()

        def completed(size):
            with lock:
                state["files"] += 1
                state["bytes"] += size
                if os.environ.get("SWING_TRANSFER_PROGRESS") == "true" and (
                    state["files"] % 100 == 0 or state["files"] == total
                ):
                    print(
                        json.dumps(
                            {
                                "event": "transfer_progress",
                                "operation": operation,
                                "total_files": total,
                                **state,
                                "seconds": time.monotonic() - started,
                            }
                        ),
                        flush=True,
                    )

        return completed

    def key(self, suffix):
        return f"{self.prefix}/{suffix}"

    def open_snapshot(self, allow_empty=False):
        """Pin the manifest before choosing which blobs to restore."""
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.key("latest.json"))
        except ClientError as e:
            if e.response["Error"]["Code"] in {"NoSuchKey", "404"} and allow_empty:
                atomic(self.marker, canonical({"etag": None, "files": {}}))
                return {"etag": None, "files": {}, "preserve_raw": True}
            raise RuntimeError(
                "Remote snapshot unavailable; check bucket/configuration. Initialize via smoke first."
            ) from None
        body = response["Body"].read()
        self.manifest_bytes = len(body)
        snapshot = json.loads(body)
        marker = {"etag": response["ETag"], "files": snapshot["files"], "preserve_raw": True}
        # Validate every path, including deliberately deferred blobs.
        for relative in marker["files"]:
            self.safe_path(relative)
        atomic(self.marker, canonical(marker))
        self.metrics = {"operation": "pull", "files": 0, "bytes": 0, "seconds": 0.0}
        return marker

    def safe_path(self, relative):
        path = self.root / relative
        if (Path(relative).is_absolute() or ".." in Path(relative).parts
                or not path.resolve().is_relative_to(self.root.resolve())):
            raise ValueError("Unsafe remote manifest path")
        return path

    def restore(self, relatives):
        """Restore only named members of the pinned snapshot, checking cached bytes too."""
        marker = json.loads(self.marker.read_text())
        started = time.monotonic()
        tasks = {}
        for relative in relatives:
            sha = marker["files"][relative]
            path = self.safe_path(relative)
            if path.exists() and digest(path.read_bytes()) == sha:
                continue
            tasks.setdefault(sha, []).append(path)

        completed = self.progress("pull", len(tasks))

        def restore_blob(task):
            sha, paths = task
            data = self.s3.get_object(Bucket=self.bucket, Key=self.key(f"blobs/{sha}"))["Body"].read()
            if digest(data) != sha:
                raise ValueError("Remote checksum mismatch")
            for path in paths:
                atomic(path, data)
            completed(len(data))
            return len(data)

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            transferred_bytes = sum(pool.map(restore_blob, tasks.items()))
        count = len(tasks)
        self.metrics = {
            "operation": "pull",
            "files": self.metrics.get("files", 0) + count,
            "bytes": self.metrics.get("bytes", 0) + transferred_bytes,
            "seconds": self.metrics.get("seconds", 0) + time.monotonic() - started,
        }
        self.download_metrics = dict(self.metrics)
        return count

    def restore_file(self, relative):
        self.restore([relative])
        return self.safe_path(relative)

    def pull(self, allow_empty=False, defer_raw=False):
        marker = self.open_snapshot(allow_empty)
        count = self.restore(p for p in marker["files"] if not (defer_raw and p.startswith("raw/")))
        marker["preserve_raw"] = defer_raw
        atomic(self.marker, canonical(marker))
        return count

    def push(self):
        if not self.marker.exists():
            raise RuntimeError("Run storage pull before push (optimistic lock required)")
        old = json.loads(self.marker.read_text())
        started = time.monotonic()
        partial = old.get("preserve_raw", False)
        if partial:
            missing = [p for p in old["files"] if not p.startswith("raw/")
                       and not self.safe_path(p).is_file()]
            if missing:
                raise ValueError("Partial publication missing non-Raw files")
        manifest = {p: sha for p, sha in old["files"].items() if partial and p.startswith("raw/")}
        tasks = {}
        for path in sorted(self.root.rglob("*")):
            if not path.is_file() or path.name.startswith(".") or path.name.endswith((".tmp", ".lock")):
                continue
            relative = str(path.relative_to(self.root)).replace("\\", "/")
            data = path.read_bytes()
            sha = digest(data)
            if partial and relative.startswith("raw/") and relative in old["files"]:
                if sha != old["files"][relative]:
                    raise ValueError("Immutable Raw replacement rejected")
            manifest[relative] = sha
            if old["files"].get(relative) != sha:
                tasks[sha] = path

        completed = self.progress("push", len(tasks))

        def upload(task):
            sha, path = task
            data = path.read_bytes()
            if digest(data) != sha:
                raise ValueError("Local file changed during publication")
            self.s3.put_object(Bucket=self.bucket, Key=self.key(f"blobs/{sha}"), Body=data)
            completed(len(data))
            return len(data)

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            transferred_bytes = sum(pool.map(upload, tasks.items()))
        uploaded = len(tasks)
        snapshot = canonical({"version": 1, "created_at": now(), "files": manifest})
        # Keep a historical manifest for recovery; latest.json is the only mutable remote object.
        self.s3.put_object(
            Bucket=self.bucket, Key=self.key(f"snapshots/{digest(snapshot)}.json"), Body=snapshot
        )
        condition = {"IfMatch": old["etag"]} if old["etag"] else {"IfNoneMatch": "*"}
        try:
            response = self.s3.put_object(
                Bucket=self.bucket, Key=self.key("latest.json"), Body=snapshot, **condition
            )
        except ClientError:
            raise RuntimeError(
                "Snapshot publish failed; possible concurrent writer. Pull before retrying."
            ) from None
        atomic(self.marker, canonical({"etag": response["ETag"], "files": manifest,
                                       "preserve_raw": partial}))
        self.metrics = {
            "operation": "push",
            "files": uploaded,
            "bytes": transferred_bytes,
            "seconds": time.monotonic() - started,
        }
        return uploaded

    def stats(self):
        total_bytes = count = 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=self.prefix + "/"
        ):
            for obj in page.get("Contents", []):
                total_bytes += obj["Size"]
                count += 1
        return {"prefix_objects": count, "prefix_bytes": total_bytes}

