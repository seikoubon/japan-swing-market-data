import json

import boto3
import pytest
from moto import mock_aws

from japan_swing_market.storage import Storage


@mock_aws
def test_private_snapshot_roundtrip_checksums_and_optimistic_lock(tmp_path):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="private-data")
    one = Storage(tmp_path / "one", client=s3, bucket="private-data")
    two = Storage(tmp_path / "two", client=s3, bucket="private-data")
    with pytest.raises(RuntimeError):
        one.pull()
    one.pull(allow_empty=True)
    (one.root / "state.json").write_text('{"test":1}')
    assert one.push() == 1
    assert two.pull() == 1
    assert (two.root / "state.json").read_text() == '{"test":1}'
    one.pull()
    (one.root / "state.json").write_text('{"test":2}')
    one.push()
    (two.root / "state.json").write_text('{"test":3}')
    with pytest.raises(RuntimeError, match="concurrent"):
        two.push()
    three = Storage(tmp_path / "three", client=s3, bucket="private-data")
    three.pull()
    assert json.loads((three.root / "state.json").read_text()) == {"test": 2}


@mock_aws
def test_failed_parallel_upload_preserves_previous_snapshot(tmp_path):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="private-data")
    storage = Storage(tmp_path / "one", client=s3, bucket="private-data")
    storage.pull(allow_empty=True)
    (storage.root / "state.json").write_text('{"v":1}')
    storage.push()
    original = s3.get_object(Bucket="private-data", Key=storage.key("latest.json"))["Body"].read()
    (storage.root / "state.json").write_text('{"v":2}')
    real_put = s3.put_object

    def fail(**kwargs):
        if "/blobs/" in kwargs["Key"]:
            raise RuntimeError("injected upload failure")
        return real_put(**kwargs)

    s3.put_object = fail
    with pytest.raises(RuntimeError, match="injected"):
        storage.push()
    s3.put_object = real_put
    assert s3.get_object(Bucket="private-data", Key=storage.key("latest.json"))["Body"].read() == original
    assert storage.stats()["prefix_bytes"] > 0
