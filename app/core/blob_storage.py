from __future__ import annotations

import time
from functools import lru_cache
from pathlib import Path
from typing import Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.core.settings import settings


class BlobStorage(Protocol):
    def ensure_ready(self) -> None: ...

    def put_file(self, local_path: Path, key: str) -> None: ...

    def get_file(self, key: str, target_path: Path) -> None: ...


class LocalBlobStorage:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir

    def ensure_ready(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def put_file(self, local_path: Path, key: str) -> None:
        target = self.root_dir / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(local_path.read_bytes())

    def get_file(self, key: str, target_path: Path) -> None:
        source = self.root_dir / key
        if not source.exists():
            raise FileNotFoundError(key)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(source.read_bytes())


class S3BlobStorage:
    def __init__(
        self,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        region: str,
        bucket: str,
    ) -> None:
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or None,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
            config=Config(
                connect_timeout=settings.s3_connect_timeout_sec,
                read_timeout=settings.s3_read_timeout_sec,
                retries={"max_attempts": settings.s3_max_retries, "mode": "standard"},
                tcp_keepalive=True,
            ),
        )

    def ensure_ready(self) -> None:
        # MinIO can come up slightly after app start; keep retries small and explicit.
        for _ in range(15):
            try:
                self.client.head_bucket(Bucket=self.bucket)
                return
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code in {"404", "NoSuchBucket"}:
                    self.client.create_bucket(Bucket=self.bucket)
                    return
                time.sleep(1)
            except Exception:
                time.sleep(1)
        # Final attempt: either succeeds or raises a useful exception.
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)

    def put_file(self, local_path: Path, key: str) -> None:
        self.client.upload_file(str(local_path), self.bucket, key)

    def get_file(self, key: str, target_path: Path) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.client.download_file(self.bucket, key, str(target_path))
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(key) from exc
            raise


@lru_cache(maxsize=1)
def get_blob_storage() -> BlobStorage:
    if settings.storage_backend == "s3":
        return S3BlobStorage(
            endpoint_url=settings.s3_endpoint_url,
            access_key_id=settings.s3_access_key_id,
            secret_access_key=settings.s3_secret_access_key,
            region=settings.s3_region,
            bucket=settings.s3_bucket,
        )
    return LocalBlobStorage(settings.blob_dir)
