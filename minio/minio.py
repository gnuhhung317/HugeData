"""Minimal MinIO helper using the official minio-py SDK.

This script provides a small CLI to create buckets, upload files, list
objects and download objects from a MinIO server. It reads connection
settings from environment variables with sensible defaults so it can be
used both locally and in Kubernetes (where credentials come from secrets).

Environment variables used:
  MINIO_ENDPOINT         (default: localhost:9000)
  MINIO_ACCESS_KEY_ID    (default: minioadmin)
  MINIO_SECRET_ACCESS_KEY(default: minioadmin)
  MINIO_SECURE           (set to '1' or 'true' to use TLS; default: false)
  MINIO_BUCKET           (default: hugedata)

Example:
  python minio.py upload --file ./example.jpg --object cameras/1.jpg
  python minio.py list --prefix cameras/
  python minio.py download --object cameras/1.jpg --dest /tmp/1.jpg
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Iterable

from minio import Minio
from minio.error import S3Error

LOG = logging.getLogger("minio_helper")


def get_client(endpoint: str | None = None,
			   access_key: str | None = None,
			   secret_key: str | None = None,
			   secure: bool | None = None) -> Minio:
	endpoint = endpoint or os.environ.get("MINIO_ENDPOINT", "localhost:9000")
	access_key = access_key or os.environ.get("MINIO_ACCESS_KEY_ID", "minioadmin")
	secret_key = secret_key or os.environ.get("MINIO_SECRET_ACCESS_KEY", "minioadmin")
	if secure is None:
		secure = os.environ.get("MINIO_SECURE", "0").lower() in ("1", "true", "yes")

	LOG.debug("Creating MinIO client for %s (secure=%s)", endpoint, secure)
	return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def ensure_bucket(client: Minio, bucket: str) -> None:
	try:
		if not client.bucket_exists(bucket):
			LOG.info("Creating bucket %s", bucket)
			client.make_bucket(bucket)
		else:
			LOG.debug("Bucket %s already exists", bucket)
	except S3Error as exc:
		LOG.error("Error checking/creating bucket %s: %s", bucket, exc)
		raise


def upload_file(client: Minio, bucket: str, object_name: str, file_path: str) -> None:
	path = Path(file_path)
	if not path.exists():
		raise FileNotFoundError(f"File not found: {file_path}")
	LOG.info("Uploading %s to %s/%s", file_path, bucket, object_name)
	client.fput_object(bucket, object_name, str(path))


def list_objects(client: Minio, bucket: str, prefix: str | None = None) -> Iterable[str]:
	LOG.info("Listing objects in %s (prefix=%s)", bucket, prefix)
	for obj in client.list_objects(bucket, prefix=prefix or "", recursive=True):
		yield obj.object_name


def download_file(client: Minio, bucket: str, object_name: str, dest_path: str) -> None:
	dest = Path(dest_path)
	dest.parent.mkdir(parents=True, exist_ok=True)
	LOG.info("Downloading %s/%s to %s", bucket, object_name, dest)
	client.fget_object(bucket, object_name, str(dest))


def _build_parser() -> argparse.ArgumentParser:
	p = argparse.ArgumentParser(description="MinIO helper using minio-py")
	p.add_argument("--endpoint", help="MinIO endpoint (host:port)")
	p.add_argument("--access-key", help="Access key")
	p.add_argument("--secret-key", help="Secret key")
	p.add_argument("--secure", action="store_true", help="Use TLS to connect to MinIO")
	p.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "hugedata"), help="Bucket name")

	sub = p.add_subparsers(dest="cmd", required=True)

	up = sub.add_parser("upload", help="Upload a file")
	up.add_argument("--file", required=True, help="Local file to upload")
	up.add_argument("--object", required=True, help="Object name in bucket")

	dl = sub.add_parser("download", help="Download an object")
	dl.add_argument("--object", required=True, help="Object name in bucket")
	dl.add_argument("--dest", required=True, help="Local destination path")

	ls = sub.add_parser("list", help="List objects")
	ls.add_argument("--prefix", default="", help="Prefix filter for objects")

	cb = sub.add_parser("create-bucket", help="Create the bucket if it does not exist")

	return p


def main(argv: list[str] | None = None) -> int:
	logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
	parser = _build_parser()
	args = parser.parse_args(argv)

	client = get_client(args.endpoint, args.access_key, args.secret_key, args.secure)
	bucket = args.bucket

	try:
		if args.cmd == "create-bucket":
			ensure_bucket(client, bucket)
			LOG.info("Bucket ready: %s", bucket)
		elif args.cmd == "upload":
			ensure_bucket(client, bucket)
			upload_file(client, bucket, args.object, args.file)
		elif args.cmd == "download":
			download_file(client, bucket, args.object, args.dest)
		elif args.cmd == "list":
			for name in list_objects(client, bucket, args.prefix):
				print(name)
		else:
			parser.print_help()
			return 1
	except S3Error as e:
		LOG.error("MinIO error: %s", e)
		return 2
	except Exception as e:
		LOG.error("Error: %s", e)
		return 3

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
