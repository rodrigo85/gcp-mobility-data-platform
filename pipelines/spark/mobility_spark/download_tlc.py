"""Download one TLC month idempotently and atomically. Standard library only.

Runs in the Spark container (the Airflow scheduler mounts the project
read-only). Skips the download if the local file already has the published
size; writes to .part and renames, so a half-written file is never left behind.

    python download_tlc.py --month 2025-01 --dest data/landing/yellow
"""

from __future__ import annotations

import argparse
import shutil
import urllib.request
from pathlib import Path

TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"


def published_size(url: str) -> int:
    request = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(request, timeout=30) as resp:
        return int(resp.headers["Content-Length"])


def download(month: str, dest: Path) -> Path:
    url = TLC_URL.format(month=month)
    target = dest / f"yellow_tripdata_{month}.parquet"
    expected = published_size(url)
    if target.exists() and target.stat().st_size == expected:
        print(f"already downloaded: {target} ({expected} bytes)")
        return target

    dest.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".part")
    with urllib.request.urlopen(url, timeout=120) as resp, partial.open("wb") as fh:
        shutil.copyfileobj(resp, fh, length=8 * 1024 * 1024)
    if partial.stat().st_size != expected:
        partial.unlink()
        raise OSError(f"incomplete download from {url}")
    partial.replace(target)
    print(f"downloaded: {target} ({expected} bytes)")
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="Download one TLC month")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--dest", required=True, type=Path)
    args = parser.parse_args()
    download(args.month, args.dest)


if __name__ == "__main__":
    main()
