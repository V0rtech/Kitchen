#!/usr/bin/env python3
"""
upload_to_r2.py
Uploads all campaign images to Cloudflare R2, preserving folder structure.

Setup:
  pip install boto3

Fill in the CONFIG block below, then run:
  python upload_to_r2.py
"""

import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# ─── CONFIG ───────────────────────────────────────────────────────────────────
R2_ACCOUNT_ID   = "YOUR_ACCOUNT_ID"         # Cloudflare dashboard → R2 → Overview
R2_ACCESS_KEY   = "YOUR_R2_ACCESS_KEY_ID"   # R2 API token Access Key ID
R2_SECRET_KEY   = "YOUR_R2_SECRET_KEY"      # R2 API token Secret Access Key
R2_BUCKET       = "kitchen-ads"             # Your R2 bucket name
R2_PUBLIC_URL   = "https://pub-XXXX.r2.dev" # Bucket public URL (Settings → Public access)
# ──────────────────────────────────────────────────────────────────────────────

CAMPAIGNS_DIR = Path("campaigns")
EXTENSIONS    = {".jpg", ".jpeg", ".png", ".webp", ".mp4"}


def make_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


CONTENT_TYPES = {
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".webp": "image/webp",
    ".mp4":  "video/mp4",
}


def upload_file(client, local_path: Path, key: str) -> bool:
    ext = local_path.suffix.lower()
    ct  = CONTENT_TYPES.get(ext, "application/octet-stream")
    try:
        client.upload_file(
            str(local_path),
            R2_BUCKET,
            key,
            ExtraArgs={"ContentType": ct},
        )
        return True
    except ClientError as e:
        print(f"  ERROR: {e}", file=sys.stderr)
        return False


def already_uploaded(client, key: str) -> bool:
    try:
        client.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def main():
    if "YOUR_ACCOUNT_ID" in R2_ACCOUNT_ID:
        print("Fill in the CONFIG block in upload_to_r2.py before running.")
        sys.exit(1)

    client = make_client()

    files = [
        p for p in CAMPAIGNS_DIR.rglob("*")
        if p.is_file() and p.suffix.lower() in EXTENSIONS
    ]

    print(f"Found {len(files)} files to upload to R2 bucket '{R2_BUCKET}'")
    print(f"Public URL base: {R2_PUBLIC_URL}\n")

    uploaded = skipped = failed = 0

    for i, path in enumerate(files, 1):
        # Use forward slashes for the R2 key, lowercase the campaign dir name
        key = path.as_posix()  # e.g. campaigns/caraway/images/1234.jpg

        if already_uploaded(client, key):
            skipped += 1
            if i % 50 == 0:
                print(f"  [{i}/{len(files)}] {skipped} skipped, {uploaded} uploaded, {failed} failed")
            continue

        print(f"  [{i}/{len(files)}] Uploading {key}...", end=" ", flush=True)
        if upload_file(client, path, key):
            uploaded += 1
            print("OK")
        else:
            failed += 1
            print("FAILED")

    print(f"\nDone. {uploaded} uploaded, {skipped} skipped, {failed} failed.")
    print(f"\nSet R2_PUBLIC_URL = \"{R2_PUBLIC_URL}\" in gallery.html")


if __name__ == "__main__":
    main()
