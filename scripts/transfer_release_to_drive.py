#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-Click Transfer & Reassembly from GitHub Release (v1.0-ibb-map) to Google Drive
Run this in Google Colab or via your AI Agent after 24-48 hours.
"""

import glob
import json
import os
import shutil
import subprocess
import urllib.request

GITHUB_REPO = "mhommdkuyr/ibb-map-gis-downloader"
RELEASE_TAG = "v1.0-ibb-map"
DRIVE_TARGET_DIR = "/content/drive/MyDrive/Ibb_Map_Exports"


def sync_release_to_drive():
    os.makedirs(DRIVE_TARGET_DIR, exist_ok=True)
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/tags/{RELEASE_TAG}"
    req = urllib.request.Request(api_url, headers={"User-Agent": "Ibb-GIS-Sync"})
    with urllib.request.urlopen(req) as resp:
        release_data = json.loads(resp.read().decode("utf-8"))

    assets = release_data.get("assets", [])
    print(f"🔍 Found {len(assets)} assets in Release {RELEASE_TAG}.")

    for asset in assets:
        name = asset["name"]
        url = asset["browser_download_url"]
        size_mb = asset["size"] / (1024 * 1024)
        dest_path = os.path.join(DRIVE_TARGET_DIR, name)

        if os.path.exists(dest_path) and os.path.getsize(dest_path) == asset["size"]:
            print(f"⏭️ Skipping {name} (already exists on Drive)")
            continue

        print(f"⬇️ Downloading {name} ({size_mb:.1f} MB) directly to Google Drive...")
        subprocess.check_call(["curl", "-L", "-C", "-", "-o", dest_path, url])

    # Reassemble any multi-part files (>1950MB) back into a single uncut file
    part_000_files = sorted(glob.glob(os.path.join(DRIVE_TARGET_DIR, "*.part_000")))
    for p0 in part_000_files:
        base_file = p0[: -len(".part_000")]
        parts = sorted(glob.glob(f"{base_file}.part_*"))
        print(f"🔗 Reassembling {len(parts)} parts into full uncut file: {os.path.basename(base_file)} ...")
        with open(base_file, "wb") as out_f:
            for part_p in parts:
                with open(part_p, "rb") as in_f:
                    shutil.copyfileobj(in_f, out_f, length=16 * 1024 * 1024)
        for part_p in parts:
            os.remove(part_p)
        print(f"✅ Reassembled full file: {base_file}")


if __name__ == "__main__":
    sync_release_to_drive()
