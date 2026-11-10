#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ibb Governorate Complete Z12-Z19 Downloader, Compositor (Satellite + Arabic Labels) & Exporter
Saves full uncut MBTiles, GeoTIFF+TFW+PRJ (WGS84), KMZ, Print PNGs, and XYZ Tile Archives.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import json
import math
import os
import sqlite3
import sys
import tarfile
import time
import zipfile

from PIL import Image
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

Image.MAX_IMAGE_PIXELS = None
MAX_WORKERS = 25  # سرعة عالية آمنة مع توزيع الطلبات على mt0..mt3.google.com

REGIONS = {
    "00_Ibb_Governorate_Full": {
        "code": "00_Ibb_Governorate_Full",
        "name_ar": "محافظة إب كاملة",
        "name_en": "Ibb Governorate Full",
        "tier": "governorate",
        "center_lat": 13.9667,
        "center_lon": 44.1667,
        "north": 14.4500,
        "south": 13.6500,
        "east": 44.8500,
        "west": 43.5500,
        "zooms": [12, 13, 14, 15],
        "stitch_zoom": 14,
    },
    "U01_Ibb_City": {
        "code": "U01_Ibb_City",
        "name_ar": "مدينة إب",
        "name_en": "Ibb City",
        "tier": "urban",
        "center_lat": 13.9667,
        "center_lon": 44.1833,
        "north": 14.0150,
        "south": 13.9250,
        "east": 44.2150,
        "west": 44.1400,
        "zooms": [17, 18, 19],
        "stitch_zoom": 17,
    },
    "U02_Al_Udayn_City": {
        "code": "U02_Al_Udayn_City",
        "name_ar": "مدينة العدين",
        "name_en": "Al Udayn City",
        "tier": "urban",
        "center_lat": 13.9700,
        "center_lon": 43.9900,
        "north": 13.9920,
        "south": 13.9480,
        "east": 44.0150,
        "west": 43.9650,
        "zooms": [17, 18, 19],
        "stitch_zoom": 17,
    },
    "U03_Al_Qaidah_City": {
        "code": "U03_Al_Qaidah_City",
        "name_ar": "مدينة القاعدة",
        "name_en": "Al Qaidah City",
        "tier": "urban",
        "center_lat": 13.7572,
        "center_lon": 44.1388,
        "north": 13.7820,
        "south": 13.7320,
        "east": 44.1620,
        "west": 44.1150,
        "zooms": [17, 18, 19],
        "stitch_zoom": 17,
    },
    "U04_Yarim_City": {
        "code": "U04_Yarim_City",
        "name_ar": "مدينة يريم",
        "name_en": "Yarim City",
        "tier": "urban",
        "center_lat": 14.2965,
        "center_lon": 44.3757,
        "north": 14.3220,
        "south": 14.2720,
        "east": 44.4020,
        "west": 44.3500,
        "zooms": [17, 18, 19],
        "stitch_zoom": 17,
    },
    "U05_Dhi_As_Sufal_City": {
        "code": "U05_Dhi_As_Sufal_City",
        "name_ar": "مدينة ذي السفال",
        "name_en": "Dhi As Sufal City",
        "tier": "urban",
        "center_lat": 13.8355,
        "center_lon": 44.1103,
        "north": 13.8550,
        "south": 13.8150,
        "east": 44.1320,
        "west": 44.0880,
        "zooms": [17, 18, 19],
        "stitch_zoom": 17,
    },
    "U06_Al_Makhadir_City": {
        "code": "U06_Al_Makhadir_City",
        "name_ar": "مدينة المخادر",
        "name_en": "Al Makhadir City",
        "tier": "urban",
        "center_lat": 14.1111,
        "center_lon": 44.1993,
        "north": 14.1320,
        "south": 14.0900,
        "east": 44.2200,
        "west": 44.1780,
        "zooms": [17, 18, 19],
        "stitch_zoom": 17,
    },
    "U07_An_Nadirah_City": {
        "code": "U07_An_Nadirah_City",
        "name_ar": "مدينة النادرة",
        "name_en": "An Nadirah City",
        "tier": "urban",
        "center_lat": 14.1015,
        "center_lon": 44.4795,
        "north": 14.1220,
        "south": 14.0810,
        "east": 44.5020,
        "west": 44.4570,
        "zooms": [17, 18, 19],
        "stitch_zoom": 17,
    },
    "U08_As_Saddah_City": {
        "code": "U08_As_Saddah_City",
        "name_ar": "مدينة السدة",
        "name_en": "As Saddah City",
        "tier": "urban",
        "center_lat": 14.1661,
        "center_lon": 44.3751,
        "north": 14.1860,
        "south": 14.1460,
        "east": 44.3980,
        "west": 44.3520,
        "zooms": [17, 18, 19],
        "stitch_zoom": 17,
    },
}

DISTRICTS_SPECS = [
    ("D01_Ibb", "مديرية إب (ريف إب)", "Ibb District", 13.9667, 44.1833, 0.08, 0.09),
    ("D02_Al_Dhihar", "مديرية الظهار", "Al Dhihar District", 13.9789, 44.1525, 0.05, 0.06),
    ("D03_Al_Mashannah", "مديرية المشنة", "Al Mashannah District", 13.9571, 44.1815, 0.05, 0.06),
    ("D04_Jiblah", "مديرية جبلة", "Jiblah District", 13.9241, 44.1459, 0.07, 0.08),
    ("D05_Badan", "مديرية بعدان", "Ba'dan District", 13.9889, 44.3231, 0.09, 0.10),
    ("D06_As_Sabrah", "مديرية السبرة", "As Sabrah District", 13.9160, 44.3240, 0.09, 0.10),
    ("D07_As_Sayyani", "مديرية السياني", "As Sayyani District", 13.8139, 44.1613, 0.08, 0.09),
    ("D08_Dhi_As_Sufal", "مديرية ذي السفال", "Dhi As Sufal District", 13.8355, 44.1103, 0.09, 0.10),
    ("D09_Mudhaykhirah", "مديرية مذيخرة", "Mudhaykhirah District", 13.8661, 43.9689, 0.09, 0.10),
    ("D10_Al_Udayn", "مديرية العدين", "Al Udayn District", 13.9700, 43.9900, 0.11, 0.12),
    ("D11_Far_Al_Udayn", "مديرية فرع العدين", "Far Al Udayn District", 13.8997, 43.8081, 0.12, 0.13),
    ("D12_Hazm_Al_Udayn", "مديرية حزم العدين", "Hazm Al Udayn District", 14.1146, 43.9045, 0.11, 0.12),
    ("D13_Hubaysh", "مديرية حبيش", "Hubaysh District", 14.1161, 44.0819, 0.09, 0.10),
    ("D14_Al_Makhadir", "مديرية المخادر", "Al Makhadir District", 14.1111, 44.1993, 0.09, 0.10),
    ("D15_Al_Qafr", "مديرية القفر", "Al Qafr District", 14.2500, 44.0800, 0.14, 0.16),
    ("D16_Yarim", "مديرية يريم", "Yarim District", 14.2965, 44.3757, 0.13, 0.14),
    ("D17_Ar_Radmah", "مديرية الرضمة", "Ar Radmah District", 14.2000, 44.5900, 0.11, 0.12),
    ("D18_As_Saddah", "مديرية السدة", "As Saddah District", 14.1661, 44.3751, 0.09, 0.10),
    ("D19_An_Nadirah", "مديرية النادرة", "An Nadirah District", 14.1015, 44.4795, 0.10, 0.11),
    ("D20_Ash_Shair", "مديرية الشعر", "Ash Sha'ir District", 14.0139, 44.3965, 0.08, 0.09),
]

for code, name_ar, name_en, lat, lon, s_lat, s_lon in DISTRICTS_SPECS:
    REGIONS[code] = {
        "code": code,
        "name_ar": name_ar,
        "name_en": name_en,
        "tier": "district",
        "center_lat": lat,
        "center_lon": lon,
        "north": round(lat + s_lat / 2.0, 4),
        "south": round(lat - s_lat / 2.0, 4),
        "east": round(lon + s_lon / 2.0, 4),
        "west": round(lon - s_lon / 2.0, 4),
        "zooms": [15, 16, 17, 18, 19],
        "stitch_zoom": 17,
    }


def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0**zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(int(n) - 1, xtile)), max(0, min(int(n) - 1, ytile))


def num2deg(xtile, ytile, zoom):
    n = 2.0**zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1.0 - 2.0 * ytile / n)))
    return math.degrees(lat_rad), lon_deg


def build_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retries
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    hdrs = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
    }
    s.headers.update(hdrs)
    return s


def fetch_tile_bytes(session, mode, z, x, y):
    srv = (x + y) % 4
    for attempt in range(4):
        try:
            if mode == "hybrid":
                u_sat = f"https://mt{srv}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"
                u_lbl = f"https://mt{(srv+1)%4}.google.com/vt/lyrs=h&hl=ar&gl=YE&x={x}&y={y}&z={z}"
                r_sat = session.get(u_sat, timeout=12)
                r_lbl = session.get(u_lbl, timeout=12)
                if (
                    r_sat.status_code == 200
                    and r_lbl.status_code == 200
                    and len(r_sat.content) > 300
                ):
                    sat_im = Image.open(BytesIO(r_sat.content)).convert("RGBA")
                    lbl_im = Image.open(BytesIO(r_lbl.content)).convert("RGBA")
                    if lbl_im.size != sat_im.size:
                        lbl_im = lbl_im.resize(sat_im.size, Image.Resampling.LANCZOS)
                    merged = Image.alpha_composite(sat_im, lbl_img := lbl_im).convert("RGB")
                    buf = BytesIO()
                    merged.save(buf, format="PNG")
                    return buf.getvalue()

            lyrs = "m" if mode == "roadmap" else "y"
            url = f"https://mt{srv}.google.com/vt/lyrs={lyrs}&hl=ar&gl=YE&x={x}&y={y}&z={z}"
            resp = session.get(url, timeout=12)
            if resp.status_code == 200 and len(resp.content) > 400:
                im = Image.open(BytesIO(resp.content)).convert("RGB")
                buf = BytesIO()
                im.save(buf, format="PNG")
                return buf.getvalue()
            elif resp.status_code in (403, 429):
                time.sleep(6 * (attempt + 1))
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return None


def init_mbtiles(db_path, reg, mode, min_z, max_z):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("CREATE TABLE IF NOT EXISTS metadata (name text PRIMARY KEY, value text);")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS tiles (zoom_level integer, tile_column integer,"
        " tile_row integer, tile_data blob);"
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS tile_index on tiles (zoom_level, tile_column, tile_row);"
    )
    meta = [
        ("name", f"{reg['name_ar']} ({mode.upper()})"),
        ("type", "baselayer"),
        ("version", "1.3"),
        ("description", f"Ibb Governorate Full Resolution {reg['name_en']} - {mode}"),
        ("format", "png"),
        ("bounds", f"{reg['west']},{reg['south']},{reg['east']},{reg['north']}"),
        ("center", f"{reg['center_lon']},{reg['center_lat']},{min_z}"),
        ("minzoom", str(min_z)),
        ("maxzoom", str(max_z)),
    ]
    cur.executemany("INSERT OR REPLACE INTO metadata VALUES (?, ?);", meta)
    conn.commit()
    return conn


def stitch_from_mbtiles(conn, reg, mode, zoom, exports_dir):
    x_min, y_min = deg2num(reg["north"], reg["west"], zoom)
    x_max, y_max = deg2num(reg["south"], reg["east"], zoom)
    cols = x_max - x_min + 1
    rows = y_max - y_min + 1

    # Cap single stitched raster at 16384x16384 so RAM stays safe on GitHub Runner
    if cols * 256 > 16384 or rows * 256 > 16384:
        return

    stitched = Image.new(
        "RGB",
        (cols * 256, rows * 256),
        (255, 255, 255) if mode == "roadmap" else (20, 30, 25),
    )
    cur = conn.cursor()
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tms_y = (1 << zoom) - 1 - y
            cur.execute(
                "SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?;",
                (zoom, x, tms_y),
            )
            row = cur.fetchone()
            if row and row[0]:
                try:
                    t_im = Image.open(BytesIO(row[0])).convert("RGB")
                    stitched.paste(t_im, ((x - x_min) * 256, (y - y_min) * 256))
                except Exception:
                    pass

    base_name = f"{reg['code']}_{mode}_Z{zoom}"
    png_p = os.path.join(exports_dir, f"{base_name}.png")
    tif_p = os.path.join(exports_dir, f"{base_name}.tif")
    tfw_p = os.path.join(exports_dir, f"{base_name}.tfw")
    prj_p = os.path.join(exports_dir, f"{base_name}.prj")
    kmz_p = os.path.join(exports_dir, f"{base_name}.kmz")

    stitched.save(png_p, format="PNG")
    stitched.save(tif_p, format="TIFF", compression="tiff_deflate")

    n_lat, w_lon = num2deg(x_min, y_min, zoom)
    s_lat, e_lon = num2deg(x_max + 1, y_max + 1, zoom)
    px_x = (e_lon - w_lon) / float(cols * 256)
    px_y = (s_lat - n_lat) / float(rows * 256)

    with open(tfw_p, "w", encoding="utf-8") as f:
        f.write(
            f"{px_x:.10f}\r\n0.0000000000\r\n0.0000000000\r\n{px_y:.10f}\r\n"
            f"{w_lon + px_x/2:.8f}\r\n{n_lat + px_y/2:.8f}\r\n"
        )
    with open(prj_p, "w", encoding="utf-8") as f:
        f.write(
            'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],'
            'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'
        )

    kml = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document><name>{reg['name_ar']} - {mode.upper()} Z{zoom}</name>
    <GroundOverlay>
      <name>{reg['name_ar']}</name><Icon><href>files/{base_name}.png</href></Icon>
      <LatLonBox><north>{n_lat:.6f}</north><south>{s_lat:.6f}</south><east>{e_lon:.6f}</east><west>{w_lon:.6f}</west></LatLonBox>
    </GroundOverlay>
  </Document>
</kml>"""
    with zipfile.ZipFile(kmz_p, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml)
        zf.write(png_p, f"files/{base_name}.png")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", required=True)
    parser.add_argument("--max-zoom", type=int, default=19)
    parser.add_argument("--work-dir", default="./workspace")
    parser.add_argument("--max-runtime-minutes", type=int, default=315)
    args = parser.parse_args()

    start_time = time.time()
    reg = REGIONS[args.region]
    exports_dir = os.path.join(args.work_dir, "exports")
    os.makedirs(exports_dir, exist_ok=True)

    zooms = [z for z in reg["zooms"] if z <= args.max_zoom]
    if not zooms:
        zooms = [min(reg["zooms"])]
    all_coords = []
    for z in zooms:
        x1, y1 = deg2num(reg["north"], reg["west"], z)
        x2, y2 = deg2num(reg["south"], reg["east"], z)
        for x in range(x1, x2 + 1):
            for y in range(y1, y2 + 1):
                all_coords.append((z, x, y))

    print(f"🚀 Region: {reg['name_ar']} ({reg['code']}) | Zooms: {zooms} | Tiles/mode: {len(all_coords):,}")
    session = build_session()
    modes_dict = {}
    status_report = {
        "region": reg["code"],
        "name_ar": reg["name_ar"],
        "zooms": zooms,
        "modes": modes_dict
    }

    for mode in ["hybrid", "roadmap"]:
        mbt_path = os.path.join(exports_dir, f"{reg['code']}_{mode}.mbtiles")
        conn = init_mbtiles(mbt_path, reg, mode, min(zooms), max(zooms))
        cur = conn.cursor()

        # Skip already downloaded tiles in MBTiles if resuming
        cur.execute("SELECT zoom_level, tile_column, tile_row FROM tiles;")
        existing = set()
        for rz, rx, rtms_y in cur.fetchall():
            ry = (1 << rz) - 1 - rtms_y
            existing.add((rz, rx, ry))

        pending = [c for c in all_coords if c not in existing]
        print(f"Mode [{mode}]: {len(existing):,} already present, {len(pending):,} pending.")

        ok_count = len(existing)
        fail_count = 0
        batch_rows = []

        # Process in chunks of 1,500 tiles to check runtime budget and commit safely
        chunk_size = 1500
        for i in range(0, len(pending), chunk_size):
            elapsed_min = (time.time() - start_time) / 60.0
            if elapsed_min >= args.max_runtime_minutes:
                print(f"⏱️ Reached safe time limit ({elapsed_min:.1f} min). Saving checkpoint for next scheduled run.")
                break

            chunk = pending[i : i + chunk_size]
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                future_map = {
                    ex.submit(fetch_tile_bytes, session, mode, z, x, y): (z, x, y)
                    for (z, x, y) in chunk
                }
                for fut in as_completed(future_map):
                    z, x, y = future_map[fut]
                    data = fut.result()
                    if data:
                        tms_y = (1 << z) - 1 - y
                        batch_rows.append((z, x, tms_y, sqlite3.Binary(data)))
                        ok_count += 1
                    else:
                        fail_count += 1

            if batch_rows:
                cur.executemany("INSERT OR REPLACE INTO tiles VALUES (?, ?, ?, ?);", batch_rows)
                conn.commit()
                batch_rows.clear()

        if reg["stitch_zoom"] in zooms or len(zooms) == 1:
            stitch_z = reg["stitch_zoom"] if reg["stitch_zoom"] in zooms else zooms[-1]
            stitch_from_mbtiles(conn, reg, mode, stitch_z, exports_dir)

        conn.close()
        status_report["modes"][mode] = {
            "total": len(all_coords),
            "completed": ok_count,
            "failed": fail_count,
            "complete": (ok_count == len(all_coords)),
        }

    status_path = os.path.join(exports_dir, f"{reg['code']}_status.json")
    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(status_report, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
