
import os
import re
import json
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_batch


SNAP_RE = re.compile(r"^\d{10}$")  # YYMMDDHHMM e.g. 2509021200


# -----------------------
# time + station name
# -----------------------
def parse_yymmddhhmm(s: str) -> datetime:
    s = str(s).strip()
    if len(s) != 10 or not s.isdigit():
        raise ValueError(f"Invalid YYMMDDHHMM: {s}")
    yy = 2000 + int(s[0:2])
    mm = int(s[2:4])
    dd = int(s[4:6])
    hh = int(s[6:8])
    mi = int(s[8:10])
    return datetime(yy, mm, dd, hh, mi)


def time_key(dt: datetime) -> int:
    return int(dt.strftime("%Y%m%d%H%M"))


def norm_name(x: str) -> str:
    x = (x or "").strip().lower()
    x = x.replace("_", " ")
    x = re.sub(r"\s+", " ", x)
    return x


def station_from_root_or_filename(root: ET.Element, filename: str) -> str:
    st = root.attrib.get("station")
    if st:
        return st
    base = os.path.basename(filename)
    base = re.sub(r"_timetable\.xml$", "", base, flags=re.IGNORECASE)
    return base.replace("_", " ")


# -----------------------
# Iteration: real folder structure
# -----------------------
def iter_snapshot_dirs(week_dir: str):
    # recursively find folders named exactly 10 digits
    for entry in sorted(os.listdir(week_dir)):
        path = os.path.join(week_dir, entry)
        if not os.path.isdir(path):
            continue
        if SNAP_RE.match(entry):
            yield entry, path
        else:
            # wrapper folder (if any)
            yield from iter_snapshot_dirs(path)


def iter_xml_files(snapshot_path: str):
    for fn in sorted(os.listdir(snapshot_path)):
        if fn.lower().endswith(".xml"):
            yield fn, os.path.join(snapshot_path, fn)


# -----------------------
# Station mapping from station_data.json
# -----------------------
def build_station_maps(station_json_path: str):
    """
    Returns:
      eva_to_station: {eva:int -> (name, lat, lon)}
      name_to_eva: {normalized_name -> eva}
    """
    data = json.load(open(station_json_path, "r", encoding="utf-8"))
    eva_to_station = {}
    name_to_eva = {}

    for s in data["result"]:
        name = s["name"]
        evas = s.get("evaNumbers", [])
        if not evas:
            continue

        main = next((e for e in evas if e.get("isMain") is True), evas[0])
        eva = int(main["number"])

        lon = lat = None
        coords = main.get("geographicCoordinates", {}).get("coordinates")
        if coords and len(coords) == 2:
            lon, lat = float(coords[0]), float(coords[1])

        eva_to_station[eva] = (name, lat, lon)

        # robust aliases
        n = norm_name(name)
        name_to_eva[n] = eva
        if n.startswith("berlin "):
            name_to_eva[n[len("berlin "):]] = eva
        if not n.startswith("berlin"):
            name_to_eva["berlin " + n] = eva

    return eva_to_station, name_to_eva


# -----------------------
# SQL
# -----------------------
SQL_UPSERT_STATION = """
INSERT INTO dim_station (eva, station_name, lat, lon)
VALUES (%s, %s, %s, %s)
ON CONFLICT (eva) DO UPDATE
SET station_name = EXCLUDED.station_name,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon;
"""

SQL_UPSERT_TIME = """
INSERT INTO dim_time (time_key, ts, date, hour, minute, dow, is_weekend)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (time_key) DO NOTHING;
"""

SQL_UPSERT_TRAIN = """
INSERT INTO dim_train (category, train_number, owner, trip_type, filter_flags)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (category, train_number, owner, trip_type, filter_flags) DO NOTHING;
"""

SQL_SELECT_TRAIN_KEY = """
SELECT train_key
FROM dim_train
WHERE category=%s AND train_number=%s AND owner=%s AND trip_type=%s AND filter_flags=%s;
"""

SQL_UPSERT_FACT_PLANNED = """
INSERT INTO fact_train_movement (
  station_key, train_key, snapshot_time_key,
  stop_id, event_type,
  planned_time_key, changed_time_key,
  event_status, planned_platform, changed_platform,
  line, planned_path, delay_minutes, is_cancelled
)
VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,%s,NULL,%s,%s,NULL,false)
ON CONFLICT (snapshot_time_key, station_key, stop_id, event_type)
DO UPDATE SET
  train_key = EXCLUDED.train_key,
  planned_time_key = COALESCE(EXCLUDED.planned_time_key, fact_train_movement.planned_time_key),
  planned_platform = COALESCE(EXCLUDED.planned_platform, fact_train_movement.planned_platform),
  line = COALESCE(EXCLUDED.line, fact_train_movement.line),
  planned_path = COALESCE(EXCLUDED.planned_path, fact_train_movement.planned_path);
"""


def upsert_time(cur, dt: datetime, cache: set[int]) -> int:
    k = time_key(dt)
    if k in cache:
        return k
    cur.execute(SQL_UPSERT_TIME, (
        k, dt, dt.date(), dt.hour, dt.minute, dt.isoweekday(), (dt.weekday() >= 5)
    ))
    cache.add(k)
    return k


def get_train_key(cur, tl: dict, train_cache: dict) -> int:
    # ensure stable UNIQUE key even if missing
    category = tl.get("c") or "UNK"
    number   = tl.get("n") or "UNK"
    owner    = tl.get("o") or ""
    ttype    = tl.get("t") or ""
    flags    = tl.get("f") or ""

    key = (category, number, owner, ttype, flags)
    if key in train_cache:
        return train_cache[key]

    cur.execute(SQL_UPSERT_TRAIN, key)
    cur.execute(SQL_SELECT_TRAIN_KEY, key)
    train_key = cur.fetchone()[0]
    train_cache[key] = train_key
    return train_key


# -----------------------
# Main ingestion: planned timetables
# -----------------------
def ingest_timetables(cur, week_timetable_dir: str, station_json_path: str, batch_size: int = 500):
    eva_to_station, name_to_eva = build_station_maps(station_json_path)

    # 1) upsert dim_station
    station_rows = [(eva, name, lat, lon) for eva, (name, lat, lon) in eva_to_station.items()]
    execute_batch(cur, SQL_UPSERT_STATION, station_rows, page_size=200)

    # 2) build eva -> station_key cache
    cur.execute("SELECT eva, station_key FROM dim_station;")
    station_key_by_eva = {int(e): int(k) for (e, k) in cur.fetchall()}

    time_cache: set[int] = set()
    train_cache: dict[tuple, int] = {}

    snap_count = 0
    xml_count = 0
    inserted_rows = 0
    skipped_station = 0

    for snapshot_str, snapshot_path in iter_snapshot_dirs(week_timetable_dir):
        snap_dt = parse_yymmddhhmm(snapshot_str)
        snap_key = upsert_time(cur, snap_dt, time_cache)

        rows = []
        # iterate all station xmls in this snapshot
        for fn, fp in iter_xml_files(snapshot_path):
            xml_count += 1
            try:
                with open(fp, "rb") as f:
                    root = ET.fromstring(f.read())
            except Exception:
                # malformed xml; skip (or log)
                continue

            station_name = station_from_root_or_filename(root, fn)
            eva = name_to_eva.get(norm_name(station_name))
            if eva is None:
                skipped_station += 1
                continue
            station_key = station_key_by_eva.get(int(eva))
            if station_key is None:
                skipped_station += 1
                continue

            for s in root.findall("s"):
                stop_id = s.attrib.get("id")
                tl_node = s.find("tl")
                tl = tl_node.attrib if tl_node is not None else {}
                train_key = get_train_key(cur, tl, train_cache)

                for tag, etype in (("ar", "A"), ("dp", "D")):
                    ev = s.find(tag)
                    if ev is None:
                        continue

                    pt = ev.attrib.get("pt")
                    pt_key = upsert_time(cur, parse_yymmddhhmm(pt), time_cache) if pt else None

                    rows.append((
                        station_key,
                        train_key,
                        snap_key,
                        stop_id,
                        etype,
                        pt_key,
                        ev.attrib.get("pp"),
                        ev.attrib.get("l"),
                        ev.attrib.get("ppth"),
                    ))

                    if len(rows) >= batch_size:
                        execute_batch(cur, SQL_UPSERT_FACT_PLANNED, rows, page_size=200)
                        inserted_rows += len(rows)
                        rows.clear()

        if rows:
            execute_batch(cur, SQL_UPSERT_FACT_PLANNED, rows, page_size=200)
            inserted_rows += len(rows)

        # commit once per snapshot folder 
        cur.connection.commit()
        snap_count += 1
        print(f"[snapshot {snapshot_str}] committed. total_snapshots={snap_count}, total_xml={xml_count}, total_fact_rows_upserted~={inserted_rows}, skipped_station={skipped_station}")

    print(f"[DONE] snapshots={snap_count}, xml_files={xml_count}, fact_rows_upserted~={inserted_rows}, skipped_station={skipped_station}")


def main():
    ap = argparse.ArgumentParser(description="Ingest planned DB timetables into PostgreSQL (Task 1.2 planned only).")
    ap.add_argument("--week-dir", required=True,
                    help="Path to weekly timetable folder, e.g. .../timetables/250902_250909_timetable")
    ap.add_argument("--station-json", required=True,
                    help="Path to station_data.json")
    ap.add_argument("--batch-size", type=int, default=500)

    # DB config (args override env)
    ap.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    ap.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5432")))
    ap.add_argument("--pg-db",   default=os.getenv("PGDATABASE", ""), required=False)
    ap.add_argument("--pg-user", default=os.getenv("PGUSER", ""), required=False)
    ap.add_argument("--pg-password", default=os.getenv("PGPASSWORD", ""), required=False)

    args = ap.parse_args()

    if not args.pg_db or not args.pg_user:
        raise SystemExit("Missing DB config: provide --pg-db and --pg-user (or set env PGDATABASE/PGUSER).")

    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        ingest_timetables(cur, args.week_dir, args.station_json, batch_size=args.batch_size)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

