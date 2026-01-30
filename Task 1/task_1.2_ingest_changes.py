
"""
Ingest DB timetable_changes (15-min snapshots) into my DIA schema.

What I do here:
- Each snapshot folder name (YYMMDDHHMM) is the snapshot timestamp.
- For each station XML inside that folder, I use root @eva to map to dim_station.
- I upsert (insert/update) fact_train_movement by the natural key:
  (snapshot_time_key, station_key, stop_id, event_type)
"""

import os
import re
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_batch

SNAP_RE = re.compile(r"^\d{10}$")  # YYMMDDHHMM, e.g. 2509021615



# Time helpers
def parse_yymmddhhmm(s: str) -> datetime:
    s = (s or "").strip()
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


def safe_timekey_from_attr(v: str):
    if v and v.isdigit() and len(v) == 10:
        return time_key(parse_yymmddhhmm(v))
    return None


def compute_delay_minutes(pt_key: int | None, ct_key: int | None, dc: str | None):
    # If dc exists and is numeric, it is the most direct delay signal.
    if dc and dc.lstrip("-").isdigit():
        return int(dc)

    if not pt_key or not ct_key:
        return None

    pt_dt = datetime.strptime(str(pt_key), "%Y%m%d%H%M")
    ct_dt = datetime.strptime(str(ct_key), "%Y%m%d%H%M")
    return int(round((ct_dt - pt_dt).total_seconds() / 60.0))



# Folder iteration
def iter_snapshot_dirs(week_dir: str):
    for entry in sorted(os.listdir(week_dir)):
        path = os.path.join(week_dir, entry)
        if os.path.isdir(path) and SNAP_RE.match(entry):
            yield entry, path


def iter_xml_files(snapshot_path: str):
    for fn in sorted(os.listdir(snapshot_path)):
        if fn.lower().endswith(".xml"):
            yield fn, os.path.join(snapshot_path, fn)



# SQL
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

# I prefer explicit columns here (more portable than relying on a constraint name).
# Natural key is (snapshot_time_key, station_key, stop_id, event_type).
SQL_UPSERT_FACT_CHANGES = """
INSERT INTO fact_train_movement (
  station_key, train_key, snapshot_time_key,
  stop_id, event_type,
  planned_time_key, changed_time_key,
  event_status,
  planned_platform, changed_platform,
  line, planned_path,
  delay_minutes, is_cancelled
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (snapshot_time_key, station_key, stop_id, event_type)
DO UPDATE SET
  -- Do NOT overwrite a real train_key with the UNK train.
  train_key = CASE
    WHEN EXCLUDED.train_key = %s THEN fact_train_movement.train_key
    ELSE EXCLUDED.train_key
  END,

  planned_time_key  = COALESCE(fact_train_movement.planned_time_key, EXCLUDED.planned_time_key),
  changed_time_key  = COALESCE(EXCLUDED.changed_time_key, fact_train_movement.changed_time_key),
  event_status      = COALESCE(EXCLUDED.event_status, fact_train_movement.event_status),

  planned_platform  = COALESCE(fact_train_movement.planned_platform, EXCLUDED.planned_platform),
  changed_platform  = COALESCE(EXCLUDED.changed_platform, fact_train_movement.changed_platform),

  line              = COALESCE(EXCLUDED.line, fact_train_movement.line),
  planned_path      = COALESCE(EXCLUDED.planned_path, fact_train_movement.planned_path),

  delay_minutes     = COALESCE(EXCLUDED.delay_minutes, fact_train_movement.delay_minutes),

  -- Once cancelled, keep it cancelled.
  is_cancelled      = (fact_train_movement.is_cancelled OR EXCLUDED.is_cancelled);
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


def ensure_unknown_train(cur) -> int:
    # One shared "UNK" train row, used only when <tl> is missing.
    unk = ("UNK", "UNK", "", "", "")
    cur.execute(SQL_UPSERT_TRAIN, unk)
    cur.execute(SQL_SELECT_TRAIN_KEY, unk)
    return cur.fetchone()[0]


def get_train_key(cur, tl: dict, train_cache: dict, unknown_train_key: int) -> int:
    # Many change XMLs don't have tl at all.
    if not tl:
        return unknown_train_key

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


def ingest_changes(cur, week_changes_dir: str, batch_size: int = 800):
    # EVA -> station_key from DB (dim_station should already have 133 rows)
    cur.execute("SELECT eva, station_key FROM dim_station;")
    station_key_by_eva = {int(e): int(k) for (e, k) in cur.fetchall()}

    time_cache: set[int] = set()
    train_cache: dict[tuple, int] = {}

    unknown_train_key = ensure_unknown_train(cur)

    snap_count = 0
    xml_count = 0
    upserted = 0
    skipped_station = 0
    bad_xml = 0

    for snapshot_str, snapshot_path in iter_snapshot_dirs(week_changes_dir):
        snap_dt = parse_yymmddhhmm(snapshot_str)
        snap_key = upsert_time(cur, snap_dt, time_cache)

        rows = []

        for _, fp in iter_xml_files(snapshot_path):
            xml_count += 1
            try:
                root = ET.parse(fp).getroot()
            except Exception:
                bad_xml += 1
                continue

            eva_attr = root.attrib.get("eva")
            if not eva_attr or not eva_attr.isdigit():
                skipped_station += 1
                continue

            station_key = station_key_by_eva.get(int(eva_attr))
            if station_key is None:
                skipped_station += 1
                continue

            for s in root.findall("s"):
                stop_id = s.attrib.get("id")

                tl_node = s.find("tl")
                tl = tl_node.attrib if tl_node is not None else {}
                train_key = get_train_key(cur, tl, train_cache, unknown_train_key)

                for tag, etype in (("ar", "A"), ("dp", "D")):
                    ev = s.find(tag)
                    if ev is None:
                        continue

                    pt_key = safe_timekey_from_attr(ev.attrib.get("pt"))
                    ct_key = safe_timekey_from_attr(ev.attrib.get("ct"))

                    # Keep time dimension consistent (I insert whatever I see).
                    if pt_key:
                        upsert_time(cur, datetime.strptime(str(pt_key), "%Y%m%d%H%M"), time_cache)
                    if ct_key:
                        upsert_time(cur, datetime.strptime(str(ct_key), "%Y%m%d%H%M"), time_cache)

                    cs = ev.attrib.get("cs")     # p/a/c (often missing)
                    cp = ev.attrib.get("cp")     # changed platform (optional)
                    pp = ev.attrib.get("pp")     # planned platform (sometimes present in changes)
                    line = ev.attrib.get("l")
                    ppth = ev.attrib.get("ppth")
                    dc = ev.attrib.get("dc")     # delay delta in minutes (if present)

                    delay = compute_delay_minutes(pt_key, ct_key, dc)
                    is_cancelled = (cs == "c")

                    rows.append((
                        station_key,
                        train_key,
                        snap_key,
                        stop_id,
                        etype,
                        pt_key,
                        ct_key,
                        cs,
                        pp,
                        cp,
                        line,
                        ppth,
                        delay,
                        is_cancelled
                    ))

                    if len(rows) >= batch_size:
                        # Note: SQL expects unk id at the end for the CASE clause.
                        execute_batch(cur, SQL_UPSERT_FACT_CHANGES,
                                      [r + (unknown_train_key,) for r in rows],
                                      page_size=200)
                        upserted += len(rows)
                        rows.clear()

        if rows:
            execute_batch(cur, SQL_UPSERT_FACT_CHANGES,
                          [r + (unknown_train_key,) for r in rows],
                          page_size=200)
            upserted += len(rows)
            rows.clear()

        # Commit per snapshot: easier to resume / debug.
        cur.connection.commit()
        snap_count += 1
        print(f"[snapshot {snapshot_str}] committed. snapshots={snap_count}, xml={xml_count}, upserted~={upserted}, skipped_station={skipped_station}, bad_xml={bad_xml}")

    print(f"[DONE] snapshots={snap_count}, xml_files={xml_count}, upserted~={upserted}, skipped_station={skipped_station}, bad_xml={bad_xml}")


def main():
    ap = argparse.ArgumentParser(description="Ingest DB timetable_changes (15-min snapshots) into PostgreSQL.")
    ap.add_argument("--week-dir", required=True,
                    help="Path to weekly timetable_changes folder, e.g. .../250902_250909_timetable_changes")
    ap.add_argument("--batch-size", type=int, default=800)

    ap.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    ap.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5432")))
    ap.add_argument("--pg-db", default=os.getenv("PGDATABASE", ""))
    ap.add_argument("--pg-user", default=os.getenv("PGUSER", ""))
    ap.add_argument("--pg-password", default=os.getenv("PGPASSWORD", ""))

    args = ap.parse_args()
    if not args.pg_db or not args.pg_user:
        raise SystemExit("DB config missing. Use --pg-db/--pg-user or set PGDATABASE/PGUSER.")

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
        ingest_changes(cur, args.week_dir, batch_size=args.batch_size)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
