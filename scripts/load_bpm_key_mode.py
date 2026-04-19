import psycopg2
import csv

DB_CONFIG = {
    "dbname": "overtune_small",
    "user": "pablo",
    "host": "/var/run/postgresql",
    "port": 5432,
}

RHYTHM_CSV = "/home/pablo/acousticbrainz/acousticbrainz-lowlevel-features-20220623/acousticbrainz-lowlevel-features-20220623-rhythm.csv"
TONAL_CSV  = "/home/pablo/acousticbrainz/acousticbrainz-lowlevel-features-20220623/acousticbrainz-lowlevel-features-20220623-tonal.csv"

BATCH_SIZE = 10000

def load_csv_batched(filepath, key_col, value_cols):
    """
    Yields dicts of {mbid: {col: value, ...}} in batches.
    Keeps only submission_offset == 0 (first submission per recording).
    """
    seen = set()
    batch = {}
    with open(filepath, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["submission_offset"] != "0":
                continue
            mbid = row[key_col]
            if mbid in seen:
                continue
            seen.add(mbid)
            batch[mbid] = {col: row[col] for col in value_cols}
            if len(batch) >= BATCH_SIZE:
                yield batch
                batch = {}
    if batch:
        yield batch

def update_batch(cur, data, update_sql):
    cur.executemany(update_sql, list(data.items()))
    return cur.rowcount

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # --- BPM ---
    print("Updating bpm from rhythm CSV...")
    total_bpm = 0
    for batch in load_csv_batched(RHYTHM_CSV, "mbid", ["bpm"]):
        args = [(float(v["bpm"]), mbid) for mbid, v in batch.items()]
        cur.executemany(
            "UPDATE track SET bpm = %s WHERE mb_recording_gid = %s::uuid AND bpm IS NULL;",
            args
        )
        total_bpm += cur.rowcount
        conn.commit()
        print(f"  {total_bpm} bpm rows updated so far...")

    print(f"  ✅ BPM done: {total_bpm} rows updated")

    # --- Key & Mode ---
    print("Updating key/mode from tonal CSV...")
    total_tonal = 0
    for batch in load_csv_batched(TONAL_CSV, "mbid", ["key_key", "key_scale"]):
        args = [(v["key_key"], v["key_scale"], mbid) for mbid, v in batch.items()]
        cur.executemany(
            "UPDATE track SET key = %s, mode = %s WHERE mb_recording_gid = %s::uuid AND key IS NULL;",
            args
        )
        total_tonal += cur.rowcount
        conn.commit()
        print(f"  {total_tonal} key/mode rows updated so far...")

    print(f"  ✅ Key/mode done: {total_tonal} rows updated")

    cur.close()
    conn.close()
    print("\n✅ All done!")

if __name__ == "__main__":
    main()
