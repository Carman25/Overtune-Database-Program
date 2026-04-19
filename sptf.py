import psycopg2
import csv

DB_CONFIG = {
    "dbname": "overtune_small",
    "user": "pablo",
    "host": "/var/run/postgresql",
    "port": 5432,
}

CSV_PATH = "/home/pablo/Downloads/updated_spotify_data_withbpm.csv"

# Spotify pitch class → key name
PITCH_CLASS = {
    0: "C", 1: "C#", 2: "D", 3: "D#", 4: "E", 5: "F",
    6: "F#", 7: "G", 8: "G#", 9: "A", 10: "A#", 11: "B"
}

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Load ISRC → track_id mapping from DB
    print("Loading ISRCs from overtune_small...")
    cur.execute("SELECT isrc, track_id FROM track_isrc;")
    isrc_map = {isrc: tid for isrc, tid in cur.fetchall()}
    print(f"  {len(isrc_map)} ISRCs loaded")

    # Read CSV and build update list
    print("Reading Spotify CSV...")
    updates = {}
    skipped_no_isrc = 0
    skipped_no_match = 0

    with open(CSV_PATH, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            isrc = row.get("ISRC", "").strip()
            bpm = row.get("bpm", "").strip()
            key_num = row.get("key", "").strip()

            if not isrc or isrc == "UNKNOWN":
                skipped_no_isrc += 1
                continue

            track_id = isrc_map.get(isrc)
            if not track_id:
                skipped_no_match += 1
                continue

            # Convert key number to name
            key_name = None
            if key_num and key_num != "-1":
                try:
                    key_name = PITCH_CLASS.get(int(float(key_num)))
                except:
                    pass

            # Convert bpm
            bpm_val = None
            if bpm:
                try:
                    bpm_val = round(float(bpm), 2)
                except:
                    pass

            if track_id not in updates:
                updates[track_id] = {"bpm": bpm_val, "key": key_name}

    print(f"  {len(updates)} tracks matched")
    print(f"  {skipped_no_isrc} rows skipped (no ISRC)")
    print(f"  {skipped_no_match} rows skipped (ISRC not in DB)")

    # Apply updates
    print("Updating track table...")
    bpm_updated = 0
    key_updated = 0

    for track_id, vals in updates.items():
        if vals["bpm"] is not None:
            cur.execute(
                "UPDATE track SET bpm = %s WHERE track_id = %s AND bpm IS NULL;",
                (vals["bpm"], track_id)
            )
            bpm_updated += cur.rowcount

        if vals["key"] is not None:
            cur.execute(
                "UPDATE track SET key = %s WHERE track_id = %s AND key IS NULL;",
                (vals["key"], track_id)
            )
            key_updated += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n✅ Done!")
    print(f"  BPM updated:  {bpm_updated}")
    print(f"  Key updated:  {key_updated}")

if __name__ == "__main__":
    main()
