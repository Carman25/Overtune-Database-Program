import os
import json
import psycopg2

DB_CONFIG = {
    "dbname": "overtune_demo",
    "user": "pablo",
    "host": "/var/run/postgresql",
    "port": 5432,
}

SAMPLE_DIR = "/home/pablo/acousticbrainz/sample/acousticbrainz-highlevel-json-20220623/highlevel"

GENRE_FIELDS = ["genre_dortmund", "genre_electronic", "genre_rosamerica"]

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Load genre names → genre_id (only 2132 rows, fine in memory)
    print("Loading genre table...")
    cur.execute("SELECT genre_id, name FROM genre;")
    genre_map = {name.lower(): gid for gid, name in cur.fetchall()}
    print(f"  {len(genre_map)} genres loaded")

    inserted = 0
    skipped_no_track = 0
    skipped_no_genre = 0
    skipped_duplicate = 0
    files_processed = 0

    # Collect all JSON file paths and their UUIDs first
    print("Scanning JSON files...")
    uuid_to_path = {}
    for root, dirs, files in os.walk(SAMPLE_DIR):
        for fname in files:
            if not fname.endswith(".json"):
                continue
            uuid = fname[:-5].rsplit("-", 1)[0]  # strip .json then trailing -N suffix
            uuid_to_path[uuid] = os.path.join(root, fname)

    print(f"  {len(uuid_to_path)} JSON files found")

    # Process in batches: look up track_ids for a batch of UUIDs at a time
    BATCH_SIZE = 5000
    all_uuids = list(uuid_to_path.keys())
    total_batches = (len(all_uuids) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num in range(total_batches):
        batch_uuids = all_uuids[batch_num * BATCH_SIZE:(batch_num + 1) * BATCH_SIZE]

        # Look up only this batch of UUIDs from DB
        cur.execute(
            "SELECT track_id, mb_recording_gid FROM track WHERE mb_recording_gid = ANY(%s::uuid[]);",
            (batch_uuids,)
        )
        track_map = {str(gid): tid for tid, gid in cur.fetchall()}

        for uuid in batch_uuids:
            track_id = track_map.get(uuid)
            if not track_id:
                skipped_no_track += 1
                continue

            filepath = uuid_to_path[uuid]
            try:
                with open(filepath, "r") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"  Failed to read {filepath}: {e}")
                continue

            hl = data.get("highlevel", {})
            seen_genres = set()

            for field in GENRE_FIELDS:
                value = hl.get(field, {}).get("value", "").lower()
                if not value or value in seen_genres:
                    continue
                seen_genres.add(value)

                genre_id = genre_map.get(value)
                if not genre_id:
                    skipped_no_genre += 1
                    continue

                try:
                    cur.execute(
                        "INSERT INTO track_genre (track_id, genre_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                        (track_id, genre_id)
                    )
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped_duplicate += 1
                except Exception as e:
                    print(f"  Insert error for track {track_id}, genre {genre_id}: {e}")
                    conn.rollback()
                    continue

            files_processed += 1

        conn.commit()
        print(f"  Batch {batch_num + 1}/{total_batches} done — {files_processed} files, {inserted} rows inserted")

    cur.close()
    conn.close()

    print("\n✅ Done!")
    print(f"  Files processed:     {files_processed}")
    print(f"  Rows inserted:       {inserted}")
    print(f"  Skipped (no track):  {skipped_no_track}")
    print(f"  Skipped (no genre):  {skipped_no_genre}")
    print(f"  Skipped (duplicate): {skipped_duplicate}")

if __name__ == "__main__":
    main()
