"""
OVERTUNE Fake Data Generator
Populates app-generated tables with consistent fake data.

Tables: consumer, playlist, playlist_track, review,
        written_by, gets, makes, consumer_track_rating

Usage:
    pip install psycopg2-binary faker
    python3 gen_data.py
"""

import random
import psycopg2
from faker import Faker
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "dbname": "overtune_small",
    "user": "pablo",
    "host": "/var/run/postgresql",
    "port": 5432,
}

NUM_CONSUMERS        = 20_000
NUM_PLAYLISTS        = 20_000
NUM_PLAYLIST_TRACKS  = 20_000
NUM_REVIEWS          = 20_000
NUM_RATINGS          = 20_000

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Helpers ───────────────────────────────────────────────────────────────────

def random_date(start_year=2015, end_year=2025):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_timestamp(start_year=2015, end_year=2025):
    dt = random_date(start_year, end_year)
    return dt.replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59)
    )

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur  = conn.cursor()

    # Clear existing app-generated data to avoid doubles
    print("Clearing existing app-generated data...")
    cur.execute("DELETE FROM consumer_track_rating;")
    cur.execute("DELETE FROM gets;")
    cur.execute("DELETE FROM written_by;")
    cur.execute("DELETE FROM review;")
    cur.execute("DELETE FROM playlist_track;")
    cur.execute("DELETE FROM makes;")
    cur.execute("DELETE FROM playlist;")
    cur.execute("DELETE FROM consumer;")
    conn.commit()
    print("  Cleared.")

    # Load real IDs from DB
    print("Fetching real user_ids from User table...")
    cur.execute('SELECT user_id FROM "User" ORDER BY random() LIMIT 50000;')
    all_user_ids = [row[0] for row in cur.fetchall()]
    print(f"  Loaded {len(all_user_ids)} user_ids")

    print("Fetching real track_ids from track table...")
    cur.execute("SELECT track_id FROM track ORDER BY random() LIMIT 100000;")
    all_track_ids = [row[0] for row in cur.fetchall()]
    print(f"  Loaded {len(all_track_ids)} track_ids")

    # ── 1. consumer ───────────────────────────────────────────────────────────
    print(f"\nInserting {NUM_CONSUMERS} consumers...")
    consumer_user_ids = random.sample(all_user_ids, NUM_CONSUMERS)
    consumer_rows = [
        (uid, fake.user_name() + str(random.randint(1, 9999)))
        for uid in consumer_user_ids
    ]
    cur.executemany(
        'INSERT INTO consumer (user_id, display_name) VALUES (%s, %s) ON CONFLICT DO NOTHING',
        consumer_rows
    )
    conn.commit()

    cur.execute("SELECT user_id FROM consumer;")
    consumer_ids = [row[0] for row in cur.fetchall()]
    print(f"  {len(consumer_ids)} consumers in DB")

    # ── 2. playlist ───────────────────────────────────────────────────────────
    print(f"\nInserting {NUM_PLAYLISTS} playlists...")
    playlist_rows = []
    for _ in range(NUM_PLAYLISTS):
        uid     = random.choice(consumer_ids)
        name    = fake.catch_phrase()[:80]
        desc    = fake.sentence()
        created = random_date()
        public  = random.choice([True, False])
        playlist_rows.append((name, desc, created, public, uid))

    cur.executemany(
        "INSERT INTO playlist (name, description, created_date, is_public, user_id) VALUES (%s, %s, %s, %s, %s)",
        playlist_rows
    )
    conn.commit()

    cur.execute("SELECT playlist_id, user_id FROM playlist;")
    playlist_data     = cur.fetchall()
    playlist_ids      = [row[0] for row in playlist_data]
    playlist_user_map = {row[0]: row[1] for row in playlist_data}
    print(f"  {len(playlist_ids)} playlists in DB")

    # ── 3. makes ─────────────────────────────────────────────────────────────
    print(f"\nInserting makes...")
    makes_rows = [(playlist_user_map[pid], pid) for pid in playlist_ids]
    cur.executemany(
        "INSERT INTO makes (user_id, playlist_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        makes_rows
    )
    conn.commit()
    print(f"  {len(makes_rows)} makes rows inserted")

    # ── 4. playlist_track ─────────────────────────────────────────────────────
    print(f"\nInserting {NUM_PLAYLIST_TRACKS} playlist_tracks...")
    pt_seen = set()
    pt_rows = []
    attempts = 0
    while len(pt_rows) < NUM_PLAYLIST_TRACKS and attempts < NUM_PLAYLIST_TRACKS * 10:
        pid = random.choice(playlist_ids)
        tid = random.choice(all_track_ids)
        key = (pid, tid)
        if key not in pt_seen:
            pt_seen.add(key)
            pt_rows.append((pid, tid, random_timestamp()))
        attempts += 1

    cur.executemany(
        "INSERT INTO playlist_track (playlist_id, track_id, added_at) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        pt_rows
    )
    conn.commit()
    print(f"  {len(pt_rows)} playlist_track rows inserted")

    # ── 5. review ─────────────────────────────────────────────────────────────
    print(f"\nInserting {NUM_REVIEWS} reviews...")
    review_rows = []
    for _ in range(NUM_REVIEWS):
        rating      = random.randint(1, 10)
        review_text = fake.paragraph(nb_sentences=random.randint(2, 6))
        created_at  = random_timestamp()
        consumer_id = random.choice(consumer_ids)
        track_id    = random.choice(all_track_ids)
        review_rows.append((rating, review_text, created_at, consumer_id, track_id))

    cur.executemany(
        "INSERT INTO review (rating, review_text, created_at, consumer_id, track_id) VALUES (%s, %s, %s, %s, %s)",
        review_rows
    )
    conn.commit()

    cur.execute("SELECT review_id, consumer_id, track_id FROM review;")
    review_data         = cur.fetchall()
    review_ids          = [row[0] for row in review_data]
    review_consumer_map = {row[0]: row[1] for row in review_data}
    review_track_map    = {row[0]: row[2] for row in review_data}
    print(f"  {len(review_ids)} reviews in DB")

    # ── 6. written_by ─────────────────────────────────────────────────────────
    print(f"\nInserting written_by...")
    written_rows = [(review_consumer_map[rid], rid) for rid in review_ids]
    cur.executemany(
        "INSERT INTO written_by (consumer_id, review_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        written_rows
    )
    conn.commit()
    print(f"  {len(written_rows)} written_by rows inserted")

    # ── 7. gets ───────────────────────────────────────────────────────────────
    print(f"\nInserting gets...")
    gets_rows = [(rid, review_track_map[rid]) for rid in review_ids]
    cur.executemany(
        "INSERT INTO gets (review_id, track_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        gets_rows
    )
    conn.commit()
    print(f"  {len(gets_rows)} gets rows inserted")

    # ── 8. consumer_track_rating ──────────────────────────────────────────────
    print(f"\nInserting {NUM_RATINGS} consumer_track_ratings...")
    ctr_seen = set()
    ctr_rows = []
    attempts = 0
    while len(ctr_rows) < NUM_RATINGS and attempts < NUM_RATINGS * 10:
        uid = random.choice(consumer_ids)
        tid = random.choice(all_track_ids)
        key = (uid, tid)
        if key not in ctr_seen:
            ctr_seen.add(key)
            ctr_rows.append((uid, tid, random.randint(1, 10), random_timestamp()))
        attempts += 1

    cur.executemany(
        "INSERT INTO consumer_track_rating (user_id, track_id, rating, rated_at) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        ctr_rows
    )
    conn.commit()
    print(f"  {len(ctr_rows)} consumer_track_rating rows inserted")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n✅ Done! Final counts:")
    for t in ["consumer", "playlist", "playlist_track", "review",
              "written_by", "gets", "makes", "consumer_track_rating"]:
        cur.execute(f"SELECT COUNT(*) FROM {t};")
        print(f"  {t}: {cur.fetchone()[0]:,}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
