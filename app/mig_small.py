"""
OVERTUNE Small Database Migration
Copies the 20K most-connected tracks and all related data
from overtune_demo into overtune_small.

FK order respected throughout. All lists used (not tuples) to avoid
psycopg2 ROW expression limit. Producers filtered to only those whose
artist_id is NULL or already inserted.

Usage:
    python3 mig_small.py
"""

import psycopg2
from psycopg2.extras import execute_values

DB_SRC = {
    "dbname": "overtune_demo",
    "user": "pablo",
    "host": "/var/run/postgresql",
    "port": 5432,
}

DB_DST = {
    "dbname": "overtune_small",
    "user": "pablo",
    "host": "/var/run/postgresql",
    "port": 5432,
}

NUM_TRACKS = 20_000


def connect(cfg):
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False
    return conn


def fetch(cur, sql, params=None):
    cur.execute(sql, params)
    return cur.fetchall()


def insert(cur, conn, table, cols, rows, page=2000):
    if not rows:
        print(f"  {table}: 0 rows (skipped)")
        return
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING"
    for i in range(0, len(rows), page):
        execute_values(cur, sql, rows[i:i + page])
    conn.commit()
    print(f"  {table}: {len(rows):,} rows")


def ids(cur, sql, params=None):
    """Run a query and return results as a plain list (never tuple)."""
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()] or [0]


def main():
    src = connect(DB_SRC)
    dst = connect(DB_DST)
    sc = src.cursor()
    dc = dst.cursor()

    # ── 1. Clear destination ──────────────────────────────────────────────────
    print("Clearing destination database...")
    for t in [
        'consumer_track_rating', 'gets', 'written_by', 'review',
        'playlist_track', 'makes', 'playlist', 'consumer',
        'may_be', 'artist_member', 'label_artist', 'label_album',
        'track_genre', 'track_rightsholder', 'track_songwriter',
        'track_producer', 'track_isrc', 'track_artist', 'track_album',
        'track', 'album', 'genre', 'rightsholder', 'label',
        'producer', 'artist', '"User"'
    ]:
        dc.execute(f"DELETE FROM {t}")
    dst.commit()
    print("  Cleared.")

    # ── 2. Select 20K most-connected tracks ───────────────────────────────────
    print(f"\nSelecting {NUM_TRACKS:,} most-connected tracks...")
    sc.execute("""
        SELECT t.track_id
        FROM track t
        LEFT JOIN track_album ta   ON ta.track_id = t.track_id
        LEFT JOIN track_artist tar ON tar.track_id = t.track_id
        GROUP BY t.track_id
        ORDER BY COUNT(DISTINCT ta.album_id) + COUNT(DISTINCT tar.artist_id) DESC
        LIMIT %s
    """, (NUM_TRACKS,))
    track_ids = [r[0] for r in sc.fetchall()]
    print(f"  Selected {len(track_ids):,} tracks")

    # ── 3. Collect related IDs ────────────────────────────────────────────────
    print("Collecting related entity IDs...")

    album_ids     = ids(sc, "SELECT DISTINCT album_id FROM track_album WHERE track_id = ANY(%s::int[])", (track_ids,))
    artist_ids    = ids(sc, "SELECT DISTINCT artist_id FROM track_artist WHERE track_id = ANY(%s::int[])", (track_ids,))
    label_ids     = ids(sc, "SELECT DISTINCT label_id FROM label_album WHERE album_id = ANY(%s::int[])", (album_ids,))
    rights_ids    = ids(sc, "SELECT DISTINCT rights_id FROM track_rightsholder WHERE track_id = ANY(%s::int[])", (track_ids,))
    songwriter_artist_ids = ids(sc, "SELECT DISTINCT artist_id FROM track_songwriter WHERE track_id = ANY(%s::int[])", (track_ids,))
    consumer_ids  = ids(sc, "SELECT user_id FROM consumer")
    review_ids    = ids(sc, "SELECT review_id FROM review WHERE track_id = ANY(%s::int[])", (track_ids,))

    print(f"  tracks={len(track_ids):,}, albums={len(album_ids):,}, artists={len(artist_ids):,}")
    print(f"  labels={len(label_ids):,}, rightsholders={len(rights_ids):,}")

    # ── 4. Base tables (FK order: User → artist → producer, label, rightsholder, genre, album, track) ──

    print("\nCopying base tables...")

    # User: artists + consumers all need User rows
    all_user_ids = list(set(artist_ids) | set(consumer_ids))
    rows = fetch(sc, 'SELECT user_id, username, email, password_hash, join_date, country FROM "User" WHERE user_id = ANY(%s::int[])', (all_user_ids,))
    insert(dc, dst, '"User"', ['user_id', 'username', 'email', 'password_hash', 'join_date', 'country'], rows)

    # artist (FK → User)
    rows = fetch(sc, "SELECT user_id, name, country, formed_year, type, bio, mb_artist_gid FROM artist WHERE user_id = ANY(%s::int[])", (artist_ids,))
    insert(dc, dst, 'artist', ['user_id', 'name', 'country', 'formed_year', 'type', 'bio', 'mb_artist_gid'], rows)

    # producer (FK → artist.user_id via artist_id — only insert if artist_id is NULL or already inserted)
    producer_ids = ids(sc, "SELECT DISTINCT producer_id FROM track_producer WHERE track_id = ANY(%s::int[])", (track_ids,))
    rows = fetch(sc, """
        SELECT producer_id, name, country, birth_year, bio, artist_id, mb_artist_gid
        FROM producer
        WHERE producer_id = ANY(%s::int[])
          AND (artist_id IS NULL OR artist_id = ANY(%s::int[]))
    """, (producer_ids, artist_ids))
    insert(dc, dst, 'producer', ['producer_id', 'name', 'country', 'birth_year', 'bio', 'artist_id', 'mb_artist_gid'], rows)

    # Get actually inserted producer IDs for filtering junction table
    inserted_producer_ids = ids(dc, "SELECT producer_id FROM producer")

    # label
    rows = fetch(sc, "SELECT label_id, name, country, founded_year, website, mb_label_gid FROM label WHERE label_id = ANY(%s::int[])", (label_ids,))
    insert(dc, dst, 'label', ['label_id', 'name', 'country', 'founded_year', 'website', 'mb_label_gid'], rows)

    # rightsholder (same label_id space — only insert ones that have matching label)
    rows = fetch(sc, "SELECT rights_id, holder_name, pro_affiliation, contact_email FROM rightsholder WHERE rights_id = ANY(%s::int[])", (rights_ids,))
    insert(dc, dst, 'rightsholder', ['rights_id', 'holder_name', 'pro_affiliation', 'contact_email'], rows)

    # genre — insert all since track_genre is empty
    rows = fetch(sc, "SELECT genre_id, name, description, parent_genre_id, mb_genre_gid FROM genre")
    insert(dc, dst, 'genre', ['genre_id', 'name', 'description', 'parent_genre_id', 'mb_genre_gid'], rows)

    # album
    rows = fetch(sc, "SELECT album_id, title, release_date, explicit_flag, mb_release_group_gid FROM album WHERE album_id = ANY(%s::int[])", (album_ids,))
    insert(dc, dst, 'album', ['album_id', 'title', 'release_date', 'explicit_flag', 'mb_release_group_gid'], rows)

    # track
    rows = fetch(sc, "SELECT track_id, title, duration_sec, bpm, key, mode, explicit_flag, mb_recording_gid FROM track WHERE track_id = ANY(%s::int[])", (track_ids,))
    insert(dc, dst, 'track', ['track_id', 'title', 'duration_sec', 'bpm', 'key', 'mode', 'explicit_flag', 'mb_recording_gid'], rows)

    # ── 5. Junction tables ────────────────────────────────────────────────────
    print("\nCopying junction tables...")

    # may_be (FK → artist, producer)
    rows = fetch(sc, """
        SELECT artist_id, producer_id FROM may_be
        WHERE artist_id = ANY(%s::int[])
          AND producer_id = ANY(%s::int[])
    """, (artist_ids, inserted_producer_ids))
    insert(dc, dst, 'may_be', ['artist_id', 'producer_id'], rows)

    # track_album (FK → track, album)
    rows = fetch(sc, "SELECT track_id, album_id, track_number FROM track_album WHERE track_id = ANY(%s::int[])", (track_ids,))
    insert(dc, dst, 'track_album', ['track_id', 'album_id', 'track_number'], rows)

    # track_artist (FK → track, artist)
    rows = fetch(sc, "SELECT track_id, artist_id, role FROM track_artist WHERE track_id = ANY(%s::int[])", (track_ids,))
    insert(dc, dst, 'track_artist', ['track_id', 'artist_id', 'role'], rows)

    # track_isrc (FK → track)
    rows = fetch(sc, "SELECT track_id, isrc, is_primary FROM track_isrc WHERE track_id = ANY(%s::int[])", (track_ids,))
    insert(dc, dst, 'track_isrc', ['track_id', 'isrc', 'is_primary'], rows)

    # track_producer (FK → track, producer — only inserted producers)
    rows = fetch(sc, """
        SELECT track_id, producer_id, credit_type FROM track_producer
        WHERE track_id = ANY(%s::int[])
          AND producer_id = ANY(%s::int[])
    """, (track_ids, inserted_producer_ids))
    insert(dc, dst, 'track_producer', ['track_id', 'producer_id', 'credit_type'], rows)

    # track_songwriter (FK → track, artist)
    rows = fetch(sc, """
        SELECT track_id, artist_id, contribution FROM track_songwriter
        WHERE track_id = ANY(%s::int[])
          AND artist_id = ANY(%s::int[])
    """, (track_ids, artist_ids))
    insert(dc, dst, 'track_songwriter', ['track_id', 'artist_id', 'contribution'], rows)

    # track_rightsholder (FK → track, rightsholder)
    rows = fetch(sc, "SELECT track_id, rights_id, rights_type, percentage FROM track_rightsholder WHERE track_id = ANY(%s::int[])", (track_ids,))
    insert(dc, dst, 'track_rightsholder', ['track_id', 'rights_id', 'rights_type', 'percentage'], rows)

    # track_genre (FK → track, genre — empty but included for completeness)
    rows = fetch(sc, "SELECT track_id, genre_id FROM track_genre WHERE track_id = ANY(%s::int[])", (track_ids,))
    insert(dc, dst, 'track_genre', ['track_id', 'genre_id'], rows)

    # label_album (FK → label, album)
    rows = fetch(sc, "SELECT label_id, album_id FROM label_album WHERE album_id = ANY(%s::int[])", (album_ids,))
    insert(dc, dst, 'label_album', ['label_id', 'album_id'], rows)

    # label_artist (FK → label, artist)
    rows = fetch(sc, """
        SELECT label_id, artist_id, start_year, end_year FROM label_artist
        WHERE artist_id = ANY(%s::int[])
          AND label_id = ANY(%s::int[])
    """, (artist_ids, label_ids))
    insert(dc, dst, 'label_artist', ['label_id', 'artist_id', 'start_year', 'end_year'], rows)

    # artist_member (FK → artist for both sides)
    rows = fetch(sc, """
        SELECT group_artist_id, member_artist_id, instrument, join_year, leave_year
        FROM artist_member
        WHERE group_artist_id = ANY(%s::int[])
          AND member_artist_id = ANY(%s::int[])
    """, (artist_ids, artist_ids))
    insert(dc, dst, 'artist_member', ['group_artist_id', 'member_artist_id', 'instrument', 'join_year', 'leave_year'], rows)

    # ── 6. App-generated tables ───────────────────────────────────────────────
    print("\nCopying app-generated tables...")

    # consumer (FK → User — User rows already inserted above)
    rows = fetch(sc, "SELECT user_id, display_name FROM consumer")
    insert(dc, dst, 'consumer', ['user_id', 'display_name'], rows)

    # Get inserted consumer IDs for downstream filtering
    inserted_consumer_ids = ids(dc, "SELECT user_id FROM consumer")

    # playlist (FK → User/consumer)
    rows = fetch(sc, "SELECT playlist_id, name, description, created_date, is_public, user_id FROM playlist WHERE user_id = ANY(%s::int[])", (inserted_consumer_ids,))
    insert(dc, dst, 'playlist', ['playlist_id', 'name', 'description', 'created_date', 'is_public', 'user_id'], rows)

    inserted_playlist_ids = ids(dc, "SELECT playlist_id FROM playlist")

    # makes (FK → User, playlist)
    rows = fetch(sc, """
        SELECT user_id, playlist_id FROM makes
        WHERE user_id = ANY(%s::int[])
          AND playlist_id = ANY(%s::int[])
    """, (inserted_consumer_ids, inserted_playlist_ids))
    insert(dc, dst, 'makes', ['user_id', 'playlist_id'], rows)

    # playlist_track (FK → playlist, track)
    rows = fetch(sc, """
        SELECT playlist_id, track_id, added_at FROM playlist_track
        WHERE track_id = ANY(%s::int[])
          AND playlist_id = ANY(%s::int[])
    """, (track_ids, inserted_playlist_ids))
    insert(dc, dst, 'playlist_track', ['playlist_id', 'track_id', 'added_at'], rows)

    # review (FK → consumer, track)
    rows = fetch(sc, """
        SELECT review_id, rating, review_text, created_at, consumer_id, track_id
        FROM review
        WHERE track_id = ANY(%s::int[])
          AND consumer_id = ANY(%s::int[])
    """, (track_ids, inserted_consumer_ids))
    insert(dc, dst, 'review', ['review_id', 'rating', 'review_text', 'created_at', 'consumer_id', 'track_id'], rows)

    inserted_review_ids = ids(dc, "SELECT review_id FROM review")

    # written_by (FK → consumer, review)
    rows = fetch(sc, """
        SELECT consumer_id, review_id FROM written_by
        WHERE review_id = ANY(%s::int[])
          AND consumer_id = ANY(%s::int[])
    """, (inserted_review_ids, inserted_consumer_ids))
    insert(dc, dst, 'written_by', ['consumer_id', 'review_id'], rows)

    # gets (FK → review, track)
    rows = fetch(sc, """
        SELECT review_id, track_id FROM gets
        WHERE review_id = ANY(%s::int[])
          AND track_id = ANY(%s::int[])
    """, (inserted_review_ids, track_ids))
    insert(dc, dst, 'gets', ['review_id', 'track_id'], rows)

    # consumer_track_rating (FK → consumer, track)
    rows = fetch(sc, """
        SELECT user_id, track_id, rating, rated_at FROM consumer_track_rating
        WHERE track_id = ANY(%s::int[])
          AND user_id = ANY(%s::int[])
    """, (track_ids, inserted_consumer_ids))
    insert(dc, dst, 'consumer_track_rating', ['user_id', 'track_id', 'rating', 'rated_at'], rows)

    # ── 7. Summary ────────────────────────────────────────────────────────────
    print("\n✅ Migration complete! Final counts in overtune_small:")
    tables = [
        '"User"', 'artist', 'producer', 'label', 'rightsholder', 'genre',
        'album', 'track', 'may_be', 'track_album', 'track_artist', 'track_isrc',
        'track_producer', 'track_songwriter', 'track_rightsholder', 'track_genre',
        'label_album', 'label_artist', 'artist_member',
        'consumer', 'playlist', 'makes', 'playlist_track',
        'review', 'written_by', 'gets', 'consumer_track_rating'
    ]
    for t in tables:
        dc.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t}: {dc.fetchone()[0]:,}")

    sc.close()
    dc.close()
    src.close()
    dst.close()


if __name__ == "__main__":
    main()
