#!/usr/bin/env python3
"""
Music Streaming Database — Command Line Interface
 
A CLI for browsing/searching a music streaming database and generating
reports. The database schema is loaded from `schema.sql` (PostgreSQL),
and all queries are embedded as parameterized SQL inside this script.
 
Usage:
    python music_cli.py
 
Requirements:
    pip install psycopg2-binary
    A running PostgreSQL server. Connection details can be set via
    environment variables or edited in DB_CONFIG below:
        PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""
 
import os
import sys
from pathlib import Path
 
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    sys.exit("ERROR: psycopg2 is required. Install with: pip install psycopg2-binary")
 
 
# Configuration
 
DB_CONFIG = {
    "host":     os.environ.get("PGHOST", "localhost"),
    "port":     os.environ.get("PGPORT", "5432"),
    "dbname":   os.environ.get("PGDATABASE", "overtune_small"),
    "user":     os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", ""),
}
 
SCHEMA_FILE = Path(__file__).parent / "schema.sql"
 
# Default max characters per column when truncation is on.
DEFAULT_MAX_COL_WIDTH = 40

# Embedded SQL queries
# %s placeholders to prevent string SQL injection
 
QUERIES = {
    # BROWSE / SEARCH
    "search_tracks": """
        SELECT t.track_id,
               t.title,
               t.duration_sec,
               STRING_AGG(DISTINCT a.name, ', ') AS artists,
               t.explicit_flag
        FROM Track t
        LEFT JOIN Track_Artist ta ON ta.track_id = t.track_id
        LEFT JOIN Artist a        ON a.user_id  = ta.artist_id
        WHERE LOWER(COALESCE(t.title, '')) LIKE LOWER(%s)
        GROUP BY t.track_id
        ORDER BY t.title
        LIMIT %s;
    """,
 
    "search_artists": """
        SELECT a.user_id      AS artist_id,
               a.name,
               a.country,
               a.type,
               a.formed_year,
               COUNT(DISTINCT ta.track_id) AS track_count
        FROM Artist a
        LEFT JOIN Track_Artist ta ON ta.artist_id = a.user_id
        WHERE LOWER(COALESCE(a.name, '')) LIKE LOWER(%s)
        GROUP BY a.user_id
        ORDER BY a.name
        LIMIT %s;
    """,
 
    "search_albums": """
        SELECT al.album_id,
               al.title,
               al.release_date,
               al.explicit_flag,
               COUNT(DISTINCT ta.track_id) AS track_count
        FROM Album al
        LEFT JOIN Track_Album ta ON ta.album_id = al.album_id
        WHERE LOWER(COALESCE(al.title, '')) LIKE LOWER(%s)
        GROUP BY al.album_id
        ORDER BY al.release_date DESC NULLS LAST, al.title
        LIMIT %s;
    """,
 
    "track_detail": """
        SELECT t.track_id,
               t.title,
               t.duration_sec,
               t.bpm,
               t.key,
               t.mode,
               -- t.isrc, --
               t.explicit_flag,
               STRING_AGG(DISTINCT a.name, ', ') AS artists,
               STRING_AGG(DISTINCT g.name, ', ') AS genres
        FROM Track t
        LEFT JOIN Track_Artist ta ON ta.track_id = t.track_id
        LEFT JOIN Artist a        ON a.user_id  = ta.artist_id
        LEFT JOIN Track_Genre tg  ON tg.track_id = t.track_id
        LEFT JOIN Genre g         ON g.genre_id = tg.genre_id
        WHERE t.track_id = %s
        GROUP BY t.track_id;
    """,
 
    "artist_tracks": """
        SELECT t.track_id, t.title, t.duration_sec, ta.role
        FROM Track t
        JOIN Track_Artist ta ON ta.track_id = t.track_id
        WHERE ta.artist_id = %s
        ORDER BY t.title;
    """,
 
    "album_tracks": """
        SELECT t.track_id, t.title, ta.track_number, t.duration_sec
        FROM Track t
        JOIN Track_Album ta ON ta.track_id = t.track_id
        WHERE ta.album_id = %s
        ORDER BY ta.track_number NULLS LAST, t.title;
    """,

    "search_producers": """
        SELECT p.producer_id,
               p.name,
               p.country,
               p.birth_year,
               a.name AS artist_alias
        FROM Producer p
        LEFT JOIN Artist a ON a.user_id = p.artist_id
        WHERE LOWER(COALESCE(p.name, '')) LIKE LOWER(%s)
        ORDER BY p.name
        LIMIT %s;
    """,
    # Tables: Track_Songwriter, Artist, Track
    "track_songwriters": """
        SELECT t.title   AS track_title,
               a.name    AS songwriter,
               ts.contribution
        FROM Track_Songwriter ts
        JOIN Track t  ON t.track_id = ts.track_id
        JOIN Artist a ON a.user_id  = ts.artist_id
        WHERE ts.track_id = %s
        ORDER BY a.name;
    """,

    # Tables: Track_Producer, Producer, Track
    "track_producers": """
        SELECT t.title        AS track_title,
               p.name         AS producer,
               tp.credit_type
        FROM Track_Producer tp
        JOIN Track t    ON t.track_id    = tp.track_id
        JOIN Producer p ON p.producer_id = tp.producer_id
        WHERE tp.track_id = %s
        ORDER BY p.name;
    """,

    # Tables: Artist_Member, Artist (self-referential M:N)
    "group_members": """
        SELECT grp.name   AS group_name,
               mem.name   AS member_name,
               am.instrument,
               am.join_year,
               am.leave_year
        FROM Artist_Member am
        JOIN Artist grp ON grp.user_id = am.group_artist_id
        JOIN Artist mem ON mem.user_id = am.member_artist_id
        WHERE am.group_artist_id = %s
        ORDER BY am.join_year NULLS LAST, mem.name;
    """,

    # Tables: Playlist, "User", Playlist_Track, Makes
    "user_playlists": """
        SELECT pl.playlist_id,
               pl.name,
               pl.is_public,
               pl.created_date,
               COUNT(DISTINCT pt.track_id) AS track_count
        FROM Playlist pl
        LEFT JOIN Playlist_Track pt ON pt.playlist_id = pl.playlist_id
        WHERE pl.user_id = %s
        GROUP BY pl.playlist_id
        ORDER BY pl.created_date DESC NULLS LAST;
    """,

    # Tables: Playlist_Track, Track, Playlist
    "playlist_tracks": """
        SELECT t.track_id,
               t.title,
               t.duration_sec,
               pt.added_at
        FROM Playlist_Track pt
        JOIN Track t ON t.track_id = pt.track_id
        WHERE pt.playlist_id = %s
        ORDER BY pt.added_at NULLS LAST, t.title;
    """,

    # Tables: Track_RightsHolder, RightsHolder, Track
    "track_rights": """
        SELECT t.title       AS track_title,
               rh.holder_name,
               rh.pro_affiliation,
               tr.rights_type,
               tr.percentage
        FROM Track_RightsHolder tr
        JOIN Track t        ON t.track_id  = tr.track_id
        JOIN RightsHolder rh ON rh.rights_id = tr.rights_id
        WHERE tr.track_id = %s
        ORDER BY tr.percentage DESC NULLS LAST;
    """,

    # Tables: Label, Label_Album, Label_Artist
    "search_labels": """
        SELECT l.label_id,
               l.name,
               l.country,
               l.founded_year,
               l.website,
               COUNT(DISTINCT la.album_id)   AS album_count,
               COUNT(DISTINCT lar.artist_id) AS artist_count
        FROM Label l
        LEFT JOIN Label_Album  la  ON la.label_id  = l.label_id
        LEFT JOIN Label_Artist lar ON lar.label_id = l.label_id
        WHERE LOWER(COALESCE(l.name, '')) LIKE LOWER(%s)
        GROUP BY l.label_id
        ORDER BY l.name
        LIMIT %s;
    """,

    # Tables: RightsHolder
    "search_rightsholders": """
        SELECT rh.rights_id,
               rh.holder_name,
               rh.pro_affiliation,
               rh.contact_email
        FROM RightsHolder rh
        WHERE LOWER(COALESCE(rh.holder_name, '')) LIKE LOWER(%s)
        ORDER BY rh.holder_name
        LIMIT %s;
    """,

    # Tables: May_Be, Artist, Producer
    "artist_producer_links": """
        SELECT a.name  AS artist_name,
               p.name  AS producer_name,
               p.producer_id
        FROM May_Be mb
        JOIN Artist a   ON a.user_id    = mb.artist_id
        JOIN Producer p ON p.producer_id = mb.producer_id
        WHERE mb.artist_id = %s
        ORDER BY p.name;
    """,

    # Tables: Written_By, Consumer, Review
    "consumer_reviews": """
        SELECT r.review_id,
               r.rating,
               r.review_text,
               r.created_at,
               t.title AS track_title
        FROM Written_By wb
        JOIN Review r   ON r.review_id  = wb.review_id
        JOIN Track t    ON t.track_id   = r.track_id
        WHERE wb.consumer_id = %s
        ORDER BY r.created_at DESC NULLS LAST;
    """,

    # Tables: Gets, Review, Track
    "track_reviews": """
        SELECT r.review_id,
               r.rating,
               r.review_text,
               r.created_at,
               c.display_name AS reviewer
        FROM Gets g
        JOIN Review r   ON r.review_id  = g.review_id
        JOIN Consumer c ON c.user_id    = r.consumer_id
        WHERE g.track_id = %s
        ORDER BY r.created_at DESC NULLS LAST;
    """,
 
    # REPORTS
    # Numeric ratings come from Consumer_Track_Rating.
    "top_rated_tracks": """
        SELECT t.track_id,
               t.title,
               STRING_AGG(DISTINCT a.name, ', ') AS artists,
               ROUND(AVG(r.rating)::numeric, 2) AS avg_rating,
               COUNT(r.rating) AS num_ratings
        FROM Track t
        JOIN Consumer_Track_Rating r ON r.track_id = t.track_id
        LEFT JOIN Track_Artist ta    ON ta.track_id = t.track_id
        LEFT JOIN Artist a           ON a.user_id  = ta.artist_id
        WHERE r.rating IS NOT NULL
        GROUP BY t.track_id
        HAVING COUNT(r.rating) >= %s
        ORDER BY avg_rating DESC, num_ratings DESC
        LIMIT %s;
    """,
 
    "popular_genres": """
        SELECT g.genre_id,
               g.name,
               COUNT(DISTINCT tg.track_id) AS track_count,
               ROUND(AVG(r.rating)::numeric, 2) AS avg_rating
        FROM Genre g
        LEFT JOIN Track_Genre tg          ON tg.genre_id = g.genre_id
        LEFT JOIN Consumer_Track_Rating r ON r.track_id  = tg.track_id
        GROUP BY g.genre_id
        ORDER BY track_count DESC, avg_rating DESC NULLS LAST
        LIMIT %s;
    """,
 
    "most_prolific_artists": """
        SELECT a.user_id AS artist_id,
               a.name,
               COUNT(DISTINCT ta.track_id) AS track_count,
               COUNT(DISTINCT tal.album_id) AS album_count
        FROM Artist a
        LEFT JOIN Track_Artist ta  ON ta.artist_id = a.user_id
        LEFT JOIN Track_Album tal  ON tal.track_id = ta.track_id
        GROUP BY a.user_id
        HAVING COUNT(DISTINCT ta.track_id) > 0
        ORDER BY track_count DESC
        LIMIT %s;
    """,
 
    "recent_reviews": """
        SELECT r.review_id,
               r.rating,
               r.review_text,
               r.created_at,
               t.title AS track_title,
               c.display_name AS reviewer
        FROM Review r
        JOIN Track t    ON t.track_id = r.track_id
        JOIN Consumer c ON c.user_id  = r.consumer_id
        ORDER BY r.created_at DESC NULLS LAST
        LIMIT %s;
    """,
 
    "label_catalog_size": """
        SELECT l.label_id,
               l.name,
               l.country,
               COUNT(DISTINCT la.album_id)   AS album_count,
               COUNT(DISTINCT lar.artist_id) AS artist_count
        FROM Label l
        LEFT JOIN Label_Album  la  ON la.label_id  = l.label_id
        LEFT JOIN Label_Artist lar ON lar.label_id = l.label_id
        GROUP BY l.label_id
        ORDER BY album_count DESC, artist_count DESC
        LIMIT %s;
    """,

    "genre_tree": """
        SELECT g.genre_id,
               g.name,
               g.description,
               parent.name AS parent_genre
        FROM Genre g
        LEFT JOIN Genre parent ON parent.genre_id = g.parent_genre_id
        ORDER BY parent.name NULLS FIRST, g.name
        LIMIT %s;
    """,
}
 
# DB helpers
 
def connect():
    """Open a connection to PostgreSQL."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        sys.exit(f"ERROR: could not connect to database: {e}")
 
 
def init_schema(conn):
    """Load schema.sql and execute it against the database."""
    if not SCHEMA_FILE.exists():
        sys.exit(f"ERROR: schema file not found at {SCHEMA_FILE}")
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print(f"✓ Schema loaded from {SCHEMA_FILE.name}")
 
 
def run_query(conn, key, params=()):
    """Execute one of the embedded queries with the given parameters."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERIES[key], params)
        return cur.fetchall()
 
 
# Display helpers
 
def print_table(rows, headers=None,truncate=True, max_col_width=DEFAULT_MAX_COL_WIDTH):
    """Print a list of dict rows as an aligned table.
    Args:
        rows:          List of dicts (one per row).
        headers:       Column names to display. Defaults to dict keys.
        truncate:      If True, clip long cell values at max_col_width.
        max_col_width: Maximum characters per column when truncate is on.
    """
    if not rows:
        print("  (no results)\n")
        return
    if headers is None:
        headers = list(rows[0].keys())
 
    widths = {h: len(h) for h in headers}
    str_rows = []
    for r in rows:
        sr = {h: ("" if r.get(h) is None else str(r.get(h))) for h in headers}
        for h in headers:
            val = r.get(h)
            text = "" if val is None else str(val)
            if truncate and len(text) > max_col_width:
                text = text[: max_col_width - 3] + "..."
            sr[h] = text
            widths[h] = max(widths[h], len(sr[h]))
        str_rows.append(sr)
 
    sep = " | "
    line = sep.join(h.ljust(widths[h]) for h in headers)
    print(line)
    print("-" * len(line))
    for sr in str_rows:
        print(sep.join(sr[h].ljust(widths[h]) for h in headers))
    print(f"\n  {len(rows)} row(s)\n")
 
 
def prompt(msg, default=None):
    suffix = f" [{default}]" if default is not None else ""
    val = input(f"{msg}{suffix}: ").strip()
    return val if val else (default if default is not None else "")
 
 
def prompt_int(msg, default):
    while True:
        raw = prompt(msg, str(default))
        try:
            return int(raw)
        except ValueError:
            print("  please enter a whole number.")
 
 

# Menu actions
 
def action_search_tracks(conn):
    term = prompt("Track title contains")
    limit = prompt_int("Max results", 20)
    rows = run_query(conn, "search_tracks", (f"%{term}%", limit))
    print_table(rows)
 
def action_search_artists(conn):
    term = prompt("Artist name contains")
    limit = prompt_int("Max results", 20)
    rows = run_query(conn, "search_artists", (f"%{term}%", limit))
    print_table(rows)
 
def action_search_albums(conn):
    term = prompt("Album title contains")
    limit = prompt_int("Max results", 20)
    rows = run_query(conn, "search_albums", (f"%{term}%", limit))
    print_table(rows)
 
def action_track_detail(conn):
    tid = prompt_int("Track ID", 1)
    rows = run_query(conn, "track_detail", (tid,))
    print_table(rows)
 
def action_artist_tracks(conn):
    aid = prompt_int("Artist ID", 1)
    rows = run_query(conn, "artist_tracks", (aid,))
    print_table(rows)
 
def action_album_tracks(conn):
    aid = prompt_int("Album ID", 1)
    rows = run_query(conn, "album_tracks", (aid,))
    print_table(rows)

def action_search_producers(conn):
    term = prompt("Producer name contains")
    limit = prompt_int("Max results", 20)
    rows = run_query(conn, "search_producers", (f"%{term}%", limit))
    print_table(rows)
 
def action_track_songwriters(conn):
    tid = prompt_int("Track ID", 1)
    rows = run_query(conn, "track_songwriters", (tid,))
    print_table(rows)


def action_track_producers(conn):
    tid = prompt_int("Track ID", 1)
    rows = run_query(conn, "track_producers", (tid,))
    print_table(rows)


def action_group_members(conn):
    gid = prompt_int("Group artist ID", 1)
    rows = run_query(conn, "group_members", (gid,))
    print_table(rows)


def action_user_playlists(conn):
    uid = prompt_int("User ID", 1)
    rows = run_query(conn, "user_playlists", (uid,))
    print_table(rows)


def action_playlist_tracks(conn):
    pid = prompt_int("Playlist ID", 1)
    rows = run_query(conn, "playlist_tracks", (pid,))
    print_table(rows)


def action_track_rights(conn):
    tid = prompt_int("Track ID", 1)
    rows = run_query(conn, "track_rights", (tid,))
    print_table(rows)


def action_search_labels(conn):
    term = prompt("Label name contains")
    limit = prompt_int("Max results", 20)
    rows = run_query(conn, "search_labels", (f"%{term}%", limit))
    print_table(rows)


def action_search_rightsholders(conn):
    term = prompt("Rights holder name contains")
    limit = prompt_int("Max results", 20)
    rows = run_query(conn, "search_rightsholders", (f"%{term}%", limit))
    print_table(rows)


def action_artist_producer_links(conn):
    aid = prompt_int("Artist ID", 1)
    rows = run_query(conn, "artist_producer_links", (aid,))
    print_table(rows)


def action_consumer_reviews(conn):
    cid = prompt_int("Consumer (user) ID", 1)
    rows = run_query(conn, "consumer_reviews", (cid,))
    print_table(rows)


def action_track_reviews(conn):
    tid = prompt_int("Track ID", 1)
    rows = run_query(conn, "track_reviews", (tid,))
    print_table(rows)

#------------
# REPORTS
#------------

def action_top_rated(conn):
    min_ratings = prompt_int("Minimum number of ratings", 1)
    limit = prompt_int("Max results", 10)
    rows = run_query(conn, "top_rated_tracks", (min_ratings, limit))
    print_table(rows)
 
def action_popular_genres(conn):
    limit = prompt_int("Max results", 10)
    rows = run_query(conn, "popular_genres", (limit,))
    print_table(rows)
 
def action_prolific_artists(conn):
    limit = prompt_int("Max results", 10)
    rows = run_query(conn, "most_prolific_artists", (limit,))
    print_table(rows)
 
def action_recent_reviews(conn):
    limit = prompt_int("Max results", 10)
    rows = run_query(conn, "recent_reviews", (limit,))
    print_table(rows)
 
def action_label_catalog(conn):
    limit = prompt_int("Max results", 10)
    rows = run_query(conn, "label_catalog_size", (limit,))
    print_table(rows)

def action_genre_tree(conn):
    limit = prompt_int("Max results", 50)
    rows = run_query(conn, "genre_tree", (limit,))
    print_table(rows)


def action_init_schema(conn):
    confirm = prompt("This will drop and recreate all tables. Type 'yes' to confirm")
    if confirm.lower() == "yes":
        init_schema(conn)
    else:
        print("  cancelled.\n")
 
# Menu dashboard. Each entry is a (label, function) pair.
# MENU = [
#     ("Search tracks by title",        action_search_tracks),
#     ("Search artists by name",        action_search_artists),
#     ("Search albums by title",        action_search_albums),
#     ("View track details",            action_track_detail),
#     ("List tracks by artist ID",      action_artist_tracks),
#     ("List tracks on album ID",       action_album_tracks),
#     ("Report: top-rated tracks",      action_top_rated),
#     ("Report: popular genres",        action_popular_genres),
#     ("Report: most prolific artists", action_prolific_artists),
#     ("Report: most recent reviews",   action_recent_reviews),
#     ("Report: label catalog sizes",   action_label_catalog),
#     ("(Re)initialize schema from schema.sql", action_init_schema),
# ]
MENU = [
    # -- Browse / search --
    ("Search tracks by title",              action_search_tracks),
    ("Search artists by name",              action_search_artists),
    ("Search albums by title",              action_search_albums),
    # ("Search users by username/email",      action_search_users),
    ("Search producers by name",            action_search_producers),
    ("Search labels by name",               action_search_labels),
    ("Search rights holders by name",       action_search_rightsholders),
    ("View track details (genres + artists)", action_track_detail),
    ("List tracks by artist ID",            action_artist_tracks),
    ("List tracks on album ID",             action_album_tracks),
    ("View songwriting credits for track",  action_track_songwriters),
    ("View producer credits for track",     action_track_producers),
    ("View group members for artist",       action_group_members),
    ("View playlists for user",             action_user_playlists),
    ("View tracks on playlist",             action_playlist_tracks),
    ("View rights holders for track",       action_track_rights),
    ("View artist/producer links (May_Be)", action_artist_producer_links),
    ("View reviews by consumer (Written_By)", action_consumer_reviews),
    ("View reviews for track (Gets)",       action_track_reviews),
    # -- Reports --
    ("Report: top-rated tracks",            action_top_rated),
    ("Report: popular genres",              action_popular_genres),
    ("Report: most prolific artists",       action_prolific_artists),
    ("Report: most recent reviews",         action_recent_reviews),
    ("Report: label catalog sizes",         action_label_catalog),
    ("Report: genre tree (sub-genres)",     action_genre_tree),
    # -- Admin --
    ("(Re)initialize schema from schema.sql", action_init_schema),
]
 
def show_menu():
    print("\n" + "-" * 60)
    print(" MUSIC STREAMING DATABASE — CLI")
    print("-" * 60)
    for i, (label, _) in enumerate(MENU, 1):
        print(f"  {i:2}. {label}")
    print("   q. Quit")
    print("-" * 60)
 
 
def main():
    conn = connect()
    print(f"Connected to {DB_CONFIG['dbname']} @ {DB_CONFIG['host']}")
 
    while True:
        show_menu()
        choice = prompt("Select option").lower()
        if choice in ("q", "quit", "exit"):
            break
        try:
            idx = int(choice) - 1
            if not (0 <= idx < len(MENU)):
                raise ValueError
        except ValueError:
            print("  invalid choice.\n")
            continue
 
        try:
            MENU[idx][1](conn)
        except psycopg2.Error as e:
            conn.rollback()
            print(f"  database error: {e}\n")
        except KeyboardInterrupt:
            print("\n  cancelled.\n")
 
    conn.close()
    print("Exited.")
 
 
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExited.")