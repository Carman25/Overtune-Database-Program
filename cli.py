#!/usr/bin/env python3
"""
Music Streaming Database — Command Line Interface
 
A CLI for browsing/searching a music streaming database and generating
reports. The database schema is loaded from a .dump file into (PostgreSQL),
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

# Import queries.py file from the same directory that contains the SQL queries
# Better organization of CLI programs.
sys.path.insert(0, str(Path(__file__).parent))
from queries import QUERIES

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

# Default consumer/user ID for creating content from the CLI.
# Change this to match a valid Consumer in your database.
# Could have a few differnet IDs for demoing?
DEFAULT_USER_ID = 20
 
# Default max characters per column when truncation is on.
default_max_col_width = 40
truncation_enabled = True
truncation_width = default_max_col_width


 
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
 
def print_table(rows, headers=None,truncate=None, max_col_width=None):
    """Print a list of dict rows as an aligned table.
    Args:
        rows:          List of dicts (one per row).
        headers:       Column names to display. Defaults to dict keys.
        truncate:      If True, clip long cell values at max_col_width.
        max_col_width: Maximum characters per column when truncate is on.
    """
    # Truncation administration
    if truncate is None:
        truncate = truncation_enabled
    if max_col_width is None:
        max_col_width = truncation_width
    
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
# The number after Max results is the default value that will be used if the user
# presses enter without inputing anything.
 
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

# Find Users to find playlists under their ID.
# Note: Users are not artists or consumers by default, but may be either or both.
def action_search_users(conn):
    term = prompt("Username or email contains")
    limit = prompt_int("Max results", 20)
    rows = run_query(conn, "search_users", (f"%{term}%", f"%{term}%", limit))
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
    term = prompt("Search group name (or press Enter to type ID directly)")
    if term:
        rows = run_query(conn, "find_groups", (f"%{term}%", 10))
        if not rows:
            print("  No groups found matching that name.\n")
            return
        print_table(rows)
        gid = prompt_int("Enter group_id from above", rows[0]["group_id"])
    else:
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

def action_track_isrcs(conn):
    tid = prompt_int("Track ID", 1)
    rows = run_query(conn, "track_isrcs", (tid,))
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
    term = prompt("Search consumer name (or press Enter to type ID directly)")
    if term:
        rows = run_query(conn, "find_consumers", (f"%{term}%", f"%{term}%", 10))
        if not rows:
            print("  No consumers found matching that name.\n")
            return
        print_table(rows)
        cid = prompt_int("Enter consumer_id from above", rows[0]["consumer_id"])
    else:
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

# Insert new reviews and playlists
def action_create_review(conn):
    print(f"  Creating review as user ID: {DEFAULT_USER_ID}")
    track_id = prompt_int("Track ID to review", 1)

    # Verify track exists
    rows = run_query(conn, "track_detail", (track_id,))
    if not rows:
        print("  Track not found.\n")
        return
    print(f"  Track: {rows[0]['title']}")

    while True:
        rating = prompt_int("Rating (1-5)", 5)
        if 1 <= rating <= 5:
            break
        print("  Rating must be between 1 and 5.")

    review_text = prompt("Review text (optional)", "")
    if not review_text:
        review_text = None

    confirm = prompt("Submit this review? (yes/no)", "yes")
    if confirm.lower() != "yes":
        print("  cancelled.\n")
        return

    with conn.cursor() as cur:
        # Insert review
        cur.execute(QUERIES["create_review"], (rating, review_text, DEFAULT_USER_ID, track_id))
        review_id = cur.fetchone()[0]

        # Link via Written_By
        cur.execute(QUERIES["create_written_by"], (DEFAULT_USER_ID, review_id))

        # Link via Gets
        cur.execute(QUERIES["create_gets"], (review_id, track_id))

    conn.commit()
    print(f"  Review #{review_id} created successfully.\n")

def action_create_playlist(conn):
    print(f"  Creating playlist as user ID: {DEFAULT_USER_ID}")
    name = prompt("Playlist name")
    if not name:
        print("  Name is required.\n")
        return

    description = prompt("Description (optional)", "")
    if not description:
        description = None

    public_input = prompt("Public? (yes/no)", "yes")
    is_public = public_input.lower() in ("yes", "y", "true")

    with conn.cursor() as cur:
        # Insert playlist
        cur.execute(QUERIES["create_playlist"], (name, description, is_public, DEFAULT_USER_ID))
        playlist_id = cur.fetchone()[0]

        # Link via Makes
        cur.execute(QUERIES["create_makes"], (DEFAULT_USER_ID, playlist_id))

    conn.commit()
    print(f"  Playlist #{playlist_id} '{name}' created successfully.")

    # Offer to add tracks
    while True:
        add_more = prompt("Add a track? (yes/no)", "no")
        if add_more.lower() not in ("yes", "y"):
            break
        track_id = prompt_int("Track ID to add", 1)
        try:
            with conn.cursor() as cur:
                cur.execute(QUERIES["add_track_to_playlist"], (playlist_id, track_id))
            conn.commit()
            print(f"  Track {track_id} added to playlist.\n")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"  Could not add track: {e}\n")

    print()

def action_add_to_playlist(conn):
    print(f"  Fetching playlists for user ID: {DEFAULT_USER_ID}")
    playlists = run_query(conn, "user_playlist_list", (DEFAULT_USER_ID,))
    if not playlists:
        print("  This user has no playlists. Create one first.\n")
        return
    print_table(playlists)
    pid = prompt_int("Enter playlist_id from above", playlists[0]["playlist_id"])

    while True:
        track_input = prompt("Track ID to add (or 'done' to stop)")
        if track_input.lower() in ("done", "d", "q", ""):
            break
        try:
            track_id = int(track_input)
        except ValueError:
            print("  Please enter a number or 'done'.")
            continue

        # Show track name for confirmation
        rows = run_query(conn, "track_detail", (track_id,))
        if not rows:
            print(f"  Track {track_id} not found.\n")
            continue
        print(f"  Adding: {rows[0]['title']}")

        try:
            with conn.cursor() as cur:
                cur.execute(QUERIES["add_track_to_playlist"], (pid, track_id))
            conn.commit()
            print(f"  Track {track_id} added.\n")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"  Could not add track: {e}\n")

    print()

def action_remove_from_playlist(conn):
    print(f"  Fetching playlists for user ID: {DEFAULT_USER_ID}")
    playlists = run_query(conn, "user_playlist_list", (DEFAULT_USER_ID,))
    if not playlists:
        print("  This user has no playlists.\n")
        return
    print_table(playlists)
    pid = prompt_int("Enter playlist_id from above", playlists[0]["playlist_id"])

    # Show current tracks on the playlist
    tracks = run_query(conn, "playlist_tracks", (pid,))
    if not tracks:
        print("  This playlist has no tracks.\n")
        return
    print_table(tracks)

    while True:
        track_input = prompt("Track ID to remove (or 'done' to stop)")
        if track_input.lower() in ("done", "d", "q", ""):
            break
        try:
            track_id = int(track_input)
        except ValueError:
            print("  Please enter a number or 'done'.")
            continue

        try:
            with conn.cursor() as cur:
                cur.execute(QUERIES["remove_track_from_playlist"], (pid, track_id))
                result = cur.fetchone()
            if result:
                conn.commit()
                print(f"  Track {track_id} removed from playlist {pid}.\n")
            else:
                conn.rollback()
                print(f"  Track {track_id} is not on this playlist.\n")
        except psycopg2.Error as e:
            conn.rollback()
            print(f"  Could not remove track: {e}\n")

    print()

# Deprecated: no longer using schema
#   def action_init_schema(conn):
#     confirm = prompt("This will drop and recreate all tables. Type 'yes' to confirm")
#     if confirm.lower() == "yes":
#         init_schema(conn)
#     else:
#         print("  cancelled.\n")

def action_toggle_truncation(conn):
    global truncation_enabled
    truncation_enabled = not truncation_enabled
    state = "ON" if truncation_enabled else "OFF"
    print(f"  Truncation is now {state} (max width: {truncation_width})\n")


def action_set_truncation_width(conn):
    global truncation_width
    new_width = prompt_int("Max column width", truncation_width)
    if new_width < 10:
        print("  Minimum width is 10.\n")
        return
    truncation_width = new_width
    print(f"  Truncation width set to {truncation_width}\n")

def action_who_am_i(conn):
    rows = run_query(conn, "who_am_i", (DEFAULT_USER_ID,))
    if not rows:
        print(f"  User ID {DEFAULT_USER_ID} does not exist in the database.\n")
        return
    row = rows[0]
    print(f"\n  User ID:      {row['user_id']}")
    print(f"  Username:     {row['username'] or '(none)'}")
    print(f"  Email:        {row['email'] or '(none)'}")
    print(f"  Country:      {row['country'] or '(none)'}")
    print(f"  Join date:    {row['join_date'] or '(none)'}")
    print(f"  Role:         {row['role']}")
    if row.get("display_name"):
        print(f"  Display name: {row['display_name']}")
    if row.get("artist_name"):
        print(f"  Artist name:  {row['artist_name']}")
        print(f"  Artist type:  {row['artist_type'] or '(none)'}")
    print()
 
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
    ("Search users by username/email",      action_search_users),
    ("Search producers by name",            action_search_producers),
    ("Search labels by name",               action_search_labels),
    ("Search rights holders by name",       action_search_rightsholders),
    ("View track details (genres + artists)", action_track_detail),
    ("List tracks by artist ID",            action_artist_tracks),
    ("List tracks on album ID",             action_album_tracks),
    ("View songwriting credits for track",  action_track_songwriters),
    ("View producer credits for track",     action_track_producers),
    ("View group members for artist by Group ID", action_group_members),
    ("View playlists for user",             action_user_playlists),
    ("View tracks on playlist",             action_playlist_tracks),
    ("View rights holders for track",       action_track_rights),
    ("View ISRCs for track",                action_track_isrcs),
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
    # -- Update --
    ("Create a review",                     action_create_review),
    ("Create a playlist (+ add tracks)",    action_create_playlist),
    ("Add tracks to existing playlist",     action_add_to_playlist),
    ("Remove track from playlist",          action_remove_from_playlist),
    # -- Admin --
    # ("(Re)initialize schema from schema.sql", action_init_schema),
    ("Who am I (current user info)",        action_who_am_i),
    ("Toggle truncation",                   action_toggle_truncation),
    ("Set truncation column width",         action_set_truncation_width),
]
 
def show_menu():
    print("\n" + "-" * 60)
    print(" MUSIC STREAMING DATABASE — CLI")
    print("-" * 60)
    
    # Menu Sections
    sections = [
        ("BROWSE / SEARCH", 0, 20),
        ("REPORTS",         20, 26),
        ("CREATE and UPDATE", 26, 30),
        ("ADMIN",           30, len(MENU)),
    ]
    for section_name, start, end in sections:
        print(f"\n  --- {section_name} ---")
        for i in range(start, end):
            label = MENU[i][0]
            print(f"  {i + 1:2}. {label}")
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