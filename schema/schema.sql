-- =============================================================================
-- OVERTUNE — Database Schema
-- =============================================================================
-- Changelog vs. original:
--
--  Track
--    • bpm           INT        → NUMERIC(6,2)   Spotify returns fractional BPM
--    • isrc          TEXT       → CHAR(12) UNIQUE ISRC is a fixed-length standard
--                                                identifier; uniqueness enforced
--    • mb_recording_gid UUID    (NEW) MusicBrainz recording MBID — enables
--                                    Spotify/AcousticBrainz enrichment by ISRC
--                                    or direct MB API lookup
--
--  Album
--    • mb_release_group_gid UUID (NEW) MusicBrainz release_group MBID
--
--  Artist
--    • mb_artist_gid UUID        (NEW) MusicBrainz artist MBID
--
--  Label
--    • mb_label_gid  UUID        (NEW) MusicBrainz label MBID
--
--  Genre
--    • mb_genre_gid  UUID        (NEW) MusicBrainz genre MBID
--
--  Producer
--    • mb_artist_gid UUID        (NEW) MusicBrainz artist MBID for the
--                                      underlying artist entity
--
--  Review
--    • created_at    DATE       → TIMESTAMP  Consistent with rated_at / added_at
--
--  Indexes (NEW)
--    • Performance indexes on all high-frequency lookup columns
--      (name, title, isrc, gid columns, and FK columns in junction tables)
-- =============================================================================

-- =========================
-- CLEAN RESET
-- =========================
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- =========================
-- BASE TABLES
-- =========================

CREATE TABLE "User" (
    user_id       SERIAL PRIMARY KEY,
    username      TEXT,
    email         TEXT,
    password_hash TEXT,
    join_date     DATE,
    country       TEXT
);

-- IS-A: Artist ⊆ User
CREATE TABLE Artist (
    user_id        INT  PRIMARY KEY,
    name           TEXT,
    country        TEXT,
    formed_year    INT,
    type           TEXT,
    bio            TEXT,
    -- MusicBrainz MBID (UUID) for the artist entity.
    -- Populated from mb.artist.gid during ETL; NULL for app-created artists.
    -- Used for deduplication and cross-referencing the MB API / Spotify lookup.
    mb_artist_gid  UUID UNIQUE,
    FOREIGN KEY (user_id) REFERENCES "User"(user_id) ON DELETE CASCADE
);

-- IS-A: Consumer ⊆ User
CREATE TABLE Consumer (
    user_id      INT PRIMARY KEY,
    display_name TEXT,
    FOREIGN KEY (user_id) REFERENCES "User"(user_id) ON DELETE CASCADE
);

CREATE TABLE Album (
    album_id              SERIAL PRIMARY KEY,
    title                 TEXT,
    release_date          DATE,
    explicit_flag         BOOLEAN,
    -- MusicBrainz release_group MBID.
    -- Populated from mb.release_group.gid during ETL.
    mb_release_group_gid  UUID UNIQUE
);

CREATE TABLE Track (
    track_id          SERIAL PRIMARY KEY,
    title             TEXT,
    duration_sec      INT,
    -- NUMERIC(6,2): Spotify audio-features returns fractional BPM (e.g. 120.04).
    -- NULL until enriched via Spotify API post-MB import.
    bpm               NUMERIC(6,2),
    -- Spotify returns key as Pitch Class integer (0=C … 11=B).
    -- Store the resolved note name (e.g. 'C', 'F#') or the raw integer as text.
    -- NULL until enriched via Spotify API.
    key               TEXT,
    -- 'major' or 'minor'. NULL until enriched via Spotify API.
    mode              TEXT,
    explicit_flag     BOOLEAN,
    -- MusicBrainz recording MBID.
    -- Critical for Spotify enrichment: MB → ISRC → Spotify audio-features.
    mb_recording_gid  UUID UNIQUE
);

CREATE TABLE Genre (
    genre_id      SERIAL PRIMARY KEY,
    name          TEXT,
    description   TEXT,
    parent_genre_id INT,
    -- MusicBrainz genre MBID.
    mb_genre_gid  UUID UNIQUE,
    FOREIGN KEY (parent_genre_id) REFERENCES Genre(genre_id) ON DELETE SET NULL
);

CREATE TABLE Label (
    label_id      SERIAL PRIMARY KEY,
    name          TEXT,
    country       TEXT,
    founded_year  INT,
    website       TEXT,
    -- MusicBrainz label MBID.
    mb_label_gid  UUID UNIQUE
);

CREATE TABLE RightsHolder (
    rights_id      SERIAL PRIMARY KEY,
    holder_name    TEXT,
    pro_affiliation TEXT,
    contact_email  TEXT
);

CREATE TABLE Producer (
    producer_id   SERIAL PRIMARY KEY,
    name          TEXT,
    country       TEXT,
    birth_year    INT,
    bio           TEXT,
    artist_id     INT,
    -- MusicBrainz artist MBID for the underlying artist entity.
    -- Mirrors Artist.mb_artist_gid when the producer is also an Artist row;
    -- allows standalone producers (not in Artist table) to still be traceable.
    mb_artist_gid UUID UNIQUE,
    FOREIGN KEY (artist_id) REFERENCES Artist(user_id) ON DELETE SET NULL
);

CREATE TABLE Playlist (
    playlist_id  SERIAL PRIMARY KEY,
    name         TEXT,
    description  TEXT,
    created_date DATE,
    is_public    BOOLEAN,
    user_id      INT,
    FOREIGN KEY (user_id) REFERENCES "User"(user_id) ON DELETE CASCADE
);

CREATE TABLE Review (
    review_id   SERIAL PRIMARY KEY,
    rating      INT,
    review_text TEXT,
    -- Changed DATE → TIMESTAMP for sub-day precision, consistent with
    -- Consumer_Track_Rating.rated_at and Playlist_Track.added_at.
    created_at  TIMESTAMP,
    consumer_id INT,
    track_id    INT,
    FOREIGN KEY (consumer_id) REFERENCES Consumer(user_id) ON DELETE CASCADE,
    FOREIGN KEY (track_id)    REFERENCES Track(track_id)   ON DELETE CASCADE
);

-- =========================
-- RELATIONSHIP TABLES
-- =========================

CREATE TABLE Makes (
    user_id     INT,
    playlist_id INT,
    PRIMARY KEY (user_id, playlist_id),
    FOREIGN KEY (user_id)     REFERENCES "User"(user_id)       ON DELETE CASCADE,
    FOREIGN KEY (playlist_id) REFERENCES Playlist(playlist_id) ON DELETE CASCADE
);

CREATE TABLE Playlist_Track (
    playlist_id INT,
    track_id    INT,
    added_at    TIMESTAMP,
    PRIMARY KEY (playlist_id, track_id),
    FOREIGN KEY (playlist_id) REFERENCES Playlist(playlist_id) ON DELETE CASCADE,
    FOREIGN KEY (track_id)    REFERENCES Track(track_id)       ON DELETE CASCADE
);

CREATE TABLE Consumer_Track_Rating (
    user_id  INT,
    track_id INT,
    rating   INT,
    rated_at TIMESTAMP,
    PRIMARY KEY (user_id, track_id),
    FOREIGN KEY (user_id)  REFERENCES Consumer(user_id)  ON DELETE CASCADE,
    FOREIGN KEY (track_id) REFERENCES Track(track_id)    ON DELETE CASCADE
);

CREATE TABLE Written_By (
    consumer_id INT,
    review_id   INT,
    PRIMARY KEY (consumer_id, review_id),
    FOREIGN KEY (consumer_id) REFERENCES Consumer(user_id)   ON DELETE CASCADE,
    FOREIGN KEY (review_id)   REFERENCES Review(review_id)   ON DELETE CASCADE
);

CREATE TABLE Gets (
    review_id INT,
    track_id  INT,
    PRIMARY KEY (review_id, track_id),
    FOREIGN KEY (review_id) REFERENCES Review(review_id) ON DELETE CASCADE,
    FOREIGN KEY (track_id)  REFERENCES Track(track_id)   ON DELETE CASCADE
);

CREATE TABLE May_Be (
    artist_id   INT,
    producer_id INT,
    PRIMARY KEY (artist_id, producer_id),
    FOREIGN KEY (artist_id)   REFERENCES Artist(user_id)       ON DELETE CASCADE,
    FOREIGN KEY (producer_id) REFERENCES Producer(producer_id) ON DELETE CASCADE
);

CREATE TABLE Track_Artist (
    track_id  INT,
    artist_id INT,
    role      TEXT,
    PRIMARY KEY (track_id, artist_id),
    FOREIGN KEY (track_id)  REFERENCES Track(track_id)   ON DELETE CASCADE,
    FOREIGN KEY (artist_id) REFERENCES Artist(user_id)   ON DELETE CASCADE
);

CREATE TABLE Track_Songwriter (
    track_id     INT,
    artist_id    INT,
    contribution TEXT,
    PRIMARY KEY (track_id, artist_id),
    FOREIGN KEY (track_id)  REFERENCES Track(track_id)  ON DELETE CASCADE,
    FOREIGN KEY (artist_id) REFERENCES Artist(user_id)  ON DELETE CASCADE
);

CREATE TABLE Artist_Member (
    group_artist_id  INT,
    member_artist_id INT,
    instrument       TEXT,
    join_year        INT,
    leave_year       INT,
    PRIMARY KEY (group_artist_id, member_artist_id),
    FOREIGN KEY (group_artist_id)  REFERENCES Artist(user_id) ON DELETE CASCADE,
    FOREIGN KEY (member_artist_id) REFERENCES Artist(user_id) ON DELETE CASCADE
);

CREATE TABLE Track_Album (
    track_id     INT,
    album_id     INT,
    track_number INT,
    PRIMARY KEY (track_id, album_id),
    FOREIGN KEY (track_id) REFERENCES Track(track_id) ON DELETE CASCADE,
    FOREIGN KEY (album_id) REFERENCES Album(album_id) ON DELETE CASCADE
);

CREATE TABLE Label_Album (
    label_id INT,
    album_id INT,
    PRIMARY KEY (label_id, album_id),
    FOREIGN KEY (label_id) REFERENCES Label(label_id) ON DELETE CASCADE,
    FOREIGN KEY (album_id) REFERENCES Album(album_id) ON DELETE CASCADE
);

CREATE TABLE Label_Artist (
    label_id   INT,
    artist_id  INT,
    start_year INT,
    end_year   INT,
    PRIMARY KEY (label_id, artist_id),
    FOREIGN KEY (label_id)  REFERENCES Label(label_id)   ON DELETE CASCADE,
    FOREIGN KEY (artist_id) REFERENCES Artist(user_id)   ON DELETE CASCADE
);

CREATE TABLE Track_Producer (
    track_id    INT,
    producer_id INT,
    credit_type TEXT,
    PRIMARY KEY (track_id, producer_id),
    FOREIGN KEY (track_id)    REFERENCES Track(track_id)       ON DELETE CASCADE,
    FOREIGN KEY (producer_id) REFERENCES Producer(producer_id) ON DELETE CASCADE
);

CREATE TABLE Track_Genre (
    track_id INT,
    genre_id INT,
    PRIMARY KEY (track_id, genre_id),
    FOREIGN KEY (track_id) REFERENCES Track(track_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES Genre(genre_id) ON DELETE CASCADE
);

CREATE TABLE Track_RightsHolder (
    track_id   INT,
    rights_id  INT,
    rights_type TEXT,
    percentage  FLOAT,
    PRIMARY KEY (track_id, rights_id),
    FOREIGN KEY (track_id)  REFERENCES Track(track_id)           ON DELETE CASCADE,
    FOREIGN KEY (rights_id) REFERENCES RightsHolder(rights_id)   ON DELETE CASCADE
);

-- Track_ISRC: one row per ISRC per track.
-- A recording can have multiple ISRCs (e.g. different territory releases of
-- the same master). Keeping them in a separate table avoids the single-value
-- limitation and supports the full Spotify enrichment pipeline — any ISRC
-- associated with a track can be used to look up audio features.
--
-- isrc        CHAR(12): fixed-length per ISO 3901 (e.g. 'USUM71703861').
-- is_primary  BOOLEAN:  marks the canonical ISRC for this track (used as the
--                       first choice when calling the Spotify API). Exactly one
--                       row per track should have is_primary = TRUE.
CREATE TABLE Track_ISRC (
    track_id    INT,
    isrc        CHAR(12),
    is_primary  BOOLEAN  NOT NULL DEFAULT FALSE,
    PRIMARY KEY (track_id, isrc),
    FOREIGN KEY (track_id) REFERENCES Track(track_id) ON DELETE CASCADE
);

-- =============================================================================
-- INDEXES
-- =============================================================================
-- These are not required for correctness but are critical for query performance
-- at MusicBrainz scale (~30 M recordings, ~1.5 M artists, ~3 M releases).
-- Create AFTER bulk ETL inserts to avoid per-row index maintenance overhead.

-- ── Entity lookup by name / title ────────────────────────────────────────────
CREATE INDEX idx_artist_name       ON Artist(name);
CREATE INDEX idx_album_title       ON Album(title);
CREATE INDEX idx_track_title       ON Track(title);
CREATE INDEX idx_label_name        ON Label(name);
CREATE INDEX idx_genre_name        ON Genre(name);
CREATE INDEX idx_producer_name     ON Producer(name);

-- ── ISRC lookup (Spotify enrichment pipeline key join) ───────────────────────
-- Track_ISRC.isrc is the key join column for Spotify enrichment.
-- The PK index covers (track_id, isrc); we add a separate index on isrc alone
-- so lookups by ISRC code (without knowing the track_id) are fast.
CREATE INDEX idx_track_isrc_isrc      ON Track_ISRC(isrc);
CREATE INDEX idx_track_isrc_primary   ON Track_ISRC(track_id) WHERE is_primary = TRUE;

-- ── MB GID lookup (cross-reference / deduplication) ─────────────────────────
-- All mb_*_gid columns carry UNIQUE constraints (implicit B-tree indexes).

-- ── BPM / key range queries (sample discovery use case) ──────────────────────
CREATE INDEX idx_track_bpm         ON Track(bpm);
CREATE INDEX idx_track_key         ON Track(key);

-- ── Junction table FK columns (the non-leading key in each PK) ──────────────
-- PostgreSQL only auto-indexes the leading column of a composite PK.
-- Reverse-direction lookups (e.g. "all tracks by artist") need these.
CREATE INDEX idx_track_artist_artist   ON Track_Artist(artist_id);
CREATE INDEX idx_track_album_album     ON Track_Album(album_id);
CREATE INDEX idx_track_genre_genre     ON Track_Genre(genre_id);
CREATE INDEX idx_track_producer_prod   ON Track_Producer(producer_id);
CREATE INDEX idx_track_rh_rights       ON Track_RightsHolder(rights_id);
CREATE INDEX idx_label_album_album     ON Label_Album(album_id);
CREATE INDEX idx_label_artist_artist   ON Label_Artist(artist_id);
CREATE INDEX idx_artist_member_member  ON Artist_Member(member_artist_id);
CREATE INDEX idx_track_songwriter_art  ON Track_Songwriter(artist_id);
CREATE INDEX idx_playlist_track_track  ON Playlist_Track(track_id);
CREATE INDEX idx_review_track          ON Review(track_id);
CREATE INDEX idx_review_consumer       ON Review(consumer_id);
