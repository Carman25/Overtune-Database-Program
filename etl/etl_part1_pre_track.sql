-- =============================================================================
-- OVERTUNE — MusicBrainz ETL Migration Script
-- =============================================================================
-- Purpose : Populate the OVERTUNE schema from a locally-imported MusicBrainz
--           PostgreSQL dump (mbdump / mbdump-derived).
--
-- Assumes  : 1. The MusicBrainz dump has been imported into a schema called
--               "musicbrainz" (search_path = musicbrainz, public).
--               If your MB tables live in "public", replace "musicbrainz." with "".
--            2. The OVERTUNE schema.sql has already been run in the same DB
--               (or a separate DB — adjust the connection string accordingly).
--            3. Run this script as a superuser or a role that can read both
--               schemas.
--
-- Coverage : ✅ Artist        ← mb.artist          (+ mb_artist_gid)
--            ✅ Album         ← mb.release_group    (+ mb_release_group_gid)
--            ✅ Track         ← mb.recording        (+ mb_recording_gid)
--            ✅ Track_ISRC    ← mb.isrc             (all ISRCs per recording, is_primary flag)
--            ✅ Genre         ← mb.genre            (+ mb_genre_gid)
--            ✅ Label         ← mb.label            (+ mb_label_gid)
--            ✅ Producer      ← mb.artist / l_artist_recording (+ mb_artist_gid)
--            ✅ RightsHolder  ← mb.label (used as a proxy; see notes)
--            ✅ Track_Artist  ← mb.artist_credit_name + l_artist_recording
--            ✅ Track_Album   ← mb.track + mb.medium
--            ✅ Label_Album   ← mb.release_label
--            ✅ Label_Artist  ← derived from release_label (see notes)
--            ✅ Track_Genre   ← mb.recording_tag + mb.tag + mb.genre
--            ✅ Track_Songwriter ← l_artist_work + l_recording_work
--            ✅ Track_Producer   ← l_artist_recording (producer link types)
--            ✅ Artist_Member    ← l_artist_artist  (member-of link type)
--            ✅ "User" / Consumer  ← synthetic stub row (MB has no user table in dump)
--
-- NOT covered by MB data (require Spotify API or app-generated data):
--            ⚠️  Track.bpm / key / mode   — add via Spotify API after import
--            ⚠️  Playlist / Playlist_Track / Makes — app-generated
--            ⚠️  Review / Written_By / Gets / Consumer_Track_Rating — app/RYM
--            ⚠️  RightsHolder.pro_affiliation / contact_email — manual
--
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 0. CONFIGURATION
-- ─────────────────────────────────────────────────────────────────────────────
-- Change this if your MB tables live in a different schema.
-- e.g. SET search_path = public, pg_catalog;
SET search_path = public, musicbrainz, pg_catalog;

-- Running without a wrapping transaction so a single step failure does not
-- abort all subsequent steps. Each INSERT is atomic on its own.

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. GENRE
-- ─────────────────────────────────────────────────────────────────────────────
-- MusicBrainz has a first-class `genre` table (added ~2020).
-- We also pull popular music tags that are not official genres as a fallback.
--
-- MB genre table columns: id, gid, name, comment, edits_pending, last_updated
-- MB genre_alias: similar to other alias tables

INSERT INTO Genre (genre_id, name, description, parent_genre_id, mb_genre_gid)
SELECT
    g.id                          AS genre_id,
    g.name                        AS name,
    NULLIF(g.comment, '')         AS description,
    NULL                          AS parent_genre_id,  -- MB genres are flat; enrich later
    g.gid                         AS mb_genre_gid
FROM musicbrainz.genre g
ON CONFLICT DO NOTHING;

-- Update sequences after bulk insert with explicit IDs
SELECT setval(pg_get_serial_sequence('Genre', 'genre_id'),
              COALESCE(MAX(genre_id), 1)) FROM Genre;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. LABEL
-- ─────────────────────────────────────────────────────────────────────────────
-- MB label columns: id, gid, name, begin_date_year, end_date_year, type,
--                   area, comment, last_updated, ...
-- MB area table: id, name  (used to get country name)

INSERT INTO Label (label_id, name, country, founded_year, website, mb_label_gid)
SELECT
    l.id                                           AS label_id,
    l.name                                         AS name,
    a.name                                         AS country,   -- area name as country proxy
    l.begin_date_year                              AS founded_year,
    NULL                                           AS website,   -- not in MB core dump
    l.gid                                          AS mb_label_gid
FROM musicbrainz.label l
LEFT JOIN musicbrainz.area a ON a.id = l.area
ON CONFLICT DO NOTHING;

SELECT setval(pg_get_serial_sequence('Label', 'label_id'),
              COALESCE(MAX(label_id), 1)) FROM Label;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. RIGHTS HOLDER  (proxy: labels as rights holders)
-- ─────────────────────────────────────────────────────────────────────────────
-- MusicBrainz does not have a dedicated rights-holder / PRO table.
-- We map labels → RightsHolder as the closest available proxy.
-- pro_affiliation and contact_email must be filled in from other sources.

INSERT INTO RightsHolder (rights_id, holder_name, pro_affiliation, contact_email)
SELECT
    l.id        AS rights_id,
    l.name      AS holder_name,
    NULL        AS pro_affiliation,
    NULL        AS contact_email
FROM musicbrainz.label l
ON CONFLICT DO NOTHING;

SELECT setval(pg_get_serial_sequence('RightsHolder', 'rights_id'),
              COALESCE(MAX(rights_id), 1)) FROM RightsHolder;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. USER + ARTIST (IS-A)
-- ─────────────────────────────────────────────────────────────────────────────
-- MB artist columns:
--   id, gid, name, sort_name, begin_date_year, end_date_year,
--   type (person=1, group=2, ...), area, begin_area, comment, ...
-- MB artist_type: id, name  ('Person', 'Group', 'Orchestra', etc.)
-- MB area: id, name

-- 4a. Insert a synthetic "User" row for every MB artist.
--     (There is no real user table in MB dumps; we create stubs.)
INSERT INTO "User" (user_id, username, email, password_hash, join_date, country)
SELECT
    a.id                                    AS user_id,
    a.name                                  AS username,
    NULL                                    AS email,
    NULL                                    AS password_hash,
    NULL                                    AS join_date,
    ar.name                                 AS country          -- area → country name
FROM musicbrainz.artist a
LEFT JOIN musicbrainz.area ar ON ar.id = a.area
ON CONFLICT DO NOTHING;

SELECT setval(pg_get_serial_sequence('"User"', 'user_id'),
              COALESCE(MAX(user_id), 1)) FROM "User";

-- 4b. Insert Artist rows (IS-A extension of "User").
INSERT INTO Artist (user_id, name, country, formed_year, type, bio, mb_artist_gid)
SELECT
    a.id                                    AS user_id,
    a.name                                  AS name,
    ar.name                                 AS country,
    a.begin_date_year                       AS formed_year,
    COALESCE(at.name, 'Unknown')            AS type,
    NULLIF(a.comment, '')                   AS bio,
    a.gid                                   AS mb_artist_gid
FROM musicbrainz.artist a
LEFT JOIN musicbrainz.area ar ON ar.id = a.area
LEFT JOIN musicbrainz.artist_type at ON at.id = a.type
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. PRODUCER
-- ─────────────────────────────────────────────────────────────────────────────
-- In MB, "producer" is encoded as a relationship type on l_artist_recording.
-- link_type names that indicate producer credits:
--   'producer', 'executive producer', 'co-producer', 'additional producer'
-- We derive a distinct Producer entity per artist who has such a credit.

INSERT INTO Producer (producer_id, name, country, birth_year, bio, artist_id, mb_artist_gid)
SELECT DISTINCT ON (a.id)
    a.id                                    AS producer_id,
    a.name                                  AS name,
    ar.name                                 AS country,
    a.begin_date_year                       AS birth_year,
    NULLIF(a.comment, '')                   AS bio,
    a.id                                    AS artist_id,       -- same entity; May_Be populated below
    a.gid                                   AS mb_artist_gid
FROM musicbrainz.l_artist_recording lar
JOIN musicbrainz.link lk          ON lk.id = lar.link
JOIN musicbrainz.link_type lt     ON lt.id = lk.link_type
JOIN musicbrainz.artist a         ON a.id  = lar.entity0
LEFT JOIN musicbrainz.area ar     ON ar.id = a.area
WHERE lt.name ILIKE ANY (ARRAY['%producer%'])
ON CONFLICT DO NOTHING;

SELECT setval(pg_get_serial_sequence('Producer', 'producer_id'),
              COALESCE(MAX(producer_id), 1)) FROM Producer;

-- May_Be: artist ↔ producer (same person)
INSERT INTO May_Be (artist_id, producer_id)
SELECT p.artist_id, p.producer_id
FROM Producer p
WHERE p.artist_id IS NOT NULL
  AND EXISTS (SELECT 1 FROM Artist a WHERE a.user_id = p.artist_id)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. ALBUM  (MB release_group as Album)
-- ─────────────────────────────────────────────────────────────────────────────
-- MB release_group columns: id, gid, name, artist_credit, type, comment, ...
-- We pick the earliest release date per release_group as the canonical date.
-- explicit_flag is not in MB; we default to FALSE.

INSERT INTO album (album_id, title, release_date, explicit_flag, mb_release_group_gid)
SELECT
    rg.id,
    rg.name,
    MIN(MAKE_DATE(rc.date_year, COALESCE(rc.date_month,1), COALESCE(rc.date_day,1))),
    FALSE,
    rg.gid
FROM musicbrainz.release_group rg
JOIN musicbrainz.release r ON r.release_group = rg.id
JOIN musicbrainz.release_country rc ON rc.release = r.id
WHERE rc.date_year IS NOT NULL
GROUP BY rg.id, rg.name, rg.gid
ON CONFLICT DO NOTHING;

INSERT INTO album (album_id, title, release_date, explicit_flag, mb_release_group_gid)
SELECT
    rg.id,
    rg.name,
    MIN(MAKE_DATE(ruc.date_year, COALESCE(ruc.date_month,1), COALESCE(ruc.date_day,1))),
    FALSE,
    rg.gid
FROM musicbrainz.release_group rg
JOIN musicbrainz.release r ON r.release_group = rg.id
JOIN musicbrainz.release_unknown_country ruc ON ruc.release = r.id
WHERE ruc.date_year IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM album al WHERE al.album_id = rg.id)
GROUP BY rg.id, rg.name, rg.gid
ON CONFLICT DO NOTHING;

INSERT INTO album (album_id, title, release_date, explicit_flag, mb_release_group_gid)
SELECT rg.id, rg.name, NULL, FALSE, rg.gid
FROM musicbrainz.release_group rg
WHERE NOT EXISTS (SELECT 1 FROM album al WHERE al.album_id = rg.id)
ON CONFLICT DO NOTHING;

SELECT setval(pg_get_serial_sequence('Album', 'album_id'),
              COALESCE(MAX(album_id), 1)) FROM Album;


-- ─────────────────────────────────────────────────────────────────────────────
