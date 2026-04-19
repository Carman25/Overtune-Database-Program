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
-- 7. TRACK  (MB recording as Track)
-- ─────────────────────────────────────────────────────────────────────────────
-- MB recording columns: id, gid, name, artist_credit, length (ms), comment, ...
-- MB isrc: id, recording, isrc  (all ISRCs loaded into Track_ISRC below)
-- bpm / key / mode come from Spotify — set NULL here, fill via Spotify API later.

INSERT INTO Track (track_id, title, duration_sec, bpm, key, mode, explicit_flag, mb_recording_gid)
SELECT
    r.id                                    AS track_id,
    r.name                                  AS title,
    (r.length / 1000)::INT                  AS duration_sec,    -- MB stores ms
    NULL                                    AS bpm,
    NULL                                    AS key,
    NULL                                    AS mode,
    FALSE                                   AS explicit_flag,
    r.gid                                   AS mb_recording_gid
FROM musicbrainz.recording r
ON CONFLICT DO NOTHING;

SELECT setval(pg_get_serial_sequence('Track', 'track_id'),
              COALESCE(MAX(track_id), 1)) FROM Track;

-- ── Track_ISRC ────────────────────────────────────────────────────────────────
-- Load ALL ISRCs for every recording, not just one.
-- is_primary = TRUE is assigned to the lexicographically first ISRC per track,
-- which is used as the preferred code for Spotify API lookups.
-- Rare MB data quality issue: the same ISRC code assigned to two different
-- recordings. ON CONFLICT DO NOTHING silently skips the duplicate.
INSERT INTO Track_ISRC (track_id, isrc, is_primary)
SELECT
    i.recording                             AS track_id,
    i.isrc                                  AS isrc,
    -- Mark the first ISRC (alphabetically) for each recording as primary
    (i.isrc = MIN(i.isrc) OVER (PARTITION BY i.recording)) AS is_primary
FROM musicbrainz.isrc i
WHERE EXISTS (SELECT 1 FROM Track t WHERE t.track_id = i.recording)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. TRACK_ALBUM  (recording → release_group via medium/track)
-- ─────────────────────────────────────────────────────────────────────────────
-- MB path: recording → track → medium → release → release_group
-- MB track columns: id, recording, medium, position, number, name, ...
-- MB medium columns: id, release, position, ...

INSERT INTO Track_Album (track_id, album_id, track_number)
SELECT DISTINCT ON (t.recording, r.release_group)
    t.recording                             AS track_id,
    r.release_group                         AS album_id,
    t.position                              AS track_number
FROM musicbrainz.track t
JOIN musicbrainz.medium m     ON m.id = t.medium
JOIN musicbrainz.release r    ON r.id = m.release
WHERE EXISTS (SELECT 1 FROM Track tr WHERE tr.track_id = t.recording)
  AND EXISTS (SELECT 1 FROM Album al WHERE al.album_id = r.release_group)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. TRACK_ARTIST  (artist_credit → recording)
-- ─────────────────────────────────────────────────────────────────────────────
-- MB path: recording.artist_credit → artist_credit_name.artist_credit → artist
-- MB artist_credit_name columns: artist_credit, position, artist, name, join_phrase

INSERT INTO Track_Artist (track_id, artist_id, role)
SELECT DISTINCT
    rec.id                                  AS track_id,
    acn.artist                              AS artist_id,
    'primary'                               AS role
FROM musicbrainz.recording rec
JOIN musicbrainz.artist_credit_name acn ON acn.artist_credit = rec.artist_credit
WHERE EXISTS (SELECT 1 FROM Track t WHERE t.track_id = rec.id)
  AND EXISTS (SELECT 1 FROM Artist a WHERE a.user_id = acn.artist)
ON CONFLICT DO NOTHING;

-- Additional performers from l_artist_recording (featured, live, etc.)
INSERT INTO Track_Artist (track_id, artist_id, role)
SELECT DISTINCT
    lar.entity1                             AS track_id,
    lar.entity0                             AS artist_id,
    lt.name                                 AS role
FROM musicbrainz.l_artist_recording lar
JOIN musicbrainz.link     lk ON lk.id = lar.link
JOIN musicbrainz.link_type lt ON lt.id = lk.link_type
WHERE lt.name NOT ILIKE '%producer%'        -- producers handled separately
  AND EXISTS (SELECT 1 FROM Track  t WHERE t.track_id   = lar.entity1)
  AND EXISTS (SELECT 1 FROM Artist a WHERE a.user_id    = lar.entity0)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 10. TRACK_SONGWRITER
-- ─────────────────────────────────────────────────────────────────────────────
-- MB models songwriting via:
--   l_artist_work  → links an artist to a "work" (composition) with a role
--   l_recording_work → links a recording to its underlying work
-- Songwriter link_type names include: 'composer', 'lyricist', 'writer',
-- 'librettist', 'arranger', 'orchestrator', 'translator'

-- Step 1: temp table matching recording → artist via work
CREATE TEMP TABLE _songwriter_map AS
SELECT DISTINCT
    lrw.entity0                             AS recording_id,
    law.entity0                             AS artist_id,
    lt.name                                 AS contribution
FROM musicbrainz.l_recording_work lrw
JOIN musicbrainz.l_artist_work law ON law.entity1 = lrw.entity1
JOIN musicbrainz.link     lk  ON lk.id  = law.link
JOIN musicbrainz.link_type lt ON lt.id  = lk.link_type
WHERE lt.name ILIKE ANY (ARRAY[
    '%composer%', '%lyricist%', '%writer%',
    '%librettist%', '%arranger%', '%orchestrator%'
]);

INSERT INTO Track_Songwriter (track_id, artist_id, contribution)
SELECT DISTINCT
    sm.recording_id,
    sm.artist_id,
    sm.contribution
FROM _songwriter_map sm
WHERE EXISTS (SELECT 1 FROM Track  t WHERE t.track_id = sm.recording_id)
  AND EXISTS (SELECT 1 FROM Artist a WHERE a.user_id  = sm.artist_id)
ON CONFLICT DO NOTHING;

DROP TABLE _songwriter_map;


-- ─────────────────────────────────────────────────────────────────────────────
-- 11. TRACK_PRODUCER
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO Track_Producer (track_id, producer_id, credit_type)
SELECT DISTINCT
    lar.entity1                             AS track_id,
    lar.entity0                             AS producer_id,
    lt.name                                 AS credit_type
FROM musicbrainz.l_artist_recording lar
JOIN musicbrainz.link      lk ON lk.id = lar.link
JOIN musicbrainz.link_type lt ON lt.id = lk.link_type
WHERE lt.name ILIKE ANY (ARRAY['%producer%'])
  AND EXISTS (SELECT 1 FROM Track    t WHERE t.track_id    = lar.entity1)
  AND EXISTS (SELECT 1 FROM Producer p WHERE p.producer_id = lar.entity0)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 12. ARTIST_MEMBER  (group membership)
-- ─────────────────────────────────────────────────────────────────────────────
-- MB l_artist_artist + link_type 'member of band'
-- link columns: begin_date_year, end_date_year
-- l_artist_artist.entity0 = member, entity1 = group

INSERT INTO Artist_Member (group_artist_id, member_artist_id, instrument, join_year, leave_year)
SELECT DISTINCT
    laa.entity1                             AS group_artist_id,
    laa.entity0                             AS member_artist_id,
    NULL                                    AS instrument,      -- instrument via link_attribute; omitted for simplicity
    lk.begin_date_year                      AS join_year,
    lk.end_date_year                        AS leave_year
FROM musicbrainz.l_artist_artist laa
JOIN musicbrainz.link      lk ON lk.id = laa.link
JOIN musicbrainz.link_type lt ON lt.id = lk.link_type
WHERE lt.name ILIKE '%member%'
  AND EXISTS (SELECT 1 FROM Artist a WHERE a.user_id = laa.entity0)
  AND EXISTS (SELECT 1 FROM Artist b WHERE b.user_id = laa.entity1)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 13. LABEL_ALBUM
-- ─────────────────────────────────────────────────────────────────────────────
-- MB release_label: release, label, catalog_number
-- We map release → release_group to get our album_id.

INSERT INTO Label_Album (label_id, album_id)
SELECT DISTINCT
    rl.label                                AS label_id,
    r.release_group                         AS album_id
FROM musicbrainz.release_label rl
JOIN musicbrainz.release r ON r.id = rl.release
WHERE rl.label IS NOT NULL
  AND EXISTS (SELECT 1 FROM Label l WHERE l.label_id = rl.label)
  AND EXISTS (SELECT 1 FROM Album a WHERE a.album_id = r.release_group)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 14. LABEL_ARTIST
-- ─────────────────────────────────────────────────────────────────────────────
-- MB has no direct label-artist signing table.
-- Best proxy: l_label_release + release.artist_credit → artist_credit_name
-- We derive: label signed artist if label released an album by that artist.
-- start_year / end_year approximated from the first and last release dates.

INSERT INTO Label_Artist (label_id, artist_id, start_year, end_year)
SELECT
    rl.label                                AS label_id,
    acn.artist                              AS artist_id,
    MIN(r.date_year)                        AS start_year,
    MAX(r.date_year)                        AS end_year
FROM musicbrainz.release_label rl
JOIN musicbrainz.release r         ON r.id = rl.release
JOIN musicbrainz.artist_credit_name acn
                                   ON acn.artist_credit = r.artist_credit
WHERE rl.label IS NOT NULL
  AND EXISTS (SELECT 1 FROM Label  l WHERE l.label_id = rl.label)
  AND EXISTS (SELECT 1 FROM Artist a WHERE a.user_id  = acn.artist)
GROUP BY rl.label, acn.artist
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 15. TRACK_GENRE
-- ─────────────────────────────────────────────────────────────────────────────
-- MB genre-to-recording links come from recording_tag where the tag name
-- matches an entry in the genre table.

INSERT INTO Track_Genre (track_id, genre_id)
SELECT DISTINCT
    rt.recording                            AS track_id,
    g.id                                    AS genre_id
FROM musicbrainz.recording_tag rt
JOIN musicbrainz.tag           tg ON tg.id   = rt.tag
JOIN musicbrainz.genre         g  ON g.name  = tg.name
WHERE EXISTS (SELECT 1 FROM Track t WHERE t.track_id = rt.recording)
  AND EXISTS (SELECT 1 FROM Genre gn WHERE gn.genre_id = g.id)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- 16. TRACK_RIGHTSHOLDER  (label proxy)
-- ─────────────────────────────────────────────────────────────────────────────
-- Since RightsHolder was mapped from Label, we can link tracks to rights holders
-- via the same label→release→track path used in Label_Album / Track_Album.

INSERT INTO Track_RightsHolder (track_id, rights_id, rights_type, percentage)
SELECT DISTINCT
    t.recording                             AS track_id,
    rl.label                                AS rights_id,
    'master'                                AS rights_type,
    NULL                                    AS percentage
FROM musicbrainz.track t
JOIN musicbrainz.medium m      ON m.id = t.medium
JOIN musicbrainz.release r     ON r.id = m.release
JOIN musicbrainz.release_label rl ON rl.release = r.id
WHERE rl.label IS NOT NULL
  AND EXISTS (SELECT 1 FROM Track       tr WHERE tr.track_id = t.recording)
  AND EXISTS (SELECT 1 FROM RightsHolder rh WHERE rh.rights_id = rl.label)
ON CONFLICT DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- DONE
-- ─────────────────────────────────────────────────────────────────────────────
-- All steps completed. Each INSERT above committed individually.

-- =============================================================================
-- POST-IMPORT NOTES
-- =============================================================================
--
-- 1. BPM / KEY / MODE (Track table)
--    These are not in MusicBrainz. Enrich after import via Spotify Web API:
--      a) For each Track_ISRC row where is_primary = TRUE, call:
--           GET https://api.spotify.com/v1/search?q=isrc:{isrc}&type=track
--         to resolve the Spotify track ID.
--      b) Then call:
--           GET https://api.spotify.com/v1/audio-features/{spotify_track_id}
--         Response fields → OVERTUNE columns:
--           tempo          → bpm   (NUMERIC(6,2), e.g. 120.04)
--           key            → key   (integer 0–11; map to note name or store raw)
--           mode           → mode  (0 = minor, 1 = major)
--    Alternatively, use mb_recording_gid to look up the MB API, get the ISRC
--    from there, then proceed to Spotify.
--    Update pattern:
--      UPDATE Track SET bpm = $1, key = $2, mode = $3
--        AND ti.isrc = $4;
--
-- 2. ISRC — MULTI-VALUE DESIGN
--    ISRCs are now stored in Track_ISRC (track_id, isrc, is_primary) rather
--    than directly on Track. Every ISRC in mb.isrc for a given recording is
--    loaded, so one track can have many codes. The is_primary flag marks the
--    lexicographically first one as the preferred Spotify lookup key.
--
--    To find all ISRCs for a track:
--      SELECT isrc, is_primary FROM Track_ISRC WHERE track_id = $1;
--
--    To audit MB recordings with more than one ISRC:
--      SELECT track_id, COUNT(*) AS n FROM Track_ISRC
--      GROUP BY track_id HAVING COUNT(*) > 1 ORDER BY n DESC;
--
--    To find ISRC codes shared across multiple recordings (MB data quality):
--      SELECT isrc, COUNT(*) AS n FROM Track_ISRC
--      GROUP BY isrc HAVING COUNT(*) > 1;
--
-- 3. MB GID COLUMNS (mb_*_gid)
--    All six GID columns (Track, Artist, Album, Label, Genre, Producer) are
--    populated from mb.*.gid during ETL and carry UNIQUE constraints.
--    Primary uses:
--      • Deduplication: prevent re-importing rows already loaded.
--      • MB API enrichment: fetch latest metadata by MBID.
--      • Spotify cross-reference: MB ISRC → Spotify audio-features.
--      • Future replication: apply MB live data feed diffs by MBID.
--
-- 4. INSTRUMENT in Artist_Member
--    MB stores instruments via link_attribute on l_artist_artist.
--    To populate:
--      SELECT laa.entity0, laa.entity1, ia.name
--      FROM musicbrainz.l_artist_artist laa
--      JOIN musicbrainz.link_attribute la ON la.link = laa.link
--      JOIN musicbrainz.instrument ia     ON ia.id   = la.attribute_type
--    Then:
--      UPDATE Artist_Member am
--      SET instrument = sub.name
--      FROM (...above query...) sub
--      WHERE am.member_artist_id = sub.entity0
--        AND am.group_artist_id  = sub.entity1;
--
-- 5. LABEL.website
--    Available via l_label_url in MB:
--      SELECT ll.entity0 AS label_id, u.url
--      FROM musicbrainz.l_label_url ll
--      JOIN musicbrainz.url u ON u.id = ll.entity1
--      JOIN musicbrainz.link lk ON lk.id = ll.link
--      JOIN musicbrainz.link_type lt ON lt.id = lk.link_type
--      WHERE lt.name = 'official homepage';
--    Then UPDATE Label SET website = u.url WHERE label_id = ll.entity0;
--
-- 6. EXPLICIT FLAG (Track / Album)
--    Not available in MB. Cross-reference Spotify track/album objects if needed.
--
-- 7. CONSUMER / PLAYLIST / REVIEW tables
--    Entirely app-generated. Populate separately once the application layer is
--    in place, or seed with synthetic data for testing.
--
-- 8. GENRE hierarchy (parent_genre_id)
--    MB genres are currently flat (no parent). A curated hierarchy is available
--    at https://musicbrainz.org/genres — apply via a separate mapping script.
--
-- 9. SCALE / PERFORMANCE TIPS
--    Full MB dump: ~1.5 M artists, ~30 M recordings, ~3 M release_groups.
--    • For development, add WHERE clauses to limit rows (e.g. WHERE a.id < 50000).
--    • Drop indexes BEFORE bulk load, recreate AFTER (schema.sql puts them last).
--    • Use COPY for production-scale loads instead of INSERT ... SELECT.
--    • Run ANALYZE on target tables after import for good query plans.
--
-- =============================================================================
