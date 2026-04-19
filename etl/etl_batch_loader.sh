#!/bin/bash
set -e
DB="psql -U pablo -d overtune_demo"

echo "=== ALBUM ==="
$DB << 'EOF'
DO $$
DECLARE
  batch_size INT := 500000;
  offset_val INT := 0;
  inserted   INT;
BEGIN
  LOOP
    INSERT INTO album (album_id, title, release_date, explicit_flag, mb_release_group_gid)
    SELECT rg.id, rg.name,
        MIN(MAKE_DATE(rc.date_year, COALESCE(rc.date_month,1), COALESCE(rc.date_day,1))),
        FALSE, rg.gid
    FROM musicbrainz.release_group rg
    JOIN musicbrainz.release r ON r.release_group = rg.id
    JOIN musicbrainz.release_country rc ON rc.release = r.id
    WHERE rc.date_year IS NOT NULL
    GROUP BY rg.id, rg.name, rg.gid
    ORDER BY rg.id
    LIMIT batch_size OFFSET offset_val
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS inserted = ROW_COUNT;
    RAISE NOTICE 'Album offset % — inserted % rows', offset_val, inserted;
    EXIT WHEN inserted = 0;
    offset_val := offset_val + batch_size;
    COMMIT;
  END LOOP;
END $$;
EOF

echo "=== TRACK ==="
$DB << 'EOF'
DO $$
DECLARE
  batch_size INT := 500000;
  offset_val INT := 0;
  inserted   INT;
BEGIN
  LOOP
    INSERT INTO track (track_id, title, duration_sec, bpm, key, mode, explicit_flag, mb_recording_gid)
    SELECT r.id, r.name, (r.length / 1000)::INT, NULL, NULL, NULL, FALSE, r.gid
    FROM musicbrainz.recording r
    ORDER BY r.id
    LIMIT batch_size OFFSET offset_val
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS inserted = ROW_COUNT;
    RAISE NOTICE 'Track offset % — inserted % rows', offset_val, inserted;
    EXIT WHEN inserted = 0;
    offset_val := offset_val + batch_size;
    COMMIT;
  END LOOP;
END $$;
EOF

echo "=== TRACK_ISRC ==="
$DB -c "
INSERT INTO track_isrc (track_id, isrc, is_primary)
SELECT i.recording, i.isrc,
    (i.isrc = MIN(i.isrc) OVER (PARTITION BY i.recording)) AS is_primary
FROM musicbrainz.isrc i
JOIN track t ON t.track_id = i.recording
ON CONFLICT DO NOTHING;"

echo "=== JUNCTION TABLES ==="
$DB -f /home/pablo/etl_part3_post_isrc.sql

echo "=== DONE ==="
