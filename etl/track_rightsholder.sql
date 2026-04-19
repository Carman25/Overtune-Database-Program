SET search_path = public, musicbrainz, pg_catalog;
INSERT INTO track_rightsholder (track_id, rights_id, rights_type, percentage)
SELECT DISTINCT
    tr.track_id,
    rh.rights_id,
    'master',
    NULL::FLOAT
FROM musicbrainz.track t
JOIN musicbrainz.medium m           ON m.id = t.medium
JOIN musicbrainz.release r          ON r.id = m.release
JOIN musicbrainz.release_label rl   ON rl.release = r.id
JOIN musicbrainz.label mbl          ON mbl.id = rl.label
JOIN musicbrainz.recording rec      ON rec.id = t.recording
JOIN track tr                       ON tr.mb_recording_gid = rec.gid
JOIN label l                        ON l.mb_label_gid = mbl.gid
JOIN rightsholder rh                ON rh.rights_id = l.label_id
ON CONFLICT DO NOTHING;
