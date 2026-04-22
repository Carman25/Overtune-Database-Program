# Embedded SQL queries
# %s placeholders to prevent string SQL injection
 
QUERIES = {
    # BROWSE / SEARCH
    # Strips away input such that searching for a specific word like "rat" results in Congratulations
    # Expansion would be to add a more specific search that matches whole words only
    # Could add a separate "search tracks by ISRC" query.
    # Note: COALESCE is used to treat NULLs as empty strings, so they don't match anything.
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
        LEFT JOIN Track_ISRC ti   ON ti.track_id = t.track_id AND ti.is_primary = TRUE
        -- TODO: Is the is primary required?--
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

    # Tables: "User", Artist, Consumer (IS-A hierarchy)
    "search_users": """
        SELECT u.user_id,
               u.username,
               u.email,
               u.join_date,
               u.country,
               CASE
                   WHEN a.user_id IS NOT NULL AND c.user_id IS NOT NULL THEN 'Artist + Consumer'
                   WHEN a.user_id IS NOT NULL THEN 'Artist'
                   WHEN c.user_id IS NOT NULL THEN 'Consumer'
                   ELSE 'User'
               END AS role
        FROM "User" u
        LEFT JOIN Artist a   ON a.user_id = u.user_id
        LEFT JOIN Consumer c ON c.user_id = u.user_id
        WHERE LOWER(COALESCE(u.username, '')) LIKE LOWER(%s)
           OR LOWER(COALESCE(u.email, ''))    LIKE LOWER(%s)
        ORDER BY u.username
        LIMIT %s;
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
    # Helper: find group artists
    "find_groups": """
        SELECT DISTINCT a.user_id AS group_id, a.name
        FROM Artist a
        JOIN Artist_Member am ON am.group_artist_id = a.user_id
        WHERE LOWER(COALESCE(a.name, '')) LIKE LOWER(%s)
        ORDER BY a.name
        LIMIT %s;
    """,

    # Tables: Playlist, "User", Playlist_Track, Makes
    "user_playlists": """
        SELECT pl.playlist_id,
               pl.name,
               pl.is_public,
               pl.created_date,
               COUNT(DISTINCT pt.track_id) AS track_count
        FROM Makes m
        JOIN Playlist pl ON pl.playlist_id = m.playlist_id
        LEFT JOIN Playlist_Track pt ON pt.playlist_id = pl.playlist_id
        WHERE m.user_id = %s
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

        # Tables: Track_ISRC, Track
    "track_isrcs": """
        SELECT t.title   AS track_title,
               ti.isrc,
               ti.is_primary
        FROM Track_ISRC ti
        JOIN Track t ON t.track_id = ti.track_id
        WHERE ti.track_id = %s
        ORDER BY ti.is_primary DESC, ti.isrc;
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

    # Helper: find consumers
    "find_consumers": """
        SELECT c.user_id AS consumer_id,
               c.display_name,
               u.username
        FROM Consumer c
        JOIN "User" u ON u.user_id = c.user_id
        WHERE LOWER(COALESCE(c.display_name, '')) LIKE LOWER(%s)
           OR LOWER(COALESCE(u.username, ''))     LIKE LOWER(%s)
        ORDER BY c.display_name
        LIMIT %s;
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
 
    # Only 2160 tracks have genre tags in the small dataset
    # Demonstrates aggregation and sorting by multiple criteria.
    # Gives a total of 9 actual genres with real results
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

    # Not easily manipulable to find a specific genre
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

    # INSERT: create a new review
    "create_review": """
        INSERT INTO Review (rating, review_text, created_at, consumer_id, track_id)
        VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s)
        RETURNING review_id;
    """,

    # INSERT: link review via Written_By
    "create_written_by": """
        INSERT INTO Written_By (consumer_id, review_id)
        VALUES (%s, %s);
    """,

    # INSERT: link review via Gets
    "create_gets": """
        INSERT INTO Gets (review_id, track_id)
        VALUES (%s, %s);
    """,

    # INSERT: create a new playlist
    "create_playlist": """
        INSERT INTO Playlist (name, description, created_date, is_public, user_id)
        VALUES (%s, %s, CURRENT_DATE, %s, %s)
        RETURNING playlist_id;
    """,

    # INSERT: link playlist via Makes
    "create_makes": """
        INSERT INTO Makes (user_id, playlist_id)
        VALUES (%s, %s);
    """,

    # INSERT: add a track to a playlist
    "add_track_to_playlist": """
        INSERT INTO Playlist_Track (playlist_id, track_id, added_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP);
    """,

    # Fetch playlists owned by a user
    "user_playlist_list": """
        SELECT pl.playlist_id, pl.name
        FROM Makes m
        JOIN Playlist pl ON pl.playlist_id = m.playlist_id
        WHERE m.user_id = %s
        ORDER BY pl.name;
    """,

    # DELETE: remove a track from a playlist
    "remove_track_from_playlist": """
        DELETE FROM Playlist_Track
        WHERE playlist_id = %s AND track_id = %s
        RETURNING playlist_id, track_id;
    """,

    # Admin: show current user details
    "who_am_i": """
        SELECT u.user_id,
               u.username,
               u.email,
               u.join_date,
               u.country,
               c.display_name,
               a.name AS artist_name,
               a.type AS artist_type,
               CASE
                   WHEN a.user_id IS NOT NULL AND c.user_id IS NOT NULL THEN 'Artist + Consumer'
                   WHEN a.user_id IS NOT NULL THEN 'Artist'
                   WHEN c.user_id IS NOT NULL THEN 'Consumer'
                   ELSE 'User only'
               END AS role
        FROM "User" u
        LEFT JOIN Artist a   ON a.user_id = u.user_id
        LEFT JOIN Consumer c ON c.user_id = u.user_id
        WHERE u.user_id = %s;
    """,
}