/*
Small seed set for functional testing.
These MERGE statements make reruns idempotent.
*/
MERGE dbo.DimUser AS target
USING (
    SELECT *
    FROM (VALUES
        (1, 'Carlos Berry', 'Switzerland', 'Premium', CAST('2023-10-17' AS DATE), NULL, CAST('2026-03-01T10:00:00' AS DATETIME2(0))),
        (2, 'Amanda Jenkins', 'Montserrat', 'Family', CAST('2024-09-28' AS DATE), NULL, CAST('2026-03-01T10:05:00' AS DATETIME2(0))),
        (3, 'Daniel Cook', 'Chile', 'Premium', CAST('2025-07-07' AS DATE), NULL, CAST('2026-03-01T10:10:00' AS DATETIME2(0))),
        (4, 'Peter Hernandez', 'Nigeria', 'Free', CAST('2024-07-16' AS DATE), NULL, CAST('2026-03-01T10:15:00' AS DATETIME2(0))),
        (5, 'Yolanda Morris', 'Aruba', 'Premium', CAST('2025-05-12' AS DATE), NULL, CAST('2026-03-01T10:20:00' AS DATETIME2(0)))
    ) AS s(user_id, user_name, country, subscription_type, start_date, end_date, updated_at)
) AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN UPDATE SET
    user_name = source.user_name,
    country = source.country,
    subscription_type = source.subscription_type,
    start_date = source.start_date,
    end_date = source.end_date,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (user_id, user_name, country, subscription_type, start_date, end_date, updated_at)
    VALUES (source.user_id, source.user_name, source.country, source.subscription_type, source.start_date, source.end_date, source.updated_at);
GO

MERGE dbo.DimArtist AS target
USING (
    SELECT *
    FROM (VALUES
        (1, 'Lisa Jackson', 'Rock', 'Canada', CAST('2026-03-01T10:00:00' AS DATETIME2(0))),
        (2, 'Adam Sampson', 'Electronic', 'United Kingdom', CAST('2026-03-01T10:05:00' AS DATETIME2(0))),
        (3, 'Joshua Christian', 'Jazz', 'United States', CAST('2026-03-01T10:10:00' AS DATETIME2(0)))
    ) AS s(artist_id, artist_name, genre, country, updated_at)
) AS source
ON target.artist_id = source.artist_id
WHEN MATCHED THEN UPDATE SET
    artist_name = source.artist_name,
    genre = source.genre,
    country = source.country,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (artist_id, artist_name, genre, country, updated_at)
    VALUES (source.artist_id, source.artist_name, source.genre, source.country, source.updated_at);
GO

MERGE dbo.DimTrack AS target
USING (
    SELECT *
    FROM (VALUES
        (1, 'Focused next generation encryption', 1, 'Piece Album', 248, CAST('2022-04-14' AS DATE), CAST('2026-03-01T10:00:00' AS DATETIME2(0))),
        (2, 'Public key object oriented archive', 2, 'Late Album', 257, CAST('2021-03-08' AS DATE), CAST('2026-03-01T10:05:00' AS DATETIME2(0))),
        (3, 'Mandatory intermediate artificial intelligence', 3, 'Too Album', 218, CAST('2023-08-24' AS DATE), CAST('2026-03-01T10:10:00' AS DATETIME2(0))),
        (4, 'Grass roots tangible attitude', 1, 'Art Album', 330, CAST('2023-01-25' AS DATE), CAST('2026-03-01T10:15:00' AS DATETIME2(0)))
    ) AS s(track_id, track_name, artist_id, album_name, duration_sec, release_date, updated_at)
) AS source
ON target.track_id = source.track_id
WHEN MATCHED THEN UPDATE SET
    track_name = source.track_name,
    artist_id = source.artist_id,
    album_name = source.album_name,
    duration_sec = source.duration_sec,
    release_date = source.release_date,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (track_id, track_name, artist_id, album_name, duration_sec, release_date, updated_at)
    VALUES (source.track_id, source.track_name, source.artist_id, source.album_name, source.duration_sec, source.release_date, source.updated_at);
GO

MERGE dbo.DimDate AS target
USING (
    SELECT *
    FROM (VALUES
        (20260301, CAST('2026-03-01' AS DATE), 1, 3, 2026, 'Sunday'),
        (20260302, CAST('2026-03-02' AS DATE), 2, 3, 2026, 'Monday'),
        (20260303, CAST('2026-03-03' AS DATE), 3, 3, 2026, 'Tuesday'),
        (20260304, CAST('2026-03-04' AS DATE), 4, 3, 2026, 'Wednesday'),
        (20260305, CAST('2026-03-05' AS DATE), 5, 3, 2026, 'Thursday')
    ) AS s(date_key, [date], [day], [month], [year], weekday)
) AS source
ON target.date_key = source.date_key
WHEN MATCHED THEN UPDATE SET
    [date] = source.[date],
    [day] = source.[day],
    [month] = source.[month],
    [year] = source.[year],
    weekday = source.weekday
WHEN NOT MATCHED THEN
    INSERT (date_key, [date], [day], [month], [year], weekday)
    VALUES (source.date_key, source.[date], source.[day], source.[month], source.[year], source.weekday);
GO

MERGE dbo.FactStream AS target
USING (
    SELECT *
    FROM (VALUES
        (1001, 1, 1, 20260301, 240, 'mobile', CAST('2026-03-01T11:00:00' AS DATETIME2(0))),
        (1002, 2, 2, 20260301, 180, 'desktop', CAST('2026-03-01T11:05:00' AS DATETIME2(0))),
        (1003, 3, 3, 20260302, 220, 'speaker', CAST('2026-03-02T11:10:00' AS DATETIME2(0))),
        (1004, 4, 4, 20260303, 320, 'mobile', CAST('2026-03-03T11:15:00' AS DATETIME2(0))),
        (1005, 5, 1, 20260304, 200, 'car', CAST('2026-03-04T11:20:00' AS DATETIME2(0)))
    ) AS s(stream_id, user_id, track_id, date_key, listen_duration, device_type, stream_timestamp)
) AS source
ON target.stream_id = source.stream_id
WHEN MATCHED THEN UPDATE SET
    user_id = source.user_id,
    track_id = source.track_id,
    date_key = source.date_key,
    listen_duration = source.listen_duration,
    device_type = source.device_type,
    stream_timestamp = source.stream_timestamp
WHEN NOT MATCHED THEN
    INSERT (stream_id, user_id, track_id, date_key, listen_duration, device_type, stream_timestamp)
    VALUES (source.stream_id, source.user_id, source.track_id, source.date_key, source.listen_duration, source.device_type, source.stream_timestamp);
GO

