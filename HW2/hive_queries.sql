-- 0. create table
CREATE table default.artists (
	mbid STRING, artist_mb STRING, artist_lastfm STRING, country_mb STRING,
	country_lastfm STRING, tags_mb STRING, tags_lastfm STRING,
	listeners_lastfm BIGINT, scrobbles_lastfm BIGINT, ambiguous_artist BOOLEAN
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','

-- 0.1 import csv from container 
LOAD DATA LOCAL INPATH '/artists.csv' OVERWRITE INTO TABLE artists

-- 1. most popular artist
select artist_lastfm from artists
where scrobbles_lastfm in ( 
    select max(scrobbles_lastfm) from artists 
    )
-- result:
-- +----------------+
-- | artist_lastfm  |
-- +----------------+
-- | The Beatles    |
-- +----------------+

-- 2. most popular tag
WITH temp AS (
    SELECT tag_lf, COUNT(*) as tag_cnt
    FROM artists
    LATERAL VIEW explode(split(tags_lastfm, ';')) tagTable AS tag_lf
    WHERE tag_lf <> ''
    GROUP BY tag_lf
    ORDER BY tag_cnt DESC
    limit 1
    )
SELECT tag_lf
FROM temp
-- result:
-- +-------------+
-- |   tag_lf    |
-- +-------------+
-- |  seen live  |
-- +-------------+

-- 3. most popular artists in most popular tags
WITH t1 AS (
    SELECT tag_lf, COUNT(*) as tag_cnt
    FROM artists
    LATERAL VIEW explode(split(tags_lastfm, ';')) tagTable AS tag_lf
    WHERE tag_lf <> ''
    GROUP BY tag_lf
    ORDER BY tag_cnt DESC
    limit 10
    ),
t2 AS (
    SELECT tag_lf, artist_lastfm, listeners_lastfm
    FROM artists
    LATERAL VIEW explode(split(tags_lastfm, ';')) tagTable AS tag_lf
)
SELECT DISTINCT artist_lastfm, listeners_lastfm
FROM t2
WHERE tag_lf in (SELECT tag_lf FROM t1)
ORDER BY listeners_lastfm DESC
limit 10
-- result:
-- +------------------------+-------------------+
-- |     artist_lastfm      | listeners_lastfm  |
-- +------------------------+-------------------+
-- | Coldplay               | 5381567           |
-- | Radiohead              | 4732528           |
-- | Red Hot Chili Peppers  | 4620835           |
-- | Rihanna                | 4558193           |
-- | Eminem                 | 4517997           |
-- | The Killers            | 4428868           |
-- | Kanye West             | 4390502           |
-- | Nirvana                | 4272894           |
-- | Muse                   | 4089612           |
-- | Queen                  | 4023379           |
-- +------------------------+-------------------+

-- 4. most popular russian witch house by scrobbles
WITH temp AS (
    SELECT tag_lf, artist_lastfm, scrobbles_lastfm
    FROM artists
    LATERAL VIEW explode(split(tags_lastfm, ';')) tagTable AS tag_lf
    WHERE tag_lf = 'witch house' AND country_lastfm = 'Russia'
)
SELECT DISTINCT artist_lastfm, scrobbles_lastfm
FROM temp
ORDER BY scrobbles_lastfm DESC
limit 10
-- result:
-- +-----------------+-------------------+
-- |  artist_lastfm  | scrobbles_lastfm  |
-- +-----------------+-------------------+
-- | Summer of Haze  | 1874020           |
-- | IC3PEAK         | 1310984           |
-- | Blvck Ceiling   | 1022404           |
-- | Радость моя     | 556252            |
-- | Ev3nmorn        | 76084             |
-- | ~▲†▲~           | 49032             |
-- | Tigerberry      | 15796             |
-- | †LOΛΣΓS†        | 14644             |
-- | Vagina Vangi    | 0                 |
-- +-----------------+-------------------+






