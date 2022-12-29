import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
SONG_DATA = config.get('S3', 'SONG_DATA') 
LOG_DATA = config.get('S3', 'LOG_DATA') 
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH') 
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          CHAR(1),
        itemInSession   VARCHAR,
        lastName        VARCHAR,
        length          DECIMAL,
        level           VARCHAR,
        location        VARCHAR(500),
        method          VARCHAR,
        page            VARCHAR(500),
        registration    DECIMAL,
        sessionId       INTEGER,
        song            VARCHAR,
        status          SMALLINT,
        ts              BIGINT,
        userAgent       VARCHAR,
        userId          smallint
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR(250), 
        artist_latitude     VARCHAR(500),
        artist_longitude    VARCHAR(500),
        artist_location     VARCHAR(500),
        artist_name         VARCHAR(500),
        song_id             VARCHAR,
        title               VARCHAR(500),
        duration            DECIMAL,
        year                SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id         BIGINT IDENTITY(0,1) PRIMARY KEY NOT NULL,
        start_time          TIMESTAMP   SORTKEY, 
        user_id             INTEGER, 
        level               VARCHAR, 
        song_id             VARCHAR     DISTKEY, 
        artist_id           VARCHAR, 
        session_id          INTEGER, 
        location            VARCHAR(500), 
        user_agent          VARCHAR(500)
    );
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id         INTEGER PRIMARY KEY NOT NULL SORTKEY,
        first_name      VARCHAR,
        last_name       VARCHAR,
        gender          VARCHAR,
        level           VARCHAR
    );

""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id         VARCHAR PRIMARY KEY NOT NULL SORTKEY DISTKEY,
        title           VARCHAR(500),
        artist_id       VARCHAR, 
        year            SMALLINT, 
        duration        DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id       VARCHAR PRIMARY KEY NOT NULL SORTKEY, 
        name            VARCHAR(500), 
        location        VARCHAR(500), 
        latitude        DECIMAL, 
        longitude       DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time      TIMESTAMP PRIMARY KEY NOT NULL SORTKEY, 
        hour            SMALLINT NOT NULL, 
        day             SMALLINT NOT NULL, 
        week            SMALLINT NOT NULL,
        month           SMALLINT NOT NULL,
        year            SMALLINT NOT NULL, 
        weekday         SMALLINT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
    ACCEPTINVCHARS AS '^';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
	start_time
	,user_id
	,LEVEL
	,song_id
	,artist_id
	,session_id
	,location
	,user_agent
	)
SELECT TIMESTAMP 'epoch' + (ste.ts / 1000) * interval '1 second' AS start_time
	,ste.userId AS user_id
	,ste.LEVEL AS LEVEL
	,sts.song_id AS song_id
	,sts.artist_id AS artist_id
	,ste.sessionId AS session_id
	,ste.location AS location
	,ste.userAgent AS user_agent
FROM staging_events AS ste
INNER JOIN staging_songs AS sts ON ste.artist = sts.artist_name
	WHERE ste.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (
	user_id
	,first_name
	,last_name
	,gender
	,LEVEL
	)
SELECT DISTINCT userId AS user_id
	,firstName AS first_name
	,lastName AS last_name
	,gender AS gender
	,LEVEL AS LEVEL
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (
	song_id
	,title
	,artist_id
	,year
	,duration
	)
SELECT DISTINCT song_id AS song_id
	,title AS title
	,artist_id AS artist_id
	,year AS year
	,duration AS duration
FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (
	artist_id
	,name
	,location
	,latitude
	,longitude
	)
SELECT DISTINCT artist_id AS artist_id
	,artist_name AS name
	,artist_location AS location
	,artist_latitude AS latitude
	,artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO TIME(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time
	,EXTRACT(hour FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS hour
	,EXTRACT(day FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS day
	,EXTRACT(week FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS week
	,EXTRACT(month FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS month
	,EXTRACT(year FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS year
	,EXTRACT(weekday FROM (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second')) AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
