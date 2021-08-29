import configparser
import json


# CONVENIENCE METHOD
def load_iam_role_arn():
    """
    Loads iam role arn.
    :return:
    """
    # Opening JSON file
    f = open('aws_role_arn.json', )

    # returns JSON object as
    # a dictionary
    data = json.load(f)

    # Closing file
    f.close()

    return data['iam_role_arn']


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
iam_role_arn = load_iam_role_arn()
s3_staging_data_region = 'us-west-2'
log_data_url = config.get('S3', 'LOG_DATA')
song_data_url = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = """
DROP TABLE IF EXISTS "staging_events";
"""
staging_songs_table_drop = """
DROP TABLE IF EXISTS "staging_songs";
"""
songplay_table_drop = """
DROP TABLE IF EXISTS "songplays_fact";
"""
user_table_drop = """
DROP TABLE IF EXISTS "users_dim";
"""
song_table_drop = """
DROP TABLE IF EXISTS "songs_dim";
"""
artist_table_drop = """
DROP TABLE IF EXISTS "artists_dim";
"""
time_table_drop = """
DROP TABLE IF EXISTS "times_dim";
"""

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE "staging_events" (
    "artist" VARCHAR(250),
    "auth" VARCHAR(250),
    "first_name" VARCHAR(250),
    "gender" VARCHAR(250),
    "item_in_session" NUMERIC,
    "last_name" VARCHAR(250),
    "length" NUMERIC,
    "level" VARCHAR(250),
    "location" VARCHAR(250),
    "method" VARCHAR(250),
    "page" VARCHAR(250),
    "registration" NUMERIC,
    "session_id" NUMERIC,
    "song" VARCHAR(250),
    "status" NUMERIC,
    "ts" BIGINT,
    "user_agent" VARCHAR(250),
    "user_id" NUMERIC
);
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    "num_songs" NUMERIC,
    "artist_id" VARCHAR(250),
    "artist_latitude" VARCHAR(250),
    "artist_longitude" VARCHAR(250),
    "artist_location" VARCHAR(250),
    "artist_name" VARCHAR(250),
    "song_id" VARCHAR(250),
    "title" VARCHAR(250),
    "duration" NUMERIC,
    "year" NUMERIC
);
""")


songplay_table_create = ("""
CREATE TABLE "songplays_fact" (
    "songplay_id" INT IDENTITY(0,1) PRIMARY KEY,
    "start_time" TIMESTAMP,
    "user_id" VARCHAR(250),
    "level" VARCHAR(250),
    "song_id" VARCHAR(250),
    "artist_id" VARCHAR(250),
    "session_id" VARCHAR(250),
    "location" VARCHAR(250),
    "user_agent" VARCHAR(250)
);
""")

user_table_create = ("""
CREATE TABLE "users_dim" (
    "user_id" NUMERIC PRIMARY KEY,
    "first_name" VARCHAR(250),
    "last_name" VARCHAR(250),
    "gender" VARCHAR(250),
    "level" VARCHAR(250)
);
""")

song_table_create = ("""
CREATE TABLE "songs_dim" (
    "song_id" VARCHAR(250) PRIMARY KEY,
    "title" VARCHAR(250),
    "artist_id" VARCHAR(250),
    "year" NUMERIC,
    "duration" NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE "artists_dim" (
    "artist_id" VARCHAR(250) PRIMARY KEY,
    "name" VARCHAR(250),
    "location" VARCHAR(250),
    "latitude" VARCHAR(250),
    "longitude" VARCHAR(250)
);
""")

time_table_create = ("""
CREATE TABLE "times_dim" (
    "start_time" TIMESTAMP PRIMARY KEY,
    "hour" NUMERIC,
    "day" NUMERIC,
    "week" NUMERIC,
    "month" NUMERIC,
    "year" NUMERIC,
    "weekday" VARCHAR(250)
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}/2018/11'
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate off region '{}';
""").format(log_data_url,
            iam_role_arn,
            s3_staging_data_region)

# During development I've used "from '{}/A/A/A'" to have a smaller dataset
# which makes things easier to work with.
staging_songs_copy = ("""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate off region '{}';
""").format(song_data_url,
            iam_role_arn,
            s3_staging_data_region)

# FINAL TABLES

# I'm assuming that a 'valid' song play
# is a song play that can be mapped to a user
# and that it involves a song, which consists
# of artist and title. That's why I'm doing the
# NOT NULL constraints on those columns.
songplay_table_insert = ("""
INSERT INTO songplays_fact (start_time, user_id, 
level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
e.user_id, e.level, s.song_id, s.artist_id, e.session_id,
e.location, e.user_agent
FROM staging_events e JOIN staging_songs s 
ON e.artist=s.artist_name
AND e.song=s.title
AND e.length=s.duration
WHERE e.artist IS NOT NULL
AND e.song IS NOT NULL
AND e.user_id IS NOT NULL;
""")

# We want to make sure we have the most up-to-date
# subscription level.
user_table_insert = ("""
INSERT INTO users_dim (user_id, first_name, last_name, gender, level)
SELECT DISTINCT e.user_id, e.first_name, e.last_name, 
e.gender, e.level
FROM songplays_fact f JOIN 
staging_events e ON f.user_id=e.user_id;
""")

song_table_insert = ("""
INSERT INTO songs_dim (song_id, title, artist_id, year, duration)
SELECT DISTINCT s.song_id, s.title, s.artist_id, 
s.year, s.duration
FROM songplays_fact f JOIN
staging_songs s ON f.song_id=s.song_id;
""")

artist_table_insert = ("""
INSERT INTO artists_dim (artist_id, name, location, latitude, longitude)
SELECT DISTINCT s.artist_id, s.artist_name, 
s.artist_location, s.artist_latitude, s.artist_longitude
FROM songplays_fact f JOIN
staging_songs s ON f.artist_id=s.artist_id;
""")

time_table_insert = ("""
INSERT INTO times_dim (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time,
extract(hour from start_time),
extract(day from start_time),
extract(week from start_time),
extract(month from start_time),
extract(year from start_time),
extract(dow from start_time)
FROM songplays_fact;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
