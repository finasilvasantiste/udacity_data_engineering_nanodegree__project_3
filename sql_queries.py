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
    "ts" NUMERIC NOT NULL NOT NULL,
    "user_agent" VARCHAR(250),
    "user_id" NUMERIC
);
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs" (
    "num_songs" NUMERIC,
    "artist_id" VARCHAR(250) NOT NULL,
    "artist_latitude" VARCHAR(250),
    "artist_longitude" VARCHAR(250),
    "artist_location" VARCHAR(250),
    "artist_name" VARCHAR(250),
    "song_id" VARCHAR(250) NOT NULL,
    "title" VARCHAR(250),
    "duration" NUMERIC,
    "year" NUMERIC
);
""")

songplay_table_create = ("""
CREATE TABLE "songplay_fact" (
    "id" double precision DEFAULT nextval('songplay_fact_seq') NOT NULL,
    "songplay_id" NUMERIC,
    "start_time" VARCHAR(250) NOT NULL,
    "user_id" VARCHAR(250),
    "level" VARCHAR(250),
    "song_id" VARCHAR(250),
    "artist_id" VARCHAR(250),
    "song_id" VARCHAR(250) NOT NULL,
    "session_id" VARCHAR(250),
    "location" NUMERIC,
    "user_agent" NUMERIC
);
""")

user_table_create = ("""
CREATE TABLE "users_dim" (
    "id" double precision DEFAULT nextval('users_dim_seq') NOT NULL,
    "user_id" NUMERIC NOT NULL,
    "first_name" VARCHAR(250),
    "last_name" VARCHAR(250),
    "gender" VARCHAR(250),
    "level" VARCHAR(250)
);
""")

song_table_create = ("""
CREATE TABLE "songs_dim" (
    "id" double precision DEFAULT nextval('songs_dim_seq') NOT NULL,
    "song_id" NUMERIC NOT NULL,
    "title" VARCHAR(250),
    "artist_id" VARCHAR(250),
    "year" NUMERIC,
    "duration" NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE "artists_dim" (
    "id" double precision DEFAULT nextval('artists_dim_seq') NOT NULL,
    "artist_id" NUMERIC NOT NULL,
    "name" VARCHAR(250),
    "location" VARCHAR(250),
    "lattitude" VARCHAR(250),
    "longitude" VARCHAR(250)
);
""")

time_table_create = ("""
CREATE TABLE "times_dim" (
    "id" double precision DEFAULT nextval('times_dim_seq') NOT NULL,
    "start_time" NUMERIC NOT NULL,
    "hour" VARCHAR(250),
    "day" VARCHAR(250),
    "week" NUMERIC,
    "month" NUMERIC,
    "year" NUMERIC,
    "weekday" NUMERIC
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

staging_songs_copy = ("""
    copy staging_songs from '{}/A/A/A'
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate off region '{}';
""").format(song_data_url,
            iam_role_arn,
            s3_staging_data_region)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# create_table_queries = [staging_events_table_create, staging_songs_table_create]
create_table_queries = [user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop]
drop_table_queries = [songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

# copy_table_queries = [staging_events_copy,
#                       staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
# insert_table_queries = [songplay_table_insert]
