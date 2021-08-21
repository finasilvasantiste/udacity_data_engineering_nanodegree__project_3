import configparser
import json


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


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


iam_role_arn = load_iam_role_arn()
s3_staging_data_region = 'us-west-2'

# DROP TABLES

staging_events_table_drop = """
DROP TABLE IF EXISTS "staging_events";
"""
staging_songs_table_drop = """
DROP TABLE IF EXISTS "staging_songs";
"""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

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
    "ts" NUMERIC NOT NULL,
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
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}/2018/11'
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate off region '{}';
""").format(config.get('S3', 'LOG_DATA'),
            iam_role_arn,
            s3_staging_data_region)

staging_songs_copy = ("""
""").format()

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
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
copy_table_queries = [staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
