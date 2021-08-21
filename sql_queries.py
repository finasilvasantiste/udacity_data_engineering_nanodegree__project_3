import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """
DROP TABLE IF EXISTS "staging_events_table";
"""
staging_songs_table_drop = ""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events_table" (
    "id" double precision DEFAULT nextval('staging_events_table_seq') NOT NULL,
    "artist" VARCHAR(250),
    "auth" VARCHAR(250) NOT NULL,
    "first_name" VARCHAR(250) NOT NULL,
    "gender" VARCHAR(250) NOT NULL,
    "item_in_session" NUMERIC NOT NULL,
    "last_name" VARCHAR(250) NOT NULL,
    "length" NUMERIC,
    "level" VARCHAR(250) NOT NULL,
    "location" VARCHAR(250) NOT NULL,
    "method" VARCHAR(250) NOT NULL,
    "page" VARCHAR(250) NOT NULL,
    "registration" NUMERIC NOT NULL,
    "session_id" NUMERIC NOT NULL,
    "song" VARCHAR(250),
    "status" NUMERIC NOT NULL,
    "ts" NUMERIC NOT NULL,
    "user_agent" VARCHAR(250) NOT NULL,
    "user_id" NUMERIC NOT NULL
);
""")

staging_songs_table_create = ("""
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
""").format()

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
create_table_queries = [staging_events_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_table_queries = [staging_events_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
