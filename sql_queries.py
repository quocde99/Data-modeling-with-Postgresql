# DROP TABLES

songplay_table_drop = "Drop table if exists songplays;"
user_table_drop = "Drop table if exists users;"
song_table_drop = "Drop table if exists songs;"
artist_table_drop = "Drop table if exists artists;"
time_table_drop = "Drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(songplay_id serial primary key, 
                                start_time timestamp not null, 
                                user_id int not null, 
                                level varchar, 
                                song_id varchar(18) not null, 
                                artist_id varchar not null, 
                                session_id int not null, 
                                location varchar, 
                                user_agent varchar(150)
                                );
""")
#CONSTRAINT fk_user_id FOREIGN KEY(user_id) REFERENCES users(user_id),
#CONSTRAINT fk_song_id FOREIGN KEY(song_id) REFERENCES songs(song_id),
#CONSTRAINT fk_artist_id FOREIGN KEY(artist_id) REFERENCES artists(artist_id)TRAINT fk_start_time FOREIGN KEY(start_time) REFERENCES time(start_time)
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(user_id int primary key, 
                            first_name varchar, 
                            last_name varchar, 
                            gender char(1), 
                            level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(song_id varchar(18) primary key, 
                            title varchar not null, 
                            artist_id char(18) not null, 
                            year int, 
                            duration numeric not null);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY, 
                                name varchar not null, 
                                location varchar, 
                                latitude float, 
                                longitude float);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY,
                                hour int,
                                day int,
                                week int,
                                month int,
                                year int,
                                weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location,user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(songplay_id) DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level || 'free';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT st.song_id, at.artist_id FROM songs AS st
    JOIN artists AS at
    ON st.artist_id = at.artist_id
    WHERE st.title = %s AND at.name = %s AND st.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]