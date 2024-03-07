# db/setup.py
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from db.db_connection import get_database_config

def create_database():
    db_config = get_database_config()
    db_name_config = db_config['dbname']
    # Connect to the default database ('postgres') to create a new database
    conn = psycopg2.connect(dbname="postgres", user=db_config['user'], password=db_config['password'], host=db_config['host'])
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name_config)))
        print(f"Database {db_name_config} created successfully.")
    except psycopg2.Error as e:
        print(f"Failed to create database {db_name_config}: {e}")
    finally:
        cur.close()
        conn.close()

def create_player_game_logs_table():
    db_config = get_database_config()
    # Connect to your database
    try:
        # Using the 'with' statement for automatically closing the connection
        with psycopg2.connect(dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'], host=db_config['host']) as conn:
            # Setting isolation level to AUTOCOMMIT for executing CREATE TABLE command
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Using the 'with' statement for cursor management
            with conn.cursor() as cur:
                # SQL command to create the team_details table
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS player_game_logs (
                        player_game_id TEXT PRIMARY KEY,
                        season_year VARCHAR(7),
                        player_id INT,
                        player_name VARCHAR(100),
                        nickname VARCHAR(50),
                        team_id INT,
                        team_abbreviation VARCHAR(3),
                        team_name VARCHAR(50),
                        game_id VARCHAR(15),
                        game_date TIMESTAMP,
                        matchup VARCHAR(50),
                        wl CHAR(1),
                        min FLOAT,
                        fgm INT,
                        fga INT,
                        fg_pct FLOAT,
                        fg3m INT,
                        fg3a INT,
                        fg3_pct FLOAT,
                        ftm INT,
                        fta INT,
                        ft_pct FLOAT,
                        oreb INT,
                        dreb INT,
                        reb INT,
                        ast INT,
                        tov INT,
                        stl INT,
                        blk INT,
                        blka INT,
                        pf INT,
                        pfd INT,
                        pts INT,
                        plus_minus INT,
                        nba_fantasy_pts FLOAT,
                        dd2 INT,
                        td3 INT,
                        wnba_fantasy_pts FLOAT,
                        gp_rank INT,
                        w_rank INT,
                        l_rank INT,
                        w_pct_rank INT,
                        min_rank INT,
                        fgm_rank INT,
                        fga_rank INT,
                        fg_pct_rank INT,
                        fg3m_rank INT,
                        fg3a_rank INT,
                        fg3_pct_rank INT,
                        ftm_rank INT,
                        fta_rank INT,
                        ft_pct_rank INT,
                        oreb_rank INT,
                        dreb_rank INT,
                        reb_rank INT,
                        ast_rank INT,
                        tov_rank INT,
                        stl_rank INT,
                        blk_rank INT,
                        blka_rank INT,
                        pf_rank INT,
                        pfd_rank INT,
                        pts_rank INT,
                        plus_minus_rank INT,
                        nba_fantasy_pts_rank INT,
                        dd2_rank INT,
                        td3_rank INT,
                        wnba_fantasy_pts_rank INT,
                        available_flag INT
                    );
                """))
                
        print("Table 'player_game_logs' created successfully.")
    
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def create_team_game_logs_table():
    db_config = get_database_config()
    # Connect to your database
    try:
        # Using the 'with' statement for automatically closing the connection
        with psycopg2.connect(dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'], host=db_config['host']) as conn:
            # Setting isolation level to AUTOCOMMIT for executing CREATE TABLE command
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Using the 'with' statement for cursor management
            with conn.cursor() as cur:
                # SQL command to create the team_details table
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS team_game_logs (
                        team_game_id TEXT PRIMARY KEY,
                        season_year VARCHAR(7),
                        team_id INT,
                        team_abbreviation VARCHAR(3),
                        team_name VARCHAR(50),
                        game_id VARCHAR(15),
                        game_date TIMESTAMP,
                        matchup VARCHAR(50),
                        wl CHAR(1),
                        min FLOAT,
                        fgm INT,
                        fga INT,
                        fg_pct FLOAT,
                        fg3m INT,
                        fg3a INT,
                        fg3_pct FLOAT,
                        ftm INT,
                        fta INT,
                        ft_pct FLOAT,
                        oreb INT,
                        dreb INT,
                        reb INT,
                        ast INT,
                        tov FLOAT,
                        stl INT,
                        blk INT,
                        blka INT,
                        pf INT,
                        pfd INT,
                        pts INT,
                        plus_minus FLOAT,
                        gp_rank INT,
                        w_rank INT,
                        l_rank INT,
                        w_pct_rank INT,
                        min_rank INT,
                        fgm_rank INT,
                        fga_rank INT,
                        fg_pct_rank INT,
                        fg3m_rank INT,
                        fg3a_rank INT,
                        fg3_pct_rank INT,
                        ftm_rank INT,
                        fta_rank INT,
                        ft_pct_rank INT,
                        oreb_rank INT,
                        dreb_rank INT,
                        reb_rank INT,
                        ast_rank INT,
                        tov_rank INT,
                        stl_rank INT,
                        blk_rank INT,
                        blka_rank INT,
                        pf_rank INT,
                        pfd_rank INT,
                        pts_rank INT,
                        plus_minus_rank INT,
                        available_flag INT
                    );
                """))
                
        print("Table 'team_game_logs' created successfully.")
    
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def create_team_details_table():
    db_config = get_database_config()
    # Connect to your database
    try:
        # Using the 'with' statement for automatically closing the connection
        with psycopg2.connect(dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'], host=db_config['host']) as conn:
            # Setting isolation level to AUTOCOMMIT for executing CREATE TABLE command
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Using the 'with' statement for cursor management
            with conn.cursor() as cur:
                # SQL command to create the team_details table
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS team_details (
                        team_id BIGINT PRIMARY KEY,
                        abbreviation VARCHAR(10),
                        nickname VARCHAR(50),
                        yearfounded INT,
                        city VARCHAR(50),
                        arena VARCHAR(100),
                        arenacapacity INT,
                        owner VARCHAR(100),
                        generalmanager VARCHAR(100),
                        headcoach VARCHAR(100),
                        dleagueaffiliation VARCHAR(100),
                        latitude DOUBLE PRECISION,
                        longitude DOUBLE PRECISION
                    );
                """))
                
        print("Table 'team_details' created successfully.")

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def create_moon_events_table():
    db_config = get_database_config()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS moon_events (
        moon_event_id TEXT PRIMARY KEY,
        date TIMESTAMP,
        body_id TEXT,
        body_name TEXT,
        distance_fromEarth_au DOUBLE PRECISION,
        distance_fromEarth_km DOUBLE PRECISION,
        horizontal_position_altitude_degrees DOUBLE PRECISION,
        horizontal_position_azimuth_degrees DOUBLE PRECISION,
        equatorial_position_right_ascension TEXT,
        equatorial_position_declination TEXT,
        position_constellation_name TEXT,
        elongation DOUBLE PRECISION,
        magnitude DOUBLE PRECISION,
        phase_string TEXT,
        game_id VARCHAR(15)
    );
    """
    
    try:
        with psycopg2.connect(dbname=db_config['dbname'], user=db_config['user'], password=db_config['password'], host=db_config['host']) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                print("Table 'moon_events' created successfully.")
    except psycopg2.Error as e:
        print(f"Failed to create table 'moon_events': {e}")

def drop_tables():
    db_config = get_database_config()
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                # Drop tables
                cur.execute("DROP TABLE IF EXISTS player_game_logs CASCADE;")
                cur.execute("DROP TABLE IF EXISTS team_game_logs CASCADE;")
                cur.execute("DROP TABLE IF EXISTS team_details CASCADE;")
                cur.execute("DROP TABLE IF EXISTS moon_events CASCADE;")
                print("Tables deleted successfully.")

    except psycopg2.Error as e:
        print(f"Failed to delete tables: {e}")

def drop_database():
    db_config = get_database_config()
    db_name_config = db_config['dbname']
    conn = None
    try:
        conn = psycopg2.connect(dbname='postgres', user=db_config['user'], password=db_config['password'], host=db_config['host'])
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {db_name};").format(db_name=sql.Identifier(db_name_config)))
            print(f"Database {db_name_config} deleted successfully.")
    
    except psycopg2.Error as e:
        print(f"Failed to delete database {db_name_config}: {e}")
    
    finally:
        if conn:
            conn.close()

def list_databases():
    db_config = get_database_config()
    conn = None
    try:
        conn = psycopg2.connect(dbname='postgres', user=db_config['user'], password=db_config['password'], host=db_config['host'])
        cur = conn.cursor()

        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")

        databases = cur.fetchall()
        for db in databases:
            print(db[0])
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if conn:
            conn.close()

def setup_database():
    create_database()
    create_player_game_logs_table()
    create_team_game_logs_table()
    create_team_details_table()
    create_moon_events_table()

def wipe_database():
    drop_tables()
    drop_database()
    list_databases()

if __name__ == "__main__":
    # setup_database()
    wipe_database()