"""Module that defines the database schemas.

This module defines the database schemas for storing NBA player and game logs,
team details, and moon event data.
"""

# Define schema for player_game_logs table
PLAYER_GAME_LOGS_TABLE = {
    'table_name': 'player_game_logs',
    'table_creation_sql': """
    CREATE TABLE IF NOT EXISTS player_game_logs (
        player_id_game_id TEXT PRIMARY KEY,
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
    """
}

# Define schema for team_game_logs table.
TEAM_GAME_LOGS_TABLE = {
    'table_name': 'team_game_logs',
    'table_creation_sql': """
    CREATE TABLE IF NOT EXISTS team_game_logs (
        team_id_game_id TEXT PRIMARY KEY,
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
    """
}

# Define schema for team_details table.
TEAM_DETAILS_TABLE = {
    'table_name': 'team_details',
    'table_creation_sql': """
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
    """
}

# Define schema for moon_events table.
MOON_EVENTS_TABLE = {
    'table_name': 'moon_events',
    'table_creation_sql': """
    CREATE TABLE IF NOT EXISTS moon_events (
        moon_event_id TEXT PRIMARY KEY,
        date TIMESTAMP,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        body_id TEXT,
        body_name TEXT,
        distance_from_earth_au DOUBLE PRECISION,
        distance_from_earth_km DOUBLE PRECISION,
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
}

# Aggregate all table schemas for easy reference.
ALL_TABLE_SCHEMAS = [
    PLAYER_GAME_LOGS_TABLE,
    TEAM_GAME_LOGS_TABLE,
    TEAM_DETAILS_TABLE,
    MOON_EVENTS_TABLE,
]
