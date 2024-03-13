"""Data Ingestion Module for NBA and Astronomical Events.

This module is designed for fetching, transforming, and inserting data related
to NBA player game logs, team game logs, team details, and astronomical
(moon-related) events into a database. It leverages external APIs to gather
NBA game data and moon phase data, then processes and inserts this data into
specific database tables for further analysis or presentation.
"""
from data_pipeline.api import nba_client, astro_client
from data_pipeline.database import queries


def fetch_and_insert_player_team_logs_data(seasons):
    """Fetches and stores player and team logs for the specified seasons.

    Args:
        seasons: A list of seasons to fetch data for.
    """
    data_map = {
        'playergamelogs': {
            'table_name': 'player_game_logs',
            'resultSets': 'PlayerGameLogs',
            'table_primary_key': 'player_id_game_id',
            'first_primary_key': 'player_id',
            'second_primary_key': 'game_id'
        },
        'teamgamelogs': {
            'table_name': 'team_game_logs',
            'resultSets': 'TeamGameLogs',
            'table_primary_key': 'team_id_game_id',
            'first_primary_key': 'team_id',
            'second_primary_key': 'game_id'
        },
    }

    for season in seasons:
        for endpoint, table in data_map.items():
            response = nba_client.fetch_nba_data(endpoint,
                                                 season_nullable=season)
            headers, records = nba_client.parse_transform_nba_data(
                response, table['resultSets'],
                table['first_primary_key'], table['second_primary_key']
            )
            queries.insert_new_data(table['table_name'],
                                    table['table_primary_key'],
                                    headers, records)


def fetch_and_insert_team_details():
    """Adds team details data based on team_id from team_game_logs."""
    unique_team_ids = queries.get_distinct_records(['team_id'],
                                                   'team_game_logs')

    for team_id in unique_team_ids:
        response = nba_client.fetch_nba_data('teamdetails', team_id=team_id)
        headers, rows = nba_client.parse_transform_nba_data(response,
                                                            'TeamBackground')
        queries.insert_new_data('team_details', 'team_id', headers, rows)

    queries.update_records_from_csv(
        'data_pipeline/data/nba_arena_location_data.csv',
        'team_details', 'abbreviation')


def fetch_and_insert_moon_data():
    """Queries db for NBA game data and fetches moon data for each game."""
    moon_data_params_list = astro_client.get_moon_data_params()
    # Uncomment to save to parameter list to csv for testing.
    # utils.save_to_csv(moon_data_params_list, 'moon_data_params_list.csv')

    moon_data_params = astro_client.clean_moon_data_params(
        moon_data_params_list)
    # Uncomment to save to parameter list to csv for testing.
    # utils.save_to_csv(moon_data_params, 'moon_data_params.csv')

    for params in moon_data_params:
        latitude, longitude, from_date, to_date, game_id_dates = params
        moon_data = astro_client.fetch_moon_data(latitude, longitude,
                                                 from_date, to_date)
        headers, rows = astro_client.parse_transform_moon_data(moon_data,
                                                               game_id_dates)
        queries.insert_new_data('moon_events', 'moon_event_id', headers, rows)


if __name__ == "__main__":
    pass
