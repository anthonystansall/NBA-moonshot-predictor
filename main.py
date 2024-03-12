# main.py
from data_pipeline.database import setup, queries
from data_pipeline.api import nba_client, astro_client
from data_pipeline.utils import utils

def main():
    setup.wipe_database_schema()
    setup.setup_database_schema()

    seasons = ['2018-19', '2019-20', '2020-21', '2021-22', '2022-23']
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
            response = nba_client.fetch_nba_data(endpoint, season_nullable=season)
            headers, records = utils.parse_transform_nba_data(response, table['resultSets'], table['first_primary_key'], table['second_primary_key'])
            queries.insert_new_data(table['table_name'], table['table_primary_key'], headers, records)

    unique_team_ids = queries.get_distinct_records(['team_id'], 'team_game_logs')
    for team_id in unique_team_ids:
        response = nba_client.fetch_nba_data('teamdetails', team_id=team_id)
        headers, rows = utils.parse_transform_nba_data(response, 'TeamBackground')
        queries.insert_new_data('team_details', 'team_id', headers, rows)
    
    queries.update_records_from_csv('data_pipeline/data/nba_arena_location_data.csv', 'team_details', 'abbreviation')

    moon_data_params_list = utils.get_moon_data_params()
    utils.save_to_csv(moon_data_params_list, 'moon_data_params_list.csv')
    moon_data_params = utils.clean_moon_data_params(moon_data_params_list)
    utils.save_to_csv(moon_data_params, 'moon_data_params.csv')
    for params in moon_data_params:
        latitude, longitude, from_date, to_date, game_id_dates = params
        moon_data = astro_client.fetch_moon_data(latitude, longitude, from_date, to_date)
        headers, rows = utils.parse_transform_moon_data(moon_data, game_id_dates)
        queries.insert_new_data('moon_events', 'moon_event_id', headers, rows)


if __name__ == "__main__":
    main()
