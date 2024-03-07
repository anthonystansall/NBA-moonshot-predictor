# data_ingestion/fetch_store_nba_data.py
from nba_api_client.client import fetch_player_game_logs, fetch_team_game_logs, fetch_team_details
from nba_api_client.utils import get_headers_rowSets, concat_primary_key
from db.queries import insert_data

def fetch_and_store_player_game_logs(season, **kwargs):
    primary_key = "player_game_id"
    response, result_set_name = fetch_player_game_logs(season, **kwargs)
    headers, row_sets = get_headers_rowSets(response, result_set_name)
    updated_headers, updated_row_sets = concat_primary_key("PLAYER_ID", "GAME_ID", primary_key, headers, row_sets)
    insert_data('player_game_logs', primary_key, updated_headers, updated_row_sets)

def fetch_and_store_team_game_logs(season, **kwargs):
    primary_key = "team_game_id"
    response, result_set_name = fetch_team_game_logs(season, **kwargs)
    headers, row_sets = get_headers_rowSets(response, result_set_name)
    updated_headers, updated_row_sets = concat_primary_key("TEAM_ID", "GAME_ID", primary_key, headers, row_sets)
    insert_data('team_game_logs', primary_key, updated_headers, updated_row_sets)

def fetch_and_store_team_details(team_id, **kwargs):
    primary_key = "team_id"
    response, result_set_name = fetch_team_details(team_id, **kwargs)
    headers, row_sets = get_headers_rowSets(response, result_set_name)
    insert_data('team_details', primary_key, headers, row_sets)

if __name__ == "__main__":
    pass