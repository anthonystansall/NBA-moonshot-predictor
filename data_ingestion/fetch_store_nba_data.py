# data_ingestion/fetch_store_nba_data.py
from nba_api_client.client import fetch_player_game_logs, fetch_team_game_logs, fetch_team_details
from nba_api_client.utils import get_headers_rowSets, concat_primary_key
from astronomy_api_client.client import fetch_moon_data, process_moon_data
from db.queries import insert_data, get_lat_long, insert_moon_data, get_game_details

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

def fetch_and_store_moon_events():
    game_details = get_game_details()  # Assume this fetches game details as before
    total_requests = len(game_details)
    
    # Display the total number of requests planned
    print(f"Total API requests planned: {total_requests}.")

    # Ask the user if they want to set a limit on the number of requests
    set_limit = input("Would you like to set a limit on the number of requests? (yes/no): ")
    if set_limit.lower() == 'yes':
        try:
            limit = int(input("Enter the maximum number of requests to make: "))
            if limit > 0:
                total_requests = min(limit, total_requests)
            else:
                print("Limit must be a positive integer. Aborting operation.")
                return
        except ValueError:
            print("Invalid input for limit. Aborting operation.")
            return

    # Ask for confirmation to proceed
    confirm = input(f"Proceed with making up to {total_requests} requests? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Operation aborted by the user.")
        return
    
    # Proceed with making requests
    successful_requests = 0
    for index, (game_id, date, time, home_team) in enumerate(game_details[:total_requests], start=1):
        print(f"Processing request {index}/{total_requests} for game ID {game_id}.")
        
        latitude, longitude = get_lat_long(home_team)  # Assume this function fetches lat-long based on home_team
        if latitude is None or longitude is None:
            print(f"Missing latitude or longitude for home team {home_team}. Skipping request.")
            continue
        
        moon_data = fetch_moon_data(date, time, latitude, longitude)
        if moon_data:
            headers, row_sets = process_moon_data(moon_data, game_id)
            insert_moon_data(headers, row_sets)
            successful_requests += 1
    
    print(f"Finished processing. {successful_requests}/{total_requests} requests were successfully completed.")


if __name__ == "__main__":
    pass