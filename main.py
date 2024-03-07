# main.py
from data_ingestion.fetch_store_nba_data import fetch_and_store_player_game_logs, fetch_and_store_team_game_logs, fetch_and_store_team_details, fetch_and_store_moon_events
from db.setup import setup_database, wipe_database
from db.queries import get_unique_team_ids, update_team_details_with_location, print_first_5_rows

def main():
    seasons = ['2018-19', '2019-20', '2020-21', '2021-22', '2022-23']
    for season in seasons:
        fetch_and_store_player_game_logs(season)
        fetch_and_store_team_game_logs(season)

    unique_team_ids = get_unique_team_ids()
    for team_id in unique_team_ids:
        fetch_and_store_team_details(team_id)    

def test_data_ingestion():
    fetch_and_store_player_game_logs('2023-24', last_n_games_nullable=1, player_id_nullable='1630578')
    fetch_and_store_team_game_logs('2023-24', last_n_games_nullable=2)
    unique_team_ids = get_unique_team_ids()
    for team_id in unique_team_ids:
        fetch_and_store_team_details(team_id)
    update_team_details_with_location("nba_arena_location_data.csv")
    print_first_5_rows('team_details')
    fetch_and_store_moon_events()

if __name__ == "__main__":
    # wipe_database()
    # setup_database()
    # test_data_ingestion()
    # test_data_ingestion() # run a second time to test if dupes are skipped
    print_first_5_rows('moon_events')
    print_first_5_rows('player_game_logs')
    print_first_5_rows('team_game_logs')
    print_first_5_rows('team_details')
