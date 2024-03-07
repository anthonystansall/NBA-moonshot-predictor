# nba_api_client/client.py
from nba_api.stats.endpoints import playergamelogs, teamgamelogs, teamdetails

def fetch_player_game_logs(season, **kwargs):
    print("Fetching player game logs...")
    result_set_name = 'PlayerGameLogs'
    response = playergamelogs.PlayerGameLogs(season_nullable=season, **kwargs).get_json()
    print("Successfully fetched player game logs.")
    return response, result_set_name

def fetch_team_game_logs(season, **kwargs):
    result_set_name = 'TeamGameLogs'
    response = teamgamelogs.TeamGameLogs(season_nullable=season, **kwargs).get_json()
    return response, result_set_name

def fetch_team_details(team_id, **kwargs):
    result_set_name = 'TeamBackground'
    response = teamdetails.TeamDetails(team_id=team_id, **kwargs).get_json()
    return response, result_set_name

if __name__ == "__main__":
    response, result_set_name = fetch_player_game_logs('2023-24', player_id_nullable='1630578', last_n_games_nullable=1)
    print(response)