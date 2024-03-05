from nba_api.stats.endpoints import playergamelogs, teamgamelogs

# Would be better to use date_from and date_to parameters, but no documentation available
seasons = ['2018-19', '2019-20', '2020-21', '2021-22', '2022-23']

def fetch_player_data(season, last_n_games=5):
    player_data = playergamelogs.PlayerGameLogs(season_nullable=season, last_n_games_nullable=last_n_games)
    return player_data.get_data_frames()[0]

def fetch_team_data(season, last_n_games=5):
    team_data = teamgamelogs.TeamGameLogs(season_nullable=season, last_n_games_nullable=last_n_games)
    return team_data.get_data_frames()[0]

if __name__ == "__main__":
    # print(fetch_player_data(season='2022-23'))
    print(fetch_team_data(season='2022-23'))