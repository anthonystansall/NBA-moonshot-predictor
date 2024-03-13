"""Main script for initializing and populating a sports database.

This script wipes the existing database schema and sets it up anew. It then
populates the database with player and team logs, team details including
geographical locations, and moon phase data associated with each game, for
specified sports seasons.
"""
from data_pipeline.database import setup
from data_pipeline.data_ingestion import data_ingestion


def main():
    """Executes the main script functions.

    Steps include wiping and setting up the database schema, fetching and
    inserting data for player and team logs, team details, and moon phases
    for each game.
    """
    # Wipe and restore database for fresh start
    setup.wipe_database_schema()
    setup.setup_database_schema()

    # Seasons to pull data for
    seasons = ['2018-19', '2019-20', '2020-21', '2021-22', '2022-23']

    # Store player logs and team game logs for seasons
    data_ingestion.fetch_and_insert_player_team_logs_data(seasons)

    # Update team_details to include latitude and longitude of home games
    data_ingestion.fetch_and_insert_team_details()

    # Store moon data for each game based on lat and long
    data_ingestion.fetch_and_insert_moon_data()


if __name__ == "__main__":
    main()
