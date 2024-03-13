"""Module for fetching and transforming moon data from Astronomy API.

This module provides functionalities to fetch moon data from an external
API based on specified parameters. It includes handling for credentials,
API requests, and data transformation to structure the fetched data
for further processing or storage.
"""

from collections import defaultdict
from datetime import datetime
import json
import os
import configparser
import base64
import requests

from data_pipeline.utils import utils
from data_pipeline.database import queries


def get_credentials():
    """Retrieves API credentials from a configuration file.

    Returns:
        A tuple containing the application ID and application secret.
    """
    config = configparser.ConfigParser()
    config_file_path = os.path.join(
        os.path.dirname(__file__), '..', 'config', 'api_credentials.ini'
        )
    config.read(config_file_path)
    application_id = config['AstronomyAPI']['application_id']
    application_secret = config['AstronomyAPI']['application_secret']
    return application_id, application_secret


def get_auth_string():
    """Generates an authorization string for API requests.

    Returns:
        A base64 encoded authorization string.
    """
    application_id, application_secret = get_credentials()
    userpass = f"{application_id}:{application_secret}"
    auth_string = base64.b64encode(userpass.encode()).decode()
    return auth_string


def fetch_moon_data(latitude, longitude, from_date, to_date, time="00:00:00"):
    """Fetches moon data from the API for specified location and date range.

    Args:
        latitude: Latitude of the location.
        longitude: Longitude of the location.
        from_date: Start date of the period for which to fetch data.
        to_date: End date of the period.
        time: Time of the day for which to fetch data. Defaults to "00:00:00".

    Returns:
        The fetched moon data or None if fetch operation fails or data exists.
    """
    if latitude is None or longitude is None:
        print(
            "Latitude or Longitude is not provided. Aborting fetch operation."
            )
        return None

    if utils.check_file_exists('moon_data',
                               from_date=from_date,
                               to_date=to_date,
                               time=time,
                               latitude=latitude,
                               longitude=longitude):
        print("File already exists with requested data.")
        print("Returning file contents instead.")
        file_path = utils.generate_file_name('moon_data',
                                             from_date=from_date,
                                             to_date=to_date,
                                             time=time,
                                             latitude=latitude,
                                             longitude=longitude)
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    else:
        print("Fetching moon data...")
        auth_string = get_auth_string()
        headers = {'Authorization': f'Basic {auth_string}'}
        test_url = 'https://api.astronomyapi.com/api/v2/bodies/positions/moon'
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'elevation': '0',
            'from_date': from_date,
            'to_date': to_date,
            'time': time,
            'output': 'rows'
        }

        try:
            response = requests.get(test_url,
                                    headers=headers,
                                    params=params,
                                    timeout=10)

            if response.status_code == 200:
                print("API Call Successful. Data received.")
                utils.save_data_to_file(response.json(),
                                        'moon_data',
                                        from_date=from_date,
                                        to_date=to_date,
                                        time=time,
                                        latitude=latitude,
                                        longitude=longitude)
                return response.json()

            print(f"API Call Failed. Status Code: {response.status_code}")
            print("Error response: ")
            print(response.text)
            return None

        except requests.exceptions.Timeout:
            print("The request timed out.")
            return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None


def get_moon_data_params():
    """Retrieves parameters needed to retrieve moon data for each game.

    Returns:
        A set of tuples containing game ID, game date, latitude, and
        longitude for each game. If no records are found or an error
        occurs, returns an empty set.
    """
    column_names = ['game_id', 'matchup', 'game_date']
    table_name = 'team_game_logs'
    records = queries.get_distinct_records(column_names, table_name)

    moon_data_params = []

    if records is None:
        print("No records found or an error occurred.")
        return moon_data_params

    all_team_details = queries.get_records('team_details',
                                           columns=['abbreviation',
                                                    'latitude',
                                                    'longitude'])
    team_details_dict = {detail[0]: (detail[1], detail[2])
                         for detail in all_team_details}

    for record in records:
        game_id, matchup, game_date = record

        try:
            home_team = utils.get_home_team(matchup)
            latitude, longitude = team_details_dict.get(home_team,
                                                        (None, None))
            if latitude is None or longitude is None:
                print(f"Could not find team details for {home_team}.")
                continue
        except ValueError as e:
            print(f"Error determining home team for game {game_id}: {e}")
            continue

        moon_data_params.append((game_id, game_date, latitude, longitude))

    moon_data_params_set = set(moon_data_params)

    return moon_data_params_set


def clean_moon_data_params(moon_data_params):
    """Cleans moon data parameters to be used by Astronomy API.

    Args:
        moon_data_params: A set of tuples, each containing a game ID, game
            date, latitude, and longitude.

    Returns:
        A list of tuples, each containing latitude, longitude, the start date
        of the year, the end date of the year, and a list of game IDs with
        dates for that location and year.
    """
    # Initialize a dictionary to group data by location and year
    loc_year_dict = defaultdict(lambda: defaultdict(list))
    for game_id, game_date, lat, lon in moon_data_params:
        year = game_date.year
        loc_year_dict[(lat, lon)][year].append((game_id, game_date))

    results = []
    for (lat, lon), years in loc_year_dict.items():
        for year, games in years.items():
            # Sort games by date within each year
            games.sort(key=lambda x: x[1])
            # Include both game_id and game_date in the list
            game_id_dates = [(game[0], game[1]) for game in games]
            # Construct the from_date and to_date for each year
            from_date = datetime(year, 1, 1).strftime('%Y-%m-%d')
            to_date = datetime(year, 12, 31).strftime('%Y-%m-%d')
            # Append the organized data to the results list
            results.append((lat, lon, from_date, to_date, game_id_dates))

    return results


def parse_transform_moon_data(response, game_id_dates):
    """Transforms moon data from API response into a structured format.

    Parses the API response to extract moon data and associates it with
    corresponding game IDs based on dates. Constructs a list of rows with
    detailed moon event information for each date in the response that matches
    the game dates.

    Args:
        response: The API response containing moon data.
        game_id_dates: A list of tuples containing game IDs and their
            corresponding dates, used to match moon events to games.

    Returns:
        A tuple containing the headers and records of the transformed data.
    """
    print("Processing moon data...")
    game_date_dict = {
        game_date.strftime('%Y-%m-%dT%H:%M:%S.000-01:00'): game_id
        for game_id, game_date in game_id_dates
    }

    observer_location = response.get('data', {}
                                     ).get('observer', {}
                                           ).get('location', {})
    latitude = observer_location.get('latitude')
    longitude = observer_location.get('longitude')

    headers = [
        'moon_event_id', 'date', 'latitude', 'longitude', 'body_id',
        'body_name', 'distance_from_earth_au', 'distance_from_earth_km',
        'horizontal_position_altitude_degrees',
        'horizontal_position_azimuth_degrees',
        'equatorial_position_right_ascension',
        'equatorial_position_declination',
        'position_constellation_name', 'elongation', 'magnitude',
        'phase_string', 'game_id'
    ]

    rows = []

    for row in response.get('data', {}).get('rows', []):
        body = row.get('body', {})
        for position in row.get('positions', []):
            date_str = position.get('date')
            moon_event_id = f"{date_str}_{latitude}_{longitude}"
            game_id = game_date_dict.get(date_str, 'null')

            event_row = [
                moon_event_id, date_str, latitude, longitude,
                body.get('id'), body.get('name'),
                position.get('distance', {}).get('fromEarth', {}).get('au'),
                position.get('distance', {}).get('fromEarth', {}).get('km'),
                position.get('position', {}).get('horizontal', {}).get(
                    'altitude', {}).get('degrees'),
                position.get('position', {}).get('horizontal', {}).get(
                    'azimuth', {}).get('degrees'),
                position.get('position', {}).get('equatorial', {}).get(
                    'rightAscension', {}).get('hours'),
                position.get('position', {}).get('equatorial', {}).get(
                    'declination', {}).get('degrees'),
                position.get('position', {}).get('constellation', {}).get(
                    'name'),
                position.get('extraInfo', {}).get('elongation'),
                position.get('extraInfo', {}).get('magnitude'),
                position.get('extraInfo', {}).get('phase', {}).get('string'),
                game_id
            ]
            rows.append(event_row)

    return headers, rows


if __name__ == "__main__":
    pass
