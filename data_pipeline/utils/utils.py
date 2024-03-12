import os
import json
import hashlib
import csv
from datetime import datetime
from collections import defaultdict
from data_pipeline.database import queries

BASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'json')


def parse_transform_nba_data(response, resultSet_name, first_primary_key=None, second_primary_key=None):
    response_json = json.loads(response)
    result_set = next((item for item in response_json['resultSets'] if item['name'] == resultSet_name), None)
    if result_set is None:
        print(f"No result set found with the name: {resultSet_name}")
        return None, None

    headers = result_set['headers']
    records = result_set['rowSet']

    headers = [header.lower() for header in headers]


    if 'game_date' in headers:
        game_date_index = headers.index('game_date')
        records = [
            record[:game_date_index] + [datetime.strptime(record[game_date_index], '%Y-%m-%dT%H:%M:%S')] + record[game_date_index + 1:]
            if record[game_date_index] else record
            for record in records
        ]

    if first_primary_key and second_primary_key:
        new_primary_key = f"{first_primary_key}_{second_primary_key}"
        headers.append(new_primary_key.lower())

        first_key_index = headers.index(first_primary_key.lower())
        second_key_index = headers.index(second_primary_key.lower())

        records = [
            record + [f"{record[first_key_index]}_{record[second_key_index]}"]
            for record in records
        ]

    return headers, records


def generate_file_name(endpoint, **kwargs):
    sorted_kwargs = sorted(kwargs.items())
    identifier = f"{endpoint}_{sorted_kwargs}"
    file_name_hash = hashlib.sha256(identifier.encode()).hexdigest()
    file_path = os.path.join(BASE_DIR, f"{file_name_hash}.json")
    return file_path


def save_data_to_file(response, endpoint, **kwargs):
    file_path = generate_file_name(endpoint, **kwargs)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as file:
        json.dump(response, file)


def check_file_exists(endpoint, **kwargs):
    file_path = generate_file_name(endpoint, **kwargs)
    return os.path.exists(file_path)


def get_home_team(matchup):
    if " @ " in matchup:
        _, home_team = matchup.split(" @ ")
    elif " vs. " in matchup:
        home_team, _ = matchup.split(" vs. ")
    else:
        raise ValueError("Unknown matchup format.")
    return home_team


def get_game_date_time(game_date):
    date = game_date.strftime("%Y-%m-%d")
    time = game_date.strftime("%H:%M:%S")
    return date, time


def get_moon_data_params():
    column_names = ['game_id', 'matchup', 'game_date']
    table_name = 'team_game_logs'
    records = queries.get_distinct_records(column_names, table_name)

    moon_data_params = []

    if records is None:
        print("No records found or an error occurred.")
        return moon_data_params

    # Fetch all team details once
    all_team_details = queries.get_records('team_details', columns=['abbreviation', 'latitude', 'longitude'])
    team_details_dict = {detail[0]: (detail[1], detail[2]) for detail in all_team_details}

    for record in records:
        game_id, matchup, game_date = record

        try:
            home_team = get_home_team(matchup)
            latitude, longitude = team_details_dict.get(home_team, (None, None))
            if latitude is None or longitude is None:
                print(f"Could not find team details for home team {home_team}.")
                continue
        except ValueError as e:
            print(f"Error determining home team for game {game_id}: {e}")
            continue

        # Store parameters for later use
        moon_data_params.append((game_id, game_date, latitude, longitude))

    moon_data_params_set = set(moon_data_params)

    return moon_data_params_set


def clean_moon_data_params(moon_data_params):
    # Group by location and then by year
    loc_year_dict = defaultdict(lambda: defaultdict(list))
    for game_id, game_date, lat, lon in moon_data_params:
        year = game_date.year
        loc_year_dict[(lat, lon)][year].append((game_id, game_date))
    
    results = []
    for (lat, lon), years in loc_year_dict.items():
        for year, games in years.items():
            games.sort(key=lambda x: x[1])  # Sort by date within each year
            game_id_dates = [(game[0], game[1]) for game in games]  # Include both game_id and game_date in the list
            # Construct the from_date and to_date for the year
            from_date = datetime(year, 1, 1).strftime('%Y-%m-%d')  # Format as 'YYYY-MM-DD'
            to_date = datetime(year, 12, 31).strftime('%Y-%m-%d')  # Format as 'YYYY-MM-DD'
            results.append((lat, lon, from_date, to_date, game_id_dates))
    
    return results


def parse_transform_moon_data(response, game_id_dates):
    print("Processing moon data...")
    game_date_dict = {game_date.strftime('%Y-%m-%dT%H:%M:%S.000-01:00'): game_id for game_id, game_date in game_id_dates}

    observer_location = response.get('data', {}).get('observer', {}).get('location', {})
    latitude = observer_location.get('latitude')
    longitude = observer_location.get('longitude')

    headers = [
        'moon_event_id', 'date', 'latitude', 'longitude', 'body_id', 'body_name', 
        'distance_from_earth_au', 'distance_from_earth_km', 
        'horizontal_position_altitude_degrees', 'horizontal_position_azimuth_degrees', 
        'equatorial_position_right_ascension', 'equatorial_position_declination', 
        'position_constellation_name', 'elongation', 'magnitude', 'phase_string', 'game_id'
    ]

    rows = []

    for row in response.get('data', {}).get('rows', []):
        body = row.get('body', {})
        for position in row.get('positions', []):
            date_str = position.get('date')
            moon_event_id = f"{date_str}_{latitude}_{longitude}"
            game_id = game_date_dict.get(date_str, 'null')

            event_row = [
                moon_event_id, date_str, latitude, longitude, body.get('id'), body.get('name'),
                position.get('distance', {}).get('fromEarth', {}).get('au'),
                position.get('distance', {}).get('fromEarth', {}).get('km'),
                position.get('position', {}).get('horizontal', {}).get('altitude', {}).get('degrees'),
                position.get('position', {}).get('horizontal', {}).get('azimuth', {}).get('degrees'),
                position.get('position', {}).get('equatorial', {}).get('rightAscension', {}).get('hours'),
                position.get('position', {}).get('equatorial', {}).get('declination', {}).get('degrees'),
                position.get('position', {}).get('constellation', {}).get('name'),
                position.get('extraInfo', {}).get('elongation'),
                position.get('extraInfo', {}).get('magnitude'),
                position.get('extraInfo', {}).get('phase', {}).get('string'),
                game_id
            ]
            rows.append(event_row)

    return headers, rows


def concat_primary_key(first_index, second_index, primarykey_name, headers, row_sets):
    print("Creating primary key...")
    # Find indexes of player_id and game_id in headers
    first_idx = headers.index(first_index)
    second_idx = headers.index(second_index)
    
    # Add 'player_game_id' to headers
    new_headers = [primarykey_name] + headers
    
    # Concatenate player_id and game_id for each row and prepend it to the row
    new_row_sets = [[f"{row[first_idx]}_{row[second_idx]}"] + row for row in row_sets]
    
    print("Successfully created primary key.")
    return new_headers, new_row_sets


def save_to_csv(data, filename, directory="data_pipeline/data"):
    """
    Saves a list of tuples to a CSV file in the specified directory.

    Parameters:
        data (list of tuples): The data to be saved.
        filename (str): The name of the file to save the data to, without the directory path.
        directory (str, optional): The directory where the file will be saved. Defaults to 'data_pipeline/data'.
    """
    # Ensure the directory exists, create it if it does not
    os.makedirs(directory, exist_ok=True)

    # Construct the full path to the file
    file_path = os.path.join(directory, filename)

    try:
        # Open the file in write mode and create a csv.writer object
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # Write all the data rows
            writer.writerows(data)

        print(f"Data successfully saved to {file_path}")
    except Exception as e:
        print(f"An error occurred while writing to the file: {e}")