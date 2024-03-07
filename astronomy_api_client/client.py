import configparser
import base64
import requests
import os

def get_credentials():
    config = configparser.ConfigParser()
    config_file_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.ini')
    config.read(config_file_path)
    application_id = config['AstronomyAPI']['application_id']
    application_secret = config['AstronomyAPI']['application_secret']
    return application_id, application_secret

def get_auth_string():
    application_id, application_secret = get_credentials()
    userpass = f"{application_id}:{application_secret}"
    auth_string = base64.b64encode(userpass.encode()).decode()
    return auth_string

def fetch_moon_data(date, time, latitude, longitude):
    # Check if latitude or longitude are None
    if latitude is None or longitude is None:
        print("Latitude or Longitude is not provided. Aborting fetch operation.")
        return None

    print("Fetching moon data...")
    auth_string = get_auth_string()
    headers = {'Authorization': f'Basic {auth_string}'}
    test_url = 'https://api.astronomyapi.com/api/v2/bodies/positions/moon'
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'elevation': '0',
        'from_date': date,
        'to_date': date,
        'time': time,
        'output': 'rows'
    }
    response = requests.get(test_url, headers=headers, params=params)
    if response.status_code == 200:
        print("API Test Successful. Data received.")
        return response.json()
    else:
        print(f"API Test Failed. Status Code: {response.status_code}")
        print("Error response: ")
        print(response.text)
        return None

def process_moon_data(data, game_id):
    print("Processing moon data...")
    headers = [
        'moon_event_id', 'date', 'body_id', 'body_name', 'distance_fromEarth_au',
        'distance_fromEarth_km', 'horizontal_position_altitude_degrees',
        'horizontal_position_azimuth_degrees', 'equatorial_position_right_ascension',
        'equatorial_position_declination', 'position_constellation_name',
        'elongation', 'magnitude', 'phase_string', 'game_id'
    ]
    row_sets = []

    for row in data.get('data', {}).get('rows', []):
        body = row.get('body', {})
        for position in row.get('positions', []):
            moon_event_id = f"{position.get('date')}_{body.get('id')}"
            event_row = [
                moon_event_id, position.get('date'), body.get('id'), body.get('name'),
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
            row_sets.append(event_row)

    return headers, row_sets

if __name__ == "__main__":
    pass