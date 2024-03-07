import configparser
import base64
import requests
from db.db_connection import save_data_to_db

def get_credentials():
    config = configparser.ConfigParser()
    config.read('config.ini')
    application_id = config['AstronomyAPI']['application_id']
    application_secret = config['AstronomyAPI']['application_secret']
    return application_id, application_secret

def get_auth_string():
    application_id, application_secret = get_credentials()
    userpass = f"{application_id}:{application_secret}"
    auth_string = base64.b64encode(userpass.encode()).decode()
    return auth_string

def fetch_moon_data():
    auth_string = get_auth_string()
    headers = {'Authorization': f'Basic {auth_string}'}
    test_url = 'https://api.astronomyapi.com/api/v2/bodies/positions/moon'  
    params = {
        'latitude': '38.775867',
        'longitude': '-84.39733',
        'elevation': '0',
        'from_date': '2020-12-20',
        'to_date': '2020-12-20',
        'time': '08:00:00',
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

def process_moon_data(data):
    events = []
    for row in data.get('data', {}).get('rows', []):
        body = row.get('body', {})
        for position in row.get('positions', []):
            event = {
                'date': position.get('date'),
                'body_id': body.get('id'),
                'body_name': body.get('name'),
                'distance_fromEarth_au': position.get('distance', {}).get('fromEarth', {}).get('au'),
                'distance_fromEarth_km': position.get('distance', {}).get('fromEarth', {}).get('km'),
                'horizontal_position_altitude_degrees': position.get('position', {}).get('horizontal', {}).get('altitude', {}).get('degrees'),
                'horizontal_position_azimuth_degrees': position.get('position', {}).get('horizontal', {}).get('azimuth', {}).get('degrees'),
                'equatorial_position_right_ascension': position.get('position', {}).get('equatorial', {}).get('rightAscension', {}).get('hours'),
                'equatorial_position_declination': position.get('position', {}).get('equatorial', {}).get('declination', {}).get('degrees'),
                'position_constellation_name': position.get('position', {}).get('constellation', {}).get('name'),
                'elongation': position.get('extraInfo', {}).get('elongation'),
                'magnitude': position.get('extraInfo', {}).get('magnitude'),
                'phase_string': position.get('extraInfo', {}).get('phase', {}).get('string')
            }
            events.append(event)
    return events

def main():
    data = fetch_moon_data()
    if data:
        events_json = process_moon_data(data)
        save_data_to_db(events_json, "moon_events_table")

if __name__ == "__main__":
    main()