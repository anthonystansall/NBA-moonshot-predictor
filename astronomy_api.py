import configparser
import base64
import requests
import pandas as pd
import sqlite3

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
    return pd.DataFrame(events)

def save_to_sqlite(df, db_name="NBA_Moonshot.db", table_name="moon_events"):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    print(f"Data saved to {table_name} in {db_name}.")

def main():
    all_events = []
    data = fetch_moon_data()
    if data:
        df = process_moon_data(data)
        all_events.append(df)
    
    if all_events:
        final_df = pd.concat(all_events, ignore_index=True)
        print(final_df)
        # final_df.to_csv('astronomy_events.csv', index=False)

if __name__ == "__main__":
    main()