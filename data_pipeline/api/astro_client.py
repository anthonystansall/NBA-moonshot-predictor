import configparser
import base64
import requests
import os
from data_pipeline.utils import utils
import json

def get_credentials():
    config = configparser.ConfigParser()
    config_file_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'api_credentials.ini')
    config.read(config_file_path)
    application_id = config['AstronomyAPI']['application_id']
    application_secret = config['AstronomyAPI']['application_secret']
    return application_id, application_secret

def get_auth_string():
    application_id, application_secret = get_credentials()
    userpass = f"{application_id}:{application_secret}"
    auth_string = base64.b64encode(userpass.encode()).decode()
    return auth_string

def fetch_moon_data(latitude, longitude, from_date, to_date, time="00:00:00"):

    if latitude is None or longitude is None:
        print("Latitude or Longitude is not provided. Aborting fetch operation.")
        return None

    if utils.check_file_exists('moon_data', from_date=from_date, to_date=to_date, time=time, latitude=latitude, longitude=longitude):
        print("File already exists with requested data. Returning file contents instead.")
        file_path = utils.generate_file_name('moon_data', from_date=from_date, to_date=to_date, time=time, latitude=latitude, longitude=longitude)
        with open(file_path, "r") as file:
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
        response = requests.get(test_url, headers=headers, params=params)
        
        if response.status_code == 200:
            print("API Call Successful. Data received.")
            utils.save_data_to_file(response.json(), 'moon_data', from_date=from_date, to_date=to_date, time=time, latitude=latitude, longitude=longitude)
            return response.json()
        else:
            print(f"API Call Failed. Status Code: {response.status_code}")
            print("Error response: ")
            print(response.text)
            return None



if __name__ == "__main__":
    pass