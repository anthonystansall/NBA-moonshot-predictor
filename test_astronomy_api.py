import requests
import base64
import configparser

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

def test_events_api():
    auth_string = get_auth_string()
    headers = {'Authorization': f'Basic {auth_string}'}
    test_url = 'https://api.astronomyapi.com/api/v2/bodies/events/moon'
    params = {
        'latitude': '38.775867',
        'longitude': '-84.39733',
        'elevation': '0',
        'from_date': '2020-12-20',
        'to_date': '2022-12-23',
        'time': '08:00:00',
        'output': 'rows'
    }
    response = requests.get(test_url, headers=headers, params=params)
    if response.status_code == 200:
        print("API Test Successful. Data received:")
        print(response.json())
    else:
        print(f"API Test Failed. Status Code: {response.status_code}")
        print("Error response: ")
        print(response.text)

def test_positions_api():
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
        print("API Test Successful. Data received:")
        print(response.json())
    else:
        print(f"API Test Failed. Status Code: {response.status_code}")
        print("Error response: ")
        print(response.text)

if __name__ == "__main__":
    # test_events_api()
    test_positions_api()
